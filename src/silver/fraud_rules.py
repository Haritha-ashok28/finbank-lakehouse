"""
The four hand-built fraud rules, applied identically to batch and streaming data, per
the design spec (the dataset's own fraud label is deliberately not used, see docs).

Rule 1 (amount threshold) and Rule 4 (spending deviation) are plain per-row/window
computations and are identical in batch and streaming.

Rule 2 (velocity) and Rule 3 (geo-jump) need a customer/card's recent transaction
history. In BATCH, that history already sits in the table, so a window function over
the full dataset is correct and simple (`batch_*` functions below). In STREAMING, that
history has to be carried across micro-batches as explicit state, which is a materially
different mechanism (Structured Streaming stateful processing) -- see
src/streaming/streaming_fraud_scoring.py, which reuses the pure haversine/threshold
math from this file but drives it from `applyInPandasWithState` instead of a window
function. Keeping the math in one place and only the state-carrying mechanism different
is what "both paths run the same four rules" actually means in code.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.config import cfg

EARTH_RADIUS_KM = 6371.0


def haversine_km_expr(lat1, lon1, lat2, lon2):
    """
    Great-circle distance in km between two (lat, lon) points, as a Spark column
    expression (all four args are column expressions, not Python floats).
    """
    lat1_rad, lon1_rad = F.radians(lat1), F.radians(lon1)
    lat2_rad, lon2_rad = F.radians(lat2), F.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = F.sin(dlat / 2) ** 2 + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(dlon / 2) ** 2
    c = 2 * F.asin(F.sqrt(a))
    return F.lit(EARTH_RADIUS_KM) * c


def apply_amount_threshold_rule(df: DataFrame, amount_col: str = "amount") -> DataFrame:
    """Rule 1: amount over a flat threshold -> high risk. Identical in batch and streaming."""
    return df.withColumn(
        "flag_amount_threshold",
        F.col(amount_col) > F.lit(cfg.fraud_amount_threshold),
    )


def batch_apply_velocity_rule(df: DataFrame, card_col: str = "card_id", ts_col: str = "date") -> DataFrame:
    """
    Rule 2 (batch): 3+ transactions on the same card within a 5-minute window.
    Uses a time-ranged window function -- only valid because in batch, ALL of a card's
    history is already sitting in the DataFrame. Do not reuse this function as-is on a
    streaming DataFrame; windowed aggregates like this aren't supported the same way in
    Structured Streaming, which is exactly why the streaming path uses stateful
    processing instead (src/streaming/streaming_fraud_scoring.py).
    """
    window_seconds = cfg.fraud_velocity_window_minutes * 60
    w = (
        Window.partitionBy(card_col)
        .orderBy(F.col(ts_col).cast("long"))
        .rangeBetween(-window_seconds, 0)
    )
    return df.withColumn("txn_count_in_window", F.count("*").over(w)).withColumn(
        "flag_velocity", F.col("txn_count_in_window") >= F.lit(cfg.fraud_velocity_count)
    )


def batch_apply_geo_jump_rule(
    df: DataFrame,
    card_col: str = "card_id",
    ts_col: str = "date",
    lat_col: str = "merchant_latitude",
    lon_col: str = "merchant_longitude",
) -> DataFrame:
    """
    Rule 3 (batch): consecutive transactions on the same card farther apart than
    plausible travel speed allows, using merchant ZIP-centroid location (joined in
    upstream in Silver) as a proxy for where the transaction physically happened.

    Two consecutive transactions 50 km apart 2 minutes apart implies ~1500 km/h travel,
    which is implausible for anyone who isn't on a plane that also didn't just land --
    flagged as a geo-jump.
    """
    w = Window.partitionBy(card_col).orderBy(ts_col)
    prev_lat = F.lag(lat_col).over(w)
    prev_lon = F.lag(lon_col).over(w)
    prev_ts = F.lag(ts_col).over(w)

    distance_km = haversine_km_expr(prev_lat, prev_lon, F.col(lat_col), F.col(lon_col))
    hours_elapsed = (F.col(ts_col).cast("long") - prev_ts.cast("long")) / 3600.0
    implied_speed_kmh = F.when(hours_elapsed > 0, distance_km / hours_elapsed).otherwise(F.lit(None))

    return (
        df.withColumn("_prev_lat", prev_lat)
        .withColumn("_implied_speed_kmh", implied_speed_kmh)
        .withColumn(
            "flag_geo_jump",
            (F.col("_prev_lat").isNotNull()) & (F.col("_implied_speed_kmh") > F.lit(cfg.fraud_geo_jump_max_speed_kmh)),
        )
        .drop("_prev_lat")
    )


def batch_apply_spending_deviation_rule(
    df: DataFrame, customer_col: str = "client_id", ts_col: str = "date", amount_col: str = "amount"
) -> DataFrame:
    """Rule 4 (batch): amount > 3x the customer's trailing 30-day average."""
    window_seconds = cfg.fraud_spending_deviation_window_days * 86400
    w = (
        Window.partitionBy(customer_col)
        .orderBy(F.col(ts_col).cast("long"))
        .rangeBetween(-window_seconds, -1)  # up to (not including) the current transaction
    )
    trailing_avg = F.avg(amount_col).over(w)
    return df.withColumn("trailing_30d_avg_amount", trailing_avg).withColumn(
        "flag_spending_deviation",
        (F.col("trailing_30d_avg_amount").isNotNull())
        & (F.col(amount_col) > F.col("trailing_30d_avg_amount") * F.lit(cfg.fraud_spending_deviation_multiplier)),
    )


def batch_score_all_rules(df: DataFrame) -> DataFrame:
    """
    Runs all four rules and derives the final fraud_flag / risk_score / risk_reason
    columns for the Fraud/Risk fact table. Expects `df` to already have merchant lat/lon
    joined in (see notebooks/08_silver_transactions_fraud.py).
    """
    scored = (
        df.transform(apply_amount_threshold_rule)
        .transform(batch_apply_velocity_rule)
        .transform(batch_apply_geo_jump_rule)
        .transform(batch_apply_spending_deviation_rule)
    )

    flag_cols = ["flag_amount_threshold", "flag_velocity", "flag_geo_jump", "flag_spending_deviation"]
    reason_expr = F.concat_ws(
        ", ",
        *[F.when(F.col(c), F.lit(c.replace("flag_", ""))) for c in flag_cols],
    )

    return scored.withColumn(
        "fraud_flag", F.greatest(*[F.col(c).cast("int") for c in flag_cols]).cast("boolean")
    ).withColumn("risk_reason", F.when(F.length(reason_expr) > 0, reason_expr)).withColumn(
        "risk_score",
        # Simple additive score out of 100: each triggered rule contributes equally.
        # Tune the weighting once you've seen how often each rule fires on real data --
        # equal weighting is a reasonable v1, not a claim that all 4 rules are equally severe.
        (sum(F.col(c).cast("int") for c in flag_cols) * F.lit(25)).cast("int"),
    )
