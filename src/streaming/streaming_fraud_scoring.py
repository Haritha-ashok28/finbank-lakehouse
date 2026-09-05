"""
Stateful streaming fraud scoring: velocity and geo-jump rules, which need a card's
recent transaction history, computed via Structured Streaming's `applyInPandasWithState`
(PySpark 3.4+) instead of the window functions used in batch (src/silver/fraud_rules.py).

WHY NOT REUSE THE BATCH WINDOW FUNCTIONS: `Window.rangeBetween` over the whole table only
works because in batch, the full history is sitting in the DataFrame already. A streaming
micro-batch only ever contains the newest few rows -- there is no "look back 5 minutes"
without something carrying that history across micro-batches. `applyInPandasWithState`
is that something: Spark keeps a small serialized state object per group (per card_id
here) between micro-batches, and expires it automatically after `state_timeout_minutes`
of inactivity so idle cards don't leak memory forever.

VERIFICATION NOTE: this sandbox has no Databricks cluster and no live Event Hub/Delta
stream to drive through this function, so `score_velocity_and_geo_jump_stateful` below
has been reasoned through carefully (state pruning, timeout handling, first-transaction-
per-card edge case) but NOT executed end-to-end. Test it against a small synthetic
stream on your real cluster before trusting it -- start with 2-3 transactions on one
card and confirm the velocity flag fires on the 3rd, exactly like
tests/test_fraud_rules.py::test_velocity_flags_third_transaction_within_5_minutes does
for the batch version.
"""

import json
from typing import Iterator, Tuple

import pandas as pd
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

from src.silver.fraud_rules import EARTH_RADIUS_KM
from src.utils.config import cfg

OUTPUT_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("date", TimestampType()),
    StructField("client_id", StringType()),
    StructField("card_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("channel", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_latitude", DoubleType()),
    StructField("merchant_longitude", DoubleType()),
    # flag_amount_threshold / flag_spending_deviation are computed upstream (stateless --
    # see notebooks/11_streaming_ingest_fraud_scoring.py) and simply passed through here.
    # They're carried through this function rather than joined back afterwards because a
    # stream-stream self-join to reattach them would need its own watermarking and adds
    # real complexity for no benefit -- easier to just not lose the columns in the first
    # place.
    StructField("flag_amount_threshold", BooleanType()),
    StructField("flag_spending_deviation", BooleanType()),
    StructField("flag_velocity", BooleanType()),
    StructField("flag_geo_jump", BooleanType()),
])

# State: a small list of [timestamp, lat, lon] for recent transactions on this card,
# JSON-encoded into a single string field. `applyInPandasWithState`'s state schema
# support for nested array<struct<...>> types is inconsistent across Spark versions --
# encoding as JSON in one StringType field sidesteps that entirely and is trivial to
# serialize/deserialize on both sides (see json.dumps/json.loads below).
STATE_SCHEMA = StructType([
    StructField("recent_json", StringType()),
])


def _haversine_km(lat1, lon1, lat2, lon2):
    import math
    lat1_r, lon1_r, lat2_r, lon2_r = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2_r - lat1_r, lon2_r - lon1_r
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    return EARTH_RADIUS_KM * 2 * math.asin(math.sqrt(a))


def make_stateful_scoring_function(
    velocity_window_seconds: int,
    velocity_threshold: int,
    geo_jump_max_speed_kmh: float,
):
    """
    Returns the (key, pdf_iterator, state) -> iterator[pd.DataFrame] function that
    `applyInPandasWithState` requires, closed over the rule thresholds so they come from
    src/utils/config.py rather than being hardcoded here.
    """

    def process_card_group(
        card_id: Tuple[str], pdf_iter: Iterator[pd.DataFrame], state: GroupState
    ) -> Iterator[pd.DataFrame]:
        if state.hasTimedOut:
            state.remove()
            return iter([])

        # `recent` holds [epoch_seconds, lat, lon] for this card's last few transactions,
        # kept small by pruning anything older than the velocity window every update.
        recent = json.loads(state.get[0]) if state.exists else []

        output_rows = []
        for pdf in pdf_iter:
            pdf = pdf.sort_values("date")
            for _, row in pdf.iterrows():
                ts_epoch = row["date"].timestamp()

                # Prune to the velocity window BEFORE counting -- this is what makes
                # "3+ in the last 5 minutes" correct as time moves forward, rather than
                # accumulating an ever-growing count for an active card.
                recent = [r for r in recent if ts_epoch - r[0] <= velocity_window_seconds]
                flag_velocity = (len(recent) + 1) >= velocity_threshold

                flag_geo_jump = False
                if recent:
                    last_ts, last_lat, last_lon = recent[-1]
                    if row["merchant_latitude"] is not None and last_lat is not None:
                        distance_km = _haversine_km(last_lat, last_lon, row["merchant_latitude"], row["merchant_longitude"])
                        hours_elapsed = max((ts_epoch - last_ts) / 3600.0, 1e-6)
                        implied_speed = distance_km / hours_elapsed
                        flag_geo_jump = implied_speed > geo_jump_max_speed_kmh

                recent.append([ts_epoch, row["merchant_latitude"], row["merchant_longitude"]])

                output_rows.append({
                    "id": row["id"], "date": row["date"], "client_id": row["client_id"],
                    "card_id": row["card_id"], "amount": row["amount"], "channel": row["channel"],
                    "merchant_id": row["merchant_id"],
                    "merchant_latitude": row["merchant_latitude"], "merchant_longitude": row["merchant_longitude"],
                    "flag_amount_threshold": bool(row["flag_amount_threshold"]),
                    "flag_spending_deviation": bool(row["flag_spending_deviation"]),
                    "flag_velocity": bool(flag_velocity), "flag_geo_jump": bool(flag_geo_jump),
                })

        # Cap state size defensively -- a card sending a burst far larger than the
        # velocity threshold should still only need the last few points, not an
        # unbounded list, even though the window-based prune above should already
        # keep this small under normal conditions.
        recent = recent[-20:]
        state.update((json.dumps(recent),))
        state.setTimeoutDuration(velocity_window_seconds * 1000 * 4)  # ms; ~4x the window

        return iter([pd.DataFrame(output_rows)]) if output_rows else iter([])

    return process_card_group


def score_velocity_and_geo_jump_stateful(streaming_df):
    """
    Apply the stateful velocity/geo-jump scoring to a streaming DataFrame that already
    has merchant_latitude/merchant_longitude, flag_amount_threshold and
    flag_spending_deviation columns present (see
    notebooks/11_streaming_ingest_fraud_scoring.py) -- all four rule flags end up on the
    same output row this way, with no need to join the stateful and stateless results
    back together afterwards.
    """
    scoring_fn = make_stateful_scoring_function(
        velocity_window_seconds=cfg.fraud_velocity_window_minutes * 60,
        velocity_threshold=cfg.fraud_velocity_count,
        geo_jump_max_speed_kmh=cfg.fraud_geo_jump_max_speed_kmh,
    )

    return (
        streaming_df.groupBy("card_id")
        .applyInPandasWithState(
            scoring_fn,
            outputStructType=OUTPUT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
        )
    )
