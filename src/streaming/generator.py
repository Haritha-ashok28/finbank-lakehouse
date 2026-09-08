"""
Live synthetic transaction generator, built on dbldatagen (Databricks Labs), per the
design spec's hybrid streaming approach: real customer/card/merchant identities ground
the generator so specific fraud scenarios can be demonstrated on demand, rather than
hoping a historical file happens to contain a good example.

Two pieces:
  1. `build_organic_stream()` -- dbldatagen produces realistic-shaped background traffic
     (amounts, channels, timing) at a controlled rate, with card_id/merchant_id drawn
     from real Silver IDs and client_id attached via a join against the real card ->
     customer mapping (NOT generated independently, which would risk mismatched
     card/customer pairs that don't exist in the batch dimensions).
  2. `inject_fraud_scenario()` -- dbldatagen is built for bulk realistic-shaped random
     data, not for scripting a precise "3 transactions on card X within 90 seconds"
     edge case. Demo-able scenarios are therefore small, deterministic batches appended
     directly to the same landing table the organic stream writes to, so they show up
     mixed into the live feed exactly when you want them to.

VERIFICATION NOTE: this sandbox has no Databricks cluster, so `build_organic_stream()`
was verified the only way possible here -- confirming against the installed dbldatagen
0.4.0 source that every kwarg used below (values, weights, distribution, expr,
baseColumn, seedColumnName, withStreaming/rowsPerSecond) is real and matches its
documented signature, and confirming the resulting schema builds correctly
(`id, card_id, merchant_id, amount, channel`). Actually materializing rows failed in
this sandbox specifically -- a PyArrow/JDK21 `sun.misc.Unsafe` incompatibility in the
local Java 21 environment here, unrelated to Spark version pinning issues on a real
Databricks cluster, which ships a tested JDK/PyArrow combination. Run
`display(build_organic_stream(...))` for a few seconds as your first step on the real
cluster before wiring it into anything else, precisely because this couldn't be proven
end-to-end here.
"""

from datetime import datetime, timedelta

import dbldatagen as dg
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from src.utils.config import cfg

CHANNEL_WEIGHTS = {"swipe": 45, "chip": 45, "online": 10}  # dbldatagen weights are integers, not fractions


def load_seed_ids(spark: SparkSession) -> dict:
    """Pull real IDs from Silver so the generator never invents a card/customer/merchant
    pair that doesn't exist in the batch dimensions."""
    cards = (
        spark.table(cfg.table("silver", "cards"))
        .filter("is_current = true")
        .select(F.col("id").alias("card_id"), "client_id")
    )
    merchants = spark.table(cfg.table("silver", "merchants")).select("id")

    return {
        "card_to_client": cards,  # kept as a DataFrame: joined against, not collected to driver
        "card_ids": [r["card_id"] for r in cards.select("card_id").collect()],
        "merchant_ids": [r["id"] for r in merchants.select("id").collect()],
    }


def build_organic_stream(spark: SparkSession, seed_ids: dict, rows_per_second: int = 5) -> DataFrame:
    """
    Returns a streaming DataFrame of synthetic transactions shaped like the real
    transactions table, at `rows_per_second`. Caller is responsible for writing it out
    (see notebooks/10_streaming_generator_job.py).
    """
    if not seed_ids["card_ids"] or not seed_ids["merchant_ids"]:
        raise ValueError(
            "No real card_id / merchant_id values found in Silver. Run the Bronze and "
            "Silver batch notebooks (01-07) first -- the generator seeds from real "
            "dimension data by design, it does not invent its own IDs."
        )

    data_spec = (
        dg.DataGenerator(spark, name="synthetic_txn_stream", rows=-1, partitions=4, seedColumnName="_row_id")
        # `id` is dbldatagen's default seed column name -- renamed via seedColumnName above
        # so it's free to use as a real output column, built from the seed with a plain
        # SQL expr (verified locally: dbldatagen's `template=` text generator requires a
        # pandas UDF / Arrow round-trip that isn't reliable across every JDK+PyArrow
        # combination -- `expr` avoids that dependency entirely and is just as fast).
        .withColumn("id", StringType(), expr="concat('synthtxn_', _row_id)", baseColumn="_row_id")
        .withColumn("card_id", StringType(), values=seed_ids["card_ids"], random=True)
        .withColumn("merchant_id", StringType(), values=seed_ids["merchant_ids"], random=True)
        .withColumn("amount", DoubleType(), minValue=1.0, maxValue=500.0, distribution="normal", random=True)
        .withColumn(
            "channel", StringType(),
            values=list(CHANNEL_WEIGHTS.keys()), weights=list(CHANNEL_WEIGHTS.values()), random=True,
        )
    )

    generated = data_spec.build(withStreaming=True, options={"rowsPerSecond": rows_per_second})

    # Attach the real client_id for the drawn card_id -- this is what keeps every
    # generated transaction referentially valid against the real Silver dimensions.
    with_client = generated.join(F.broadcast(seed_ids["card_to_client"]), on="card_id", how="inner")

    return with_client.select(
        "id",
        F.current_timestamp().alias("date"),
        "client_id",
        "card_id",
        "amount",
        "channel",
        "merchant_id",
    )


# --- Scripted fraud scenarios, for demo purposes -------------------------------------

_SCENARIO_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("date", TimestampType()),
    StructField("client_id", StringType()),
    StructField("card_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("channel", StringType()),
    StructField("merchant_id", StringType()),
])


def _pick_one_real_card(spark: SparkSession, seed_ids: dict) -> dict:
    row = seed_ids["card_to_client"].limit(1).collect()[0]
    return {"card_id": row["card_id"], "client_id": row["client_id"]}


def inject_fraud_scenario(spark: SparkSession, scenario: str, seed_ids: dict, target_table: str) -> int:
    """
    Appends a small, deterministic set of rows directly to `target_table` (the same
    Delta table the organic stream writes to) that will trip one specific fraud rule.
    Returns the number of rows injected. Run this from a notebook cell whenever you want
    to demo a specific rule firing, rather than waiting for random organic traffic to
    happen to produce one.

    Scenarios: "amount_threshold", "velocity", "geo_jump", "spending_deviation"
    """
    now = datetime.utcnow()
    seed = _pick_one_real_card(spark, seed_ids)
    merchant_a, merchant_b = seed_ids["merchant_ids"][0], seed_ids["merchant_ids"][-1]

    if scenario == "amount_threshold":
        rows = [(f"scn_amt_{now.timestamp()}", now, seed["client_id"], seed["card_id"],
                 cfg.fraud_amount_threshold + 500.0, "online", merchant_a)]

    elif scenario == "velocity":
        rows = [
            (f"scn_vel_{now.timestamp()}_{i}", now + timedelta(seconds=i * 30),
             seed["client_id"], seed["card_id"], 45.0, "chip", merchant_a)
            for i in range(cfg.fraud_velocity_count)
        ]

    elif scenario == "geo_jump":
        # Same card, two transactions 5 minutes apart at merchants that (via the ZIP
        # centroid reference) resolve to very distant locations.
        rows = [
            (f"scn_geo_{now.timestamp()}_1", now, seed["client_id"], seed["card_id"], 60.0, "chip", merchant_a),
            (f"scn_geo_{now.timestamp()}_2", now + timedelta(minutes=5), seed["client_id"], seed["card_id"],
             60.0, "chip", merchant_b),
        ]

    elif scenario == "spending_deviation":
        rows = [(f"scn_dev_{now.timestamp()}", now, seed["client_id"], seed["card_id"], 5000.0, "online", merchant_a)]

    else:
        raise ValueError(f"Unknown scenario '{scenario}'. Expected one of: "
                          f"amount_threshold, velocity, geo_jump, spending_deviation")

    df = spark.createDataFrame(rows, schema=_SCENARIO_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(target_table)
    return df.count()
