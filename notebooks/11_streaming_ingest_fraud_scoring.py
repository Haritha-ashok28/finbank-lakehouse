# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming: ingest + fraud scoring
# MAGIC Reads the generator's landing table as a stream, joins the small reference tables
# MAGIC (merchant geo, customer spending average -- both refreshed from batch per the design
# MAGIC spec), applies the stateless amount-threshold rule and the stateful velocity/geo-jump
# MAGIC rules, and writes results to a streaming fraud table.
# MAGIC
# MAGIC Run 10_streaming_generator_job.py (at least briefly) before this, so the landing
# MAGIC table exists and has rows to stream.

# COMMAND ----------

import sys
sys.path.append("../")

from pyspark.sql import functions as F
from src.utils.config import cfg
from src.silver.fraud_rules import apply_amount_threshold_rule
from src.streaming.streaming_fraud_scoring import score_velocity_and_geo_jump_stateful

# COMMAND ----------

landing_table = cfg.table("bronze", "transactions_stream_landing")
raw_stream = spark.readStream.format("delta").table(landing_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join reference tables (static reads, broadcast -- these are small and refreshed
# MAGIC ### from batch, not themselves streams)

# COMMAND ----------

merchants = spark.table(cfg.table("silver", "merchants")).select(
    F.col("id").alias("merchant_id"), F.col("merchant_zip")
)
geo_ref = spark.table(cfg.table("silver", "merchant_geo_reference")).select(
    F.col("zip"), F.col("latitude").alias("merchant_latitude"), F.col("longitude").alias("merchant_longitude")
)
spending_ref = spark.table(cfg.table("silver", "customer_spending_reference"))

enriched_stream = (
    raw_stream
    .join(F.broadcast(merchants), on="merchant_id", how="left")
    .join(F.broadcast(geo_ref), F.col("merchant_zip") == F.col("zip"), how="left")
    .join(F.broadcast(spending_ref), on="client_id", how="left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rule 1 (stateless) + Rule 4 (reference lookup) -- no state needed for these

# COMMAND ----------

with_simple_flags = apply_amount_threshold_rule(enriched_stream).withColumn(
    "flag_spending_deviation",
    (F.col("avg_amount_30d").isNotNull())
    & (F.col("amount") > F.col("avg_amount_30d") * F.lit(cfg.fraud_spending_deviation_multiplier)),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rules 2 + 3 (stateful) -- velocity and geo-jump, carried across micro-batches per card_id
# MAGIC
# MAGIC `flag_amount_threshold` / `flag_spending_deviation` ride along as plain columns into
# MAGIC the stateful function and come back out on the same row -- deliberately NOT
# MAGIC recomputed via a join afterwards, since joining two streaming DataFrames derived
# MAGIC from the same source is a stream-stream join that needs its own watermarking and
# MAGIC adds real complexity for no benefit here.

# COMMAND ----------

stateful_scored = score_velocity_and_geo_jump_stateful(
    with_simple_flags.select(
        "id", "date", "client_id", "card_id", "amount", "channel", "merchant_id",
        "merchant_latitude", "merchant_longitude",
        "flag_amount_threshold", "flag_spending_deviation",
    )
)

flag_cols = ["flag_amount_threshold", "flag_velocity", "flag_geo_jump", "flag_spending_deviation"]
reason_expr = F.concat_ws(", ", *[F.when(F.col(c), F.lit(c.replace("flag_", ""))) for c in flag_cols])

fraud_stream = stateful_scored.withColumn(
    "fraud_flag", F.greatest(*[F.col(c).cast("int") for c in flag_cols]).cast("boolean")
).withColumn(
    "risk_score", (sum(F.col(c).cast("int") for c in flag_cols) * F.lit(25)).cast("int")
).withColumn(
    "risk_reason", F.when(F.length(reason_expr) > 0, reason_expr)
).select(
    "id", F.col("id").alias("transaction_id"), "client_id", "card_id",
    "fraud_flag", "risk_score", "risk_reason", F.current_timestamp().alias("scored_at"),
)

# COMMAND ----------

query = (
    fraud_stream.writeStream.format("delta")
    .option("checkpointLocation", cfg.checkpoint_path("fraud_risk_streaming"))
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(cfg.table("silver", "fraud_risk_streaming"))
)

query.awaitTermination()
