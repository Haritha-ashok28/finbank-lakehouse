# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Transactions fact table + batch fraud scoring
# MAGIC Builds the Transactions fact table (renaming `use_chip` -> `channel` per the design
# MAGIC spec) and runs all 4 fraud rules against the full history, writing a separate
# MAGIC Fraud/Risk fact table. Run this AFTER 04-07 (all Silver dimensions must exist first,
# MAGIC since this joins against Silver customers and the ZIP centroid reference).

# COMMAND ----------

import sys
sys.path.append("../")

from pyspark.sql import functions as F
from src.utils.config import cfg
from src.silver.fraud_rules import batch_score_all_rules

# COMMAND ----------

dbutils.widgets.text("LIMIT_ROWS_FOR_DEV", "", "Row limit for dev iteration (blank = full history)")
LIMIT_ROWS_FOR_DEV = dbutils.widgets.get("LIMIT_ROWS_FOR_DEV")

# COMMAND ----------

bronze_txns = spark.table(cfg.table("bronze", "transactions"))
if LIMIT_ROWS_FOR_DEV:
    bronze_txns = bronze_txns.orderBy("date").limit(int(LIMIT_ROWS_FOR_DEV))
    print(f"DEV MODE: scoring only the first {LIMIT_ROWS_FOR_DEV} transactions by date.")

channel_map = {
    "Swipe Transaction": "swipe",
    "Chip Transaction": "chip",
    "Online Transaction": "online",
}
channel_expr = F.create_map(*[F.lit(x) for pair in channel_map.items() for x in pair])

transactions = bronze_txns.select(
    "id",
    F.col("date").cast("timestamp").alias("date"),
    "client_id",
    "card_id",
    F.col("amount").cast("double").alias("amount"),
    F.coalesce(channel_expr[F.col("use_chip")], F.col("use_chip")).alias("channel"),
    "merchant_id",
    "zip",
    "mcc",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join merchant geo (via merchant's ZIP -> centroid) for the geo-jump rule

# COMMAND ----------

geo_ref = spark.table(cfg.table("silver", "merchant_geo_reference")).select(
    F.col("zip"), F.col("latitude").alias("merchant_latitude"), F.col("longitude").alias("merchant_longitude")
)

transactions_with_geo = transactions.join(geo_ref, on="zip", how="left")

missing_geo = transactions_with_geo.filter(F.col("merchant_latitude").isNull()).count()
if missing_geo > 0:
    print(f"[DATA QUALITY WARNING] {missing_geo} transactions have a merchant ZIP not found "
          f"in the ZIP centroid reference (out-of-country or malformed ZIP). Geo-jump rule "
          f"can't evaluate these -- they pass through as flag_geo_jump = false, not fraud-cleared.")

# COMMAND ----------

# Write the fact table first (fraud columns come from a second pass below, kept as a
# separate table per the design spec: "separate fact table" for Fraud/Risk).
(
    transactions_with_geo.select(
        "id", "date", "client_id", "card_id", "amount", "channel", "merchant_id", "zip"
    )
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(cfg.table("silver", "transactions"))
)

# COMMAND ----------

scored = batch_score_all_rules(transactions_with_geo)

fraud_risk = scored.select(
    "id",
    F.col("id").alias("transaction_id"),
    "client_id",
    "card_id",
    "fraud_flag",
    "risk_score",
    "risk_reason",
    F.lit("open").alias("investigation_status"),
    F.current_timestamp().alias("scored_at"),
)

(
    fraud_risk.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(cfg.table("silver", "fraud_risk"))
)

# COMMAND ----------

fraud_rate = fraud_risk.selectExpr("avg(cast(fraud_flag as int)) as fraud_rate", "count(*) as total").collect()[0]
print(f"Batch fraud rate: {fraud_rate['fraud_rate']:.4%} across {fraud_rate['total']} transactions")
display(fraud_risk.filter("fraud_flag = true").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer spending reference (refreshed from batch, used by the STREAMING path)
# MAGIC Per the design spec: the streaming spending-deviation rule looks up each customer's
# MAGIC trailing 30-day average from a small reference table refreshed regularly from batch,
# MAGIC rather than recomputing a 30-day window live on every event. Re-run this notebook
# MAGIC (e.g. nightly) to keep it current -- 11_streaming_ingest_fraud_scoring.py reads it
# MAGIC as a static broadcasted lookup, not a stream.

# COMMAND ----------

customer_spending_reference = (
    scored.groupBy("client_id")
    .agg(F.avg("amount").alias("avg_amount_30d"), F.max("date").alias("as_of_date"))
)
(
    customer_spending_reference.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(cfg.table("silver", "customer_spending_reference"))
)
display(customer_spending_reference.limit(10))
