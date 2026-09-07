# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver: Transactions fact table + batch fraud scoring
# MAGIC Builds the Transactions fact table (renaming `use_chip` -> `channel` per the design
# MAGIC spec) and runs all 4 fraud rules against the full history, writing a separate
# MAGIC Fraud/Risk fact table. Run this AFTER 04-07 (all Silver dimensions must exist first,
# MAGIC since this joins against Silver customers and the ZIP centroid reference).

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append("../")

from pyspark.sql import functions as F
from src.utils.config import cfg
from src.silver.fraud_rules import batch_score_all_rules
from src.utils.transforms import parse_currency
from src.utils.transforms import clean_zip
from src.utils.data_quality import check_orphan_keys
from src.utils.governance import set_table_and_column_comments

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
    parse_currency("amount").alias("amount"),
    F.coalesce(channel_expr[F.col("use_chip")], F.col("use_chip")).alias("channel"),
    "merchant_id",
    clean_zip("zip").alias("zip"),
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

transactions_with_geo.filter(F.col("merchant_latitude").isNull()) \
    .groupBy("channel", F.col("zip").isNull().alias("zip_is_null")) \
    .count() \
    .orderBy(F.desc("count")) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Referential integrity checks
# MAGIC Transactions is the one table every fraud rule and every Gold aggregate joins
# MAGIC against, so this is the most important orphan-key check in the whole pipeline, not
# MAGIC an optional extra. Uses the same shared helper as the Cards-vs-Customers check in
# MAGIC notebook 05 instead of hand-rolling the join three times.

# COMMAND ----------

check_orphan_keys(
    transactions_with_geo, "card_id",
    spark.table(cfg.table("silver", "cards")).filter("is_current = true"),
    "id", "transactions (card_id)",
)
check_orphan_keys(
    transactions_with_geo, "client_id",
    spark.table(cfg.table("silver", "customers")).filter("is_current = true"),
    "id", "transactions (client_id)",
)
check_orphan_keys(
    transactions_with_geo, "merchant_id",
    spark.table(cfg.table("silver", "merchants")),
    "id", "transactions (merchant_id)",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the fact table
# MAGIC Partitioned by transaction year/month: at 13.3M+ rows, Gold aggregates and any
# MAGIC time-bounded query benefit from partition pruning instead of scanning the whole
# MAGIC table. Fraud columns come from a second pass below, kept as a separate table per
# MAGIC the design spec ("separate fact table" for Fraud/Risk).

# COMMAND ----------

transactions_final = (
    transactions_with_geo.select(
        "id", "date", "client_id", "card_id", "amount", "channel", "merchant_id", "zip"
    )
    .withColumn("txn_year", F.year("date"))
    .withColumn("txn_month", F.month("date"))
)

(
    transactions_final
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("txn_year", "txn_month")
    .saveAsTable(cfg.table("silver", "transactions"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta hygiene: retention properties + OPTIMIZE/ZORDER
# MAGIC Table properties only need to be set once (idempotent to rerun); OPTIMIZE/ZORDER is
# MAGIC safe and cheap to rerun on every load too, but on a 13.3M-row table it's the
# MAGIC slowest cell in this notebook -- expect it to take noticeably longer than everything
# MAGIC above it. ZORDER by client_id/card_id since those are the columns the fraud rules'
# MAGIC window functions and the orphan checks above filter/join on most.

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {cfg.table("silver", "transactions")}
    SET TBLPROPERTIES (
        'delta.deletedFileRetentionDuration' = 'interval 30 days',
        'delta.logRetentionDuration' = 'interval 90 days'
    )
""")

spark.sql(f"OPTIMIZE {cfg.table('silver', 'transactions')} ZORDER BY (client_id, card_id)")

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

# COMMAND ----------

set_table_and_column_comments(
    spark,
    cfg.table("silver", "transactions"),
    table_comment=(
        "Transactions fact table (~13.3M rows), partitioned by txn_year/txn_month. "
        "The central fact table every Gold aggregate and fraud rule joins against."
    ),
    column_comments={
        "amount": (
            "Cleaned numeric transaction amount (see parse_currency in "
            "src/utils/transforms.py). Source arrives as a \"$\"-formatted string."
        ),
        "zip": (
            "Merchant ZIP as a 5-digit zero-padded string (see clean_zip). Joined "
            "against silver.merchant_geo_reference for the geo-jump fraud rule."
        ),
        "channel": "swipe / chip / online, renamed from the source file's use_chip column.",
        "txn_year": "Partition column, year(date).",
        "txn_month": "Partition column, month(date).",
    },
)

set_table_and_column_comments(
    spark,
    cfg.table("silver", "fraud_risk"),
    table_comment=(
        "Fraud/Risk fact table, one row per transaction, populated by the 4 hand-built "
        "fraud rules (src/silver/fraud_rules.py). Kept separate from silver.transactions "
        "per the design spec."
    ),
    column_comments={
        "fraud_flag": "True if any of the 4 fraud rules fired for this transaction.",
        "risk_score": (
            "0/25/50/75/100: 25 points per fraud rule that fired, equal weighting "
            "(v1 -- see README for a note on reweighting once real fire-rates are known)."
        ),
        "risk_reason": "Comma-separated names of the rules that fired, e.g. \"velocity, geo_jump\". NULL if none fired.",
        "investigation_status": "Workflow status for the Investigation Queue Gold table/persona. Always \"open\" at scoring time.",
    },
)

set_table_and_column_comments(
    spark,
    cfg.table("silver", "customer_spending_reference"),
    table_comment=(
        "Per-customer trailing average spend, refreshed from batch. Read by the "
        "STREAMING spending-deviation rule as a static broadcasted lookup, not itself a stream."
    ),
    column_comments={
        "avg_amount_30d": "Customer's average transaction amount over the scored batch history at last refresh.",
        "as_of_date": "Latest transaction date included in this average -- how stale this reference is.",
    },
)
