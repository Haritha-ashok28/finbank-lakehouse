# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold: aggregates for Power BI / AI-BI Dashboards
# MAGIC Gold only aggregates results computed in Silver -- no scoring logic happens here,
# MAGIC per the design spec. Tables here map roughly 1:1 to the four personas in the design
# MAGIC doc (Fraud/Risk Analyst, Relationship Manager, Compliance/Auditor, Business Leader).
# MAGIC Run this AFTER 04-08 (needs Silver customers/cards/merchants/transactions/fraud_risk
# MAGIC to all exist).

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append("../")

from pyspark.sql import functions as F
from src.utils.config import cfg
from src.utils.data_quality import row_count_sanity_check
from src.utils.governance import set_table_and_column_comments

# COMMAND ----------

cfg.ensure_schemas(spark)

# COMMAND ----------

txns = spark.table(cfg.table("silver", "transactions"))
fraud = spark.table(cfg.table("silver", "fraud_risk"))
customers = spark.table(cfg.table("silver", "customers")).filter("is_current = true")
merchants = spark.table(cfg.table("silver", "merchants"))

# COMMAND ----------

# MAGIC %md ### 1. Daily fraud summary (Fraud/Risk Analyst + Compliance/Auditor persona)

# COMMAND ----------

daily_fraud_summary = (
    txns.join(fraud, txns.id == fraud.transaction_id)
    .withColumn("txn_date", F.to_date("date"))
    .groupBy("txn_date")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum(F.col("fraud_flag").cast("int")).alias("flagged_transactions"),
        F.avg("risk_score").alias("avg_risk_score"),
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("fraud_flag"), F.col("amount")).otherwise(0)).alias("flagged_amount"),
    )
    .withColumn("fraud_rate", F.col("flagged_transactions") / F.col("total_transactions"))
)
(
    daily_fraud_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(cfg.table("gold", "daily_fraud_summary"))
)
row_count_sanity_check(daily_fraud_summary, "gold.daily_fraud_summary")

# COMMAND ----------

# MAGIC %md ### 2. Customer risk profile (Relationship Manager persona)
# MAGIC Note: `txns` and `fraud` both carry `client_id`, so the `groupBy` below deliberately
# MAGIC references the column via `txns.client_id` (bound to the pre-join dataframe) rather
# MAGIC than the bare string `"client_id"`, which would be ambiguous after the join.

# COMMAND ----------

customer_risk_profile = (
    txns.join(fraud, txns.id == fraud.transaction_id)
    .groupBy(txns.client_id)
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("amount").alias("total_spend"),
        F.avg("amount").alias("avg_transaction_amount"),
        F.sum(F.col("fraud_flag").cast("int")).alias("flagged_transaction_count"),
        F.max("risk_score").alias("max_risk_score"),
        F.max("date").alias("last_transaction_date"),
    )
    .join(customers.select("id", "income_tier", "address"), F.col("client_id") == F.col("id"))
    .drop("id")
)
(
    customer_risk_profile.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(cfg.table("gold", "customer_risk_profile"))
)
row_count_sanity_check(customer_risk_profile, "gold.customer_risk_profile")

# COMMAND ----------

# MAGIC %md ### 3. Merchant category spend (Business Leader / Power BI persona)

# COMMAND ----------

merchant_category_spend = (
    txns.join(merchants, txns.merchant_id == merchants.id)
    .groupBy("mcc_description")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy(F.desc("total_amount"))
)
(
    merchant_category_spend.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(cfg.table("gold", "merchant_category_spend"))
)
row_count_sanity_check(merchant_category_spend, "gold.merchant_category_spend")

# COMMAND ----------

# MAGIC %md ### 4. Investigation queue (Fraud/Risk Analyst + Compliance/Auditor persona)
# MAGIC Auditor has full unmasked access per the security matrix -- this Gold table is still
# MAGIC PII-light (no card numbers), full row-level access is enforced at the Silver/Bronze
# MAGIC layer via Unity Catalog, not by what Gold happens to select.
# MAGIC
# MAGIC Both `fraud` and `txns` carry their own `client_id`/`card_id` columns (fraud_risk
# MAGIC stores a copy of the transaction's keys), so selecting either bare name after the
# MAGIC join is ambiguous -- same class of bug fixed in notebook 05's orphan-key check.
# MAGIC Every column below is picked explicitly from the dataframe that owns it instead.

# COMMAND ----------

investigation_queue = (
    fraud.filter("fraud_flag = true AND investigation_status = 'open'")
    .join(txns, fraud.transaction_id == txns.id)
    .select(
        fraud.transaction_id,
        fraud.client_id,
        fraud.card_id,
        txns.date,
        txns.amount,
        fraud.risk_score,
        fraud.risk_reason,
    )
    .orderBy(F.desc("risk_score"))
)
(
    investigation_queue.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(cfg.table("gold", "investigation_queue"))
)
row_count_sanity_check(investigation_queue, "gold.investigation_queue")

# COMMAND ----------

display(daily_fraud_summary.orderBy(F.desc("txn_date")).limit(10))

# COMMAND ----------

# MAGIC %md ### Table + column comments (Unity Catalog governance)

# COMMAND ----------

set_table_and_column_comments(
    spark,
    cfg.table("gold", "daily_fraud_summary"),
    table_comment=(
        "Daily rollup of transaction volume and fraud activity. Powers the Fraud/Risk "
        "Analyst and Compliance/Auditor personas' day-over-day dashboards."
    ),
    column_comments={
        "txn_date": "Calendar date the transactions occurred on (from silver.transactions.date).",
        "fraud_rate": "flagged_transactions / total_transactions for this date.",
        "flagged_amount": "Sum of amount for transactions where fraud_flag = true on this date.",
    },
)

set_table_and_column_comments(
    spark,
    cfg.table("gold", "customer_risk_profile"),
    table_comment=(
        "Per-customer spend and fraud-flag summary, joined with income tier and address "
        "from Silver customers. Powers the Relationship Manager persona. Only includes "
        "customers with at least one transaction (inner join against silver.transactions)."
    ),
    column_comments={
        "max_risk_score": "Highest risk_score across all of this customer's scored transactions.",
        "flagged_transaction_count": "Count of this customer's transactions where fraud_flag = true.",
        "income_tier": "Current SCD2-tracked income tier from silver.customers (see design spec).",
    },
)

set_table_and_column_comments(
    spark,
    cfg.table("gold", "merchant_category_spend"),
    table_comment=(
        "Transaction volume and spend aggregated by merchant category (mcc_description). "
        "PII-light, powers the Business Leader / Power BI persona per the security matrix "
        "(Gold layer only, no raw customer/card data)."
    ),
    column_comments={
        "mcc_description": "Human-readable merchant category, joined from silver.merchants.",
    },
)

set_table_and_column_comments(
    spark,
    cfg.table("gold", "investigation_queue"),
    table_comment=(
        "Open, unresolved fraud cases (fraud_flag = true AND investigation_status = 'open'), "
        "ordered by risk_score descending. Powers the Fraud/Risk Analyst and "
        "Compliance/Auditor personas' case-working view."
    ),
    column_comments={
        "risk_reason": "Comma-separated names of the fraud rules that fired for this transaction.",
        "investigation_status": (
            "Always 'open' at Gold-refresh time; a real workflow would update this in "
            "Silver as cases get worked and re-run this notebook to refresh the queue."
        ),
    },
)
