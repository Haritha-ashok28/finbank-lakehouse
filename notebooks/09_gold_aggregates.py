# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: aggregates for Power BI
# MAGIC Gold only aggregates results computed in Silver -- no scoring logic happens here,
# MAGIC per the design spec. Tables here map roughly 1:1 to the four personas in the design
# MAGIC doc (Fraud/Risk Analyst, Relationship Manager, Compliance/Auditor, Business Leader).

# COMMAND ----------

import sys
sys.path.append("../")

from pyspark.sql import functions as F
from src.utils.config import cfg

# COMMAND ----------

txns = spark.table(cfg.table("silver", "transactions"))
fraud = spark.table(cfg.table("silver", "fraud_risk"))
customers = spark.table(cfg.table("silver", "customers")).filter("is_current = true")
merchants = spark.table(cfg.table("silver", "merchants"))

# COMMAND ----------

# MAGIC %md ### 1. Daily fraud summary (Fraud/Risk Analyst persona)

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
daily_fraud_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    cfg.table("gold", "daily_fraud_summary")
)

# COMMAND ----------

# MAGIC %md ### 2. Customer risk profile (Relationship Manager persona)

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
customer_risk_profile.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    cfg.table("gold", "customer_risk_profile")
)

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
merchant_category_spend.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    cfg.table("gold", "merchant_category_spend")
)

# COMMAND ----------

# MAGIC %md ### 4. Investigation queue (Compliance/Auditor persona)
# MAGIC Auditor has full unmasked access per the security matrix -- this Gold table is still
# MAGIC PII-light (no card numbers), full row-level access is enforced at the Silver/Bronze
# MAGIC layer via Unity Catalog, not by what Gold happens to select.

# COMMAND ----------

investigation_queue = (
    fraud.filter("fraud_flag = true AND investigation_status = 'open'")
    .join(txns, fraud.transaction_id == txns.id)
    .select("transaction_id", "client_id", "card_id", "date", "amount", "risk_score", "risk_reason")
    .orderBy(F.desc("risk_score"))
)
investigation_queue.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    cfg.table("gold", "investigation_queue")
)

display(daily_fraud_summary.orderBy(F.desc("txn_date")).limit(10))
