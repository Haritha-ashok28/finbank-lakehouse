# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver: Merchants (SCD1)
# MAGIC Deduplicated from the transactions file (merchant_id, city, state, zip) and joined
# MAGIC with mcc_codes.json for a human-readable category. No history needed per the design
# MAGIC spec -- a merchant's recorded category/location just gets overwritten if it changes.

# COMMAND ----------

import sys
sys.path.append("../")

import json

from pyspark.sql import functions as F
from src.silver.scd_utils import scd1_upsert
from src.utils.config import cfg
from src.utils.data_quality import null_count_report
from src.utils.governance import set_table_and_column_comments

# COMMAND ----------

# mcc_codes.json is a small flat {"mcc_code": "description"} map, not JSON-lines, so
# spark.read.json would mis-parse it as one row with hundreds of columns. It's tiny
# (a few hundred entries) -- read the raw text and parse it directly instead.
raw_mcc_text = "\n".join(row.value for row in spark.read.text(cfg.raw_path(cfg.mcc_codes_file)).collect())
mcc_dict = json.loads(raw_mcc_text)
mcc_df = spark.createDataFrame(list(mcc_dict.items()), ["mcc", "mcc_description"])

# COMMAND ----------

bronze_transactions = spark.table(cfg.table("bronze", "transactions"))

merchants = (
    bronze_transactions
    .select("merchant_id", "merchant_city", "merchant_state", "zip", "mcc")
    .dropDuplicates(["merchant_id"])
    .join(mcc_df, on="mcc", how="left")
    .withColumnRenamed("merchant_id", "id")
    .withColumnRenamed("zip", "merchant_zip")
    .select("id", "merchant_city", "merchant_state", "merchant_zip", "mcc", "mcc_description")
)

print(f"Distinct merchants: {merchants.count()}")

# COMMAND ----------

display(null_count_report(merchants))

# COMMAND ----------

scd1_upsert(
    spark=spark,
    source_df=merchants,
    target_table=cfg.table("silver", "merchants"),
    business_key_cols=["id"],
)

display(spark.table(cfg.table("silver", "merchants")).limit(20))

# COMMAND ----------

set_table_and_column_comments(
    spark,
    cfg.table("silver", "merchants"),
    table_comment=(
        "Merchant dimension, deduplicated from the Transactions source file. "
        "SCD1: overwritten in place, no history kept."
    ),
    column_comments={
        "id": "Merchant id (from transactions.merchant_id).",
        "merchant_zip": "Merchant ZIP code as ingested (not yet joined to lat/long -- see silver.merchant_geo_reference).",
        "mcc_description": "Human-readable merchant category, joined from mcc_codes.json. NULL if the mcc wasn't found in the lookup.",
    },
)
