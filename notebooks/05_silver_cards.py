# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver: Cards (whole-entity SCD2)
# MAGIC Every tracked column change (status/limit/type) creates a new version row.
# MAGIC `card_number` and `cvv` are masked here in Silver already -- Unity Catalog column
# MAGIC masks (per the security matrix) are the enforcement layer for who can see the
# MAGIC unmasked Bronze copy, but Silver/Gold consumers should never need the raw PAN at all.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append("../")

from pyspark.sql import functions as F
from src.silver.scd_utils import scd2_merge
from src.utils.config import cfg
from src.utils.transforms import parse_currency

# COMMAND ----------

bronze_cards = spark.table(cfg.table("bronze", "cards"))

cards_clean = bronze_cards.select(
    "id",
    "client_id",
    "card_brand",
    "card_type",
    F.concat(F.lit("****-****-****-"), F.substring("card_number", -4, 4)).alias("card_number_masked"),
    "expires",
    "has_chip",
    parse_currency("credit_limit").alias("credit_limit"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Referential integrity check against Silver customers
# MAGIC A card whose client_id has no matching current customer is either an ordering
# MAGIC problem (run 04_silver_customers first) or a real data quality issue worth flagging.

# COMMAND ----------

orphans = cards_clean.join(
    spark.table(cfg.table("silver", "customers"))
    .filter("is_current = true")
    .select(F.col("id").alias("customer_id")),
    cards_clean.client_id == F.col("customer_id"),
    "left_anti",
)

orphan_count = orphans.count()
if orphan_count > 0:
    print(f"[DATA QUALITY WARNING] {orphan_count} cards reference a client_id with no Silver customer row.")
display(orphans.limit(20))

# COMMAND ----------

scd2_merge(
    spark=spark,
    source_df=cards_clean,
    target_table=cfg.table("silver", "cards"),
    business_key_cols=["id"],
    tracked_cols=["card_type", "has_chip", "credit_limit"],
)

display(spark.table(cfg.table("silver", "cards")).filter("is_current = true").limit(20))