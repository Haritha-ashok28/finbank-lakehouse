# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Customers (hybrid SCD2 + SCD3)
# MAGIC - `income_tier` -> SCD2 (full history, new row per change)
# MAGIC - `address` -> SCD3 (current + previous_address only, updated in place)
# MAGIC
# MAGIC `income_tier` doesn't exist in the raw file -- it's derived here from `yearly_income`
# MAGIC (bucketed) since the design spec calls for tracking income *tier* history, not the raw
# MAGIC dollar figure. Adjust the bucket boundaries once you've seen the real income distribution.

# COMMAND ----------

import sys
sys.path.append("../")

from pyspark.sql import functions as F
from src.silver.scd_utils import scd2_with_scd3_merge
from src.utils.config import cfg

# COMMAND ----------

bronze_customers = spark.table(cfg.table("bronze", "customers"))

customers_with_tier = bronze_customers.withColumn(
    "income_tier",
    F.when(F.col("yearly_income") < 30000, "low")
    .when(F.col("yearly_income") < 80000, "medium")
    .when(F.col("yearly_income") < 150000, "high")
    .otherwise("very_high"),
).select(
    "id", "current_age", "retirement_age", "birth_year", "birth_month", "gender",
    "address", "latitude", "longitude", "yearly_income", "income_tier",
    "per_capita_income", "total_debt", "credit_score", "num_credit_cards",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing-value / dtype sanity check
# MAGIC Cheap to run every load; catches upstream data quality regressions before they reach Gold.

# COMMAND ----------

null_counts = customers_with_tier.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in customers_with_tier.columns]
)
display(null_counts)

# COMMAND ----------

scd2_with_scd3_merge(
    spark=spark,
    source_df=customers_with_tier,
    target_table=cfg.table("silver", "customers"),
    business_key_cols=["id"],
    scd2_tracked_cols=["income_tier"],
    scd3_col="address",
)

display(spark.table(cfg.table("silver", "customers")).filter("is_current = true").limit(20))
