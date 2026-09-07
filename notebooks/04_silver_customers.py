# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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
from src.utils.transforms import parse_currency
from src.utils.data_quality import null_count_report
from src.utils.governance import set_table_and_column_comments

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

cfg.ensure_schemas(spark)

# COMMAND ----------

bronze_customers = spark.table(cfg.table("bronze", "customers"))

customers_cleaned = (
    bronze_customers
    .withColumn("yearly_income", parse_currency("yearly_income"))
    .withColumn("per_capita_income", parse_currency("per_capita_income"))
    .withColumn("total_debt", parse_currency("total_debt"))
)

customers_with_tier = customers_cleaned.withColumn(
    "income_tier",
    # NULL yearly_income gets its own bucket instead of falling through to
    # otherwise("very_high"), which would silently mislabel anyone whose income
    # couldn't be parsed as a high earner.
    F.when(F.col("yearly_income").isNull(), "unknown")
    .when(F.col("yearly_income") < 30000, "low")
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

display(null_count_report(customers_with_tier))

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

# COMMAND ----------

set_table_and_column_comments(
    spark,
    cfg.table("silver", "customers"),
    table_comment=(
        "Customer dimension. Hybrid SCD: income_tier is full SCD2 history (new row per "
        "change), address is SCD3 (current + previous_address only, no full history)."
    ),
    column_comments={
        "income_tier": (
            "Bucketed yearly_income (low/medium/high/very_high/unknown). SCD2 tracked: "
            "a new row is added every time this changes."
        ),
        "address": (
            "Current address. SCD3 tracked: only the current value and previous_address "
            "are kept, no full change history."
        ),
        "previous_address": "Address value immediately before the current one. NULL if the address has never changed.",
        "yearly_income": (
            "Cleaned numeric income (see parse_currency in src/utils/transforms.py). "
            "Source arrives as a \"$\"-formatted string."
        ),
        "is_current": "True for the row that is the currently active version of this business key.",
        "effective_start": "Timestamp this version became current.",
        "effective_end": "Timestamp this version stopped being current. NULL while is_current = true.",
    },
)
