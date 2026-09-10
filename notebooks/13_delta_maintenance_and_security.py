# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 13: Delta Maintenance & Row-Level Security
# MAGIC Delta time travel, VACUUM, and a region-based row filter for the Relationship Manager
# MAGIC persona. Also extends the column-mask pattern from `12_governance_and_masking.py`
# MAGIC (which masks `birth_year`) to `card_number`.
# MAGIC
# MAGIC Run after `09_gold_aggregates.py`. The time-travel and DESCRIBE cells are read-only and
# MAGIC safe to run anytime. The VACUUM cells delete files, always run the DRY RUN version first
# MAGIC and read the output before running the real one.

# COMMAND ----------

from src.utils.config import cfg
cfg.ensure_schemas(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Delta Time Travel + DESCRIBE HISTORY
# MAGIC Proves the Transactions fact table has real, queryable version history, not just a
# MAGIC current snapshot.

# COMMAND ----------

# Every write and metadata change on this table is a numbered, timestamped version
display(spark.sql(f"DESCRIBE HISTORY {cfg.table('silver', 'fraud_risk')}"))

# COMMAND ----------

display(spark.sql(f"""
    SELECT 0 AS version, count(*) AS null_risk_score_rows
    FROM {cfg.table('silver', 'fraud_risk')} VERSION AS OF 0
    WHERE risk_score IS NULL
 
    UNION ALL
    SELECT 1, count(*) FROM {cfg.table('silver', 'fraud_risk')} VERSION AS OF 1 WHERE risk_score IS NULL
    UNION ALL
    SELECT 2, count(*) FROM {cfg.table('silver', 'fraud_risk')} VERSION AS OF 2 WHERE risk_score IS NULL
    UNION ALL
    SELECT 8, count(*) FROM {cfg.table('silver', 'fraud_risk')} VERSION AS OF 8 WHERE risk_score IS NULL
 
    ORDER BY version
"""))
# Expected: version 0 shows 1,189,254 NULL risk_score rows (the bug), 1/2/8 all show 0 (fixed).

# COMMAND ----------

# Timestamp-based time travel works the same way, useful when you remember roughly when
# something happened but not the exact version number. Uncomment and adjust the date.
# early_by_time = (
#     spark.read.format("delta")
#     .option("timestampAsOf", "2026-09-06")
#     .table(cfg.table("silver", "transactions"))
# )
# print(early_by_time.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. VACUUM
# MAGIC Removes files no longer referenced by the Delta log and older than the retention window.
# MAGIC **Always run the DRY RUN cell first and read the output before running the real VACUUM.**
# MAGIC This notebook keeps the default 7-day (168 hour) retention and does not disable Delta's
# MAGIC safety check, going below 7 days can break time travel and any query mid-read against an
# MAGIC older version.

# COMMAND ----------

# DRY RUN: lists the files that WOULD be deleted. Deletes nothing. Always run this first.
display(spark.sql(f"VACUUM {cfg.table('silver', 'transactions')} DRY RUN"))

# COMMAND ----------

# Real VACUUM on Transactions, only after reviewing the dry run output above.
spark.sql(f"VACUUM {cfg.table('silver', 'transactions')} RETAIN 168 HOURS")

# COMMAND ----------

# Same pattern for the smaller Silver dimension tables, good practice for consistency even
# though they don't accumulate much stale file volume.
for tbl in ["customers", "cards", "merchants", "merchant_geo_reference"]:
    print(f"--- {tbl} dry run ---")
    display(spark.sql(f"VACUUM {cfg.table('silver', tbl)} DRY RUN"))

# COMMAND ----------

# Once you've reviewed every dry run above, uncomment to actually run them.
# for tbl in ["customers", "cards", "merchants", "merchant_geo_reference"]:
#     spark.sql(f"VACUUM {cfg.table('silver', tbl)} RETAIN 168 HOURS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Column mask: card_number
# MAGIC Extends the `12_governance_and_masking.py` pattern (which masks `birth_year`).
# MAGIC `card_number` is shown as last-4-only to everyone except the Compliance/Auditor group,
# MAGIC matching the original access matrix (full value, no masking, for Compliance).

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE FUNCTION finbank.bronze.mask_card_number_last4(card_number BIGINT)
RETURN CASE
  WHEN is_account_group_member('compliance_auditors') THEN card_number
  ELSE concat('****-****-****-', right(cast(card_number AS STRING), 4))
END
""")
 
spark.sql(f"""
ALTER TABLE {cfg.table('bronze', 'cards')}
ALTER COLUMN card_number SET MASK finbank.bronze.mask_card_number_last4
""")
 
print(
    "Column mask applied: bronze.cards.card_number is last-4-only for everyone except members "
    "of the compliance_auditors account group, who see the full value. silver.cards is already "
    "safe by construction and needs no mask, it never carries the real number."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Row filter: Relationship Manager region access
# MAGIC `customers` has no state/region column, and `address` is a single unstructured string
# MAGIC (not a parseable "City, ST" format), so there's nothing reliable to split on there.
# MAGIC `latitude`/`longitude` are clean numeric columns instead, so region is derived from those.
# MAGIC
# MAGIC **This is a synthetic region for demo purposes**, not real US state boundaries: customers
# MAGIC are split into 4 equal-sized buckets (`Region_1`-`Region_4`) by longitude quartile. It's
# MAGIC honest about being an approximation, but it's enough to prove the row filter mechanism
# MAGIC actually works, which is the point of this section.
# MAGIC
# MAGIC Since there's only one real login on this workspace, the RM-to-region mapping is also
# MAGIC faked: a small lookup table assigns your own account to one region, so you can prove the
# MAGIC filter works by watching your own query results shrink to just that region.

# COMMAND ----------

# One-time backfill: add a synthetic `region` column derived from longitude quartiles.
# Safe to re-run, it just recomputes the same 4 buckets from current data.
from pyspark.sql import functions as F
from pyspark.sql.window import Window

customers_df = spark.table(cfg.table("silver", "customers"))
region_window = Window.orderBy("longitude")

customers_with_region = customers_df.withColumn(
    "region", F.concat(F.lit("Region_"), F.ntile(4).over(region_window).cast("string"))
)

customers_with_region.write.format("delta").mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(cfg.table("silver", "customers"))

display(spark.sql(f"SELECT region, count(*) AS n FROM {cfg.table('silver', 'customers')} GROUP BY region ORDER BY region"))

# COMMAND ----------

# Fake RM-to-region assignment table. Real setup would have one row per Relationship Manager;
# this has one row so you can prove the filter works against your own login.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.table('silver', 'rm_region_assignments')} (
  rm_email STRING,
  assigned_region STRING
)
""")

spark.sql(f"""
DELETE FROM {cfg.table('silver', 'rm_region_assignments')}
""")

# Replace with your actual login email if it differs
spark.sql(f"""
INSERT INTO {cfg.table('silver', 'rm_region_assignments')} VALUES
  ('selvan3101@outlook.com', 'Region_1')
""")

# COMMAND ----------

# The row filter function: full access for Fraud/Risk Analysts and Compliance (matches the
# original access matrix), everyone else only sees rows matching their assigned region.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {cfg.table('silver', 'region_filter_for_rm')}(region STRING)
RETURN
  is_account_group_member('compliance_auditors')
  OR is_account_group_member('fraud_risk_analysts')
  OR EXISTS (
    SELECT 1 FROM {cfg.table('silver', 'rm_region_assignments')} a
    WHERE a.rm_email = current_user() AND a.assigned_region = region
  )
""")

spark.sql(f"""
ALTER TABLE {cfg.table('silver', 'customers')}
SET ROW FILTER {cfg.table('silver', 'region_filter_for_rm')} ON (region)
""")

print("Row filter applied: silver.customers now only returns rows matching the querying "
      "user's assigned_region, unless they're in fraud_risk_analysts or compliance_auditors.")

# COMMAND ----------

# Proof it actually works: total row count should now be roughly 1/4 of the unfiltered count
# (matching Region_1, the region assigned to your account above), not the full table.
display(spark.sql(f"SELECT count(*) AS visible_rows FROM {cfg.table('silver', 'customers')}"))