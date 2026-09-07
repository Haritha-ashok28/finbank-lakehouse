# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Governance: Unity Catalog column mask (security matrix enforcement)
# MAGIC Implements one real access-control rule from the design spec's persona matrix as a
# MAGIC live Unity Catalog rule, rather than leaving it as documentation only: `birth_year`
# MAGIC on `silver.customers` is masked/generalized (rounded down to the decade) for the
# MAGIC Fraud & Risk Analyst persona, and shown as-is to every other role (Relationship
# MAGIC Manager, Compliance/Auditor), matching the access matrix in the design spec.
# MAGIC
# MAGIC Run this once per environment. Both statements below are idempotent (`CREATE OR
# MAGIC REPLACE FUNCTION`, and re-running `SET MASK` on an already-masked column just
# MAGIC reapplies the same rule), so rerunning this notebook is always safe.
# MAGIC
# MAGIC This assumes an account-level group named `fraud_risk_analysts` exists in your
# MAGIC Databricks account console (Account Console -> User management -> Groups). If it
# MAGIC doesn't exist yet, this still runs fine -- `is_account_group_member` just returns
# MAGIC false for everyone, so the mask always falls through to the unmasked branch until
# MAGIC the group exists and someone is actually a member of it.
# MAGIC
# MAGIC This is the ONE mask built as a real, live rule (the design spec's access matrix
# MAGIC has several more: card_number full-vs-masked by role, Customers/Cards row-filtered
# MAGIC by region for Relationship Manager, Gold-only access for business users). The same
# MAGIC `CREATE FUNCTION` + `SET MASK` pattern extends directly to those if there's time to
# MAGIC build out the rest of the matrix; a row filter uses `ALTER TABLE ... SET ROW FILTER`
# MAGIC instead of `SET MASK`, same idea.

# COMMAND ----------

import sys
sys.path.append("../")

from src.utils.config import cfg

# COMMAND ----------

mask_function_name = cfg.table("silver", "mask_birth_year_for_fraud_analyst")
customers_table = cfg.table("silver", "customers")

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {mask_function_name}(birth_year INT)
    RETURNS INT
    RETURN CASE
        WHEN is_account_group_member('fraud_risk_analysts') THEN (birth_year DIV 10) * 10
        ELSE birth_year
    END
""")

spark.sql(f"""
    ALTER TABLE {customers_table}
    ALTER COLUMN birth_year
    SET MASK {mask_function_name}
""")

print(
    f"Column mask applied: {customers_table}.birth_year is decade-generalized for "
    f"members of the fraud_risk_analysts account group, shown as-is to everyone else."
)
