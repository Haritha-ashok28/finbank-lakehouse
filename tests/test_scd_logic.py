"""
Unit tests for the pure (Delta-free) change-detection logic in src/silver/scd_utils.py.

These do NOT exercise the actual Delta MERGE calls (scd1_upsert / scd2_merge /
scd2_with_scd3_merge) -- there's no Databricks cluster or Delta JAR available in this
environment. What they DO verify is the part most likely to hide a subtle bug: correctly
deciding WHICH rows changed, especially around NULLs, which is exactly where the first
draft of this file had a real bug (using `<>` instead of `IS DISTINCT FROM`, which
silently misses a NULL -> value change). Run these locally before trusting the merge
functions, and add a real end-to-end run against a Databricks dev catalog before this
touches anything that matters.
"""

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.types import StructType, StructField, StringType

from src.silver.scd_utils import rows_with_changed_cols

ID_TIER_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("income_tier", StringType(), True),
])


def test_detects_simple_value_change(spark):
    source = spark.createDataFrame([("c1", "gold"), ("c2", "silver")], ["id", "income_tier"])
    target_current = spark.createDataFrame([("c1", "silver"), ("c2", "silver")], ["id", "income_tier"])

    changed = rows_with_changed_cols(source, target_current, ["id"], ["income_tier"])
    changed_ids = {row["id"] for row in changed.collect()}

    assert changed_ids == {"c1"}, "c1's income_tier changed silver->gold, c2 did not change"


def test_null_to_value_counts_as_a_change(spark):
    """This is the exact bug the first draft had: <> would miss NULL -> value."""
    source = spark.createDataFrame([("c1", "gold")], ["id", "income_tier"])
    target_current = spark.createDataFrame([("c1", None)], ID_TIER_SCHEMA)

    changed = rows_with_changed_cols(source, target_current, ["id"], ["income_tier"])

    assert changed.count() == 1, "NULL -> 'gold' must be detected as a change"


def test_null_to_null_is_not_a_change(spark):
    source = spark.createDataFrame([("c1", None)], ID_TIER_SCHEMA)
    target_current = spark.createDataFrame([("c1", None)], ID_TIER_SCHEMA)

    changed = rows_with_changed_cols(source, target_current, ["id"], ["income_tier"])

    assert changed.count() == 0, "NULL -> NULL is not a change"


def test_unmatched_business_key_is_excluded_not_flagged_changed(spark):
    """A brand-new business key isn't a 'change' -- it's an insert, handled elsewhere."""
    source = spark.createDataFrame([("c1", "gold"), ("c99", "bronze")], ["id", "income_tier"])
    target_current = spark.createDataFrame([("c1", "gold")], ["id", "income_tier"])

    changed = rows_with_changed_cols(source, target_current, ["id"], ["income_tier"])

    assert changed.count() == 0, "c1 unchanged, c99 has no current row so it's an insert, not a change"


def test_multi_column_tracked_change_detection(spark):
    source = spark.createDataFrame([("card1", "GOLD", 5000)], ["id", "card_type", "credit_limit"])
    target_current = spark.createDataFrame([("card1", "GOLD", 3000)], ["id", "card_type", "credit_limit"])

    changed = rows_with_changed_cols(source, target_current, ["id"], ["card_type", "credit_limit"])

    assert changed.count() == 1, "credit_limit changed even though card_type did not"
