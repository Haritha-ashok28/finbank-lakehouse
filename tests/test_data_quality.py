"""
Unit tests for src/utils/data_quality.py, using the same local Delta-free Spark session
as the rest of tests/ (see conftest.py). These are the new orphan-check / null-count /
row-count / rescued-data helpers added to close the data-quality gaps flagged against
the design spec's "no scope-cutting" checklist.
"""

import pytest

from src.utils.data_quality import (
    check_orphan_keys,
    null_count_report,
    row_count_sanity_check,
    rescued_data_check,
)


def test_check_orphan_keys_finds_orphans(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "fk"])
    ref = spark.createDataFrame([("a",), ("b",)], ["key"])
    orphan_count = check_orphan_keys(df, "fk", ref, "key", "test rows")
    assert orphan_count == 1  # the row with fk="c" has no match in ref


def test_check_orphan_keys_no_orphans(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "fk"])
    ref = spark.createDataFrame([("a",), ("b",), ("c",)], ["key"])
    orphan_count = check_orphan_keys(df, "fk", ref, "key", "test rows")
    assert orphan_count == 0


def test_null_count_report_all_columns(spark):
    df = spark.createDataFrame([(1, None), (2, "x"), (None, "y")], ["a", "b"])
    report = null_count_report(df).collect()[0]
    assert report["a"] == 1
    assert report["b"] == 1


def test_null_count_report_specific_columns(spark):
    df = spark.createDataFrame([(1, None), (2, "x")], ["a", "b"])
    report = null_count_report(df, cols=["b"]).collect()[0]
    assert report["b"] == 1
    assert "a" not in report.asDict()


def test_row_count_sanity_check_passes(spark):
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    assert row_count_sanity_check(df, "test", min_expected=1) == 2


def test_row_count_sanity_check_raises_on_empty(spark):
    df = spark.createDataFrame([], "id INT")
    with pytest.raises(ValueError):
        row_count_sanity_check(df, "test", min_expected=1)


def test_rescued_data_check_counts_non_null(spark):
    df = spark.createDataFrame([(1, None), (2, "malformed")], ["id", "_rescued_data"])
    assert rescued_data_check(df, "test") == 1


def test_rescued_data_check_missing_column_returns_zero(spark):
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    assert rescued_data_check(df, "test") == 0
