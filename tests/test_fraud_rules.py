"""
Unit tests for the pure fraud rule math in src/silver/fraud_rules.py, run locally with
plain PySpark (no Delta, no cluster needed). These exercise the actual thresholds from
src/utils/config.py, so if you tune a threshold there, these tests still describe the
real behavior.
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.silver.fraud_rules import (
    apply_amount_threshold_rule,
    batch_apply_velocity_rule,
    batch_apply_geo_jump_rule,
    batch_apply_spending_deviation_rule,
    haversine_km_expr,
)
from pyspark.sql import functions as F

BASE_TS = datetime(2024, 1, 1, 12, 0, 0)


def test_amount_threshold_flags_above_2000_only(spark):
    df = spark.createDataFrame([(1, 2500.0), (2, 100.0), (3, 2000.01)], ["id", "amount"])
    result = apply_amount_threshold_rule(df).orderBy("id").collect()

    assert result[0]["flag_amount_threshold"] is True
    assert result[1]["flag_amount_threshold"] is False
    assert result[2]["flag_amount_threshold"] is True


def test_velocity_flags_third_transaction_within_5_minutes(spark):
    rows = [
        ("c1", BASE_TS),
        ("c1", BASE_TS + timedelta(minutes=2)),
        ("c1", BASE_TS + timedelta(minutes=4)),  # 3rd txn within 5 min of the first -> flag
    ]
    df = spark.createDataFrame(rows, ["card_id", "date"])
    result = batch_apply_velocity_rule(df).orderBy("date").collect()

    assert [r["txn_count_in_window"] for r in result] == [1, 2, 3]
    assert result[2]["flag_velocity"] is True
    assert result[0]["flag_velocity"] is False and result[1]["flag_velocity"] is False


def test_velocity_does_not_flag_transactions_spread_out(spark):
    rows = [
        ("c1", BASE_TS),
        ("c1", BASE_TS + timedelta(minutes=10)),
        ("c1", BASE_TS + timedelta(minutes=20)),
    ]
    df = spark.createDataFrame(rows, ["card_id", "date"])
    result = batch_apply_velocity_rule(df).collect()

    assert all(r["flag_velocity"] is False for r in result)


def test_haversine_known_distance_nyc_to_la(spark):
    """NYC -> LA is ~3936 km; allow a few percent tolerance for the approximation.

    Reuses the session-scoped `spark` fixture rather than creating its own SparkSession:
    PySpark's SparkContext is a JVM-wide singleton per process, so an earlier attempt at
    this test that called SparkSession.builder...getOrCreate() and then spark.stop() at
    the end tore down the shared context out from under every other test in this file.
    """
    df = spark.createDataFrame([(40.7128, -74.0060, 34.0522, -118.2437)], ["lat1", "lon1", "lat2", "lon2"])
    result = df.withColumn(
        "distance_km", haversine_km_expr(F.col("lat1"), F.col("lon1"), F.col("lat2"), F.col("lon2"))
    ).collect()[0]["distance_km"]

    assert 3850 < result < 4000, f"expected ~3936km, got {result}"


def test_geo_jump_flags_impossible_travel(spark):
    """NYC then LA 5 minutes later on the same card -> clearly impossible -> flag."""
    rows = [
        ("c1", BASE_TS, 40.7128, -74.0060),
        ("c1", BASE_TS + timedelta(minutes=5), 34.0522, -118.2437),
    ]
    df = spark.createDataFrame(rows, ["card_id", "date", "merchant_latitude", "merchant_longitude"])
    result = batch_apply_geo_jump_rule(df).orderBy("date").collect()

    assert result[0]["flag_geo_jump"] is False, "first transaction has no prior point to compare"
    assert result[1]["flag_geo_jump"] is True


def test_geo_jump_does_not_flag_nearby_transactions(spark):
    """Two transactions a few km apart, 10 minutes apart -> plausible, no flag."""
    rows = [
        ("c1", BASE_TS, 40.7128, -74.0060),
        ("c1", BASE_TS + timedelta(minutes=10), 40.7300, -73.9950),  # ~2km away
    ]
    df = spark.createDataFrame(rows, ["card_id", "date", "merchant_latitude", "merchant_longitude"])
    result = batch_apply_geo_jump_rule(df).orderBy("date").collect()

    assert result[1]["flag_geo_jump"] is False


def test_spending_deviation_flags_amount_over_3x_trailing_average(spark):
    rows = [
        ("cust1", BASE_TS - timedelta(days=10), 100.0),
        ("cust1", BASE_TS - timedelta(days=5), 100.0),
        ("cust1", BASE_TS, 500.0),  # 5x the trailing average of 100 -> flag
    ]
    df = spark.createDataFrame(rows, ["client_id", "date", "amount"])
    result = batch_apply_spending_deviation_rule(df).orderBy("date").collect()

    assert result[0]["flag_spending_deviation"] is False, "no trailing history yet"
    assert result[2]["flag_spending_deviation"] is True
    assert result[2]["trailing_30d_avg_amount"] == 100.0


def test_spending_deviation_does_not_flag_normal_spending(spark):
    rows = [
        ("cust1", BASE_TS - timedelta(days=10), 100.0),
        ("cust1", BASE_TS - timedelta(days=5), 120.0),
        ("cust1", BASE_TS, 150.0),  # well under 3x the ~110 average -> no flag
    ]
    df = spark.createDataFrame(rows, ["client_id", "date", "amount"])
    result = batch_apply_spending_deviation_rule(df).orderBy("date").collect()

    assert result[2]["flag_spending_deviation"] is False
