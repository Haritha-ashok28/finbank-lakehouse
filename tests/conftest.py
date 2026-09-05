"""
Shared pytest fixture: a local, Delta-free Spark session for unit testing pure
DataFrame logic (change detection, fraud rules) without needing a Databricks cluster.

Run with: pip install -r requirements-dev.txt && pytest tests/
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("finbank-unit-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
