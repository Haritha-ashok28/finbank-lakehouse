# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze: Transactions ingestion
# MAGIC Auto Loader ingestion of `transactions_data.csv` (~24M rows) into `finbank.bronze.transactions`.
# MAGIC
# MAGIC This is the file too large to profile comfortably with pandas locally -- Auto Loader
# MAGIC streams it in incrementally rather than loading it all into driver memory, which is
# MAGIC exactly the problem it's meant to solve. If you want a fast first pass instead of the
# MAGIC full 24M rows while you're iterating on Silver logic, set `LIMIT_ROWS_FOR_DEV` below.

# COMMAND ----------

import sys
sys.path.append("../")  # Databricks Repos sets cwd to the notebook's folder; repo root is one level up

from src.ingestion.bronze_common import ingest_csv_autoloader
from src.utils.config import cfg
from src.utils.schemas import TRANSACTIONS_SCHEMA_HINTS, TRANSACTIONS_EXPECTED_COLUMNS

# COMMAND ----------

dbutils.widgets.text("LIMIT_ROWS_FOR_DEV", "", "Row limit for dev iteration (blank = full file)")
LIMIT_ROWS_FOR_DEV = dbutils.widgets.get("LIMIT_ROWS_FOR_DEV")

# COMMAND ----------

transactions_bronze = ingest_csv_autoloader(
    spark=spark,
    source_folder=cfg.transactions_folder,
    stream_name="bronze_transactions",
    target_table=cfg.table("bronze", "transactions"),
    schema_hints=TRANSACTIONS_SCHEMA_HINTS,
    expected_columns=TRANSACTIONS_EXPECTED_COLUMNS,
)

if LIMIT_ROWS_FOR_DEV:
    print(f"Dev mode: {cfg.table('bronze', 'transactions')} has more rows than the "
          f"{LIMIT_ROWS_FOR_DEV} you're using for iteration -- Silver notebooks apply "
          f"their own LIMIT_ROWS_FOR_DEV widget rather than truncating Bronze itself, "
          f"so Bronze always holds everything that's actually landed.")

display(transactions_bronze.limit(20))

# COMMAND ----------

print(f"Bronze transactions row count: {transactions_bronze.count()}")