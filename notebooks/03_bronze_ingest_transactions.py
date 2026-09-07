# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze: Transactions ingestion
# MAGIC Auto Loader ingestion of `transactions_data.csv` (~13.3M rows) into `finbank.bronze.transactions`.
# MAGIC
# MAGIC This is the file too large to profile comfortably with pandas locally -- Auto Loader
# MAGIC streams it in incrementally rather than loading it all into driver memory, which is
# MAGIC exactly the problem it's meant to solve. If you want a fast first pass instead of the
# MAGIC full ~13.3M rows while you're iterating on Silver logic, set `LIMIT_ROWS_FOR_DEV` below
# MAGIC (this is used by the Silver notebooks' own dev-mode widget -- Bronze itself always
# MAGIC ingests everything that's actually landed in the raw folder).

# COMMAND ----------

import sys
sys.path.append("../")  # Databricks Repos sets cwd to the notebook's folder; repo root is one level up

from src.ingestion.bronze_common import ingest_csv_autoloader
from src.utils.config import cfg
from src.utils.schemas import TRANSACTIONS_SCHEMA_HINTS, TRANSACTIONS_EXPECTED_COLUMNS
from src.utils.data_quality import row_count_sanity_check, rescued_data_check
from src.utils.governance import set_table_and_column_comments

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

row_count_sanity_check(transactions_bronze, "Bronze transactions")
rescued_data_check(transactions_bronze, "Bronze transactions")

# COMMAND ----------

set_table_and_column_comments(
    spark,
    cfg.table("bronze", "transactions"),
    table_comment=(
        "Raw Auto Loader landing zone for transactions_data.csv (CaixaBank/Kaggle, ~13.3M rows). "
        "One row per source CSV row, no transformation applied."
    ),
    column_comments={
        "id": "Transaction id from the source file.",
        "client_id": "FK to customers.id.",
        "card_id": "FK to cards.id.",
        "merchant_id": "FK to silver.merchants.id (merchants are derived from this table, not a separate source file).",
        "amount": (
            "Raw string as ingested, e.g. \"$134.09\" -- NOT yet numeric. "
            "Cleaned in Silver via parse_currency()."
        ),
        "zip": (
            "Merchant ZIP as ingested. May have lost leading zeros (a pandas float-export "
            "quirk) -- normalized in Silver via clean_zip()."
        ),
        "_rescued_data": (
            "Auto Loader's rescued-data column: anything that didn't match the schema hints "
            "lands here instead of being dropped."
        ),
    },
)
