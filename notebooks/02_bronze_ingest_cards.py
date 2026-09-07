# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze: Cards ingestion
# MAGIC Auto Loader ingestion of `cards_data.csv` into `finbank.bronze.cards`.

# COMMAND ----------

import sys
sys.path.append("../")  # Databricks Repos sets cwd to the notebook's folder; repo root is one level up

from src.ingestion.bronze_common import ingest_csv_autoloader
from src.utils.config import cfg
from src.utils.schemas import CARDS_SCHEMA_HINTS, CARDS_EXPECTED_COLUMNS
from src.utils.data_quality import row_count_sanity_check, rescued_data_check
from src.utils.governance import set_table_and_column_comments

# COMMAND ----------

cards_bronze = ingest_csv_autoloader(
    spark=spark,
    source_folder=cfg.cards_folder,
    stream_name="bronze_cards",
    target_table=cfg.table("bronze", "cards"),
    schema_hints=CARDS_SCHEMA_HINTS,
    expected_columns=CARDS_EXPECTED_COLUMNS,
)

display(cards_bronze.limit(20))

# COMMAND ----------

row_count_sanity_check(cards_bronze, "Bronze cards")
rescued_data_check(cards_bronze, "Bronze cards")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick referential sanity check (card.client_id -> customers.id)
# MAGIC Cheap enough to run right after ingestion; a non-zero orphan count here means either
# MAGIC the customers file needs re-ingesting first, or the join key isn't what the design doc assumed.

# COMMAND ----------

orphans = spark.sql(f"""
    SELECT count(*) AS orphan_cards
    FROM {cfg.table('bronze', 'cards')} c
    LEFT ANTI JOIN {cfg.table('bronze', 'customers')} u
    ON c.client_id = u.id
""")
display(orphans)

# COMMAND ----------

set_table_and_column_comments(
    spark,
    cfg.table("bronze", "cards"),
    table_comment=(
        "Raw Auto Loader landing zone for cards_data.csv (CaixaBank/Kaggle). "
        "One row per source CSV row, no transformation applied."
    ),
    column_comments={
        "id": "Card id from the source file.",
        "client_id": "FK to customers.id (bronze.customers / silver.customers).",
        "credit_limit": (
            "Raw string as ingested, e.g. \"$24295\" -- NOT yet numeric. "
            "Cleaned in Silver via parse_currency()."
        ),
        "_rescued_data": (
            "Auto Loader's rescued-data column: anything that didn't match the schema hints "
            "lands here instead of being dropped."
        ),
    },
)
