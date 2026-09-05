# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Cards ingestion
# MAGIC Auto Loader ingestion of `cards_data.csv` into `finbank.bronze.cards`.

# COMMAND ----------

import sys
sys.path.append("../")  # Databricks Repos sets cwd to the notebook's folder; repo root is one level up

from src.ingestion.bronze_common import ingest_csv_autoloader
from src.utils.config import cfg
from src.utils.schemas import CARDS_SCHEMA_HINTS, CARDS_EXPECTED_COLUMNS

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

print(f"Bronze cards row count: {cards_bronze.count()}")

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
