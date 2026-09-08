# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze: Customers ingestion
# MAGIC Auto Loader ingestion of `users_data.csv` into `finbank.bronze.customers`.
# MAGIC
# MAGIC Run this once your ADLS Gen2 External Location and Unity Catalog are set up and the raw
# MAGIC CSV has been uploaded to the `raw` container (see README for the exact folder layout).

# COMMAND ----------

import sys
sys.path.append("../")  # Databricks Repos sets cwd to the notebook's folder; repo root is one level up

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

from src.ingestion.bronze_common import ingest_csv_autoloader
from src.utils.config import cfg
from src.utils.schemas import CUSTOMERS_SCHEMA_HINTS, CUSTOMERS_EXPECTED_COLUMNS
from src.utils.data_quality import row_count_sanity_check, rescued_data_check
from src.utils.governance import set_table_and_column_comments

# COMMAND ----------

cfg.ensure_schemas(spark)

# COMMAND ----------

# DBTITLE 1,Customers bronze ingestion
customers_bronze = ingest_csv_autoloader(
    spark=spark,
    source_folder=cfg.customers_folder,
    stream_name="bronze_customers",
    target_table=cfg.table("bronze", "customers"),
    schema_hints=CUSTOMERS_SCHEMA_HINTS,
    expected_columns=CUSTOMERS_EXPECTED_COLUMNS,
)

display(customers_bronze.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row-count + rescued-data sanity checks
# MAGIC Raises if the load came back empty (a misconfigured folder path or a bad upload
# MAGIC should stop the pipeline here, not flow an empty table downstream). Also surfaces
# MAGIC whether Auto Loader's `_rescued_data` column actually caught anything -- it's been
# MAGIC enabled since the first version of this notebook, but nothing was checking it.

# COMMAND ----------

row_count_sanity_check(customers_bronze, "Bronze customers")
rescued_data_check(customers_bronze, "Bronze customers")

# COMMAND ----------

set_table_and_column_comments(
    spark,
    cfg.table("bronze", "customers"),
    table_comment=(
        "Raw Auto Loader landing zone for users_data.csv (CaixaBank/Kaggle). "
        "One row per source CSV row, no transformation applied."
    ),
    column_comments={
        "id": "Customer id from the source file. Becomes the client_id foreign key everywhere downstream.",
        "yearly_income": (
            "Raw string as ingested, e.g. \"$59696\" -- NOT yet numeric. "
            "Cleaned in Silver via parse_currency()."
        ),
        "_rescued_data": (
            "Auto Loader's rescued-data column: anything that didn't match the schema hints "
            "lands here instead of being dropped."
        ),
        "_source_file": "Lineage: which raw file this row came from.",
        "_ingested_at": "Timestamp this row was written into Bronze.",
    },
)