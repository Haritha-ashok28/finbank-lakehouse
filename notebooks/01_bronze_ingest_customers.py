# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Customers ingestion
# MAGIC Auto Loader ingestion of `users_data.csv` into `finbank.bronze.customers`.
# MAGIC
# MAGIC Run this once your ADLS Gen2 External Location and Unity Catalog are set up and the raw
# MAGIC CSV has been uploaded to the `raw` container (see README for the exact folder layout).

# COMMAND ----------

import sys
sys.path.append("../")  # Databricks Repos sets cwd to the notebook's folder; repo root is one level up

from src.ingestion.bronze_common import ingest_csv_autoloader
from src.utils.config import cfg
from src.utils.schemas import CUSTOMERS_SCHEMA_HINTS, CUSTOMERS_EXPECTED_COLUMNS

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.table('bronze', '').rsplit('.', 1)[0]}")

# COMMAND ----------

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

print(f"Bronze customers row count: {customers_bronze.count()}")
