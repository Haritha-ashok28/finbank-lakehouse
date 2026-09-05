# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Merchant Geo Reference (SCD1, load-once reference table)
# MAGIC Merchants only carry a ZIP code, not lat/long -- this table fills that gap so the
# MAGIC geo-jump fraud rule can compute a real distance between a customer's last known
# MAGIC location and a merchant's approximate location.
# MAGIC
# MAGIC Source: `zip_centroids.csv` in this repo (`data/reference/`), built from the `zipcodes`
# MAGIC PyPI package (~42,800 US ZIP codes with lat/long/city/state/county, offline/bundled
# MAGIC data, no external API dependency). Upload this file to your `raw` container's
# MAGIC `reference/` folder once, then this notebook just needs re-running if the reference
# MAGIC file itself is ever updated.

# COMMAND ----------

import sys
sys.path.append("../")

from src.silver.scd_utils import scd1_upsert
from src.utils.config import cfg

# COMMAND ----------

zip_centroids = (
    spark.read.option("header", "true").option("inferSchema", "true")
    .csv(cfg.raw_path("reference/zip_centroids.csv"))
    .withColumnRenamed("zip_code", "zip")
)

print(f"ZIP centroids loaded: {zip_centroids.count()}")

# COMMAND ----------

scd1_upsert(
    spark=spark,
    source_df=zip_centroids,
    target_table=cfg.table("silver", "merchant_geo_reference"),
    business_key_cols=["zip"],
)

display(spark.table(cfg.table("silver", "merchant_geo_reference")).limit(20))
