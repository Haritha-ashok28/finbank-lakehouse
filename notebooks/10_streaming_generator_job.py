# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming: live transaction generator
# MAGIC Runs the dbldatagen-based organic traffic generator, writing to a Delta "landing"
# MAGIC table that `11_streaming_ingest_fraud_scoring.py` reads from as its streaming source.
# MAGIC A Delta table as the intermediate sink (rather than Event Hubs directly) is the
# MAGIC pragmatic v1 here -- see the README for how to swap in a real Event Hub producer if
# MAGIC you want to match the Event Hubs box in your architecture diagram exactly; the
# MAGIC fraud-scoring logic downstream doesn't care which one feeds it.
# MAGIC
# MAGIC Run 01-07 (Bronze + Silver dimensions) before this -- the generator seeds from real
# MAGIC card_id/client_id/merchant_id values in Silver, it does not invent its own.

# COMMAND ----------

import sys
sys.path.append("../")

from src.streaming.generator import load_seed_ids, build_organic_stream, inject_fraud_scenario
from src.utils.config import cfg

# COMMAND ----------

dbutils.widgets.text("ROWS_PER_SECOND", "5", "Organic traffic rate")
dbutils.widgets.dropdown("RUN_MODE", "organic_only", ["organic_only", "inject_scenario_now"])
dbutils.widgets.dropdown(
    "SCENARIO", "amount_threshold",
    ["amount_threshold", "velocity", "geo_jump", "spending_deviation"],
)

rows_per_second = int(dbutils.widgets.get("ROWS_PER_SECOND"))
run_mode = dbutils.widgets.get("RUN_MODE")
scenario = dbutils.widgets.get("SCENARIO")

landing_table = cfg.table("bronze", "transactions_stream_landing")

# COMMAND ----------

seed_ids = load_seed_ids(spark)
print(f"Seeded from {len(seed_ids['card_ids'])} real cards and {len(seed_ids['merchant_ids'])} real merchants.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start the organic background stream
# MAGIC This cell runs until cancelled -- it's meant to run as a Databricks Job, not sit
# MAGIC blocking an interactive notebook for the whole demo. Use the next cell to inject a
# MAGIC scripted scenario from a *different* notebook run while this one keeps streaming.

# COMMAND ----------

organic_stream = build_organic_stream(spark, seed_ids, rows_per_second=rows_per_second)

query = (
    organic_stream.writeStream.format("delta")
    .option("checkpointLocation", cfg.checkpoint_path("transactions_stream_landing"))
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(landing_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### On-demand fraud scenario injection
# MAGIC Run this cell (or re-run this notebook with `RUN_MODE = inject_scenario_now`) any
# MAGIC time you want a specific rule to fire in the live demo, instead of waiting for
# MAGIC organic traffic to happen to trip one.

# COMMAND ----------

if run_mode == "inject_scenario_now":
    injected = inject_fraud_scenario(spark, scenario, seed_ids, landing_table)
    print(f"Injected {injected} rows for scenario '{scenario}'.")

# COMMAND ----------

query.awaitTermination()
