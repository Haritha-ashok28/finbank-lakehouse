"""
Central pipeline configuration for the FinBank Transaction Intelligence Lakehouse.

Everything environment-specific (storage paths, catalog/schema names) lives here so the
rest of the codebase never hardcodes a path. Fill in the ADLS_STORAGE_ACCOUNT and container
names for your own Azure environment before running anything.

Usage (from a Databricks notebook or another module):
    from src.utils.config import cfg
    print(cfg.bronze_path("transactions"))
"""

import os
from dataclasses import dataclass, field


@dataclass
class PipelineConfig:
    # --- Unity Catalog naming ---------------------------------------------------
    # TODO: confirm these match the catalog/schemas you actually create in Unity Catalog.
    catalog: str = os.environ.get("FINBANK_CATALOG", "finbank")
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    # --- ADLS Gen2 storage (External Location) -----------------------------------
    # TODO: replace with your real storage account + container names. This is the
    # single place that needs to change once your Azure resources exist.
    storage_account: str = os.environ.get("FINBANK_STORAGE_ACCOUNT", "REPLACE_ME_STORAGE_ACCOUNT")
    raw_container: str = "raw"
    checkpoints_container: str = "checkpoints"

    # --- Source layout in the raw container ---------------------------------------
    # The 3 high-volume sources are folders, not fixed filenames: Auto Loader watches
    # a FOLDER and picks up whatever files land in it over time (that's the entire
    # point of using it for incremental loading). Point it at one exact filename
    # instead and it will only ever see that one file -- a second file dropped next to
    # it, even with a different name, is silently invisible to a stream watching a
    # single path. Upload each source's CSV (and any later incremental part files,
    # e.g. for testing incremental loads) INTO its folder:
    #   raw/customers/users_data.csv, raw/customers/users_data_part2.csv, ...
    #   raw/cards/cards_data.csv
    #   raw/transactions/transactions_data.csv
    # mcc_codes.json is a one-time static lookup read with spark.read.json (not Auto
    # Loader), so it stays a plain fixed file at the raw container's root.
    customers_folder: str = "customers"
    cards_folder: str = "cards"
    transactions_folder: str = "transactions"
    mcc_codes_file: str = "mcc_codes.json"

    # --- Fraud rule thresholds (tune these once you see real distributions) -------
    fraud_amount_threshold: float = 2000.00
    fraud_velocity_count: int = 3
    fraud_velocity_window_minutes: int = 5
    fraud_geo_jump_max_speed_kmh: float = 900.0  # ~commercial flight speed; anything faster is "impossible"
    fraud_spending_deviation_multiplier: float = 3.0
    fraud_spending_deviation_window_days: int = 30

    def _abfss(self, container: str, path: str = "") -> str:
        return f"abfss://{container}@{self.storage_account}.dfs.core.windows.net/{path}"

    def raw_path(self, filename: str) -> str:
        """Path to a single fixed raw file (e.g. mcc_codes.json, the ZIP reference CSV)."""
        return self._abfss(self.raw_container, filename)

    def raw_folder(self, folder_name: str) -> str:
        """Path to a raw source FOLDER that Auto Loader watches for incoming files."""
        return self._abfss(self.raw_container, f"{folder_name}/")

    def checkpoint_path(self, stream_name: str) -> str:
        """Structured Streaming / Auto Loader checkpoint location for a given stream."""
        return self._abfss(self.checkpoints_container, f"checkpoints/{stream_name}")

    def schema_location_path(self, stream_name: str) -> str:
        """Auto Loader schema inference/evolution location for a given stream."""
        return self._abfss(self.checkpoints_container, f"schemas/{stream_name}")

    def table(self, layer: str, name: str) -> str:
        """Fully qualified three-part Unity Catalog table name, e.g. table('silver', 'customers')."""
        schema = {"bronze": self.bronze_schema, "silver": self.silver_schema, "gold": self.gold_schema}[layer]
        return f"{self.catalog}.{schema}.{name}"
    
    def ensure_schemas(self, spark) -> None:
        """
        Create the catalog and all three medallion schemas if they don't already exist.

        Idempotent (CREATE ... IF NOT EXISTS), so it's safe to call from the first
        notebook of every layer (01 for bronze, 04 for silver, 09 for gold) instead of
        assuming an earlier notebook already ran in this session. Unity Catalog schemas
        persist across clusters/sessions once created -- in practice this is a no-op
        after the first successful run -- it's here so a fresh workspace, or a reviewer
        running the repo cold, doesn't hit SCHEMA_NOT_FOUND just because they ran a
        notebook out of order or skipped 01.
        """
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")
        for schema in (self.bronze_schema, self.silver_schema, self.gold_schema):
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{schema}")


cfg = PipelineConfig()
