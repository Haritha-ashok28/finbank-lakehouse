"""
Shared Auto Loader ingestion logic for all three Bronze sources.

One function, parameterized per source, instead of copy-pasted per notebook -- so a fix
to ingestion behavior (e.g. how corrupt records are handled) only needs to happen once.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config import cfg
from src.utils.schemas import validate_expected_columns


def ingest_csv_autoloader(
    spark: SparkSession,
    source_folder: str,
    stream_name: str,
    target_table: str,
    schema_hints: str,
    expected_columns: list,
    header: bool = True,
) -> DataFrame:
    """
    Ingest a CSV source FOLDER into a Bronze Delta table using Auto Loader (cloudFiles),
    with schema inference seeded by `schema_hints` and evolution enabled.

    `source_folder` is a folder under the raw container (e.g. "customers"), not a single
    filename -- Auto Loader watches the folder and picks up whatever CSV(s) land there,
    which is what makes incremental loading possible (drop a second file in later and
    re-run, only the new file gets processed, tracked via `checkpoint_path`). Pointing
    this at one fixed filename instead would make every later file invisible to it.

    Adds standard Bronze metadata columns (_ingested_at, _source_file) so lineage back
    to the raw file is always traceable, and runs `validate_expected_columns` once the
    stream starts so schema drift against the design doc is visible in the driver logs
    immediately rather than discovered downstream in Silver.
    """
    raw_path = cfg.raw_folder(source_folder)
    checkpoint_path = cfg.checkpoint_path(stream_name)
    schema_location = cfg.schema_location_path(stream_name)

    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", str(header).lower())
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaHints", schema_hints)
        .option("cloudFiles.inferColumnTypes", "true")
        # New columns showing up in a later file drop shouldn't break the stream;
        # they get added to the target table automatically (mergeSchema below).
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
    )

    df = reader.load(raw_path)

    validate_expected_columns(df.columns, expected_columns, stream_name)

    bronze_df = (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    query = (
        bronze_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)  # batch-style: process what's there, then stop
        .toTable(target_table)
    )
    query.awaitTermination()
    return spark.table(target_table)
