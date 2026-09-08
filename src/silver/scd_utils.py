"""
Reusable Slowly Changing Dimension merge patterns, shared across every Silver dimension
so the MERGE logic is written and tested once instead of copy-pasted per notebook.

Three patterns, matching what the design spec assigns per dimension:

- scd1_upsert            : Merchants, Merchant Geo Reference -- overwrite in place, no history.
- scd2_merge             : Cards -- whole-entity history, a new row every time any tracked
                            column changes (status, credit_limit, type, ...).
- scd2_with_scd3_merge   : Customers -- a hybrid. income_tier is tracked as full SCD2 history
                            (a new row per change). address is tracked as SCD3 (only the
                            current value plus the single most recent prior value, updated
                            in place, never creating its own new row).

DESIGN NOTE / KNOWN LIMITATION: this sandbox has no Databricks cluster and no network
path to Maven Central to pull the real Delta Lake JAR, so the `DeltaTable.merge(...)`
calls below have NOT been executed against a live Delta table -- only reasoned through
and, for the change-detection logic specifically, unit tested locally with plain
PySpark (no Delta) in tests/test_scd_logic.py. That's why the "which rows actually
changed" logic is factored into the pure, Delta-free `rows_with_changed_cols()` function
below: it's the part most likely to have a subtle bug (NULL handling, key matching), and
it's the part that's actually possible to test without a cluster. Before trusting this
in your real pipeline, run it against a real Unity Catalog table with a handful of test
rows and confirm effective_start/effective_end/is_current/previous_<col> look right.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def rows_with_changed_cols(
    source_df: DataFrame,
    target_current_df: DataFrame,
    business_key_cols: list,
    tracked_cols: list,
) -> DataFrame:
    """
    Pure, Delta-free logic: the subset of `source_df` rows whose `tracked_cols` differ
    from the current row in `target_current_df` for the same business key.

    Uses `IS DISTINCT FROM` rather than `<>` deliberately -- `<>` treats NULL <> NULL and
    NULL <> 5 both as NULL (i.e. "not a match, but also not flagged as changed"), which
    would silently miss a real change from NULL to a value. `IS DISTINCT FROM` treats
    NULL as a comparable value, so NULL -> 5 correctly counts as a change and
    NULL -> NULL correctly does not.

    Rows in source_df with no matching business key in target_current_df are NOT
    returned here (that's a brand-new dimension member, handled separately by the
    caller's `whenNotMatchedInsertAll`).
    """
    condition = " OR ".join([f"t.{c} IS DISTINCT FROM s.{c}" for c in tracked_cols])
    joined = source_df.alias("s").join(target_current_df.alias("t"), on=business_key_cols, how="inner")
    return joined.where(condition).select("s.*")


def scd1_upsert(spark: SparkSession, source_df: DataFrame, target_table: str, business_key_cols: list) -> None:
    """Plain upsert: matched rows are overwritten, new rows are inserted. No history kept."""
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").saveAsTable(target_table)
        return

    target = DeltaTable.forName(spark, target_table)
    key_condition = " AND ".join([f"t.{c} = s.{c}" for c in business_key_cols])

    (
        target.alias("t")
        .merge(source_df.alias("s"), key_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def scd2_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    business_key_cols: list,
    tracked_cols: list,
    effective_ts=None,
) -> None:
    """
    Whole-entity SCD2: whenever any column in `tracked_cols` differs from the current
    (is_current = true) row for a business key, expire that row and insert a new
    current version. Rows whose tracked columns are unchanged are left untouched.

    Adds/uses these standard SCD2 columns on the target table:
        effective_start (timestamp), effective_end (timestamp, null while current),
        is_current (boolean)
    """
    effective_ts = effective_ts or F.current_timestamp()

    if not spark.catalog.tableExists(target_table):
        (
            source_df
            .withColumn("effective_start", effective_ts)
            .withColumn("effective_end", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .write.format("delta").saveAsTable(target_table)
        )
        return

    target_current_df = spark.table(target_table).filter("is_current = true")
    key_condition = " AND ".join([f"t.{c} = s.{c}" for c in business_key_cols])
    change_condition = " OR ".join([f"t.{c} IS DISTINCT FROM s.{c}" for c in tracked_cols])

    changed_rows = rows_with_changed_cols(source_df, target_current_df, business_key_cols, tracked_cols)
    new_versions = (
        changed_rows
        .withColumn("effective_start", effective_ts)
        .withColumn("effective_end", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    target = DeltaTable.forName(spark, target_table)

    # Step 1: expire the current row for every changed business key; insert brand-new
    # business keys straight away (their SCD columns get patched in step 2, since
    # whenNotMatchedInsertAll only inserts columns that exist in `source_df`).
    (
        target.alias("t")
        .merge(source_df.alias("s"), key_condition)
        .whenMatchedUpdate(
            condition=change_condition,
            set={"effective_end": effective_ts, "is_current": F.lit(False)},
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Step 2: patch the SCD columns on rows that just got inserted as brand-new members
    # (they have no effective_start yet because `source_df` never carries one).
    spark.sql(f"""
        UPDATE {target_table}
        SET effective_start = current_timestamp(), effective_end = NULL, is_current = true
        WHERE effective_start IS NULL
    """)

    # Step 3: append the new "current" version row for every business key that changed.
    if new_versions.take(1):
        new_versions.write.format("delta").mode("append").saveAsTable(target_table)


def scd2_with_scd3_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    business_key_cols: list,
    scd2_tracked_cols: list,
    scd3_col: str,
    effective_ts=None,
) -> None:
    """
    Customers-shaped hybrid: `scd2_tracked_cols` (e.g. ["income_tier"]) get full SCD2
    row-versioned history. `scd3_col` (e.g. "address") gets SCD3 treatment: only ever a
    current value plus one `previous_<scd3_col>` column, updated in place, never causing
    a new row on its own -- but its old value IS carried into a new SCD2 row's
    `previous_<col>` when an SCD2 attribute change happens at the same time, since that
    was genuinely the "previous" value as of that transition.
    """
    effective_ts = effective_ts or F.current_timestamp()
    previous_col = f"previous_{scd3_col}"

    if not spark.catalog.tableExists(target_table):
        (
            source_df
            .withColumn(previous_col, F.lit(None).cast(dict(source_df.dtypes)[scd3_col]))
            .withColumn("effective_start", effective_ts)
            .withColumn("effective_end", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .write.format("delta").saveAsTable(target_table)
        )
        return

    target_current_df = spark.table(target_table).filter("is_current = true")
    key_condition = " AND ".join([f"t.{c} = s.{c}" for c in business_key_cols])
    scd2_change_condition = " OR ".join([f"t.{c} IS DISTINCT FROM s.{c}" for c in scd2_tracked_cols])

    # --- Case 1: an SCD2-tracked attribute changed -> new version row ------------------
    scd2_changed_rows = rows_with_changed_cols(source_df, target_current_df, business_key_cols, scd2_tracked_cols)
    prior_scd3_values = target_current_df.select(*business_key_cols, F.col(scd3_col).alias("_prior_scd3_value"))

    new_versions = (
        scd2_changed_rows.alias("s")
        .join(prior_scd3_values.alias("p"), on=business_key_cols, how="left")
        .select("s.*", F.col("_prior_scd3_value").alias(previous_col))
        .withColumn("effective_start", effective_ts)
        .withColumn("effective_end", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    insert_values = {c: F.col(f"s.{c}") for c in source_df.columns}
    insert_values[previous_col] = F.lit(None).cast(dict(source_df.dtypes)[scd3_col])
    insert_values["effective_start"] = effective_ts
    insert_values["effective_end"] = F.lit(None).cast("timestamp")
    insert_values["is_current"] = F.lit(True)

    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("t")
        .merge(source_df.alias("s"), key_condition)
        .whenMatchedUpdate(
            condition=scd2_change_condition,
            set={"effective_end": effective_ts, "is_current": F.lit(False)},
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    if new_versions.take(1):
        new_versions.select(spark.table(target_table).columns).write.format("delta").mode("append").saveAsTable(target_table)

    # --- Case 2: only the SCD3 attribute changed -> update current row in place --------
    # Re-read target_current_df: step above may have changed which rows are current.
    target_current_df_2 = spark.table(target_table).filter("is_current = true")
    scd2_changed_keys = scd2_changed_rows.select(*business_key_cols).distinct()

    scd3_changed_rows = (
        rows_with_changed_cols(source_df, target_current_df_2, business_key_cols, [scd3_col])
        .join(scd2_changed_keys, on=business_key_cols, how="left_anti")  # exclude ones already handled in case 1
    )

    if scd3_changed_rows.take(1):
        old_values = target_current_df_2.select(*business_key_cols, F.col(scd3_col).alias("_old_value"))
        rows_for_inplace = scd3_changed_rows.join(old_values, on=business_key_cols, how="inner")

        (
            target.alias("t")
            .merge(rows_for_inplace.alias("s"), key_condition + " AND t.is_current = true")
            .whenMatchedUpdate(set={
                previous_col: F.col("s._old_value"),
                scd3_col: F.col(f"s.{scd3_col}"),
            })
            .execute()
        )
