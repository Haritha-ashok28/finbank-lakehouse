"""
Shared data-quality check helpers used across Bronze and Silver notebooks.

Written once instead of hand-rolled per notebook -- notebook 08's Transactions fact
table (the one table every fraud rule and every Gold aggregate joins against) nearly
shipped with zero referential-integrity checks despite the exact same pattern already
existing for Cards-vs-Customers in notebook 05, simply because there was no one-line
way to reuse it.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def check_orphan_keys(
    df: DataFrame,
    fk_col: str,
    ref_df: DataFrame,
    ref_key_col: str,
    label: str,
) -> int:
    """
    Count rows in `df` whose `fk_col` has no matching `ref_key_col` value in `ref_df`,
    print a [DATA QUALITY WARNING] if any exist, and return the count so a caller can
    log or act on it further.

    Does not fail the job by itself -- an orphan key is a signal to investigate, not
    automatically a reason to stop the whole pipeline (matches the pattern already
    established by the Cards-vs-Customers check in notebook 05).
    """
    ref_keys = ref_df.select(F.col(ref_key_col).alias("_ref_key")).distinct()
    orphans = df.join(ref_keys, df[fk_col] == F.col("_ref_key"), "left_anti")
    orphan_count = orphans.count()
    if orphan_count > 0:
        print(f"[DATA QUALITY WARNING] {orphan_count} {label} reference a {fk_col} with no matching row.")
    return orphan_count


def null_count_report(df: DataFrame, cols: list = None) -> DataFrame:
    """
    Per-column null counts as a single-row DataFrame, ready for display(). Cheap to run
    on every load; catches upstream data quality regressions before they reach Gold.
    Defaults to every column in `df` if `cols` isn't given.
    """
    cols = cols or df.columns
    return df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols])


def row_count_sanity_check(df: DataFrame, label: str, min_expected: int = 1) -> int:
    """
    Print and return the row count for `df`. Raises if the count is below
    `min_expected` -- a Bronze load that silently produces zero rows (a bad upload, a
    misconfigured folder path, a stalled stream) should stop the pipeline immediately
    rather than quietly flow an empty table downstream, where it looks like "no data
    yet" instead of "something is actually broken."
    """
    count = df.count()
    print(f"[ROW COUNT] {label}: {count}")
    if count < min_expected:
        raise ValueError(f"[ROW COUNT FAILURE] {label} has {count} rows, expected at least {min_expected}.")
    return count


def rescued_data_check(df: DataFrame, label: str, rescued_col: str = "_rescued_data") -> int:
    """
    Count rows where Auto Loader's rescuedDataColumn actually caught something (a
    malformed row, an unexpected type, an extra column). Enabling rescuedDataColumn
    keeps these rows instead of silently dropping them, but that's only useful if
    something actually looks at the column -- this makes that check a one-liner per
    notebook instead of a thing everyone means to add later.
    """
    if rescued_col not in df.columns:
        return 0
    rescued_count = df.filter(F.col(rescued_col).isNotNull()).count()
    if rescued_count > 0:
        print(f"[DATA QUALITY WARNING] {label}: {rescued_count} rows had data rescued into "
              f"{rescued_col} (malformed/unexpected input). Inspect before trusting downstream Silver output.")
    return rescued_count
