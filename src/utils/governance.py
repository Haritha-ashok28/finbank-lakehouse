"""
Unity Catalog documentation helper: table/column comments.

Shared across every Bronze/Silver/Gold notebook so documenting a table is a one-line
call instead of a hand-written ALTER TABLE per notebook. Column masks and row filters
(the other half of Unity Catalog governance) live in
notebooks/12_governance_and_masking.py instead -- those are one-time security setup,
not something that needs to re-run on every normal pipeline execution the way table
comments do.
"""


def set_table_and_column_comments(spark, table_name: str, table_comment: str, column_comments: dict = None) -> None:
    """
    Apply a table-level COMMENT and (optionally) per-column COMMENTs via Unity Catalog.

    Safe to call on every run: COMMENT ON TABLE and ALTER TABLE ... ALTER COLUMN ...
    COMMENT are both idempotent, they just overwrite whatever comment was there before.
    """
    escaped_table_comment = table_comment.replace("'", "\\'")
    spark.sql(f"COMMENT ON TABLE {table_name} IS '{escaped_table_comment}'")
    for col, comment in (column_comments or {}).items():
        escaped = comment.replace("'", "\\'")
        spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {col} COMMENT '{escaped}'")
