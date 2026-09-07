"""
Shared column-cleaning helpers used across multiple Silver notebooks.
"""

from pyspark.sql import functions as F
from pyspark.sql.column import Column


def parse_currency(col_name: str) -> Column:
    """
    Parse a currency-formatted string column into a nullable double.

    The CaixaBank/Kaggle source stores every dollar-figure column (yearly_income,
    per_capita_income, total_debt, credit_limit, transaction amount) as a string with a
    literal "$" prefix and sometimes thousands separators, e.g. "$59,696.00" or
    "$59696". Casting one of these directly with .cast("double") throws under Spark's
    ANSI mode the moment it hits a real value instead of a bare number.

    Strips "$" and "," first, then uses try_cast (not cast) so a value that still can't
    parse after cleanup becomes NULL instead of failing the whole job. NULLs are not
    silently swallowed -- each call site below either buckets them explicitly or counts
    and warns on them, the same pattern this repo already uses for the merchant-geo
    join in notebook 08.
    """
    return F.expr(f"try_cast(regexp_replace({col_name}, '[$,]', '') as double)")


def clean_zip(col_name: str) -> Column:
    """
    Normalize a ZIP code column into a consistent 5-digit zero-padded string.

    Two different data sources lose leading zeros the same way: pandas silently
    upcasts an int column to float the moment it contains any missing value, so a
    transaction's ZIP shows up as the string "2134.0" instead of "02134"; separately,
    Spark's own CSV schema inference on zip_centroids.csv reads "00501" as the integer
    501. Routing through try_cast-to-double handles both shapes (a bare int string and
    a ".0"-suffixed one), and zero-padding back to 5 characters is safe here because
    every ZIP code in this dataset is a standard 5-digit US ZIP.
    """
    as_number = F.expr(f"try_cast({col_name} as double)")
    return F.lpad(as_number.cast("bigint").cast("string"), 5, "0")