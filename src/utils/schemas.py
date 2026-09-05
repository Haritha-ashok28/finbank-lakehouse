"""
Expected schemas for the three CaixaBank/Kaggle source files, based on the design spec
(claude/project-2-finbank-design-spec.md) and Kaggle's column preview.

IMPORTANT: a few columns on users_data.csv and cards_data.csv were not fully visible in
Kaggle's preview when the design was written (see the "confirm remaining column names"
item in the design spec's Open/still-to-build list). These StructTypes are used as Auto
Loader SCHEMA HINTS, not a rigid contract: Auto Loader still infers/evolves the rest of
the schema, and `validate_expected_columns()` below tells you exactly what's missing or
extra the first time you actually run ingestion against the real files, instead of
silently guessing.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType, DateType, TimestampType
)

# Columns confirmed directly from the design spec / Kaggle preview.
# Anything marked CONFIRM was a "probably present, verify" column in the design doc.
CUSTOMERS_EXPECTED_COLUMNS = [
    "id",                   # customer id -> becomes client_id foreign key target
    "current_age",
    "retirement_age",
    "birth_year",
    "birth_month",
    "gender",
    "address",
    "latitude",
    "longitude",
    "per_capita_income",    # CONFIRM exact name against real header
    "yearly_income",        # CONFIRM exact name against real header
    "total_debt",           # CONFIRM exact name against real header
    "credit_score",         # CONFIRM exact name against real header
    "num_credit_cards",     # CONFIRM exact name against real header
]

CARDS_EXPECTED_COLUMNS = [
    "id",                   # card id
    "client_id",            # FK -> customers.id
    "card_brand",
    "card_type",
    "card_number",
    "expires",
    "cvv",
    "has_chip",
    "credit_limit",
    "acct_open_date",       # CONFIRM exact name against real header
    "year_pin_last_changed",# CONFIRM exact name against real header
    "card_on_dark_web",     # CONFIRM exact name against real header
]

TRANSACTIONS_EXPECTED_COLUMNS = [
    "id",
    "date",
    "client_id",            # FK -> customers.id
    "card_id",               # FK -> cards.id
    "amount",
    "use_chip",              # renamed to `channel` in Silver
    "merchant_id",
    "merchant_city",
    "merchant_state",
    "zip",                    # merchant zip -> joined to zip_centroids in Silver for geo
    "mcc",                    # FK -> mcc_codes.json description lookup
]

# Auto Loader schema hint syntax: "colname type, colname type, ...". We only pin down
# types for the columns we're confident about; unlisted columns are still inferred.
CUSTOMERS_SCHEMA_HINTS = (
    "id STRING, current_age INT, retirement_age INT, birth_year INT, birth_month INT, "
    "gender STRING, address STRING, latitude DOUBLE, longitude DOUBLE"
)

CARDS_SCHEMA_HINTS = (
    "id STRING, client_id STRING, card_brand STRING, card_type STRING, "
    "expires STRING, has_chip STRING, credit_limit STRING"
)

TRANSACTIONS_SCHEMA_HINTS = (
    "id STRING, date TIMESTAMP, client_id STRING, card_id STRING, amount STRING, "
    "use_chip STRING, merchant_id STRING, merchant_city STRING, merchant_state STRING, "
    "zip STRING, mcc STRING"
)


# Columns Auto Loader adds itself as part of its own mechanics (rescued malformed data,
# in future maybe other internal columns) rather than anything present in the source
# file. These are expected on every run and shouldn't be reported as schema drift.
AUTOLOADER_INTERNAL_COLUMNS = {"_rescued_data"}


def validate_expected_columns(actual_columns, expected_columns, source_name: str) -> dict:
    """
    Compare what Auto Loader actually inferred against what the design doc assumed.

    Returns a dict report instead of raising, so a Bronze ingestion run never fails just
    because a column name drifted -- it logs the drift loudly so you notice and go fix
    the design doc / downstream Silver code, which is the actual point of profiling.

    Auto Loader's own internal columns (AUTOLOADER_INTERNAL_COLUMNS) are excluded from
    the comparison since their presence isn't drift against the source file, it's just
    Auto Loader doing what rescuedDataColumn was configured to do.
    """
    actual_set = set(actual_columns) - AUTOLOADER_INTERNAL_COLUMNS
    expected_set = set(expected_columns)
    report = {
        "source": source_name,
        "missing_from_actual": sorted(expected_set - actual_set),
        "unexpected_in_actual": sorted(actual_set - expected_set),
        "matched": sorted(actual_set & expected_set),
    }
    if report["missing_from_actual"] or report["unexpected_in_actual"]:
        print(f"[SCHEMA DRIFT] {source_name}: expected-but-missing={report['missing_from_actual']} "
              f"actual-but-unexpected={report['unexpected_in_actual']}")
    else:
        print(f"[SCHEMA OK] {source_name}: all {len(report['matched'])} expected columns present.")
    return report
