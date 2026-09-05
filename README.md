# FinBank Transaction Intelligence Lakehouse

Batch + streaming fraud/transaction/customer analytics platform on Azure Databricks,
built around a fictional Canadian digital bank ("FinBank"). Project 2 of a 5-project
Azure Databricks bootcamp portfolio. Full narrative design (personas, architecture
diagrams, security matrix) is in [`docs/design-spec.md`](docs/design-spec.md) — this
README is about running what's actually in this repo.

## Before you run anything: read this section

This repo was scaffolded end-to-end in a sandbox with **no Databricks cluster, no Azure
resources, and no network access to Kaggle**. Everything here was built to be correct by
careful reasoning and, wherever it was actually possible without a cluster, by real local
tests — not by guessing and hoping. Concretely, here's what was and wasn't verified, so
you know where to spend your own testing time first:

| Component | How it was verified |
|---|---|
| ZIP centroid reference table | **Real data.** ~42,800 US ZIP codes with lat/long, built from the offline `zipcodes` PyPI package. |
| SCD1 / SCD2 / SCD3 change-detection logic | **Unit tested locally** (`tests/test_scd_logic.py`, 5/5 passing) with a real local PySpark session. Caught and fixed a real NULL-handling bug (`<>` vs `IS DISTINCT FROM`) this way. The actual Delta `MERGE INTO` calls were NOT run against a live Delta table — no cluster available. |
| Fraud rules (amount, velocity, geo-jump, spending deviation) | **Unit tested locally** (`tests/test_fraud_rules.py`, 8/8 passing) against real timestamps and coordinates (e.g. confirmed the haversine distance NYC->LA is ~3936km, confirmed the velocity rule fires exactly on the 3rd transaction in 5 minutes). |
| dbldatagen live generator | Column spec (`values`, `weights`, `expr`, `distribution`, streaming `rowsPerSecond`) confirmed against the installed dbldatagen 0.4.0 source and its schema builds correctly. Actually generating rows failed in this sandbox due to a PyArrow/JDK21 incompatibility unrelated to Databricks (Databricks runtimes ship a tested JDK/PyArrow combination) — **run `display(build_organic_stream(...))` for a few seconds as your first step on your real cluster.** |
| Stateful streaming fraud scoring (`applyInPandasWithState`) | API signature confirmed against the installed PySpark 3.5.3 source. Reasoned through carefully (state pruning, timeouts, first-transaction edge case) but **not executed end-to-end** — no live stream to drive it through here. Test with 2-3 transactions on one card first, same idea as the batch velocity unit test. |
| Bronze Auto Loader ingestion | Structurally correct Auto Loader usage; **not run against the real CSVs**, since they live on Kaggle and weren't downloaded in this sandbox. |

None of this is a reason to distrust the repo — it's the opposite: everything that could
be checked without a cluster, was, including one real bug fix. But you should not assume
the untested pieces are bug-free just because they compile-check as reasonable. Budget
your first hour on the real cluster for exactly the "not verified" rows above.

## Repo structure

```
config/                      (currently unused -- see src/utils/config.py instead)
data/reference/               zip_centroids.csv (real, offline-built US ZIP -> lat/long)
docs/design-spec.md            full narrative design doc
notebooks/                     Databricks notebooks, run in numeric order (01 -> 11)
src/
  utils/config.py              all environment-specific settings (fill in your Azure info here)
  utils/schemas.py              expected source-file schemas + drift detector
  ingestion/bronze_common.py    shared Auto Loader ingestion function
  silver/scd_utils.py           SCD1 / SCD2 / SCD3 merge patterns
  silver/fraud_rules.py         the 4 fraud rules (batch versions)
  streaming/generator.py        dbldatagen live transaction generator
  streaming/streaming_fraud_scoring.py   stateful velocity/geo-jump scoring
tests/                          pytest unit tests, run locally (no cluster needed)
requirements-dev.txt            local test dependencies only
```

## Setup

1. **Fill in `src/utils/config.py`**: replace `storage_account` with your real ADLS Gen2
   storage account name (everything else has a sensible default). This is the only file
   that needs editing before running anything.
2. **Create the Azure resources** (not automated here — do this in the Azure Portal or
   your own IaC): a storage account with `raw` and `checkpoints` containers, a Unity
   Catalog metastore + External Location pointing at the storage account, a Databricks
   workspace attached to that metastore.
3. **Get the data**: download the CaixaBank/Kaggle dataset yourself (this sandbox had no
   Kaggle credentials and no network path to kaggle.com) —
   [kaggle.com/datasets/computingvictor/transactions-fraud-datasets](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets).
   Upload into your `raw` container using this exact folder layout:

   ```
   raw/
     customers/users_data.csv
     cards/cards_data.csv
     transactions/transactions_data.csv
     mcc_codes.json                        (fixed file, not a folder)
     reference/zip_centroids.csv           (from this repo's data/reference/)
   ```

   The 3 high-volume sources are FOLDERS, not fixed filenames, on purpose: Auto Loader
   watches a folder for incoming files, so this is also what lets you test incremental
   loading for real -- upload `users_data.csv` alone first, run notebook `01`, then drop
   a second file (different filename, e.g. `users_data_part2.csv`, header row included)
   into the same `customers/` folder and re-run `01`. The Bronze table's row count
   should grow by exactly the second file's row count, not double or stay flat -- that's
   the proof Auto Loader picked up only the new file via its checkpoint, rather than
   reprocessing everything. `mcc_codes.json` and `zip_centroids.csv` stay fixed single
   files since they're read once with a plain batch read, not watched incrementally.
4. **Import this repo as a Databricks Repo** (Repos need Git — push this to your own
   GitHub first) so the `notebooks/*.py` files open as real notebooks and `src/` imports
   correctly via the `sys.path.append("../")` pattern each notebook uses.

## Build order

Run notebooks in numeric order. Each one's docstring says what it needs to have run
first; the short version:

1. `01`-`03` — Bronze ingestion (Auto Loader) for customers, cards, transactions
2. `04`-`07` — Silver dimensions (customers, cards, merchants, ZIP geo reference)
3. `08` — Silver transactions fact table + batch fraud scoring (also builds the customer
   spending reference table the streaming path needs)
4. `09` — Gold aggregates for Power BI
5. `10` — start the live generator (run this as a Databricks Job, not blocking in a
   notebook cell, if you want it running continuously during a demo)
6. `11` — streaming ingestion + stateful fraud scoring

Notebooks `03` and `08` accept a `LIMIT_ROWS_FOR_DEV` widget so you can iterate against a
small slice of the ~24M-row transactions file instead of the full history while you're
still debugging Silver logic.

## Running the tests

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest tests/ -v
```

13 tests, all passing as of this repo (5 SCD logic, 8 fraud rules). These run with a
local Spark session and no Delta/cluster dependency, so they're fast and safe to run
before every push.

## Design decisions worth knowing about before you extend this

- **Event Hubs vs. Delta landing table**: the design doc's architecture diagram shows
  Event Hubs as the streaming transport. This repo's generator (`10_streaming_generator_job.py`)
  writes to a Delta table instead, and the ingestion notebook (`11_...`) reads that same
  Delta table as a stream. This is simpler to stand up and test, and Structured Streaming
  doesn't care which one feeds it — the fraud-scoring logic is identical either way. If
  you want the diagram to match the code exactly, swap the `write` in `generator.py` for
  an Event Hubs producer (`azure-eventhub` SDK) and the `readStream` in notebook `11` for
  the Event Hubs Structured Streaming connector.
- **Customers is a hybrid SCD2+SCD3 dimension** (`income_tier` gets full row-versioned
  history, `address` gets a single previous-value column). This is the most structurally
  complex code in the repo (`scd2_with_scd3_merge` in `src/silver/scd_utils.py`) — read
  its docstring before modifying it, and re-run `tests/test_scd_logic.py` after any change.
- **`income_tier` doesn't exist in the raw file** — it's derived from `yearly_income` with
  placeholder bucket boundaries (`04_silver_customers.py`). Adjust once you've seen the
  real income distribution from your own profiling pass.

## What's still open

From the design spec's original list, still genuinely unstarted:

- Confirm the uncertain column names in `users_data.csv` / `cards_data.csv` against the
  real downloaded files (`src/utils/schemas.py`'s `validate_expected_columns` will tell
  you the first time you run Bronze ingestion for real — check the driver logs)
- Decide the actual filtering strategy for the 24M-row transactions file for a
  bootcamp-scale build (the `LIMIT_ROWS_FOR_DEV` widget is a dev-iteration convenience,
  not a real filtering decision)
- Power BI report build on top of the Gold tables
- GitHub repo push + CI (this repo isn't a git repo yet — see below)
- Regenerate `FinBank-Project-Brief.docx` against the current dataset

## Git

This directory isn't an initialized git repo yet. To push it:

```bash
git init
git add .
git commit -m "Initial FinBank lakehouse scaffold"
git remote add origin <your-github-repo-url>
git push -u origin main
```
