# FinBank Transaction Intelligence Lakehouse

A batch and streaming fraud detection lakehouse built on Azure Databricks, modeled
around a digital bank's transaction, customer, and card data. The project covers the
full medallion architecture (Bronze/Silver/Gold), a hand-built fraud detection engine
run identically on historical and live data, a synthetic live transaction generator for
demoing fraud scenarios on demand, and Unity Catalog governance over the whole thing.

Full architecture diagrams, personas, and data model are in
[`docs/design-spec.md`](docs/design-spec.md).

<!-- Optional but strong: a hero screenshot of the AI/BI dashboard or a GIF of a fraud
     scenario firing live goes here once you have one. A visual at the top of a data
     project README does more work than any amount of text. -->

## Results

<!-- Fill these in from your own completed run -- these are the numbers that make a
     recruiter stop scrolling, don't leave them as placeholders in what you actually publish. -->

- Processed **[X] million** historical transactions across **[X]** customers and **[X]** cards
- Custom fraud engine flagged **[X]%** of historical transactions across 4 independent rules
- Live generator sustains **[X]** synthetic transactions/second with sub-[X]-second fraud scoring latency
- [Link to the published AI/BI dashboard, if shared publicly or via screenshot]

## Highlights

- **Custom fraud detection engine, not a borrowed label.** The dataset ships its own
  fraud flags, deliberately not used as ground truth since there's no way to verify what
  logic produced them. Instead, four rules (amount threshold, card velocity, impossible
  travel speed via geolocation, spending deviation from a rolling average) are built
  from scratch and applied identically to batch and streaming data.
- **One rule engine, two execution strategies.** The same fraud logic runs as Spark
  window functions in batch and as stateful Structured Streaming (`applyInPandasWithState`)
  for live data, since a streaming micro-batch can't "look back" at history the way a
  batch job over a full table can.
- **A hybrid SCD2 + SCD3 dimension.** The Customers table tracks income tier with full
  row-versioned history (SCD2) and address with a single previous-value column (SCD3) in
  the same table, a pattern most tutorials skip because it's genuinely harder to get
  right than a single-strategy SCD dimension.
- **A live transaction generator seeded from real identities.** Built with dbldatagen
  and seeded from real customer/card/merchant IDs already in the batch dimensions
  (rather than inventing its own), so specific fraud scenarios can be triggered on
  demand for a demo instead of hoping the historical data happens to contain one.
- **Unit tested where it matters.** The SCD change-detection logic and all four fraud
  rules are covered by local PySpark unit tests, no cluster required. One of them caught
  a real bug during development (see Testing below).
- **Governed end to end.** Unity Catalog external locations, a managed-identity storage
  credential, and [row filters / column masks once implemented] enforce the access
  matrix in `docs/design-spec.md` rather than relying on notebook-level discipline.

## Architecture

Two pipelines share the same Silver dimensions and the same fraud rule logic:

- **Batch**: Auto Loader ingests three source files into Bronze, Silver applies
  SCD1/SCD2/SCD3 merges and builds the Transactions fact table, fraud scoring runs as a
  batch step, Gold aggregates the results for reporting.
- **Streaming**: a live transaction generator writes to a Delta landing table, which
  Structured Streaming reads incrementally, joins against small reference tables
  refreshed from batch, and scores in real time with stateful processing for the two
  rules that need recent history.

<!-- Export the two architecture diagrams from your design-spec artifacts as PNGs and
     embed them here, e.g.:
     ![Batch architecture](docs/images/batch-architecture.png)
     ![Streaming architecture](docs/images/streaming-architecture.png)
     A picture here saves a reviewer from reading the data model to understand the shape
     of the system. -->

See `docs/design-spec.md` for the full diagrams, the security/governance matrix, and the
four consumer personas this is designed around (fraud analyst, relationship manager,
compliance auditor, business/dashboard user).

## Tech stack

Azure Databricks, Unity Catalog, ADLS Gen2, Delta Lake, PySpark (batch + Structured
Streaming), Auto Loader, dbldatagen, Databricks AI/BI Dashboards, pytest, GitHub Actions.

## Data source

[Financial Transactions Dataset](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets)
(CaixaBank Tech, 2024 AI Hackathon, Apache 2.0), roughly 2,000 customers, 6,000 cards,
and 24 million transactions from 2010-2019, with separate linked Customer, Card, and
Transaction tables rather than one flat file.

## Repo structure

```
data/reference/                 zip_centroids.csv: ~42,800 US ZIP codes with lat/long,
                                 built offline for the geo-jump fraud rule
docs/
  design-spec.md                full architecture, data model, and security design
  images/                       architecture diagrams, dashboard screenshots
notebooks/                      Databricks notebooks, run in numeric order (01 -> 11)
src/
  utils/config.py               environment settings (storage account, catalog, thresholds)
  utils/schemas.py              expected source schemas + drift detection
  ingestion/bronze_common.py    shared Auto Loader ingestion function
  silver/scd_utils.py           SCD1 / SCD2 / SCD3 merge patterns
  silver/fraud_rules.py         the 4 fraud rules (batch)
  streaming/generator.py        dbldatagen live transaction generator
  streaming/streaming_fraud_scoring.py   stateful velocity/geo-jump scoring
tests/                          pytest unit tests, run locally, no cluster needed
.github/workflows/              CI: runs pytest on every push
```

## Infrastructure

Provisioned on Azure:

- Resource group, ADLS Gen2 storage account (`raw` and `checkpoints` containers)
- Azure Databricks workspace (Premium tier, for Unity Catalog)
- Unity Catalog metastore, storage credential via a managed-identity Access Connector,
  and External Locations over both containers
- A `finbank` catalog with `bronze` / `silver` / `gold` schemas

## Getting started

Clone the repo, set `storage_account` in `src/utils/config.py` to your own storage
account, import the repo into Databricks as a Repo, and run the notebooks in order:

1. `01`-`03`: Bronze ingestion (Auto Loader) for customers, cards, transactions
2. `04`-`07`: Silver dimensions (customers, cards, merchants, ZIP geo reference)
3. `08`: Transactions fact table + batch fraud scoring
4. `09`: Gold aggregates, feeds the AI/BI dashboard layer
5. `10`: live transaction generator (run as a Databricks Job for a continuous demo)
6. `11`: streaming ingestion and stateful fraud scoring

Raw files land in `raw/customers/`, `raw/cards/`, `raw/transactions/` (folders, not
fixed filenames, since Auto Loader watches a folder for incoming files), with
`mcc_codes.json` and the ZIP reference table as fixed single files alongside them.

## Testing

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest tests/ -v
```

13 tests covering the two pieces of logic most worth trusting before they touch real
data: SCD change detection (5 tests) and the fraud rule engine (8 tests, checked against
real coordinates and timestamps, for example confirming the haversine distance from New
York to Los Angeles comes out to roughly 3,936 km). These run locally with plain PySpark,
no cluster or Delta runtime needed, and run automatically on every push via GitHub
Actions (see `.github/workflows/`).

Writing the NULL-handling test for SCD change detection caught a real bug during
development: the initial implementation used `<>` for comparing old and new values,
which silently misses a change from `NULL` to an actual value (`NULL <> 5` evaluates to
`NULL`, not `TRUE`, in SQL). Switching to `IS DISTINCT FROM` fixed it, and the test now
guards against it regressing.

## Design decisions

- **Structured Streaming over a Delta table, not Azure Event Hubs.** The live generator
  writes to a Delta table and the streaming job reads it incrementally, rather than
  standing up an Event Hubs namespace. Structured Streaming doesn't care which one feeds
  it, so the fraud-scoring logic is identical either way, and this keeps the project on
  Databricks-native primitives instead of an extra managed service for a project at this
  scale.
- **A hybrid SCD2 + SCD3 customer dimension.** `income_tier` gets full row-versioned
  history, `address` gets a single previous-value column, in the same table. This is the
  most structurally involved code in the repo (`scd2_with_scd3_merge` in
  `src/silver/scd_utils.py`); its docstring explains the two update paths in detail.
- **No borrowed fraud label.** Both the dataset's own label and historical replay as a
  validation technique were dropped in favor of rules built from well-known fraud
  patterns, applied consistently across batch and streaming.
- **`income_tier` is derived, not sourced.** It doesn't exist in the raw file; it's
  bucketed from `yearly_income` with boundaries tuned against the real income
  distribution once profiled.

## Future enhancements

- Row filters and column masks enforcing the full access matrix in `docs/design-spec.md`
  per persona, not just documented
- Swap the Delta-table streaming transport for Azure Event Hubs if a real message broker
  becomes worth the added operational surface
- Extend the fraud engine with a model-based anomaly score alongside the rule-based flags

## License

Code in this repository is licensed under the MIT License (see `LICENSE`). The
underlying dataset is separately licensed by its publisher under Apache 2.0.
