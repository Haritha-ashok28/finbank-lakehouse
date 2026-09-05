# Project 2: FinBank Transaction Intelligence Lakehouse, Design Spec

Full design spec (rich reference version): https://claude.ai/code/artifact/caa7878c-8bd7-4034-b65b-bbccfdbd9b9f
Plain-language project brief (name, goal, domain, personas, objectives, datasets, architecture): https://claude.ai/code/artifact/f60638fd-d71f-40c5-9954-400b8ea5997d
Both artifacts show the batch and streaming pipelines as two separate, icon-based architecture diagrams rather than one combined diagram, for readability. An earlier version of the brief was delivered as FinBank-Project-Brief.docx (based on the Sparkov-era design, below) — not yet regenerated against the current dataset, offer to do so if she wants the docx to match.

## Summary

Batch + streaming fraud, transaction, and customer analytics platform on Azure Databricks, built around a fictional Canadian digital bank ("FinBank"). This is Project 2 of a 5-project Azure Databricks bootcamp portfolio (Project 1 was Agri/Food, pure batch, built on Synapse). Base dataset: the Financial Transactions Dataset published on Kaggle by CaixaBank Tech for their 2024 AI Hackathon (Apache 2.0 license, computingvictor/transactions-fraud-datasets) — chosen over the earlier Sparkov pick specifically because it ships proper separate, linked Customer and Card tables rather than one flat file needing synthesized entities. Streaming leg is a hybrid design: real customer/card/merchant identities ground the batch dimension tables, a live event generator (dbldatagen) seeded from those same IDs produces the live event stream, so specific fraud scenarios can be demonstrated on demand rather than hoped for in the source file.

## Dataset switch: Sparkov -> CaixaBank/Kaggle

The project started on Sparkov (a single flat credit-card-transaction file, 555,720 rows / 21 columns), which required synthesizing Account and Card as entities by hand since Sparkov only provided customers, merchants, and transactions. Partway through, evaluated the Kaggle dataset at kaggle.com/datasets/computingvictor/transactions-fraud-datasets and switched to it because it already ships as separate, real, linked tables:

- **users_data.csv** (Customer, 2,000 rows, 14 columns): current_age, retirement_age, birth_year/month, gender, address, latitude, longitude, per_capita_income, plus a few income/credit columns to confirm against the file header
- **cards_data.csv** (Card, ~6,146 rows, 13 columns): id, client_id (links to Customer), card_brand, card_type, card_number, expires, cvv, has_chip, credit_limit, plus a few issue/status columns to confirm
- **transactions_data.csv** (Transaction, ~24 million rows, 12 columns, 2010-2019): id, date, client_id, card_id, amount, use_chip, merchant_id, merchant_city, merchant_state, zip, plus an mcc column and an errors flag not used here. 24M rows is far more than a bootcamp build needs; plan to filter to a recent window or a subset of customers.
- **mcc_codes.json**: merchant category code -> description lookup, used to build the Merchant dimension alongside merchant_id/city/state/zip from the transaction file
- **train_fraud_labels.json**: a real fraud/not-fraud label per transaction, intentionally not used as ground truth (same reasoning as Sparkov's is_fraud, see Fraud rule engine below)

Consequences of the switch, carried into both published artifacts:
- **Account entity dropped entirely.** Card links straight to Customer via client_id, so the extra synthesized layer isn't needed.
- **channel is no longer synthesized.** The file's own use_chip column (Swipe Transaction / Chip Transaction / Online Transaction) is renamed to channel instead.
- **Merchant geo is derived, not given.** Customers carry a real latitude/longitude; merchants only carry a ZIP code. A small public US ZIP-code centroid reference table is joined in Silver to approximate merchant location for the geo-jump fraud rule, on both batch and streaming paths.
- **Batch architecture diagram simplified.** With nothing left to synthesize, all three source files (Customer, Card, Transaction) now land through the same External Location + Auto Loader path — no more special-cased bypass arrow into Bronze.

## Data model and SCD assignments (current, post-switch)

- Customers (users_data.csv, one row per customer already, no grouping needed): address -> SCD3, income_tier -> SCD2
- Cards (cards_data.csv, real, 1 customer -> many cards via client_id): whole entity -> SCD2 (status, credit_limit, type tracked with full history)
- Merchants (transactions_data.csv, deduplicated on merchant_id, category joined from mcc_codes.json): SCD1
- Merchant Geo Reference (public US ZIP-centroid table, zip -> latitude/longitude, loaded once): SCD1
- Transactions (id, date, client_id, card_id, amount, channel [from use_chip], merchant_id, merchant_zip): fact table, ~24 million rows before any filtering. train_fraud_labels.json exists but is intentionally not used (see Fraud rule engine below).
- Fraud/Risk: separate fact table (fraud_flag, risk_score, risk_reason, investigation_status), populated entirely by her own rule engine, run the same way on batch and streaming data

No Account entity and no balance-as-SCD-attribute question anymore; anything account-level (running balance, utilization) is a computed measure derived from the Transactions fact table in Gold, not a dimension attribute.

## Live event generator: dbldatagen

The streaming leg's generator is built with dbldatagen (Databricks Labs' synthetic data library) rather than a raw hand-rolled Python script. It's an actual Databricks-native tool: you define a schema (columns, types, value ranges, distributions) declaratively and it produces rows at a controlled rate, including writing into a streaming source. Considered alternatives were Plaid Sandbox and Stripe test mode (both can fire real webhook events), but both would add integration complexity (signup, API keys, webhook plumbing) unrelated to the Databricks skills this project demonstrates, and neither lets her script specific fraud scenarios against her own customer/card/merchant IDs as easily as controlling her own generator does. The generator now seeds from the real client_id/card_id/merchant_id values in the CaixaBank/Kaggle tables (previously Sparkov's cc_num-derived IDs).

## Fraud rule engine (own rules, no borrowed label)

The dataset's own fraud label (train_fraud_labels.json now, was Sparkov's is_fraud) is not used as ground truth: it reflects whatever logic its creator built in, which nobody outside that creator actually knows, so checking rules against it would only prove agreement with a stranger's arbitrary definition, not real accuracy. Instead fraud_flag is built from scratch using well-known real-world fraud patterns, applied the same way to historical and live data. Historical replay as a validation technique was dropped along with the borrowed label.

1. Amount > $2,000 -> high risk
2. 3+ transactions on one card within 5 minutes -> suspicious (velocity)
3. Consecutive transactions farther apart than plausible travel time allows -> suspicious (impossible travel / geo-jump), using the customer's real latitude/longitude and the merchant's ZIP-centroid location
4. Amount > 3x customer's trailing 30-day average -> suspicious (spending deviation)

Both paths run the same four rules, but the mechanics differ because the data arrives differently:
- **Batch (historical data):** runs as a normal batch step during Silver processing. All the history a rule needs (e.g. a customer's last five minutes of activity) is already sitting in the table.
- **Streaming (live data):** runs as each transaction arrives, via Structured Streaming during Silver processing. Rules 2 and 3 (velocity, geo-jump) use stateful processing — a short in-memory window of recent activity per customer/card. Rules 3 and 4 also join small reference tables (merchant ZIP centroid; customer's rolling 30-day average), refreshed regularly from batch rather than computed live. Gold only aggregates the results; scoring does not happen in Gold.

## Security & governance (Unity Catalog row filters + column masks)

- Fraud & Risk Analyst: Transactions, Fraud/Risk, Customers, Merchants; no row filter; card_number masked to last 4, birth_year masked/generalized
- Relationship Manager: Customers/Cards filtered to assigned region; card_number masked to last 4
- Compliance/Auditor: full access, logged, no masking
- Power BI/business users: Gold layer only, no raw PII

## Databricks features mapped to the pipeline

Unity Catalog, External Location (ADLS Gen2), Auto Loader (batch ingestion of all three source files), Structured Streaming (Event Hubs consumption, with stateful processing for velocity/geo-jump rules), dbldatagen (live event generation), MERGE/upsert (SCD2/SCD3 on Customers and Cards), Delta time travel + DESCRIBE HISTORY, OPTIMIZE + Z-ORDER, VACUUM, Databricks secret scopes, UC row filters/column masks, GitHub-linked Databricks Repos.

## Bootcamp spec compliance

Base spec only required Bronze/Silver/Gold + Power BI reporting; this design exceeds it while still covering the required Power BI/SQL endpoint step explicitly (a gap identified in the original inspiration doc, which stopped at Gold).

## Open / still to build (as of design phase)

Confirm the remaining column names for users_data.csv and cards_data.csv against the actual downloaded file headers (a few income/credit and issue/status columns respectively weren't visible in Kaggle's column preview); confirm exact row counts; decide the filtering strategy to bring ~24M transaction rows down to a manageable bootcamp-scale build; source or build the public US ZIP-centroid reference table; missing-value and dtype profiling (deferred to implementation phase per her own call); the live transaction generator itself, built with dbldatagen; Power BI report build; GitHub repo structure and CI; regenerate FinBank-Project-Brief.docx against the current dataset if she wants an updated download.

---

**Status as of this repo (see root README.md): the items above have moved from "open" into
actual code.** The ZIP-centroid reference table is built and included
(`data/reference/zip_centroids.csv`). Bronze/Silver/Gold notebooks, the fraud engine, the
dbldatagen generator, and the streaming scoring job are all written. Column-name and
row-count confirmation against the real Kaggle files, and the Power BI report build, are
still open — see the root README's "What's still open" section for the up-to-date list.
