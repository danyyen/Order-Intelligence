
# Order Intelligence Pipeline

Turns raw legacy ERP exports containing sensitive customer, product, and employee data into privacy-safe, analytics-ready datasets — built because the source system has no API, direct database connection, or automated extraction.

A local-first Python pipeline that builds stable shared identities, pseudonymizes sensitive fields, validates order history, open orders, and inventory, and lands only approved batches in Amazon S3.

**Production-scale validation:** ~563K records processed end-to-end in approximately 2 minutes, with 27 automated tests covering ingestion, orchestration, mapping integrity, rollback behaviour, and regression scenarios.

## Business Flow Infographic
Legacy ERP
(Excel exports)
        │
        ▼
Python Pipeline
(Standardize + Pseudonymize + Validate)
        │
        ▼
Approved S3 Dataset
        │
        ▼
Snowflake (planned)
        │
        ▼
Power BI / ML (planned)

Outputs

✓ Clean data
✓ Privacy-safe
✓ Shared identities
✓ Analytics-ready

## Highlights

- **3 complete data pipelines** — order history, open orders, and inventory
- **~563K records processed** in the largest production-scale validation run
- **27 automated tests** covering orchestration, ingestion idempotency, mapping integrity, rollback behavior, and regression scenarios
- **Stable shared identities** for customers, products, and SKUs across datasets and pipeline runs
- **Privacy before cloud** — direct customer, product, and employee identifiers are removed before S3 upload
- **Failure-isolated dataset tracks** — one dataset failure does not prevent independent datasets from completing
- **Quality-authorized S3 landing** — failed or mismatched quality reports cannot authorize an upload
- **No confidential business data** committed to the repository

### Pipeline Run





**What's in this repo:** pipeline code, configuration, documentation, automated tests, and screenshots from a real run. Runtime data, identity mappings, logs, Excel exports, CSV outputs, quality reports, and credentials are excluded by `.gitignore`.

## Status

| Piece | Status |
|---|---|
| Shared identity foundation | Complete |
| Order history pipeline | Complete |
| Open orders pipeline | Complete |
| Inventory snapshot pipeline | Complete |
| Mapping-integrity validation | Complete |
| Privacy validation and quality gates | Complete |
| S3 landing | Complete |
| Automated tests | Complete — 27 tests |
| CI | Not configured yet |
| Snowflake / dbt | Planned |
| Airflow | Planned |
| BI / ML | Planned |

## Why this exists

Order history, open orders, and inventory all come from the same legacy ERP. Extracting the data requires manually refreshed Excel exports because there is no API, direct database connection, or scheduler. Those exports contain real customer names, product descriptions, SKU codes, and employee usernames next to operational fields such as order amounts, dates, routes, quantities, and warehouse positions.

The goal is to make that data usable for analytics without allowing direct customer and product identifiers to flow into the cloud. The pipeline establishes stable pseudonymous identities locally, removes the raw identifiers, validates the result, and only then allows an approved batch to reach S3.

The stages are deliberately file-based while the business rules are being proven. The filesystem is the interface between stages today; when the warehouse layer is introduced, those boundaries can become tables and scheduled tasks instead.

This repository is the landing-zone layer: ingest reliably, standardize inconsistent source columns, preserve identities across runs, block unsafe or malformed outputs, and upload only batches that passed their checks. Snowflake, dbt models, dashboards, forecasting, and production scheduling are later layers, not features claimed here today.

For the reasoning behind specific design choices, known tradeoffs, and issues discovered during development, see **[docs/DECISIONS.md](docs/DECISIONS.md)**.

## Production-Scale Validation

The pipeline was validated end-to-end against real operational data volumes during development.

| Dataset | Largest development run |
|---|---:|
| Order History | ~553,000 rows |
| Open Orders | ~4,500 rows |
| Inventory | ~5,800 rows |
| Products | 947 |
| Customers | 1,770 |

The full run processed approximately **563,000 operational records in 124.07 seconds**, including pseudonymization, validation, quality gating, and Amazon S3 delivery.

Production data is not included in this repository. Automated tests create temporary synthetic fixtures at runtime.


## Proof of a Real Run

![Full pipeline run](docs/images/result_output.jpg)

*Production-scale execution of the complete pipeline across order history, open orders, and inventory.*

![Before and after comparison](docs/images/column_comparison_before_and_after.jpg)

*Raw source fields compared with the pseudonymized output produced by the pipeline.*

![S3 landing result](docs/images/s3_landing.jpg)

*Quality-approved pseudonymized datasets and matching quality reports landed in Amazon S3.*

More screenshots, including the stage status table, quality report, source exports, and before/after column comparison, are available in [docs/images](docs/images).


## Architecture

![Pipeline architecture diagram](docs/images/architecture_1.jpg)

The current execution model has one shared foundation followed by three dataset tracks:

```text
shared foundation
├── ingest and standardize order history
├── profile and map customers
├── profile and map products
├── assign stable SKU identities
└── validate both shared mapping files
        │
        ├── order history
        │   └── pseudonymize → validate → quality gate → S3
        │
        ├── open orders
        │   └── ingest → standardize → backfill missing products/customers
        │       → pseudonymize → validate → quality gate → S3
        │
        └── inventory
            └── ingest → standardize → backfill missing products
                → pseudonymize → validate → quality gate → S3
```

### Why order history builds the foundation

The source system does not provide clean master customer, product, or SKU tables. Order history is therefore used to seed the shared identity mappings. Existing pseudo identifiers are preserved across runs, and only previously unseen entities receive new identifiers.

Open orders can contain a customer or product that has not reached order history yet. Inventory can contain a product that has never been ordered. Their backfill stages add only those missing entities to the existing shared mapping before pseudonymization. Open orders and inventory use an explicit `Unknown Category` pseudonymous bucket when their sources do not provide a trustworthy product category.

The shared mappings are validated before any dataset track consumes them. The validator checks required columns, null identifiers, duplicate join keys, split identities, pseudo-identifier collisions, and expected pseudonym formats.

### Failure isolation

A critical failure in the shared foundation stops the entire run because none of the dataset tracks can safely trust incomplete or corrupted mappings.

After the foundation succeeds, each dataset is isolated:

- An order-history failure stops only the order-history track.
- An open-orders failure stops only the open-orders track.
- An inventory failure stops only the inventory track.
- A failed validation or quality gate prevents that dataset's upload.
- An S3 failure does not stop the remaining dataset tracks.

Tracks execute sequentially because open orders and inventory can update the same local product mapping file. They are failure-isolated, but they are not run concurrently.

The run report records each track as `completed` or `failed`, and summarizes the whole run as:

- `completed`: every selected dataset track succeeded
- `partially_completed`: at least one selected dataset succeeded and at least one failed
- `failed`: the shared foundation failed, or every selected dataset track failed

Every stage remains a standalone Python script. `run_pipeline.py` orchestrates them as subprocesses, captures their output in a timestamped log, and writes a structured JSON run report. It is intentionally a lightweight bridge toward a future scheduler, not a replacement for Airflow.

## Privacy boundary

Worth being precise about what “pseudonymized” means here:

- Customer names, product descriptions, and SKU codes are replaced with stable fake identifiers from the shared local mappings.
- `source_customer_code`, `ship_to_customer_code`, mapping helper columns, and employee usernames are removed before final order outputs are written.
- Inventory's raw SKU is removed before its final output is written.
- `company_code` remains as a non-personal operational field required by the current output contract and quality gates.
- The mapping files contain the relationship between real and pseudo identities. They remain local, are excluded from Git, and are never uploaded to S3.
- This is pseudonymization, not anonymization. Dates, routes, quantities, amounts, order numbers, warehouse positions, and other operational context can still be sensitive.
- Order number and purchase order number remain unchanged because they serve as operational transaction identifiers in this environment. A different privacy policy may require pseudonymizing them too.


## Quality gates

Each dataset has its own privacy validator and quality gate. Depending on the dataset, the checks cover:

- required output columns
- non-empty batches
- nulls in critical business and pseudonymous fields
- expected customer, product, and SKU pseudonym formats
- complete deterministic row hashes
- duplicate row hashes, reported as warnings with duplicate exports for inspection
- negative numeric values, reported as warnings
- exact linkage between the dataset's batch or snapshot identifier and its quality report

The S3 upload stage independently reopens the matching quality report and refuses to upload if its status is not `passed` or `passed_with_warnings`. This prevents a stale, mismatched, or failed report from authorizing the wrong dataset.

These are deterministic assertions, not ML-based anomaly detection. They are intended to catch broken schemas, failed pseudonymization, corrupt mappings, empty exports, and suspicious records before the data reaches S3.

## Reproducibility

The repository does not include business exports or committed runtime datasets. Automated tests create temporary synthetic Excel workbooks and mapping CSVs, so ingestion, idempotency, mapping-integrity, and orchestration behavior can be tested without production data.

Running the complete pipeline requires source workbooks matching the schemas described in:

- `config/column_mapping.py`
- `config/column_mapping_open_orders.py`
- `config/column_mapping_inventory.py`

**Runtime requirements:**

```text
Python 3.10+ recommended
AWS credentials with permission to write to the target S3 bucket
```

Install runtime dependencies:

```powershell
python -m pip install -r requirements.txt
```

Install test dependencies and run the suite:

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

**Environment variables** required by upload stages:

```powershell
$env:S3_BUCKET = "your-bucket-name"
$env:S3_PREFIX = "order-intelligence"   # optional; this is the default
$env:AWS_REGION = "us-east-1"            # optional; this is the default
```

**Common commands:**

```powershell
# List the exact stages grouped by track
python src\run_script\run_pipeline.py --list

# Preview a full run without executing any stages
python src\run_script\run_pipeline.py --dry-run

# Run the shared foundation and all three dataset tracks through S3
python src\run_script\run_pipeline.py

# Run everything except the three S3 upload stages
python src\run_script\run_pipeline.py --skip upload_s3 upload_open_orders_s3 upload_inventory_s3

# Rebuild and validate only the shared mapping foundation
python src\run_script\run_pipeline.py --track shared

# Run one dataset using existing mappings; mapping integrity is checked first
python src\run_script\run_pipeline.py --track order_history
python src\run_script\run_pipeline.py --track open_orders
python src\run_script\run_pipeline.py --track inventory

# Resume from a stage; later tracks in the global plan may also run
python src\run_script\run_pipeline.py --from pseudonymize_open_orders

# Force ingestion of source files already recorded in their manifests
python src\run_script\run_pipeline.py --force
```

For a standalone non-shared track, the runner automatically prepends the read-only shared-mapping validator. It does not rebuild the foundation. Inventory only uses product mappings at the data level, although the current shared validator verifies both mapping files as one foundation contract.

Every stage can also run directly for focused debugging, for example:

```powershell
python src\privacy\pseudonymize_order_history.py
```

Order history and open orders use microsecond-precision batch identifiers and keep separate ingestion manifests, so processing one sheet never blocks the other sheet from the same workbook. Inventory uses the snapshot date encoded in its source filename and refuses to overwrite an existing snapshot unless `--force` is explicitly supplied.

## Repository structure

```text
src/
├── ingestion/          Separate Excel ingestion for order history, open orders,
│                       and inventory, with dataset-specific idempotency guards
├── standardization/    Schema mapping and drift checks, one script per dataset
├── privacy/            Shared identity construction and validation, backfills,
│                       pseudonymization, and per-dataset privacy validation
├── quality/            Dataset-specific quality gates and reports
├── cloud/              Quality-authorized S3 uploads, one per dataset
└── run_script/         Track-aware subprocess orchestrator
config/
├── paths.py                        Portable directory definitions
├── column_mapping.py               Order-history source schema
├── column_mapping_open_orders.py   Open-orders source schema
└── column_mapping_inventory.py     Inventory source schema
tests/
├── test_ingestion_idempotency.py    Split ingestion, force, and rollback behavior
├── test_run_pipeline.py             Track isolation and run statuses
└── test_validate_shared_mappings.py Mapping integrity and collision checks
docs/
├── DECISIONS.md                    Tradeoffs, limitations, and engineering notes
└── images/                         Architecture and run evidence
```

`data/`, `logs/`, Excel files, CSV files, JSON runtime artifacts, credentials, and local mapping tables are excluded from Git. See `.gitignore`.

## Known limitations

- Source exports are manual. There is no scheduler or source API, so freshness depends on someone refreshing the workbooks.
- Shared mappings are local CSV files rather than a transactional database. Atomic file replacement and rollback protect individual writes, but concurrent pipeline runs are not supported.
- Order history seeds identity because clean master customer/product/SKU tables are unavailable. Identity quality therefore depends on the source transaction fields used to construct those keys.
- Open-orders-only and inventory-only products do not have a trustworthy source category and are assigned to an explicit `Unknown Category` pseudonymous bucket.
- Customer identity currently includes the normalized customer name. A name correction can therefore create a new identity; the durable-key/SCD2 design is deferred to the warehouse layer.
- Order history, open orders, and inventory can be exported at different times, so one pipeline run is not a perfectly synchronized source snapshot.
- Multi-file ingestion commits are fail-closed and roll back ordinary write failures, but they are not database transactions. An abrupt machine or process termination during the small commit window can require manual inspection and a forced retry.
- Automated tests run locally, but CI has not yet been configured.

## What's next

The next major layer is Snowflake and dbt: durable warehouse tables, incremental fact loading from the existing row hashes, mapping provenance, and a proper SCD2 customer dimension using a dbt snapshot. After that come BI, forecasting or ML, production scheduling, and CI/CD. Those will be separate deliverables when they exist.
