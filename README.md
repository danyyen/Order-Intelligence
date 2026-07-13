# Order Intelligence Pipeline

A local first Python pipeline that ingests legacy Excel exports, pseudonymizes sensitive fields through shared identity mappings, validates data quality, and lands approved batches in S3. Snowflake, dbt, BI, and ML come next.

#### Highlights
- Synthetic datasets included for end-to-end execution
- Fully reproducible without access to production data
- No confidential customer or business data included in the repository

**What's in this repo:** pipeline code, plus small synthetic samples, fewer than twenty rows per dataset, so the pipeline can actually be run end to end without needing real business data. The `.gitignore` excludes real data files by default.

## Status

| Piece | Status |
|---|---|
| Order history pipeline | Complete |
| Open orders pipeline | Complete |
| Inventory snapshot pipeline | Complete |
| Shared pseudonymous mappings | Complete |
| Quality gates | Complete |
| S3 landing | Complete |
| Automated tests / CI | Not yet |
| Snowflake / dbt | Planned |
| Airflow | Planned |
| BI / ML | Planned |

## Why this exists

Order history, open orders, and inventory all live inside the same legacy business system. Getting any of it out means someone manually running an export to Excel. There's no API and no direct database connection, nothing scheduled. Each export contains real customer names, real product identifiers, and real employee usernames sitting in plain columns next to order amounts and delivery routes.

I wanted to build real analytics on top of this data, the kind of thing that eventually turns into dashboards, demand forecasting, maybe a machine learning layer, without any of that flowing through in the clear to whoever ends up touching it downstream. That's the actual reason this pipeline pseudonymizes customer names, product descriptions, and SKU codes, and drops employee usernames entirely, before anything leaves the local machine. It isn't a portfolio decoration. I intentionally kept the stages file-based while validating the business rules. The filesystem acts as the interface between stages today; once a warehouse layer is introduced, those boundaries naturally become database tables instead of CSV outputs. Once Snowflake exists the interface naturally becomes tables instead of CSV outputs.

This repo is the first real piece of that. Get the export in reliably, strip the sensitive fields consistently across every run, check the output isn't garbage before it goes anywhere, and land it in S3. Everything after that (a real warehouse, dbt models, BI, machine learning, eventually Airflow for scheduling) is later work, not built yet. 

For the reasoning behind specific design choices, the things that didn't work on the first try, and how I actually know this pipeline does what it claims, see **[docs/DECISIONS.md](docs/DECISIONS.md)**.

## Tested Against

| Dataset | Scale |
|---------|------:|
| Order History | ~553,000 rows |
| Open Orders | ~4,500 rows |
| Inventory | ~5,800 rows |
| Products | 947 |
| Customers | 1,770 |
These figures represent the largest real dataset processed during development. The repository includes only synthetic samples for reproducibility.

## Proof of a real run

![Full Pipeline run](docs/images/result_output.jpg)
![Comparison result](docs/images/column_comparison_before_and_after.jpg)
![S3 result](docs/images/s3_landing.jpg)

More screenshots, including the stage status table and the before/after column comparison, are in [docs/images](docs/images).


## Architecture

![Pipeline architecture diagram](docs/images/architecture_1.jpg)

All three datasets (order history, open orders, and inventory) come from the same legacy system, pulled through separate export processes. They all pseudonymize through the same shared customer, product, and SKU identity mappings, so a given customer or product resolves to the same fake identity no matter which dataset it shows up in.

Because the mappings are shared, order history's mapping stages (profiling and assigning pseudo IDs to every customer, product, and SKU) have to run first. Everything else builds on top of that. Open orders and inventory can each reference a customer or product that hasn't shown up in order history yet, an order placed before it ships, stock received before it's ever been ordered, so a small backfill stage in each of those tracks adds exactly those missing entities to the shared mapping before pseudonymizing.

From there, each of the three datasets runs its own pseudonymize, validate, quality gate, and upload sequence, landing in its own S3 path.

On failure isolation, since this trips people up: a run is fail fast within a dataset's own track. If order history's quality gate fails, order history stops there and doesn't upload a bad batch. That does not take down open orders or inventory, those are separate subprocess chains and keep going on their own. What I mean by "independent" is dataset level, not stage level. You can also rerun a single dataset's track on its own with `--from`, without touching the other two.

Every stage is a standalone script that reads whatever the previous stage's latest output is and writes its own output. **The filesystem is the interface between stages, not shared memory**. That makes each stage independently runnable and debuggable on its own. Each output row includes a deterministic row hash that will later support incremental warehouse loading.

`run_pipeline.py` orchestrates all of it as subprocesses, not a scheduler, not a DAG engine, just a script that runs each stage in order, logs everything, and stops a given track on its first critical failure. This is meant to be a step toward Airflow, not a replacement for it. Once there's a real warehouse layer to coordinate against, the stage logic here should translate fairly directly into Airflow tasks. Why not Airflow yet, why pseudonymization is incremental rather than rebuilt each run, and a few other real design calls are covered in [docs/DECISIONS.md](docs/DECISIONS.md).


## Privacy boundary

Worth being precise about this instead of just saying "pseudonymized" and moving on.
- Customer names, product descriptions, and SKU codes are mapped to stable fake identifiers through the shared mapping tables, computed before any sensitive field is dropped. Employee usernames and internal constant fields (created_by_user, company_code) aren't pseudonymized at all, they're dropped outright inside the pseudonymization scripts themselves, since nothing downstream needs the raw values and there's no mapping table to ever leak.
- Mappings live locally, never in the repo, never in S3. The `.gitignore` keeps them out by default.
- This is pseudonymization, not anonymization. Order dates, routes, quantities, and amounts still travel downstream and can still be sensitive operational information, especially for low volume customer/product combinations where surrounding context might make an identity guessable. I'm not claiming this data is safe to hand to anyone; I'm claiming the direct identifiers are gone.  
- Order number and purchase order number remain unchanged because, in this environment, they function as operational transaction identifiers rather than direct personal identifiers. Organizations with different privacy requirements may choose to pseudonymize these fields as well.

#### Real Production-Sized Run

- Processed approximately **553,000** order history rows
- Processed approximately **4,500** open order rows
- Processed approximately **5,800** inventory rows
- Completed in approximately **2 minutes (124.07 seconds)**
- Deterministically pseudonymized sensitive identifiers
- Uploaded approved batches to Amazon S3

## Quality gates

Each dataset has its own gate, but broadly, a batch gets rejected if:

- required columns are missing or renamed unexpectedly by the source export
- a customer, product, or SKU can't be resolved through the mapping even after backfill
- duplicate business keys show up where there should be exactly one row
- dates fall outside a sane range, or quantities go negative
- row counts drift too far from the source file without explanation

None of this is ML based, it's straightforward assertions, but it's the difference between catching a broken export before it lands in S3 versus finding out three dashboards downstream.


## Reproducibility

The repo includes small synthetic samples, fewer than twenty rows per dataset, matching the real column structures. That's enough to clone this and actually run the full pipeline end to end without needing your own export. If you do have a real export in the same shape, the column mappings in `config/column_mapping.py`, `config/column_mapping_open_orders.py`, and `config/column_mapping_inventory.py` describe exactly what each script expects.

**Requirements:**
```
Python 3.9+ (3.10+ recommended, boto3 drops 3.9 support in April 2026)
pip install -r requirements.txt
AWS CLI configured with credentials that can write to your target S3 bucket
```

**Environment variables** (required before the upload stages will run):
```powershell
$env:S3_BUCKET = "your-bucket-name"
$env:S3_PREFIX = "order-intelligence"   # optional, this is already the default
$env:AWS_REGION = "us-east-1"            # optional, this is already the default
```

**Running it:**
```powershell
# See the plan without running anything
python src\run_script\run_pipeline.py --list
python src\run_script\run_pipeline.py --dry-run

# Full run, all three datasets, through to S3
python src\run_script\run_pipeline.py

# Everything except the actual S3 uploads, the safest way to validate a change
python src\run_script\run_pipeline.py --to quality_gate_inventory

# Resume from a specific stage after fixing something
python src\run_script\run_pipeline.py --from pseudonymize_open_orders

# Rerun just one dataset's track without touching the others
python src\run_script\run_pipeline.py --from standardize_inventory
```

Each stage can also be run standalone, for example `python src\privacy\pseudonymize_order_history.py`, for debugging a single step without rerunning everything ahead of it.

Order history and open orders identify a batch by an intraday identifier down to the minute. Inventory identifies a snapshot by date only. See [docs/DECISIONS.md](docs/DECISIONS.md) for why that distinction is deliberate. Ingestion for both the main export and the inventory export refuses to silently reprocess an unchanged source file. Pass `--force` to any ingestion script if reingesting the same file is actually intentional.

## Repository structure

```
src/
├── ingestion/          Excel to CSV, one script for the main export, one for inventory
├── standardization/    Column renaming and validation, one script per dataset
├── privacy/            Pseudonymization: customer, product, and SKU mapping,
│                        per dataset pseudonymize and validate scripts, backfill
│                        stages for entities not yet seen in order history, and
│                        the SCD2 migration script (designed, not yet deployed)
├── quality/             Data quality gates, one per dataset
├── cloud/                S3 uploads, one per dataset
└── run_script/           Pipeline orchestrator
config/
├── paths.py                        Single source of truth for every directory path
├── column_mapping.py               Order history: raw source code to readable name
├── column_mapping_open_orders.py   Open orders: same idea, different raw schema
└── column_mapping_inventory.py     Inventory: same idea, different raw schema again
docs/
├── DECISIONS.md          Tradeoffs, limitations, debugging notes, evaluation
└── images/                Architecture diagram and run evidence
```

`data/` and `logs/` are excluded from this repository by default, aside from the small synthetic samples noted above. See `.gitignore`.

## Known limitations

Being upfront about where this stands right now:

- The source export is manual. There's no scheduler and no way to guarantee freshness beyond whoever remembers to refresh the Excel file.
- Mappings live in local files, not a transactional store. There's no locking, so two runs writing to the same mapping at once isn't handled.
- No automated tests yet. Bugs so far have been caught through live test runs, not a test suite. That's next on the list, before Snowflake work goes much further.
- Order history, open orders, and inventory can be exported at different times, so a single "pull" isn't a perfectly aligned snapshot across all three.
- SCD2 for the customer dimension is designed and tested but deliberately deferred to a dbt snapshot rather than implemented here because Customer history is ultimately a warehouse concern rather than a landing-zone concern.

## What's next

A real warehouse layer (Snowflake), dbt models, including finally deploying the SCD2 customer dimension as a proper `dbt snapshot`, and incremental fact loading using the `row_hash` that's already being computed, then BI and ML on top of that. Separate work, separate write up, when it exists.