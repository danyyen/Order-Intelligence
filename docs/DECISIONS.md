# Engineering decisions

This is the deeper engineering log behind [../README.md](../README.md): the tradeoffs in the current pipeline, why the architecture looks the way it does, what broke during development, and how the implementation is evaluated beyond “the script exited 0.”

## Design decisions

**Why subprocesses?** Each stage remains independently runnable and debuggable. A stage can fail, time out, or be rerun without sharing in-memory state with the other stages. `run_pipeline.py` coordinates those scripts, captures their output, and records a structured result; it does not hide the underlying stage boundaries.

**Why a filesystem interface?** Every intermediate artifact is inspectable, and a downstream stage does not run until its required file exists. Atomic file replacement prevents partially written individual files from appearing complete. This is appropriate for a local, single-run pipeline, although it is not a substitute for database transactions or concurrent-write protection.

**Why pseudonymize before S3?** The privacy boundary is the local machine, not an IAM policy. Direct customer and product identifiers should not exist in cloud storage even temporarily. The local mapping tables are therefore never uploaded.

**Why shared mappings?** The same real customer or product must resolve to the same pseudo identity in order history, open orders, and inventory. Dataset-specific mappings would make cross-dataset joins silently incorrect.

**Why does order history seed identity?** Clean customer, product, and SKU master tables are not available from the source system. Order history is the broadest accessible transactional source, so it establishes the initial shared customer/product/SKU universe. Open orders and inventory then add only entities that have not appeared in order history yet.

**Why validate the shared foundation separately?** A corrupted mapping is a common dependency failure, not three unrelated dataset failures. `validate_shared_mappings.py` checks both mapping files once before any dataset consumes them: required columns, null identifiers, unique join keys, split identities, pseudo-identifier collisions, and expected pseudonym formats. Each consumer still keeps local defensive checks because a mapping can change after foundation validation through a backfill stage.

**Why isolate failures by dataset?** A failed order-history quality gate should block that order-history upload, but it should not prevent open orders or inventory from being processed. The shared foundation is the only global gate. Once it succeeds, a critical failure stops only the affected dataset track and the remaining tracks continue sequentially.

**Why are tracks sequential rather than parallel?** Open orders and inventory can both append new products/SKUs to `product_mapping_final.csv`. Failure isolation does not make concurrent mutation safe. Until the mappings move into a transactional store, tracks run one after another.

**Why defer SCD2?** Customer attribute history belongs in the warehouse layer. A durable customer key and history-tracking design were explored after discovering that the live key includes `customer_name`, but deploying a second Python implementation now would duplicate functionality that dbt snapshots provide directly. The current landing pipeline therefore preserves the existing identity contract and documents the limitation.

## Tradeoffs

These decisions had reasonable alternatives; the rejected option and its cost are part of the design.

**Incremental mappings instead of rebuilding them.** Rebuilding from scratch could assign a different pseudo ID to the same real entity on every run. The current scripts retain existing assignments and generate identifiers only for unseen keys. This is more complex, but identity continuity is a fundamental downstream requirement.

**Subprocess orchestration instead of Airflow, for now.** The source exports are manual, runs occur on one machine, and there is no warehouse dependency graph yet. Airflow would introduce a scheduler, metadata database, and operational services before those capabilities are needed. The current explicit track/stage model is intended to make a later DAG migration straightforward.

**A shared foundation plus independent tracks instead of one flat pipeline.** The original runner used one global stage list and stopped on the first critical failure. That incorrectly treated an order-history upload failure as a reason not to process inventory. The current runner keeps the dependency that matters—the shared mappings—while allowing order history, open orders, and inventory to succeed or fail independently.

**Split ingestion instead of reading both workbook sheets in one stage.** Order history and open orders live in the same workbook, but they have different dependency roles. The original combined ingestor could allow a broken Open Orders sheet to fail the shared foundation. They now have separate scripts, metadata, and manifests, so each sheet is independently idempotent and a bad Open Orders sheet affects only its own track.

**Fail loudly on schema drift instead of passing unknown columns through.** An unfamiliar source column may contain a new identifier or reflect a changed export contract. Standardization stops and requires an explicit mapping decision instead of silently carrying an unexplained field into an approved output.

**Backfill identities from open orders and inventory instead of rejecting every unseen entity.** Without master tables, a legitimate new customer can exist in open orders before appearing in history, and a product can exist in inventory before being ordered. Rejecting those rows would make the pipeline operationally incomplete. Backfills preserve the shared identity contract but accept that product category is unavailable in those sources; such products enter an explicit `Unknown Category` pseudonymous bucket rather than receiving a guessed category.

**One quality-gate script per dataset instead of one generalized framework.** The three scripts share patterns but enforce different schemas and business expectations. Keeping the proven dataset-specific implementations reduces abstraction risk at the cost of duplication. A future refactor should happen with equivalent regression coverage, not for DRYness alone.

**Transaction identifiers remain unmasked.** `order_number` and `purchase_order_number` retain analytical value for joins, duplicate investigation, and operational tracking. They are not treated as direct personal identifiers in this environment. Purchase-order numbering still carries residual risk if a customer embeds identifying information in its convention; organizations with stricter policies should pseudonymize it.

**Employee usernames are dropped; `company_code` is retained.** `created_by_user` is a direct employee identifier and is removed before the final order output. `company_code` is retained as a non-personal operational field because it is part of the current output and quality-gate contract. An earlier design note said both were dropped; that no longer describes the deployed code.

**Warnings do not automatically become failures.** Missing required fields, incomplete pseudonymization, empty outputs, or invalid mappings block a track. Duplicate row hashes and negative numeric values are warnings because they can be legitimate business conditions that require review rather than automatic deletion. Their details are preserved in quality reports and, for duplicate hashes, inspection CSVs.

## Limitations

Things this pipeline genuinely does not do:

- **No direct source connection.** Every run depends on manually refreshed Excel exports, so freshness is bounded by a human step.
- **No clean master data.** Customer and product identity is derived from transactions rather than authoritative dimensions.
- **No scheduler or automated alerting.** Runs are manually triggered. Failures produce nonzero exit codes, logs, and JSON reports, but nobody is paged.
- **No transactional mapping store.** Mapping CSVs are written atomically as individual files, but concurrent pipeline runs are unsupported.
- **No perfectly atomic multi-file ingestion transaction.** Ordinary write failures roll back metadata and manifest state, and the downstream-visible CSV is published last. An abrupt process or machine termination during the small commit window can still require inspection and a forced retry.
- **No synchronized source snapshot guarantee.** The three source exports may be refreshed at different times. Cross-dataset analysis is only temporally meaningful when the operator captures them together.
- **No incremental warehouse load yet.** Each pipeline run processes full extracts. Deterministic `row_hash` values are already produced for a future warehouse merge, but the landing pipeline does not consume them incrementally.
- **Customer identity still includes normalized `customer_name`.** A name correction can create a new live identity even when source and ship-to codes are unchanged. The durable-key/SCD2 solution is deferred to the warehouse layer.
- **Standalone SKU components can lose leading zeros.** Numeric-looking `first_half_sku_code` and `unique_sku_code` values may be inferred as integers when read from CSV. `full_sku_code`, the actual join key, contains a dash and remains reliable. The standalone components should be forced to text before they become external contracts.
- **Inventory SKU reconstruction assumes an eleven-digit code split six-plus-five.** The rule matches observed source data and unmapped reconstructions fail loudly, but it remains an encoding assumption rather than a source-system guarantee.
- **Unknown product categories remain unknown.** Open orders and inventory do not provide a trustworthy category for previously unseen products, so the pipeline records `Unknown Category` instead of inventing business meaning.
- **Pseudonymization is not anonymization.** Dates, routes, amounts, quantities, warehouse positions, and transaction identifiers can still reveal sensitive operational context.
- **Local tests, no CI yet.** The repository has 27 automated tests, but they are not currently executed by a hosted CI workflow.

## Debugging notes

Specific failures discovered during development, and what they changed:

**A pandas merge failed only on the second incremental run.** `sku_mapping_pipeline.py` used a code path that did not execute on the first run because no previous pseudo sequence existed. Adding a genuinely new product on a second run exposed pandas suffixing duplicate merge columns and caused a `KeyError`. The fix was to select only the required lookup columns before merging. A successful first run was not sufficient evidence of incremental correctness.

**A quality-report timestamp broke the batch contract.** One version created a fresh timestamp for the quality-report filename instead of reusing the batch identifier embedded in the pseudonymized data filename. The quality checks passed, but the upload stage correctly rejected the mismatch. Quality reports now inherit the data file's identifier.

**An upload stage stopped checking quality status.** A regression removed the `overall_status` check from the order-history uploader. The script could have completed successfully while uploading data whose gate had failed. The check was restored, and every uploader now independently reads the matching report and accepts only `passed` or `passed_with_warnings`.

**Customer-key migration exposed 428 duplicate historical identities.** Testing a durable key based on source and ship-to codes found that 1,770 mapping rows represented only 1,342 durable identities; name variants had received separate pseudo IDs. A deterministic migration rule and audit output were designed. That experiment demonstrated the current key limitation, but the migration is not deployed in this landing pipeline.

**A config file was syntactically valid but unusable.** `column_mapping.py` was accidentally saved as notebook-style JSON. It parsed as a Python dictionary expression but exported no `COLUMN_MAPPING`, so the visible failure appeared later as an import error. This reinforced the need to test imports and contracts, not only syntax.

**One duplicate source row appeared among 553,098 records.** The quality gate traced a row-hash collision back to a genuine duplicate in the original export rather than a merge fan-out. Duplicate hashes remain warnings with an inspection export instead of being deleted automatically.

**Live and experimental customer scripts diverged.** Experimental SCD2 versions of customer scripts were accidentally placed into the live pipeline even though the persisted mapping schema still followed the original contract. The result was a missing-column failure several stages later. The live pipeline was restored to one consistent schema; SCD2 remains a warehouse decision rather than a second set of active landing scripts.

**A glob was safe until a second dataset shared the folder.** `quality_report_*.json` began matching open-order reports as well as order-history reports. Lexicographic ordering could select the wrong dataset's report. The batch-matching safeguard prevented upload, and the pattern was tightened to `quality_report_[0-9]*.json` for order history.

**A circular SKU check created false confidence.** Inventory reconstruction was compared against components derived from the same mapping, so the check could not independently prove the reconstruction rule. Re-examining the preconditions exposed the real issue: standalone component columns had lost leading zeros, while dashed `full_sku_code` remained trustworthy. The circular check was removed and the limitation documented.

**One flat failure loop contradicted dataset independence.** The original orchestrator used `break` after any critical failure. An order-history quality or S3 failure therefore prevented open orders and inventory from starting. Stages are now labeled by track; shared failure blocks all dependents, while dataset failure stops only that dataset. Regression tests simulate failures in each track and assert the exact downstream skip behavior.

**Combined workbook ingestion coupled unrelated failures.** The original ingestor read Order History and Open Orders together. An empty or malformed Open Orders sheet could fail the shared mapping foundation even when Order History was valid. Separate ingestion scripts and manifests now preserve the physical shared workbook while isolating the logical datasets.

**Second-level run IDs overwrote audit records.** Two quick dry runs could generate the same log and JSON report filename. Pipeline run IDs and transactional ingestion batch IDs now include microseconds, and forced ingestion tests verify distinct outputs.

**A failed CSV publish could leave false success metadata.** Multi-file ingestion originally published artifacts in an unsafe order. The current scripts write files atomically, publish the downstream-visible CSV last, and roll back metadata and manifest entries after ordinary commit failures. Tests inject a simulated disk failure and verify that a plain retry remains possible.

## Evaluation

How the current pipeline is checked:

- **Automated regression suite.** Twenty-seven tests cover split ingestion, independent manifests, duplicate-ingestion refusal, forced reingestion, commit rollback, shared-mapping nulls and collisions, track selection, failure isolation, and overall run statuses.
- **Shared-mapping integrity.** The foundation validator checks both directions of every real-to-pseudo relationship. It detects both one real key splitting into multiple pseudo identities and one pseudo identity colliding across multiple real keys.
- **Track failure simulations.** Orchestrator tests replace subprocess execution with controlled stage results and verify that shared failure blocks all tracks while a dataset failure blocks only its own downstream stages.
- **Critical validation before upload.** Privacy validators and quality gates are critical within their own tracks. Uploaders independently confirm the matching report identifier and acceptable quality status.
- **Join cardinality checks.** Pseudonymization stages compare row counts before and after mapping joins and reject mapping fan-out or row loss.
- **Stable identifier linkage.** Order history and open orders carry microsecond-precision batch IDs through pseudonymized filenames, quality reports, and S3 paths. Inventory uses its source snapshot date. Upload stages reject mismatched data/report pairs.
- **Warnings remain visible.** Duplicate row hashes and negative numeric values are recorded rather than silently cleaned, preserving evidence for human review.
- **Real-scale validation still matters.** The largest development run processed roughly 553,000 order-history rows, 4,500 open orders, and 5,800 inventory rows. Real runs exposed integration failures—shared globs, schema divergence, and source duplicates—that isolated unit scenarios did not.

The test suite does not prove business truth, source freshness, anonymity, or safe concurrency. It does provide repeatable evidence that the implemented identity, isolation, idempotency, rollback, validation, and upload-authorization contracts behave as documented.
