# Engineering decisions

This is the deeper log behind [../README.md](../README.md) — the trade-offs I made and why, what this pipeline genuinely can't do yet, what actually broke while building it, and how I know it works beyond "the script exited 0."

---

## Trade-offs

A few decisions that had a real alternative I considered and rejected, and why:

**Incremental pseudonym mapping instead of rebuilding from scratch each run.** The simpler version of this pipeline would regenerate `Customer CUST-000123` fresh every time it runs. I didn't do that, because downstream consumers need that ID to mean the same real customer next week as it does this week. The incremental version is more code (every mapping stage has to diff against what it already assigned and only touch what's new), but rebuilding from scratch would silently break identity continuity every single run.

**Subprocess orchestration instead of Airflow, for now.** I could have reached for Airflow immediately. I didn't, because right now this runs on one machine, triggered by one person, on an irregular schedule bounded by a manual Excel export — Airflow's scheduler, metadata database, and webserver solve problems I don't have yet. When there's an actual warehouse layer with real cross-system dependencies to coordinate, that's when the orchestration needs outgrow a Python script. Building the DAG logic into `run_pipeline.py` now means the stage dependency graph is already explicit when that migration happens.

**Fail loudly on schema drift instead of passing unknown columns through.** If the AS/400 export gains a new column tomorrow that nobody's told this pipeline about, `standardize_order_columns.py` stops the whole run rather than silently carrying an unmapped, cryptically-named column all the way to the pseudonymized output. That's more annoying in the short term (someone has to go update `column_mapping.py` before the pipeline runs again) but the alternative is a raw AS/400 field code sitting unnoticed in what's supposed to be clean, documented output.

**SCD Type 2 for the customer dimension: designed, tested, not deployed.** I found a real bug in the original customer identity key — it included `customer_name`, which meant a routine name correction would silently mint a brand-new fake identity for an existing customer, breaking continuity. I designed and tested a proper SCD2 fix (durable key on stable business codes only, name changes tracked as versioned history). It's not live in the current pipeline. dbt's `snapshot` feature is the more idiomatic place to actually implement SCD2, and I didn't want to hand-build it now and then rebuild it properly in dbt later — so it's staying as a documented, tested, ready-to-deploy design until the warehouse layer exists.

**One quality gate script per dataset instead of one generalized script.** `data_quality_gate.py` and `data_quality_gate_open_orders.py` are near-duplicates of each other. A cleaner design would parameterize one script for both datasets. I chose not to, because `data_quality_gate.py` was already built, tested, and working against real 553,098-row production data by the time open orders needed the same logic — refactoring a proven script to be generic felt riskier than a small amount of duplication. This is a real DRY violation I'm aware of, not an oversight.

---

## Limitations

Things this pipeline genuinely does not do, stated plainly:

- **No direct database connection to the source ERP.** Every run depends on someone manually exporting Excel from the AS/400 system. The pipeline's freshness is bounded by that manual step, not by anything automated.
- **Cross-dataset analysis isn't valid against what's currently on file.** The inventory snapshot I have is from February 2025; order history is from April 2026 — over a year apart. Comparing "current inventory" against "current demand" using these specific files would produce a misleading answer, so I'm not doing that analysis yet. Going forward, I'm taking all three extracts (order history, open orders, inventory) within about a minute of each other, but that's a discipline I have to remember, not something the pipeline enforces yet.
- **No scheduler.** This runs when I run it, on my machine. There's no cron job, no Airflow DAG, no recurring trigger.
- **SCD2 isn't deployed** (see trade-offs above). The customer identity key in the live pipeline today still includes `customer_name`.
- **No incremental/CDC loading.** Every run reprocesses the full order history rather than loading only what changed. A stable `row_hash` is already computed on every row specifically so a future warehouse-layer merge can use it — but nothing consumes it that way yet.
- **Inventory isn't pseudonymized or ingested yet.** Only order history and open orders are live in this pipeline. The inventory source has its own quirks (SKU codes stored without leading zeros, unlike the dash-composite format order history uses) that need their own handling, designed but not yet built.
- **No automated alerting.** A failed run produces a non-zero exit code and a log file. Nobody gets paged.
- **Testing is targeted smoke tests, not a coverage-driven test suite.** I wrote synthetic data to specifically exercise the scenarios that mattered (a second incremental run, a customer renaming mid-stream, a corrupted mapping file) rather than aiming for blanket code coverage. That approach caught three real bugs (below) that a purely static code review didn't.

---

## Debugging notes

Specific things that actually broke, and how I found them — not a hypothetical list.

**A pandas merge silently broke on the second incremental run, not the first.** `sku_mapping_pipeline.py` merges a dataframe against itself to look up prior pseudo-SKU sequences. On the very first run, that code path never executes (there's nothing prior to look up yet), so it looked fine. The bug only showed up when I wrote a synthetic test that ran the script twice — first run clean, second run adds a genuinely new product — and hit a `KeyError` because pandas silently renamed a duplicate column to `_x`/`_y` instead of raising. Fixed by explicitly selecting only the columns needed before the merge. This is the kind of bug that a first successful run gives you false confidence about.

**A quality gate regression that would have permanently blocked every future S3 upload.** At one point, `data_quality_gate.py` started generating a fresh timestamp for its own report filename instead of reusing the batch ID already embedded in the order file's name. Nothing about that change threw an error — the gate still ran, still passed. The problem only became visible when I compared the two filenames directly: `order_history_pseudonymized_20260706_154622.csv` against a quality report timestamped several seconds later. The upload script requires those two batch IDs to match before it'll upload anything, so this would have silently failed every upload from that point on, with an error message that (without the history of what changed) would have looked like a totally unrelated bug.

**An S3 upload script that stopped checking the thing it exists to check.** A later version of `upload_pseudonymized_to_s3.py` no longer read the quality report's `overall_status` at all before uploading — meaning a failed quality gate wouldn't have stopped anything from reaching S3. It didn't error. It didn't warn. It just uploaded. I only caught this by diffing it against the working version, not by running it — this is the failure mode that worries me most in this whole project, because a script that runs cleanly and does the wrong thing doesn't announce itself.

**Migrating the customer mapping surfaced 428 pre-existing duplicate identities.** When I fixed the customer identity key (see trade-offs above), migrating the existing 1,770-row mapping file to the corrected schema found that those 1,770 rows only represented 1,342 actual distinct customers — 428 rows were name-variants ("Beta Inc" / "Beta Incorporated" / "BETA INC.") that the old key design had silently issued separate fake identities for. I had to design a deterministic resolution rule (lowest-numbered existing pseudo ID wins per group) and produce an audit CSV of exactly what got merged into what, rather than just picking one and moving on.

**A config file that was valid Python and also completely broken.** `column_mapping.py` got accidentally saved as literal Jupyter notebook JSON instead of a plain dict. It didn't throw a syntax error — a JSON object happens to also be valid Python dict-literal syntax, so it parsed fine and then defined nothing importable. The failure showed up three files downstream as a confusing `ImportError`, not where the actual mistake was.

**One duplicate order row in 553,098.** The quality gate flagged exactly one `row_hash` collision in the largest real run so far. Traced it back to the original May ingestion batch — a genuinely duplicated row in the source export, not something the pipeline introduced. Small enough to not block the run (it's a warning, not a failure), but it's the reason there's a documented, tested path for adding a dedup step later if it starts happening more than once in 553,098 rows.

---

## Evaluation

How I actually know this pipeline is doing what it claims, not just "the script exited 0":

- **The quality gate distinguishes failures from warnings on purpose.** Missing required columns, unmapped customers/SKUs, or a pseudonymization check that finds a real name leaking through — those fail the run and block the upload. A duplicate row hash or a negative quantity — those warn but don't block, because they need a human to look, not an automated rollback.
- **Batch IDs are a structural guarantee, not a naming convention.** The pseudonymized order file, its quality report, and its S3 upload path all carry the same batch ID, and the upload script explicitly refuses to upload if the order file and quality report don't match. This isn't just for readability — it's what stops a stale quality report from ever getting paired with a newer, unvalidated data file.
- **Row counts are checked across every join.** If a merge against the customer or product mapping ever produces more or fewer rows than went in, the pipeline stops instead of silently shipping duplicated or dropped rows.
- **The real evaluation method during development was adversarial smoke testing, not just running the happy path.** For every non-trivial stage, I built a synthetic scenario specifically designed to break it — a second incremental run, a mid-stream name change, a deliberately corrupted mapping file — before trusting it against real data. Three of the bugs listed above were caught exactly this way, not by code review.
