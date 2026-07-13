"""
src/run_script/run_pipeline.py

Production orchestrator for the legacy Ecosystem Order Intelligence pipeline:
Excel -> CSV -> Standardize -> Pseudonymize (customer/product/SKU) ->
Quality Gate -> S3 Upload.

Every stage below has been individually hardened and (where practical)
smoke-tested. This runner just chains them together as subprocesses,
exactly like running `python <script>.py` by hand — nothing about the
stage scripts themselves needs to change.

Before running:
    - S3_BUCKET must be set in the environment (upload_pseudonymized_to_s3.py
      refuses to guess a bucket name). Optionally also S3_PREFIX, AWS_REGION.
      e.g. in PowerShell:  $env:S3_BUCKET = "your-real-bucket-name"
    - AWS credentials must be configured (aws configure, env vars, or IAM role).

Design notes
------------
- Stages communicate via the filesystem (each stage reads the "latest"
  file matching a glob pattern) — this runner does not alter that contract.
- A non-zero exit code from any CRITICAL stage stops the pipeline
  immediately. data_quality_gate.py raises on a failed batch, and
  upload_pseudonymized_to_s3.py independently re-checks the quality
  status itself before uploading — so "quality gate blocks upload" is
  enforced twice, not just relied on here.
- Every run produces:
    1. A timestamped log file (console + file), with each stage's own
       stdout/stderr streamed into it for a full audit trail.
    2. A JSON run report under data/metadata/pipeline_runs/

Usage
-----
    python run_pipeline.py                    # run the full pipeline
    python run_pipeline.py --list             # show configured stages
    python run_pipeline.py --from map_skus    # resume from a stage
    python run_pipeline.py --to quality_gate  # stop after a stage
    python run_pipeline.py --skip validate    # skip a stage by name
    python run_pipeline.py --dry-run          # show the plan, run nothing

Exit codes
----------
    0   pipeline completed (quality gate may still be passed_with_warnings)
    1   a critical stage failed
    2   configuration error (missing script, bad args) — nothing was run
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# Bootstrap: resolve PROJECT_ROOT from this file's own location so config/
# can be imported, before anything else happens.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import LOG_DIR, PIPELINE_RUN_DIR, ensure_all_dirs_exist

SRC_DIR = PROJECT_ROOT / "src"
NOTEBOOK_LOG_DIR = LOG_DIR  # kept for naming clarity below

PYTHON_EXE = sys.executable  # use the same interpreter that launched this runner

DEFAULT_TIMEOUT = 15 * 60   # seconds
UPLOAD_TIMEOUT = 30 * 60


@dataclass
class Stage:
    name: str                  # short identifier, used for --skip / --from / --to
    script: str                 # path relative to src/
    critical: bool = True       # True = failure stops the whole pipeline
    retryable: bool = False     # True = retried on failure before giving up
    max_retries: int = 2
    timeout: int = DEFAULT_TIMEOUT


STAGES: list[Stage] = [
    Stage("ingest_excel", "ingestion/excel_to_csv_pipeline.py"),
    Stage("standardize", "standardization/standardize_order_columns.py"),
    Stage("profile_customers", "privacy/profile_customers.py"),
    Stage("map_customers", "privacy/customer_mapping_pipeline.py"),
    Stage("profile_products", "privacy/product_profile.py"),
    Stage("map_products", "privacy/pseudonymize_products.py"),
    Stage("map_skus", "privacy/sku_mapping_pipeline.py"),
    Stage("pseudonymize_orders", "privacy/pseudonymize_order_history.py"),
    Stage("validate", "privacy/validate_pseudonymize_order_history.py", critical=False),
    Stage("quality_gate", "quality/data_quality_gate.py"),
    Stage(
        "upload_s3",
        "cloud/upload_pseudonymized_to_s3.py",
        retryable=True,
        max_retries=3,
        timeout=UPLOAD_TIMEOUT,
    ),
    # --- Open orders track. Runs after the order history track completes,
    # since pseudonymize_open_orders depends on the customer/product/SKU
    # mappings built during map_customers/map_products/map_skus above. ---
    Stage("standardize_open_orders", "standardization/standardize_open_orders_columns.py"),
    Stage("add_open_orders_products", "privacy/add_open_orders_only_products.py"),
    Stage("add_open_orders_customers", "privacy/add_open_orders_only_customers.py"),
    Stage("pseudonymize_open_orders", "privacy/pseudonymize_open_orders.py"),
    Stage("validate_open_orders", "privacy/validate_pseudonymize_open_orders.py", critical=False),
    Stage("quality_gate_open_orders", "quality/data_quality_gate_open_orders.py"),
    Stage(
        "upload_open_orders_s3",
        "cloud/upload_open_orders_to_s3.py",
        retryable=True,
        max_retries=3,
        timeout=UPLOAD_TIMEOUT,
    ),
    # --- Inventory track. Runs after order history's product/SKU mapping
    # stages, since pseudonymize_inventory depends on them. Independent of
    # the open orders track (no shared dependency between the two). ---
    Stage("ingest_inventory", "ingestion/ingest_inventory.py"),
    Stage("standardize_inventory", "standardization/standardize_inventory_columns.py"),
    Stage("add_inventory_products", "privacy/add_inventory_only_products.py"),
    Stage("pseudonymize_inventory", "privacy/pseudonymize_inventory.py"),
    Stage("validate_inventory", "privacy/validate_pseudonymize_inventory.py", critical=False),
    Stage("quality_gate_inventory", "quality/data_quality_gate_inventory.py"),
    Stage(
        "upload_inventory_s3",
        "cloud/upload_inventory_to_s3.py",
        retryable=True,
        max_retries=3,
        timeout=UPLOAD_TIMEOUT,
    ),
]


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

def setup_logging(run_id: str) -> logging.Logger:
    log_file = LOG_DIR / f"pipeline_run_{run_id}.log"

    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    logger.info(f"Log file: {log_file}")
    return logger


def notify_failure(logger: logging.Logger, stage: Stage, error_summary: str) -> None:
    """
    Placeholder alerting hook. Replace with an SNS publish, Slack webhook,
    or SES call once you're ready — the call site won't need to change.
    """
    logger.error(f"[ALERT] Stage '{stage.name}' failed: {error_summary}")


# ---------------------------------------------------------------------------
# STAGE EXECUTION
# ---------------------------------------------------------------------------

def run_stage(stage: Stage, logger: logging.Logger, dry_run: bool = False) -> dict:
    script_path = SRC_DIR / stage.script
    result = {
        "name": stage.name,
        "script": str(script_path),
        "critical": stage.critical,
        "status": None,
        "attempts": 0,
        "duration_seconds": None,
        "exit_code": None,
        "error": None,
    }

    if dry_run:
        logger.info(f"[DRY RUN] Would execute stage '{stage.name}' -> {script_path}")
        result["status"] = "skipped_dry_run"
        return result

    attempts_allowed = stage.max_retries + 1 if stage.retryable else 1
    start = time.time()

    for attempt in range(1, attempts_allowed + 1):
        result["attempts"] = attempt
        logger.info(f"--- Stage '{stage.name}' (attempt {attempt}/{attempts_allowed}) ---")

        try:
            proc = subprocess.run(
                [PYTHON_EXE, str(script_path)],
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                timeout=stage.timeout,
            )
        except subprocess.TimeoutExpired:
            msg = f"Stage '{stage.name}' timed out after {stage.timeout}s"
            logger.error(msg)
            result["error"] = msg
            if attempt < attempts_allowed:
                backoff = 2 ** attempt
                logger.warning(f"Retrying after {backoff}s...")
                time.sleep(backoff)
                continue
            result["status"] = "timeout"
            break

        # Stream the child script's own output into our log for a full audit trail
        if proc.stdout.strip():
            for line in proc.stdout.strip().splitlines():
                logger.info(f"    | {line}")
        if proc.stderr.strip():
            for line in proc.stderr.strip().splitlines():
                logger.warning(f"    ! {line}")

        result["exit_code"] = proc.returncode

        if proc.returncode == 0:
            result["status"] = "success"
            break
        else:
            result["error"] = (
                proc.stderr.strip().splitlines()[-1] if proc.stderr.strip() else "non-zero exit"
            )
            if attempt < attempts_allowed:
                backoff = 2 ** attempt
                logger.warning(
                    f"Stage '{stage.name}' failed (exit {proc.returncode}). "
                    f"Retrying after {backoff}s..."
                )
                time.sleep(backoff)
                continue
            result["status"] = "failed"

    result["duration_seconds"] = round(time.time() - start, 2)
    return result


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def select_stages(args) -> list[Stage]:
    names = [s.name for s in STAGES]

    start_idx = 0
    end_idx = len(STAGES) - 1

    if args.from_stage is not None:
        if args.from_stage not in names:
            raise SystemExit(f"Unknown stage name for --from: {args.from_stage}")
        start_idx = names.index(args.from_stage)

    if args.to_stage is not None:
        if args.to_stage not in names:
            raise SystemExit(f"Unknown stage name for --to: {args.to_stage}")
        end_idx = names.index(args.to_stage)

    selected = STAGES[start_idx:end_idx + 1]

    if args.skip:
        skip_set = set(args.skip)
        unknown = skip_set - set(names)
        if unknown:
            raise SystemExit(f"Unknown stage name(s) in --skip: {unknown}")
        selected = [s for s in selected if s.name not in skip_set]

    return selected


def verify_scripts_exist(stages: list[Stage], logger: logging.Logger) -> None:
    missing = [s for s in stages if not (SRC_DIR / s.script).exists()]
    if missing:
        for s in missing:
            logger.error(f"Missing script for stage '{s.name}': {SRC_DIR / s.script}")
        raise SystemExit(
            "Aborting before running anything — one or more stage scripts "
            "were not found. Fix paths above and re-run."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the legacy Ecosystem order intelligence pipeline.")
    parser.add_argument("--list", action="store_true", help="List configured stages and exit.")
    parser.add_argument("--from", dest="from_stage", default=None, help="Resume from this stage name (inclusive).")
    parser.add_argument("--to", dest="to_stage", default=None, help="Stop after this stage name (inclusive).")
    parser.add_argument("--skip", nargs="*", default=[], help="Stage name(s) to skip.")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without executing anything.")
    args = parser.parse_args()

    if args.list:
        for i, s in enumerate(STAGES):
            flags = []
            if not s.critical:
                flags.append("non-critical")
            if s.retryable:
                flags.append(f"retryable x{s.max_retries}")
            flag_str = f" ({', '.join(flags)})" if flags else ""
            print(f"{i:2d}. {s.name:22s} src/{s.script}{flag_str}")
        return 0

    # Ensure every directory this pipeline reads/writes exists before any
    # stage runs — matters on a fresh checkout or a new machine.
    ensure_all_dirs_exist()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger = setup_logging(run_id)
    logger.info(f"=== Pipeline run {run_id} starting ===")

    try:
        stages_to_run = select_stages(args)
    except SystemExit as e:
        logger.error(str(e))
        return 2

    verify_scripts_exist(stages_to_run, logger)

    logger.info("Execution plan:")
    for s in stages_to_run:
        logger.info(f"  - {s.name} (src/{s.script})")

    run_report = {
        "run_id": run_id,
        "started_at": datetime.now().isoformat(),
        "dry_run": args.dry_run,
        "stages": [],
        "overall_status": None,
    }

    pipeline_start = time.time()
    failed = False

    for stage in stages_to_run:
        stage_result = run_stage(stage, logger, dry_run=args.dry_run)
        run_report["stages"].append(stage_result)

        if stage_result["status"] in ("failed", "timeout"):
            notify_failure(logger, stage, stage_result["error"] or "unknown error")
            if stage.critical:
                logger.error(
                    f"Critical stage '{stage.name}' failed — stopping pipeline. "
                    f"Downstream stages will NOT run."
                )
                failed = True
                break
            else:
                logger.warning(f"Non-critical stage '{stage.name}' failed — continuing pipeline.")

    run_report["finished_at"] = datetime.now().isoformat()
    run_report["total_duration_seconds"] = round(time.time() - pipeline_start, 2)
    run_report["overall_status"] = "failed" if failed else "completed"

    report_path = PIPELINE_RUN_DIR / f"pipeline_run_{run_id}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(run_report, f, indent=2)

    logger.info(f"Run report saved: {report_path}")
    logger.info(
        f"=== Pipeline run {run_id} {run_report['overall_status'].upper()} "
        f"in {run_report['total_duration_seconds']}s ==="
    )

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())