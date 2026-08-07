"""
src/run_script/run_pipeline.py

Production orchestrator for the legacy ERP Order Intelligence pipeline:
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
- Stages belong to one of four tracks: "shared" (the foundation every
  other track depends on, ending with validate_shared_mappings),
  "order_history", "open_orders", "inventory". A critical failure in
  "shared" stops the entire pipeline — nothing downstream can trust an
  incomplete or corrupted foundation. A critical failure inside one of
  the other three tracks stops only that track; the remaining tracks
  keep running independently, each as its own subprocess chain.
- overall_status is one of:
    "completed"           every track that ran, succeeded
    "partially_completed"  shared succeeded, but only some dataset
                            tracks that ran succeeded
    "failed"               shared failed, or every dataset track that
                            ran failed
- Every run produces:
    1. A timestamped log file (console + file), with each stage's own
       stdout/stderr streamed into it for a full audit trail.
    2. A JSON run report under data/metadata/pipeline_runs/, including
       a per-track status summary.

Usage
-----
    python run_pipeline.py                    # run the full pipeline
    python run_pipeline.py --list             # show configured stages, by track
    python run_pipeline.py --from map_skus    # resume from a stage
    python run_pipeline.py --to quality_gate  # stop after a stage
    python run_pipeline.py --skip validate    # skip a stage by name
    python run_pipeline.py --track inventory  # run only one track (auto-
                                               # revalidates shared mappings
                                               # first, doesn't rebuild them)
    python run_pipeline.py --dry-run          # show the plan, run nothing

Exit codes
----------
    0   overall_status == "completed"
    1   overall_status == "partially_completed" or "failed"
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

# Track execution order, used for sorting/log output and as the valid
# choices for --track. "shared" always runs first and gates the other
# three; the other three are independent of each other.
TRACK_ORDER = ["shared", "order_history", "open_orders", "inventory"]


@dataclass
class Stage:
    name: str                  # short identifier, used for --skip / --from / --to
    script: str                 # path relative to src/
    track: str                  # "shared" | "order_history" | "open_orders" | "inventory"
    critical: bool = True       # True = failure stops this stage's track (or the whole run, if track == "shared")
    retryable: bool = False     # True = retried on failure before giving up
    max_retries: int = 2
    timeout: int = DEFAULT_TIMEOUT


STAGES: list[Stage] = [
    # --- Shared foundation. Every track below depends on this completing,
    # ending with an explicit integrity check on the mappings themselves.
    # A critical failure anywhere here stops the entire pipeline. ---
    Stage("ingest_order_history", "ingestion/ingest_order_history.py", track="shared"),
    Stage("standardize", "standardization/standardize_order_columns.py", track="shared"),
    Stage("profile_customers", "privacy/profile_customers.py", track="shared"),
    Stage("map_customers", "privacy/customer_mapping_pipeline.py", track="shared"),
    Stage("profile_products", "privacy/product_profile.py", track="shared"),
    Stage("map_products", "privacy/pseudonymize_products.py", track="shared"),
    Stage("map_skus", "privacy/sku_mapping_pipeline.py", track="shared"),
    Stage("validate_shared_mappings", "privacy/validate_shared_mappings.py", track="shared"),

    # --- Order history track. Independent of open_orders/inventory below;
    # a failure here does not stop them. ---
    Stage("pseudonymize_orders", "privacy/pseudonymize_order_history.py", track="order_history"),
    Stage("validate", "privacy/validate_pseudonymize_order_history.py", track="order_history"),
    Stage("quality_gate", "quality/data_quality_gate.py", track="order_history"),
    Stage(
        "upload_s3",
        "cloud/upload_pseudonymized_to_s3.py",
        track="order_history",
        retryable=True,
        max_retries=3,
        timeout=UPLOAD_TIMEOUT,
    ),

    # --- Open orders track. Depends only on the shared foundation above
    # (for the customer/product/SKU mappings) — independent of the order
    # history and inventory tracks. ---
    Stage("ingest_open_orders", "ingestion/ingest_open_orders.py", track="open_orders"),
    Stage("standardize_open_orders", "standardization/standardize_open_orders_columns.py", track="open_orders"),
    Stage("add_open_orders_products", "privacy/add_open_orders_only_products.py", track="open_orders"),
    Stage("add_open_orders_customers", "privacy/add_open_orders_only_customers.py", track="open_orders"),
    Stage("pseudonymize_open_orders", "privacy/pseudonymize_open_orders.py", track="open_orders"),
    Stage("validate_open_orders", "privacy/validate_pseudonymize_open_orders.py", track="open_orders"),
    Stage("quality_gate_open_orders", "quality/data_quality_gate_open_orders.py", track="open_orders"),
    Stage(
        "upload_open_orders_s3",
        "cloud/upload_open_orders_to_s3.py",
        track="open_orders",
        retryable=True,
        max_retries=3,
        timeout=UPLOAD_TIMEOUT,
    ),

    # --- Inventory track. Depends only on the shared foundation above —
    # independent of both the order history and open orders tracks. ---
    Stage("ingest_inventory", "ingestion/ingest_inventory.py", track="inventory"),
    Stage("standardize_inventory", "standardization/standardize_inventory_columns.py", track="inventory"),
    Stage("add_inventory_products", "privacy/add_inventory_only_products.py", track="inventory"),
    Stage("pseudonymize_inventory", "privacy/pseudonymize_inventory.py", track="inventory"),
    Stage("validate_inventory", "privacy/validate_pseudonymize_inventory.py", track="inventory"),
    Stage("quality_gate_inventory", "quality/data_quality_gate_inventory.py", track="inventory"),
    Stage(
        "upload_inventory_s3",
        "cloud/upload_inventory_to_s3.py",
        track="inventory",
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
    logger.error(f"[ALERT] Stage '{stage.name}' (track '{stage.track}') failed: {error_summary}")


# ---------------------------------------------------------------------------
# STAGE EXECUTION
# ---------------------------------------------------------------------------

def run_stage(stage: Stage, logger: logging.Logger, dry_run: bool = False, extra_args: list[str] | None = None) -> dict:
    script_path = SRC_DIR / stage.script
    result = {
        "name": stage.name,
        "script": str(script_path),
        "track": stage.track,
        "critical": stage.critical,
        "status": None,
        "attempts": 0,
        "duration_seconds": None,
        "exit_code": None,
        "error": None,
    }

    if dry_run:
        logger.info(f"[DRY RUN] Would execute stage '{stage.name}' (track '{stage.track}') -> {script_path}")
        result["status"] = "skipped_dry_run"
        return result

    attempts_allowed = stage.max_retries + 1 if stage.retryable else 1
    start = time.time()
    command = [PYTHON_EXE, str(script_path)] + (extra_args or [])

    for attempt in range(1, attempts_allowed + 1):
        result["attempts"] = attempt
        logger.info(f"--- Stage '{stage.name}' (track '{stage.track}', attempt {attempt}/{attempts_allowed}) ---")

        try:
            proc = subprocess.run(
                command,
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


def skipped_result(stage: Stage) -> dict:
    """Result shape for a stage never attempted because its track (or the
    shared foundation it depends on) already failed."""
    return {
        "name": stage.name,
        "script": str(SRC_DIR / stage.script),
        "track": stage.track,
        "critical": stage.critical,
        "status": "skipped_upstream_failure",
        "attempts": 0,
        "duration_seconds": None,
        "exit_code": None,
        "error": None,
    }


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

VALIDATE_SHARED_MAPPINGS_STAGE_NAME = "validate_shared_mappings"


def select_stages(args) -> list[Stage]:
    names = [s.name for s in STAGES]
    stages_by_name = {s.name: s for s in STAGES}

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

    skip_set = set(args.skip)
    if skip_set:
        unknown = skip_set - set(names)
        if unknown:
            raise SystemExit(f"Unknown stage name(s) in --skip: {unknown}")
        selected = [s for s in selected if s.name not in skip_set]

    if args.track is not None:
        selected = [s for s in selected if s.track == args.track]
        if not selected:
            raise SystemExit(
                f"No stages left after filtering to --track {args.track} "
                f"(check it isn't excluded by --from/--to/--skip)."
            )

        # A standalone dataset track still depends on the shared mappings
        # being valid. Rather than silently trusting a prior run to have
        # verified that, prepend the (read-only, fast — two CSV reads)
        # integrity check, unless the caller explicitly opted out via
        # --skip or it's already part of the selection some other way.
        already_included = any(s.name == VALIDATE_SHARED_MAPPINGS_STAGE_NAME for s in selected)
        if (
            args.track != "shared"
            and VALIDATE_SHARED_MAPPINGS_STAGE_NAME not in skip_set
            and not already_included
        ):
            selected = [stages_by_name[VALIDATE_SHARED_MAPPINGS_STAGE_NAME]] + selected

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
    parser = argparse.ArgumentParser(description="Run the legacy ERP order intelligence pipeline.")
    parser.add_argument("--list", action="store_true", help="List configured stages (grouped by track) and exit.")
    parser.add_argument("--from", dest="from_stage", default=None, help="Resume from this stage name (inclusive).")
    parser.add_argument("--to", dest="to_stage", default=None, help="Stop after this stage name (inclusive).")
    parser.add_argument("--skip", nargs="*", default=[], help="Stage name(s) to skip.")
    parser.add_argument(
        "--track", dest="track", default=None, choices=TRACK_ORDER,
        help="Run only stages belonging to this track (shared, order_history, "
             "open_orders, inventory). Applied after --from/--to/--skip. Does "
             "NOT rebuild the shared foundation — same assumption as resuming "
             "a single track with --from — but for a non-shared track it DOES "
             "auto-prepend validate_shared_mappings (unless --skip'd) to "
             "re-check the mappings this track depends on before trusting them.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without executing anything.")
    parser.add_argument(
        "--force", action="store_true",
        help="Pass --force through to ingestion stages (ingest_order_history, "
             "ingest_open_orders, ingest_inventory), forcing re-ingestion even "
             "if this exact source content was already ingested.",
    )
    args = parser.parse_args()

    if args.list:
        current_track = None
        for i, s in enumerate(STAGES):
            if s.track != current_track:
                current_track = s.track
                print(f"\n[{current_track}]")
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

    # Microsecond precision, not just seconds — two runs (e.g. two quick
    # --dry-run invocations) starting in the same second would otherwise
    # collide on both the log filename and the run-report filename,
    # silently overwriting one run's audit record with the other's.
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
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
        logger.info(f"  - [{s.track}] {s.name} (src/{s.script})")

    run_report = {
        "run_id": run_id,
        "started_at": datetime.now().isoformat(),
        "dry_run": args.dry_run,
        "stages": [],
        "track_status": {},
        "overall_status": None,
    }

    FORCE_CAPABLE_STAGES = {"ingest_order_history", "ingest_open_orders", "ingest_inventory"}

    pipeline_start = time.time()

    # Failure isolation: a critical failure in "shared" stops everything
    # (no track can trust an incomplete or corrupted foundation). A
    # critical failure in any other track stops only that track — the
    # remaining tracks keep running independently, each its own
    # subprocess chain.
    shared_failed = False
    track_failed: dict[str, bool] = {}

    for stage in stages_to_run:
        if stage.track == "shared":
            blocked = shared_failed
        else:
            blocked = shared_failed or track_failed.get(stage.track, False)

        if blocked:
            logger.warning(
                f"Skipping stage '{stage.name}' (track '{stage.track}') — "
                f"upstream failure already stopped this track."
            )
            run_report["stages"].append(skipped_result(stage))
            continue

        stage_extra_args = ["--force"] if (args.force and stage.name in FORCE_CAPABLE_STAGES) else None
        stage_result = run_stage(stage, logger, dry_run=args.dry_run, extra_args=stage_extra_args)
        run_report["stages"].append(stage_result)

        if stage_result["status"] in ("failed", "timeout"):
            notify_failure(logger, stage, stage_result["error"] or "unknown error")
            if stage.critical:
                if stage.track == "shared":
                    shared_failed = True
                    logger.error(
                        f"Critical shared-foundation stage '{stage.name}' failed — "
                        f"stopping the entire pipeline. No track can run against "
                        f"an incomplete or unverified foundation."
                    )
                else:
                    track_failed[stage.track] = True
                    logger.error(
                        f"Critical stage '{stage.name}' failed in track "
                        f"'{stage.track}' — stopping that track only. Other "
                        f"tracks continue independently."
                    )
            else:
                logger.warning(f"Non-critical stage '{stage.name}' failed — continuing.")

    run_report["finished_at"] = datetime.now().isoformat()
    run_report["total_duration_seconds"] = round(time.time() - pipeline_start, 2)

    tracks_present = sorted(
        {s.track for s in stages_to_run},
        key=lambda t: TRACK_ORDER.index(t),
    )
    for t in tracks_present:
        if t == "shared":
            run_report["track_status"][t] = "failed" if shared_failed else "completed"
        else:
            run_report["track_status"][t] = "failed" if (shared_failed or track_failed.get(t)) else "completed"

    dataset_tracks_present = [t for t in tracks_present if t != "shared"]

    if shared_failed:
        overall_status = "failed"
    elif not dataset_tracks_present:
        # Nothing but "shared" (or no) tracks were even requested this run.
        overall_status = "completed"
    elif all(track_failed.get(t) for t in dataset_tracks_present):
        overall_status = "failed"
    elif any(track_failed.get(t) for t in dataset_tracks_present):
        overall_status = "partially_completed"
    else:
        overall_status = "completed"

    run_report["overall_status"] = overall_status

    report_path = PIPELINE_RUN_DIR / f"pipeline_run_{run_id}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(run_report, f, indent=2)

    logger.info(f"Run report saved: {report_path}")
    for t, status in run_report["track_status"].items():
        logger.info(f"  Track '{t}': {status.upper()}")
    logger.info(
        f"=== Pipeline run {run_id} {run_report['overall_status'].upper()} "
        f"in {run_report['total_duration_seconds']}s ==="
    )

    return 0 if overall_status == "completed" else 1


if __name__ == "__main__":
    sys.exit(main())
