"""
src/quality/data_quality_gate.py

Stage: run a suite of checks against the latest pseudonymized order
history file and write a structured quality report. Raises (non-zero
exit) if any check fails, which stops the pipeline before the S3 upload
stage runs.

Critical: the quality report's filename batch_id MUST match the batch_id
already embedded in the pseudonymized order file's name — upload_pseudonymized_to_s3.py
depends on this to confirm it's uploading a report that actually belongs
to the file it's uploading, not a stale one from a different run.

Output contract (unchanged):
    data/quality_reports/quality_report_<batch_id>.json
    data/quality_reports/duplicate_row_hashes_<batch_id>.csv (if any duplicates)
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import PSEUDONYMIZED_DIR, QUALITY_DIR

BATCH_ID_PATTERN = re.compile(r"(\d{8}_\d{6})")

REQUIRED_COLUMNS = [
    "company_code",
    "customer_name",
    "purchase_order_number",
    "order_number",
    "order_type",
    "order_date",
    "scheduled_ship_date",
    "shipped_date",
    "delivery_route",
    "order_status",
    "ordered_quantity",
    "full_sku_code",
    "product_description",
    "product_category",
    "row_hash",
    "pseudonymized_at",
]

CRITICAL_NULL_COLUMNS = [
    "order_number",
    "full_sku_code",
    "customer_name",
    "product_description",
    "product_category",
    "row_hash",
]

NUMERIC_COLUMNS_TO_CHECK = [
    "ordered_quantity",
    "final_order_quantity",
    "shipped_order_quantity",
    "shipped_order_weight",
    "order_amount",
]


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("data_quality_gate")


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def extract_batch_id(filepath: Path) -> str:
    match = BATCH_ID_PATTERN.search(filepath.name)
    if not match:
        raise ValueError(f"Could not find batch_id in filename: {filepath.name}")
    return match.group(1)


def find_input_file() -> Path:
    candidates = sorted(PSEUDONYMIZED_DIR.glob("order_history_pseudonymized_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No pseudonymized order files found in {PSEUDONYMIZED_DIR}. "
            "Run pseudonymize_order_history.py first."
        )
    return candidates[-1]


def atomic_write_json(data: dict, output_file: Path) -> None:
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    os.replace(tmp_file, output_file)


def atomic_write_csv(df: pd.DataFrame, output_file: Path) -> None:
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_csv(tmp_file, index=False, encoding="utf-8-sig")
    os.replace(tmp_file, output_file)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)

    try:
        input_file = find_input_file()

        # Reuse the batch_id already embedded in the order file's name so
        # the quality report and order file stay linked to the same run —
        # upload_pseudonymized_to_s3.py enforces that these two match.
        batch_id = extract_batch_id(input_file)
        quality_report_file = QUALITY_DIR / f"quality_report_{batch_id}.json"

        df = pd.read_csv(input_file)

        logger.info(f"Running quality gate on: {input_file}")
        logger.info(f"Batch ID: {batch_id}")
        logger.info(f"Rows: {len(df):,}")
        logger.info(f"Columns: {len(df.columns):,}")

        duplicate_cols = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicate_cols:
            raise ValueError(f"Input file has duplicate column names: {duplicate_cols}")

        quality_results = {
            "run_id": batch_id,
            "batch_id": batch_id,
            "input_file": str(input_file),
            "validated_at": datetime.now().isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "checks": {},
        }

        # 1. REQUIRED COLUMNS
        missing_columns = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        quality_results["checks"]["required_columns"] = {
            "status": "passed" if not missing_columns else "failed",
            "missing_columns": missing_columns,
        }

        # 2. ROW COUNT
        quality_results["checks"]["row_count"] = {
            "status": "passed" if len(df) > 0 else "failed",
            "row_count": len(df),
        }

        # 3. CRITICAL NULL CHECK
        null_counts = {
            col: int(df[col].isna().sum())
            for col in CRITICAL_NULL_COLUMNS
            if col in df.columns
        }
        failed_null_columns = {col: c for col, c in null_counts.items() if c > 0}
        quality_results["checks"]["critical_nulls"] = {
            "status": "passed" if not failed_null_columns else "failed",
            "null_counts": null_counts,
        }

        # 4. PSEUDONYMIZATION CHECKS
        if "customer_name" in df.columns:
            bad_customers = df[~df["customer_name"].astype(str).str.startswith("Customer CUST-")]
        else:
            bad_customers = df  # will already be caught by required_columns check
        if "product_description" in df.columns:
            bad_products = df[~df["product_description"].astype(str).str.contains(" Product ", na=False)]
        else:
            bad_products = df

        quality_results["checks"]["customer_pseudonymization"] = {
            "status": "passed" if len(bad_customers) == 0 else "failed",
            "bad_customer_rows": int(len(bad_customers)),
        }
        quality_results["checks"]["product_pseudonymization"] = {
            "status": "passed" if len(bad_products) == 0 else "failed",
            "bad_product_rows": int(len(bad_products)),
        }

        # 5. ROW HASH COMPLETENESS
        if "row_hash" in df.columns:
            missing_row_hash = int(df["row_hash"].isna().sum())
        else:
            missing_row_hash = len(df)
        quality_results["checks"]["row_hash_completeness"] = {
            "status": "passed" if missing_row_hash == 0 else "failed",
            "missing_row_hash": missing_row_hash,
        }

        # 6. DUPLICATE ROW HASH CHECK (warning, not fatal)
        if "row_hash" in df.columns:
            duplicate_row_hash_count = int(df["row_hash"].duplicated().sum())
        else:
            duplicate_row_hash_count = 0

        quality_results["checks"]["duplicate_row_hash"] = {
            "status": "warning" if duplicate_row_hash_count > 0 else "passed",
            "duplicate_row_hash_count": duplicate_row_hash_count,
        }

        if duplicate_row_hash_count > 0:
            duplicate_rows = df[df["row_hash"].duplicated(keep=False)].sort_values("row_hash")
            duplicate_output = QUALITY_DIR / f"duplicate_row_hashes_{batch_id}.csv"
            atomic_write_csv(duplicate_rows, duplicate_output)
            quality_results["checks"]["duplicate_row_hash"]["duplicate_file"] = str(duplicate_output)

        # 7. NEGATIVE NUMERIC CHECKS (warning, not fatal)
        negative_issues = {}
        for col in NUMERIC_COLUMNS_TO_CHECK:
            if col in df.columns:
                numeric_series = pd.to_numeric(df[col], errors="coerce")
                negative_count = int((numeric_series < 0).sum())
                if negative_count > 0:
                    negative_issues[col] = negative_count

        quality_results["checks"]["negative_numeric_values"] = {
            "status": "warning" if negative_issues else "passed",
            "negative_counts": negative_issues,
        }

        # --- FINAL STATUS ---
        failed_checks = [
            name for name, result in quality_results["checks"].items()
            if result["status"] == "failed"
        ]
        warning_checks = [
            name for name, result in quality_results["checks"].items()
            if result["status"] == "warning"
        ]

        if failed_checks:
            overall_status = "failed"
        elif warning_checks:
            overall_status = "passed_with_warnings"
        else:
            overall_status = "passed"

        quality_results["overall_status"] = overall_status
        quality_results["failed_checks"] = failed_checks
        quality_results["warning_checks"] = warning_checks

        atomic_write_json(quality_results, quality_report_file)
        logger.info(f"Quality report saved: {quality_report_file}")
        logger.info(f"Overall status: {overall_status}")

        if failed_checks:
            logger.error(f"Failed checks: {failed_checks}")
            raise ValueError(f"Data quality gate failed: {failed_checks}")

        if warning_checks:
            logger.warning(f"Warning checks: {warning_checks}")

        logger.info("Data quality gate completed.")
        return 0

    except Exception:
        logger.exception("Data quality gate failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())