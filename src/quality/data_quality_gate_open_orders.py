"""
src/quality/data_quality_gate_open_orders.py

Quality gate for the latest pseudonymized open orders file — mirrors
data_quality_gate.py (order history), with a required-column and
critical-null list matching open orders' own schema.

Same batch-id linkage principle as the order history gate: the report's
filename MUST match the batch_id already embedded in the open orders
pseudonymized file's name, since upload_open_orders_to_s3.py depends on
that to confirm it's uploading a report that actually belongs to the
file it's uploading.

Output contract:
    data/quality_reports/quality_report_open_orders_<batch_id>.json
    data/quality_reports/duplicate_row_hashes_open_orders_<batch_id>.csv (if any)
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
    "source_customer_code",
    "ship_to_customer_code",
    "customer_name",
    "purchase_order_number",
    "order_number",
    "order_type",
    "order_date",
    "scheduled_ship_date",
    "order_status",
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
    "row_hash",
]

NUMERIC_COLUMNS_TO_CHECK = [
    "final_order_quantity",
    "shipped_order_quantity",
    "estimated_order_weight",
    "shipped_order_weight",
    "order_amount",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("data_quality_gate_open_orders")


def extract_batch_id(filepath: Path) -> str:
    match = BATCH_ID_PATTERN.search(filepath.name)
    if not match:
        raise ValueError(f"Could not find batch_id in filename: {filepath.name}")
    return match.group(1)


def find_input_file() -> Path:
    candidates = sorted(PSEUDONYMIZED_DIR.glob("open_orders_pseudonymized_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No pseudonymized open orders files found in {PSEUDONYMIZED_DIR}. "
            "Run pseudonymize_open_orders.py first."
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


def main() -> int:
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)

    try:
        input_file = find_input_file()
        batch_id = extract_batch_id(input_file)
        quality_report_file = QUALITY_DIR / f"quality_report_open_orders_{batch_id}.json"

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
            "dataset": "open_orders",
            "input_file": str(input_file),
            "validated_at": datetime.now().isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "checks": {},
        }

        missing_columns = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        quality_results["checks"]["required_columns"] = {
            "status": "passed" if not missing_columns else "failed",
            "missing_columns": missing_columns,
        }

        quality_results["checks"]["row_count"] = {
            "status": "passed" if len(df) > 0 else "failed",
            "row_count": len(df),
        }

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

        bad_customers = df[~df["customer_name"].astype(str).str.startswith("Customer CUST-")] if "customer_name" in df.columns else df
        bad_products = df[~df["product_description"].astype(str).str.contains(" Product ", na=False)] if "product_description" in df.columns else df

        quality_results["checks"]["customer_pseudonymization"] = {
            "status": "passed" if len(bad_customers) == 0 else "failed",
            "bad_customer_rows": int(len(bad_customers)),
        }
        quality_results["checks"]["product_pseudonymization"] = {
            "status": "passed" if len(bad_products) == 0 else "failed",
            "bad_product_rows": int(len(bad_products)),
        }

        missing_row_hash = int(df["row_hash"].isna().sum()) if "row_hash" in df.columns else len(df)
        quality_results["checks"]["row_hash_completeness"] = {
            "status": "passed" if missing_row_hash == 0 else "failed",
            "missing_row_hash": missing_row_hash,
        }

        duplicate_row_hash_count = int(df["row_hash"].duplicated().sum()) if "row_hash" in df.columns else 0
        quality_results["checks"]["duplicate_row_hash"] = {
            "status": "warning" if duplicate_row_hash_count > 0 else "passed",
            "duplicate_row_hash_count": duplicate_row_hash_count,
        }

        if duplicate_row_hash_count > 0:
            duplicate_rows = df[df["row_hash"].duplicated(keep=False)].sort_values("row_hash")
            duplicate_output = QUALITY_DIR / f"duplicate_row_hashes_open_orders_{batch_id}.csv"
            atomic_write_csv(duplicate_rows, duplicate_output)
            quality_results["checks"]["duplicate_row_hash"]["duplicate_file"] = str(duplicate_output)

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

        failed_checks = [n for n, r in quality_results["checks"].items() if r["status"] == "failed"]
        warning_checks = [n for n, r in quality_results["checks"].items() if r["status"] == "warning"]

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