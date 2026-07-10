"""
src/quality/data_quality_gate_inventory.py

Quality gate for the latest pseudonymized inventory file. Uses
snapshot_date (not an intraday batch_id) as the linking identifier
between the pseudonymized file and its quality report, consistent with
inventory being a once-per-snapshot business event rather than a
transactional extract.

Output contract:
    data/quality_reports/quality_report_inventory_<snapshot_date>.json
    data/quality_reports/duplicate_row_hashes_inventory_<snapshot_date>.csv (if any)
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

SNAPSHOT_DATE_PATTERN = re.compile(r"(\d{8})")

REQUIRED_COLUMNS = [
    "warehouse_code",
    "snapshot_date",
    "full_sku_code",
    "product_description",
    "product_category",
    "quantity_on_hand",
    "row_hash",
    "pseudonymized_at",
]

CRITICAL_NULL_COLUMNS = ["full_sku_code", "product_description", "row_hash"]

NUMERIC_COLUMNS_TO_CHECK = ["reserved_quantity", "quantity_on_hand", "weight"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("data_quality_gate_inventory")


def extract_snapshot_date(filepath: Path) -> str:
    match = SNAPSHOT_DATE_PATTERN.search(filepath.name)
    if not match:
        raise ValueError(f"Could not find snapshot_date in filename: {filepath.name}")
    return match.group(1)


def find_input_file() -> Path:
    candidates = sorted(PSEUDONYMIZED_DIR.glob("inventory_pseudonymized_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No pseudonymized inventory files found in {PSEUDONYMIZED_DIR}. "
            "Run pseudonymize_inventory.py first."
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
        snapshot_date = extract_snapshot_date(input_file)
        quality_report_file = QUALITY_DIR / f"quality_report_inventory_{snapshot_date}.json"

        df = pd.read_csv(input_file)

        logger.info(f"Running quality gate on: {input_file}")
        logger.info(f"Snapshot date: {snapshot_date}")
        logger.info(f"Rows: {len(df):,}")
        logger.info(f"Columns: {len(df.columns):,}")

        duplicate_cols = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicate_cols:
            raise ValueError(f"Input file has duplicate column names: {duplicate_cols}")

        quality_results = {
            "run_id": snapshot_date,
            "snapshot_date": snapshot_date,
            "dataset": "inventory",
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

        null_counts = {col: int(df[col].isna().sum()) for col in CRITICAL_NULL_COLUMNS if col in df.columns}
        failed_null_columns = {col: c for col, c in null_counts.items() if c > 0}
        quality_results["checks"]["critical_nulls"] = {
            "status": "passed" if not failed_null_columns else "failed",
            "null_counts": null_counts,
        }

        bad_products = (
            df[~df["product_description"].astype(str).str.contains(" Product ", na=False)]
            if "product_description" in df.columns else df
        )
        bad_skus = (
            df[~df["full_sku_code"].astype(str).str.startswith("SKU-")]
            if "full_sku_code" in df.columns else df
        )
        quality_results["checks"]["product_pseudonymization"] = {
            "status": "passed" if len(bad_products) == 0 else "failed",
            "bad_product_rows": int(len(bad_products)),
        }
        quality_results["checks"]["sku_pseudonymization"] = {
            "status": "passed" if len(bad_skus) == 0 else "failed",
            "bad_sku_rows": int(len(bad_skus)),
        }

        missing_row_hash = int(df["row_hash"].isna().sum()) if "row_hash" in df.columns else len(df)
        quality_results["checks"]["row_hash_completeness"] = {
            "status": "passed" if missing_row_hash == 0 else "failed",
            "missing_row_hash": missing_row_hash,
        }

        # Duplicates WITHIN one snapshot are worth a warning; duplicates
        # ACROSS different snapshot dates are normal and expected (this
        # check only ever examines one file/date at a time, so it's
        # correctly scoped).
        duplicate_row_hash_count = int(df["row_hash"].duplicated().sum()) if "row_hash" in df.columns else 0
        quality_results["checks"]["duplicate_row_hash"] = {
            "status": "warning" if duplicate_row_hash_count > 0 else "passed",
            "duplicate_row_hash_count": duplicate_row_hash_count,
        }
        if duplicate_row_hash_count > 0:
            duplicate_rows = df[df["row_hash"].duplicated(keep=False)].sort_values("row_hash")
            duplicate_output = QUALITY_DIR / f"duplicate_row_hashes_inventory_{snapshot_date}.csv"
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

        overall_status = "failed" if failed_checks else ("passed_with_warnings" if warning_checks else "passed")
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