"""
src/privacy/validate_pseudonymize_open_orders.py

Non-critical secondary sanity check on the latest pseudonymized open
orders file — mirrors validate_pseudonymize_order_history.py.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import PSEUDONYMIZED_DIR, METADATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("validate_pseudonymize_open_orders")

REQUIRED_COLUMNS = [
    "customer_name",
    "product_description",
    "product_category",
    "row_hash",
    "pseudonymized_at",
]

MAX_DUPLICATES_TO_PRINT = 20


def find_input_file(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"Specified --file does not exist: {path}")
        return path

    candidates = sorted(PSEUDONYMIZED_DIR.glob("open_orders_pseudonymized_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No files matching 'open_orders_pseudonymized_*.csv' found in "
            f"{PSEUDONYMIZED_DIR}. Run pseudonymize_open_orders.py first."
        )
    return candidates[-1]


def atomic_write_csv(df: pd.DataFrame, output_file: Path) -> None:
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_csv(tmp_file, index=False, encoding="utf-8-sig")
    os.replace(tmp_file, output_file)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the latest pseudonymized open orders file.")
    parser.add_argument("--file", default=None, help="Explicit path to a pseudonymized open orders CSV.")
    args = parser.parse_args()

    try:
        input_file = find_input_file(args.file)
        logger.info(f"Validating file: {input_file}")

        df = pd.read_csv(input_file)

        if df.empty:
            raise ValueError(f"{input_file.name} has 0 rows. Nothing to validate.")

        logger.info(f"Rows: {len(df):,}")
        logger.info(f"Columns: {len(df.columns):,}")

        duplicate_cols = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicate_cols:
            raise ValueError(f"Input file has duplicate column names: {duplicate_cols}")

        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        bad_customers = df[~df["customer_name"].astype(str).str.startswith("Customer CUST-")]
        if len(bad_customers) > 0:
            raise ValueError(f"Non-pseudonymized customer names found: {len(bad_customers):,}")

        bad_products = df[~df["product_description"].astype(str).str.contains(" Product ", na=False)]
        if len(bad_products) > 0:
            raise ValueError(f"Non-pseudonymized product descriptions found: {len(bad_products):,}")

        missing_hash = df["row_hash"].isna().sum()
        if missing_hash > 0:
            raise ValueError(f"Missing row_hash values: {missing_hash:,}")

        duplicate_hash_count = df["row_hash"].duplicated().sum()
        logger.info(f"Duplicate row_hash count: {duplicate_hash_count:,}")

        if duplicate_hash_count > 0:
            duplicates = df[df["row_hash"].duplicated(keep=False)].sort_values("row_hash")
            dup_file = METADATA_DIR / f"validation_duplicate_row_hashes_{input_file.stem}.csv"
            atomic_write_csv(duplicates, dup_file)
            logger.warning(f"Duplicate rows saved for inspection: {dup_file}")

            if len(duplicates) > MAX_DUPLICATES_TO_PRINT:
                logger.warning(f"Showing first {MAX_DUPLICATES_TO_PRINT} of {len(duplicates):,} duplicate rows:")
                logger.warning(f"\n{duplicates.head(MAX_DUPLICATES_TO_PRINT)}")
            else:
                logger.warning(f"\n{duplicates}")

        logger.info("Pseudonymized open orders validation passed.")
        return 0

    except Exception:
        logger.exception("Pseudonymized open orders validation failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())