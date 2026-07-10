"""
src/privacy/profile_customers.py

Stage: build the current universe of unique customers
(customer_mapping_base.csv) from the latest standardized order history
file, keyed on a hashed composite of source_customer_code,
ship_to_customer_code, and customer_name.

Output contract (unchanged):
    data/metadata/customer_mapping_base.csv
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import STANDARDIZED_DIR, METADATA_DIR


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("profile_customers")

IDENTITY_COLUMNS = ["source_customer_code", "ship_to_customer_code", "customer_name"]


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def find_input_file(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"Specified --file does not exist: {path}")
        return path

    candidates = sorted(STANDARDIZED_DIR.glob("order_history_standardized_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No files matching 'order_history_standardized_*.csv' found in "
            f"{STANDARDIZED_DIR}. Run the standardization stage first."
        )
    return candidates[-1]


def atomic_write_csv(df: pd.DataFrame, output_file: Path) -> None:
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_csv(tmp_file, index=False, encoding="utf-8-sig")
    os.replace(tmp_file, output_file)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Build the customer mapping base file.")
    parser.add_argument("--file", default=None, help="Explicit path to a standardized order history CSV.")
    args = parser.parse_args()

    METADATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        input_file = find_input_file(args.file)
        logger.info(f"Reading: {input_file}")

        df = pd.read_csv(input_file)

        if df.empty:
            raise ValueError(
                f"{input_file.name} has 0 rows. Refusing to build a customer "
                "profile from an empty file."
            )

        logger.info(f"Loaded rows: {len(df):,}")

        missing_columns = [c for c in IDENTITY_COLUMNS if c not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        duplicate_input_cols = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicate_input_cols:
            raise ValueError(f"Input file has duplicate column names: {duplicate_input_cols}")

        # A blank/NaN value in any identity field would otherwise become the
        # literal string "nan" after astype(str), silently merging unrelated
        # customers with missing data into a single hashed identity. Fail
        # loudly instead so the source data issue gets fixed at the root.
        null_counts = df[IDENTITY_COLUMNS].isna().sum()
        rows_with_null_identity = df[IDENTITY_COLUMNS].isna().any(axis=1).sum()
        if rows_with_null_identity > 0:
            raise ValueError(
                f"{rows_with_null_identity:,} row(s) have a null value in one "
                f"or more customer identity fields, which would corrupt "
                f"customer hashing if allowed through:\n{null_counts}"
            )
        logger.info("Customer identity null check passed.")

        for col in IDENTITY_COLUMNS:
            df[col] = df[col].astype(str).str.strip().str.upper()

        df["customer_business_key"] = (
            df["source_customer_code"]
            + "|"
            + df["ship_to_customer_code"]
            + "|"
            + df["customer_name"]
        )

        df["customer_hash_key"] = df["customer_business_key"].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest()
        )

        customer_base = (
            df[[
                "customer_hash_key",
                "source_customer_code",
                "ship_to_customer_code",
                "customer_name",
            ]]
            .drop_duplicates(subset=["customer_hash_key"])
            .sort_values("customer_hash_key")
        )

        output_file = METADATA_DIR / "customer_mapping_base.csv"
        atomic_write_csv(customer_base, output_file)

        logger.info(f"Exported {len(customer_base):,} unique customer records")
        logger.info(str(output_file))

        return 0

    except Exception:
        logger.exception("Customer profiling failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())