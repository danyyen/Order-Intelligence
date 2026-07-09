"""
src/privacy/profile_customers.py

Stage: build the current universe of unique customer *versions*
(customer_mapping_base.csv) from the latest standardized order history
file.

SCD Type 2 design:
- customer_hash_key: DURABLE identity, built from source_customer_code +
  ship_to_customer_code only. Never changes for a given real customer.
- customer_version_key: built from source_customer_code +
  ship_to_customer_code + customer_name. Changes whenever the customer's
  name changes — that's what customer_mapping_pipeline.py uses to detect
  a new version needing a new SCD2 row.
- first_observed_date: the earliest order_date seen for this exact
  version, used as effective_date downstream. This is a real business
  date, not a pipeline-processing timestamp.

Output contract:
    data/metadata/customer_mapping_base.csv
        columns: customer_version_key, customer_hash_key,
                 source_customer_code, ship_to_customer_code,
                 customer_name, first_observed_date
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
REQUIRED_COLUMNS = IDENTITY_COLUMNS + ["order_date"]


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


def md5_of(*parts: str) -> str:
    return hashlib.md5("|".join(parts).encode()).hexdigest()


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

        missing_columns = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        duplicate_input_cols = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicate_input_cols:
            raise ValueError(f"Input file has duplicate column names: {duplicate_input_cols}")

        # A blank/NaN value in either identifying field would otherwise
        # become the literal string "nan" after astype(str), silently
        # merging unrelated customers into a single hashed identity.
        rows_with_null_identity = df[
            ["source_customer_code", "ship_to_customer_code"]
        ].isna().any(axis=1).sum()
        if rows_with_null_identity > 0:
            raise ValueError(
                f"{rows_with_null_identity:,} row(s) have a null "
                f"source_customer_code or ship_to_customer_code, which would "
                f"corrupt customer hashing if allowed through."
            )
        logger.info("Customer identity null check passed.")

        for col in IDENTITY_COLUMNS:
            df[col] = df[col].astype(str).str.strip().str.upper()

        # order_date arrives as a legacy-style integer, e.g. 20260101.
        parsed_dates = pd.to_datetime(df["order_date"], format="%Y%m%d", errors="coerce")
        unparseable = parsed_dates.isna().sum()
        if unparseable > 0:
            raise ValueError(
                f"{unparseable:,} row(s) have an order_date that could not be "
                f"parsed as YYYYMMDD. Check the source data before continuing."
            )
        df["_order_date_parsed"] = parsed_dates

        df["customer_hash_key"] = df.apply(
            lambda r: md5_of(r["source_customer_code"], r["ship_to_customer_code"]), axis=1
        )
        df["customer_version_key"] = df.apply(
            lambda r: md5_of(r["source_customer_code"], r["ship_to_customer_code"], r["customer_name"]),
            axis=1,
        )

        customer_base = (
            df.groupby("customer_version_key")
            .agg(
                customer_hash_key=("customer_hash_key", "first"),
                source_customer_code=("source_customer_code", "first"),
                ship_to_customer_code=("ship_to_customer_code", "first"),
                customer_name=("customer_name", "first"),
                first_observed_date=("_order_date_parsed", "min"),
            )
            .reset_index()
            .sort_values("customer_version_key")
        )

        customer_base["first_observed_date"] = customer_base["first_observed_date"].dt.strftime("%Y-%m-%d")

        output_file = METADATA_DIR / "customer_mapping_base.csv"
        atomic_write_csv(customer_base, output_file)

        logger.info(f"Exported {len(customer_base):,} unique customer version(s)")
        logger.info(f"Distinct customer identities (customer_hash_key): {customer_base['customer_hash_key'].nunique():,}")
        logger.info(str(output_file))

        return 0

    except Exception:
        logger.exception("Customer profiling failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())