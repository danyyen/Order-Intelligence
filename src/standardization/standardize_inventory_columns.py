"""
src/standardization/standardize_inventory_columns.py

Stage: rename inventory's already-readable column headers to the
pipeline's naming convention, and validate no unrecognized column has
appeared (schema drift), same principle as standardize_order_columns.py.

Output contract:
    data/standardized_csv/inventory_standardized_<snapshot_date>.csv
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

from config.paths import RAW_CSV_DIR, STANDARDIZED_DIR
from config.column_mapping_inventory import COLUMN_MAPPING_INVENTORY, DROP_COLUMNS_INVENTORY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("standardize_inventory_columns")

# Columns added by the ingestion stage itself, not part of the raw source
# header — always recognized, need no explicit mapping entry.
PASSTHROUGH_COLUMNS = {"warehouse_code", "snapshot_date"}


def find_input_file(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"Specified --file does not exist: {path}")
        return path

    candidates = sorted(RAW_CSV_DIR.glob("inventory_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No files matching 'inventory_*.csv' found in {RAW_CSV_DIR}. "
            "Run ingest_inventory.py first."
        )
    return candidates[-1]


def check_schema_drift(columns: list[str]) -> None:
    recognized = set(COLUMN_MAPPING_INVENTORY.keys()) | set(DROP_COLUMNS_INVENTORY) | PASSTHROUGH_COLUMNS
    unmapped = sorted(set(columns) - recognized)
    if unmapped:
        raise ValueError(
            f"Unrecognized inventory column(s) found: {unmapped}. Add them to "
            f"config/column_mapping_inventory.py before re-running."
        )


def atomic_write_csv(df: pd.DataFrame, output_file: Path) -> None:
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_csv(tmp_file, index=False, encoding="utf-8-sig")
    os.replace(tmp_file, output_file)


def main() -> int:
    parser = argparse.ArgumentParser(description="Standardize inventory column names.")
    parser.add_argument("--file", default=None, help="Explicit path to a raw inventory CSV.")
    args = parser.parse_args()

    STANDARDIZED_DIR.mkdir(parents=True, exist_ok=True)

    try:
        input_file = find_input_file(args.file)
        logger.info(f"Reading: {input_file}")

        df = pd.read_csv(input_file)

        if df.empty:
            raise ValueError(f"{input_file.name} has 0 rows. Refusing to standardize an empty file.")

        df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(" ", "_")

        check_schema_drift(list(df.columns))

        if DROP_COLUMNS_INVENTORY:
            df = df.drop(columns=[c for c in DROP_COLUMNS_INVENTORY if c in df.columns])

        df = df.rename(columns=COLUMN_MAPPING_INVENTORY)

        duplicate_cols = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicate_cols:
            raise ValueError(f"Column mapping produced duplicate column names: {duplicate_cols}")

        if "snapshot_date" not in df.columns:
            raise ValueError("snapshot_date column missing — was this file produced by ingest_inventory.py?")
        snapshot_date = str(df["snapshot_date"].iloc[0])

        output_file = STANDARDIZED_DIR / f"inventory_standardized_{snapshot_date}.csv"
        atomic_write_csv(df, output_file)

        logger.info("STANDARDIZATION COMPLETE")
        logger.info(f"Rows: {len(df):,}")
        logger.info(f"Columns: {len(df.columns):,}")
        logger.info(f"Saved to: {output_file}")

        return 0

    except Exception:
        logger.exception("Inventory standardization failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())