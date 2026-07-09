"""
src/standardization/standardize_open_orders_columns.py

Rename raw column codes to meaningful names for the Open Orders
sheet, using config/column_mapping_open_orders.py. Validates that no
unrecognized (new, unmapped) columns have appeared, and explicitly drops
the columns confirmed not needed (see DROP_COLUMNS_OPEN_ORDERS).

Output contract:
    data/standardized_csv/open_orders_standardized_<run_date>.csv
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import RAW_CSV_DIR, STANDARDIZED_DIR
from config.column_mapping_open_orders import COLUMN_MAPPING_OPEN_ORDERS, DROP_COLUMNS_OPEN_ORDERS


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("standardize_open_orders_columns")


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def find_input_file(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"Specified --file does not exist: {path}")
        return path

    candidates = sorted(RAW_CSV_DIR.glob("open_orders_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No files matching 'open_orders_*.csv' found in {RAW_CSV_DIR}. "
            "Run the ingestion stage first."
        )
    return candidates[-1]


def check_schema_drift(columns: list[str], mapping: dict, drop_list: list[str]) -> None:
    """
    Every real source column should either have an explicit entry in
    COLUMN_MAPPING_OPEN_ORDERS (including passthrough columns like
    batch_id) or be explicitly listed in DROP_COLUMNS_OPEN_ORDERS. A
    column that's neither is a new/changed field — fail loudly
    instead of silently dropping or silently passing it through.
    """
    known = set(mapping.keys()) | set(drop_list)
    unrecognized = sorted(set(columns) - known)
    if unrecognized:
        raise ValueError(
            f"Unrecognized source column(s) found: {unrecognized}. These are "
            f"not in config/column_mapping_open_orders.py (mapped or dropped). "
            f"This usually means the export gained a new field. Add it "
            f"to COLUMN_MAPPING_OPEN_ORDERS or DROP_COLUMNS_OPEN_ORDERS "
            f"before re-running."
        )

    missing_from_source = sorted(set(mapping.keys()) - set(columns))
    if missing_from_source:
        logger.warning(
            f"Columns present in COLUMN_MAPPING_OPEN_ORDERS but absent from "
            f"this source file: {missing_from_source}. Not fatal, but worth "
            f"confirming the source export hasn't dropped a field."
        )


def atomic_write_csv(df: pd.DataFrame, output_file: Path) -> None:
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_csv(tmp_file, index=False, encoding="utf-8-sig")
    os.replace(tmp_file, output_file)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Standardize raw open orders column names.")
    parser.add_argument("--file", default=None, help="Explicit path to a raw open_orders CSV.")
    args = parser.parse_args()

    STANDARDIZED_DIR.mkdir(parents=True, exist_ok=True)

    try:
        latest_file = find_input_file(args.file)
        logger.info(f"Reading: {latest_file}")

        df = pd.read_csv(latest_file, low_memory=False)

        if df.empty:
            raise ValueError(
                f"{latest_file.name} has 0 rows. Refusing to standardize an "
                "empty file — check the ingestion stage output."
            )

        df.columns = df.columns.astype(str).str.strip().str.lower()

        check_schema_drift(list(df.columns), COLUMN_MAPPING_OPEN_ORDERS, DROP_COLUMNS_OPEN_ORDERS)

        present_drops = [c for c in DROP_COLUMNS_OPEN_ORDERS if c in df.columns]
        if present_drops:
            df = df.drop(columns=present_drops)
            logger.info(f"Dropped columns (confirmed not needed): {present_drops}")

        df = df.rename(columns=COLUMN_MAPPING_OPEN_ORDERS)

        duplicate_cols = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicate_cols:
            raise ValueError(
                f"Column mapping produced duplicate output column names: "
                f"{duplicate_cols}. Check config/column_mapping_open_orders.py "
                f"for two raw columns mapping to the same target name."
            )

        df["standardized_at"] = datetime.now().isoformat()

        run_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = STANDARDIZED_DIR / f"open_orders_standardized_{run_date}.csv"

        atomic_write_csv(df, output_file)

        logger.info("STANDARDIZATION COMPLETE")
        logger.info(f"Rows: {len(df):,}")
        logger.info(f"Columns: {len(df.columns):,}")
        logger.info(f"Saved to: {output_file}")
        logger.info(f"Column names: {df.columns.to_list()}")

        return 0

    except Exception:
        logger.exception("Open orders standardization failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())