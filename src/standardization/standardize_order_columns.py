"""
src/standardization/standardize_order_columns.py

Stage 2: Rename raw order column codes to meaningful names using
config/column_mapping.py, and validate that no unrecognized (i.e. new,
unmapped) columns have appeared in the source data.

Output contract (unchanged):
    data/standardized_csv/order_history_standardized_<run_date>.csv
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Resolve the project root relative to this file's location, instead of a
# hardcoded machine-specific path, so this works regardless of who runs it
# or where the project folder lives.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import RAW_CSV_DIR, STANDARDIZED_DIR
from config.column_mapping import COLUMN_MAPPING


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("standardize_order_columns")


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def find_input_file(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"Specified --file does not exist: {path}")
        return path

    candidates = sorted(RAW_CSV_DIR.glob("order_history_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No files matching 'order_history_*.csv' found in {RAW_CSV_DIR}. "
            "Run the ingestion stage first."
        )
    return candidates[-1]


def check_schema_drift(columns: list[str], mapping: dict) -> None:
    """
    Every real source column should have an explicit entry in
    COLUMN_MAPPING (including passthrough columns like batch_id, which are
    already mapped to themselves). If a column shows up that isn't in the
    mapping, that's a new/changed field — fail loudly instead of
    silently passing an unrecognized column name downstream.
    """
    unmapped = sorted(set(columns) - set(mapping.keys()))
    if unmapped:
        raise ValueError(
            f"Unrecognized source column(s) found: {unmapped}. "
            "These are not in config/column_mapping.py. This usually means "
            "the export gained a new field. Add it to COLUMN_MAPPING "
            "(even as a passthrough) before re-running."
        )

    missing_from_source = sorted(set(mapping.keys()) - set(columns))
    if missing_from_source:
        logger.warning(
            f"Columns present in COLUMN_MAPPING but absent from this "
            f"source file: {missing_from_source}. Not fatal, but worth "
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
    parser = argparse.ArgumentParser(description="Standardize raw order history column names.")
    parser.add_argument("--file", default=None, help="Explicit path to a raw order_history CSV.")
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

        check_schema_drift(list(df.columns), COLUMN_MAPPING)

        df = df.rename(columns=COLUMN_MAPPING)

        duplicate_cols = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicate_cols:
            raise ValueError(
                f"Column mapping produced duplicate output column names: "
                f"{duplicate_cols}. Check config/column_mapping.py for two "
                f"raw columns mapping to the same target name."
            )

        df["standardized_at"] = datetime.now().isoformat()

        run_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = STANDARDIZED_DIR / f"order_history_standardized_{run_date}.csv"

        atomic_write_csv(df, output_file)

        logger.info("STANDARDIZATION COMPLETE")
        logger.info(f"Rows: {len(df):,}")
        logger.info(f"Columns: {len(df.columns):,}")
        logger.info(f"Saved to: {output_file}")
        logger.info(f"New column names: {df.columns.to_list()}")

        return 0

    except Exception:
        logger.exception("Standardization failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())