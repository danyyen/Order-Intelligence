"""
src/ingestion/ingest_inventory.py

Stage: ingest the warehouse inventory snapshot Excel export into a
tagged CSV. Unlike the main AS400 export, this is a separate source
file entirely, with no intraday batch_id — inventory is a once-per-
snapshot business event, so it's dated (YYYYMMDD) rather than
timestamped to the minute. The filename must encode that date
(e.g. inventory_20250228.xlsx).

Data is expected to be pre-filtered to a single warehouse (currently
305) — that's captured as a constant tag, not detected from the data,
since the source file has no WAREHOUSE column at all.

Output contract:
    data/raw_csv/inventory_<snapshot_date>.csv
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import RAW_EXCEL_DIR, RAW_CSV_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("ingest_inventory")

WAREHOUSE_CODE = "305"  # data is pre-filtered to this warehouse only

SNAPSHOT_DATE_PATTERN = re.compile(r"(\d{8})")


def extract_snapshot_date(filepath: Path) -> str:
    match = SNAPSHOT_DATE_PATTERN.search(filepath.stem)
    if not match:
        raise ValueError(
            f"Could not find an 8-digit YYYYMMDD date in filename: {filepath.name}. "
            f"Rename the file to include the snapshot date, e.g. inventory_20250228.xlsx"
        )
    date_str = match.group(1)
    try:
        pd.to_datetime(date_str, format="%Y%m%d")
    except ValueError:
        raise ValueError(f"'{date_str}' extracted from filename is not a valid YYYYMMDD date.")
    return date_str


def find_input_file(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"Specified --file does not exist: {path}")
        return path

    candidates = [p for p in RAW_EXCEL_DIR.glob("inventory_*.xlsx") if not p.name.startswith("~$")]
    if not candidates:
        raise FileNotFoundError(
            f"No files matching 'inventory_*.xlsx' found in {RAW_EXCEL_DIR}."
        )
    if len(candidates) > 1:
        newest = max(candidates, key=lambda p: p.stat().st_mtime)
        logger.warning(
            f"Multiple inventory files found. Using most recently modified: "
            f"{newest.name}. Pass --file to be explicit."
        )
        return newest
    return candidates[0]


def atomic_write_csv(df: pd.DataFrame, output_file: Path) -> None:
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_csv(tmp_file, index=False, encoding="utf-8-sig")
    os.replace(tmp_file, output_file)


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest the warehouse inventory snapshot Excel export.")
    parser.add_argument("--file", default=None, help="Explicit path to the source inventory .xlsx file.")
    parser.add_argument("--force", action="store_true", help="Overwrite if this snapshot date was already ingested.")
    args = parser.parse_args()

    RAW_CSV_DIR.mkdir(parents=True, exist_ok=True)

    try:
        input_file = find_input_file(args.file)
        snapshot_date = extract_snapshot_date(input_file)
        logger.info(f"Source file: {input_file}")
        logger.info(f"Snapshot date: {snapshot_date}")

        output_file = RAW_CSV_DIR / f"inventory_{snapshot_date}.csv"
        if output_file.exists() and not args.force:
            raise RuntimeError(
                f"{output_file} already exists — this snapshot date has already "
                f"been ingested. Refusing to overwrite. Re-run with --force if "
                f"this is intentional."
            )

        INVENTORY_SHEET_NAME = "SDC inventory"
        workbook = pd.ExcelFile(input_file, engine="openpyxl")
        if INVENTORY_SHEET_NAME not in workbook.sheet_names:
            raise ValueError(
                f"Expected sheet '{INVENTORY_SHEET_NAME}' not found in "
                f"{input_file.name}. Sheets available in the workbook: "
                f"{workbook.sheet_names}"
            )

        df = pd.read_excel(input_file, sheet_name=INVENTORY_SHEET_NAME, engine="openpyxl")

        if df.empty:
            raise ValueError(f"{input_file.name} has 0 rows. Refusing to ingest an empty file.")

        df.columns = (
            df.columns.astype(str).str.strip().str.lower().str.replace(" ", "_")
        )

        duplicate_cols = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicate_cols:
            raise ValueError(f"Duplicate column names after cleaning: {duplicate_cols}")

        df["warehouse_code"] = WAREHOUSE_CODE
        df["snapshot_date"] = snapshot_date

        atomic_write_csv(df, output_file)

        logger.info(f"Exported {len(df):,} rows -> {output_file}")
        return 0

    except Exception:
        logger.exception("Inventory ingestion failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())