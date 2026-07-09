"""
src/ingestion/excel_to_csv_pipeline.py

Stage 1: Ingest the raw Legacy Excel export into per-sheet CSVs, tagged
with a batch_id, for the rest of the pipeline to consume.

Output contract (unchanged from the original version — nothing downstream
needs to change):
    data/raw_csv/order_history_<batch_id>.csv
    data/raw_csv/open_orders_<batch_id>.csv
    data/metadata/batch_metadata_<batch_id>.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import RAW_EXCEL_DIR, RAW_CSV_DIR, METADATA_DIR

SHEETS = {
    "Order History": "order_history",
    "Open Orders": "open_orders",
}

MANIFEST_FILE = METADATA_DIR / "ingestion_manifest.json"


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("excel_to_csv_pipeline")


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def file_hash(path: Path) -> str:
    hasher = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def find_input_file(explicit_path: str | None) -> Path:
    """
    Resolve the Excel file to ingest. Prefer an explicit --file argument;
    otherwise auto-detect the newest .xlsx in RAW_EXCEL_DIR, ignoring Excel
    lock files (~$...) so a currently-open workbook isn't picked up.
    """
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"Specified --file does not exist: {path}")
        return path

    candidates = [
        p for p in RAW_EXCEL_DIR.glob("*.xlsx")
        if not p.name.startswith("~$")
    ]

    if not candidates:
        raise FileNotFoundError(
            f"No .xlsx files found in {RAW_EXCEL_DIR}. "
            "Place the source export there or pass --file explicitly."
        )

    if len(candidates) > 1:
        newest = max(candidates, key=lambda p: p.stat().st_mtime)
        others = [p.name for p in candidates if p != newest]
        logger.warning(
            f"Multiple .xlsx files found in {RAW_EXCEL_DIR}. "
            f"Using most recently modified: {newest.name}. "
            f"Ignored: {others}. Pass --file to be explicit."
        )
        return newest

    return candidates[0]


def load_manifest() -> list[dict]:
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_manifest(manifest: list[dict]) -> None:
    with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def check_already_ingested(source_hash: str, manifest: list[dict], force: bool) -> None:
    """
    Idempotency guard: refuse to re-ingest a byte-identical source file
    unless --force is passed. Without this, re-running the pipeline on the
    same export creates a second batch of identical rows that flows all
    the way through pseudonymization as "new" data.
    """
    prior = [m for m in manifest if m["source_file_hash"] == source_hash]
    if prior and not force:
        prior_batches = [m["batch_id"] for m in prior]
        raise RuntimeError(
            f"This exact source file has already been ingested "
            f"(batch_id(s): {prior_batches}). Refusing to re-ingest the same "
            f"file to avoid duplicate downstream data. If this is intentional "
            f"(e.g. reprocessing after a downstream failure), re-run with --force."
        )
    if prior and force:
        logger.warning(
            f"Source file was previously ingested as batch_id(s) {[m['batch_id'] for m in prior]}, "
            f"but --force was passed. Proceeding anyway."
        )


def clean_columns(columns: pd.Index) -> pd.Index:
    return (
        columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
        .str.replace("-", "_")
    )


def atomic_write_csv(df: pd.DataFrame, output_file: Path) -> None:
    """Write to a temp file first, then rename, so a crash mid-write never
    leaves a corrupt file at the final, downstream-visible path."""
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_csv(tmp_file, index=False, encoding="utf-8-sig")
    os.replace(tmp_file, output_file)  # atomic on both Windows and POSIX


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest Excel export into raw CSVs.")
    parser.add_argument("--file", default=None, help="Explicit path to the source .xlsx file.")
    parser.add_argument(
        "--force", action="store_true",
        help="Re-ingest even if this exact file was already ingested before.",
    )
    args = parser.parse_args()

    RAW_CSV_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        excel_path = find_input_file(args.file)
        logger.info(f"Source file: {excel_path}")

        source_hash = file_hash(excel_path)
        manifest = load_manifest()
        check_already_ingested(source_hash, manifest, args.force)

        # Validate expected sheets exist before processing anything
        workbook = pd.ExcelFile(excel_path, engine="openpyxl")
        missing_sheets = [s for s in SHEETS if s not in workbook.sheet_names]
        if missing_sheets:
            raise ValueError(
                f"Expected sheet(s) not found in {excel_path.name}: {missing_sheets}. "
                f"Sheets available in the workbook: {workbook.sheet_names}"
            )

        metadata = {
            "batch_id": batch_id,
            "source_file": str(excel_path),
            "source_file_hash": source_hash,
            "created_at": datetime.now().isoformat(),
            "sheets": {},
        }

        for sheet_name, output_name in SHEETS.items():
            logger.info(f"Reading sheet: {sheet_name}")

            df = pd.read_excel(excel_path, sheet_name=sheet_name, engine="openpyxl")

            if df.empty:
                raise ValueError(
                    f"Sheet '{sheet_name}' in {excel_path.name} has 0 rows. "
                    "Refusing to ingest an empty sheet — check the source export."
                )

            df.columns = clean_columns(df.columns.astype(str))

            duplicate_cols = df.columns[df.columns.duplicated()].unique().tolist()
            if duplicate_cols:
                raise ValueError(
                    f"Sheet '{sheet_name}' produced duplicate column names after "
                    f"cleaning: {duplicate_cols}. Check the source header row."
                )

            df["batch_id"] = batch_id
            df["ingested_at"] = datetime.now().isoformat()

            output_file = RAW_CSV_DIR / f"{output_name}_{batch_id}.csv"
            atomic_write_csv(df, output_file)

            metadata["sheets"][sheet_name] = {
                "output_file": str(output_file),
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns),
            }

            logger.info(f"Exported {len(df):,} rows -> {output_file}")

        duration = time.time() - start_time
        metadata["duration_seconds"] = round(duration, 2)

        metadata_file = METADATA_DIR / f"batch_metadata_{batch_id}.json"
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=4)
        logger.info(f"Metadata saved -> {metadata_file}")

        manifest.append({
            "batch_id": batch_id,
            "source_file": str(excel_path),
            "source_file_hash": source_hash,
            "ingested_at": metadata["created_at"],
        })
        save_manifest(manifest)

        logger.info(f"Completed in {duration / 60:.2f} minutes")
        return 0

    except Exception:
        logger.exception("Ingestion failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())