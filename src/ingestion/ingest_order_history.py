"""
src/ingestion/ingest_order_history.py

Stage: ingest the Order History sheet from the raw legacy ERP Excel
export into a tagged CSV. This is the first stage of the shared
foundation — the customer/product/SKU mapping stages depend on this
sheet specifically, so a failure here correctly stops the entire
pipeline (see run_pipeline.py).

Split out from the original excel_to_csv_pipeline.py, which used to
read both the Order History and Open Orders sheets in one pass: a
broken Open Orders sheet was able to fail Order History ingestion too,
and from there take down every downstream track, including inventory,
which doesn't even read this workbook. Open Orders now has its own
independent ingestion stage, ingest_open_orders.py, living in the
open_orders track instead of shared.

Output contract (unchanged from the original combined version):
    data/raw_csv/order_history_<batch_id>.csv
    data/metadata/batch_metadata_order_history_<batch_id>.json
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

SHEET_NAME = "Order History"
OUTPUT_NAME = "order_history"

MANIFEST_FILE = METADATA_DIR / "ingestion_manifest_order_history.json"


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("ingest_order_history")


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
    otherwise auto-detect the newest matching .xlsx in RAW_EXCEL_DIR.

    Only files starting with "orH" are considered, matching the naming
    convention already in use for this source. With inventory's workbook
    also living in the same folder, an unrestricted "any .xlsx" match
    would be ambiguous whenever both are present at once, which is the
    normal case for synchronized snapshot capture.
    """
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"Specified --file does not exist: {path}")
        return path

    candidates = [
        p for p in RAW_EXCEL_DIR.glob("orH*.xlsx")
        if not p.name.startswith("~$")
    ]

    if not candidates:
        raise FileNotFoundError(
            f"No files matching 'orH*.xlsx' found in {RAW_EXCEL_DIR}. "
            "Place the source export there (named starting with 'orH'), "
            "or pass --file explicitly."
        )

    if len(candidates) > 1:
        newest = max(candidates, key=lambda p: p.stat().st_mtime)
        others = [p.name for p in candidates if p != newest]
        logger.warning(
            f"Multiple orH*.xlsx files found in {RAW_EXCEL_DIR}. "
            f"Using most recently modified: {newest.name}. "
            f"Ignored: {others}. Pass --file to be explicit, or archive "
            f"the old one to avoid this warning."
        )
        return newest

    return candidates[0]


def load_manifest() -> list[dict]:
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_manifest(manifest: list[dict]) -> None:
    """Write to a temp file first, then rename — see atomic_write_csv's
    docstring for why. A partially-written manifest is worse than a
    stale one: check_already_ingested relies on it being trustworthy to
    prevent a retry from silently creating a duplicate batch."""
    tmp_file = MANIFEST_FILE.with_suffix(MANIFEST_FILE.suffix + ".tmp")
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    os.replace(tmp_file, MANIFEST_FILE)


def check_already_ingested(source_hash: str, manifest: list[dict], force: bool) -> None:
    """
    Idempotency guard: refuse to re-ingest a byte-identical source file
    unless --force is passed. Keyed against this stage's own manifest
    only (ingestion_manifest_order_history.json) — Open Orders ingests
    from the same physical workbook independently and keeps its own
    manifest, so ingesting one sheet never blocks or falsely
    "already ingested" the other.
    """
    prior = [m for m in manifest if m["source_file_hash"] == source_hash]
    if prior and not force:
        prior_batches = [m["batch_id"] for m in prior]
        raise RuntimeError(
            f"This exact source file has already been ingested for Order "
            f"History (batch_id(s): {prior_batches}). Refusing to re-ingest "
            f"the same file to avoid duplicate downstream data. If this is "
            f"intentional (e.g. reprocessing after a downstream failure), "
            f"re-run with --force."
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


def atomic_write_json(data: dict, output_file: Path) -> None:
    """Write to a temp file first, then rename, so a crash mid-write never
    leaves a corrupt or partial file at the final path."""
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    os.replace(tmp_file, output_file)


def rollback_failed_batch(batch_id: str, metadata_file: Path) -> None:
    """
    Undo a partial commit: metadata was already written, but the rest
    of the commit (the manifest entry, the CSV publish) didn't fully
    land. Re-reads the manifest from disk rather than trusting
    in-memory state — the failure could have happened before or after
    the manifest write itself landed, and filtering out this batch_id
    is a harmless no-op either way if it was never actually written.

    Without this, a failed commit would leave metadata pointing at a
    CSV that doesn't exist and (if the manifest write did land) would
    permanently block a plain retry, since check_already_ingested would
    see this source file as already ingested even though nothing usable
    was ever produced.
    """
    if metadata_file.exists():
        metadata_file.unlink()
        logger.warning(f"Rolled back metadata file: {metadata_file}")

    current_manifest = load_manifest()
    if any(m.get("batch_id") == batch_id for m in current_manifest):
        remaining = [m for m in current_manifest if m.get("batch_id") != batch_id]
        save_manifest(remaining)
        logger.warning(f"Rolled back manifest entry for batch_id {batch_id}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest the Order History sheet from the legacy ERP Excel export.")
    parser.add_argument("--file", default=None, help="Explicit path to the source .xlsx file.")
    parser.add_argument(
        "--force", action="store_true",
        help="Re-ingest even if this exact file was already ingested before.",
    )
    args = parser.parse_args()

    RAW_CSV_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    # Microsecond precision, not just seconds — two --force re-runs
    # started within the same second would otherwise collide on the
    # same output_file/metadata_file names, one silently overwriting
    # the other's CSV while the manifest gained a duplicate entry.
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    try:
        excel_path = find_input_file(args.file)
        logger.info(f"Source file: {excel_path}")

        source_hash = file_hash(excel_path)
        manifest = load_manifest()
        check_already_ingested(source_hash, manifest, args.force)

        workbook = pd.ExcelFile(excel_path, engine="openpyxl")
        if SHEET_NAME not in workbook.sheet_names:
            raise ValueError(
                f"Expected sheet '{SHEET_NAME}' not found in {excel_path.name}. "
                f"Sheets available in the workbook: {workbook.sheet_names}"
            )

        logger.info(f"Reading sheet: {SHEET_NAME}")
        df = pd.read_excel(excel_path, sheet_name=SHEET_NAME, engine="openpyxl")

        if df.empty:
            raise ValueError(
                f"Sheet '{SHEET_NAME}' in {excel_path.name} has 0 rows. "
                "Refusing to ingest an empty sheet — check the source export."
            )

        df.columns = clean_columns(df.columns.astype(str))

        duplicate_cols = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicate_cols:
            raise ValueError(
                f"Sheet '{SHEET_NAME}' produced duplicate column names after "
                f"cleaning: {duplicate_cols}. Check the source header row."
            )

        df["batch_id"] = batch_id
        df["ingested_at"] = datetime.now().isoformat()

        output_file = RAW_CSV_DIR / f"{OUTPUT_NAME}_{batch_id}.csv"
        duration = time.time() - start_time
        metadata = {
            "batch_id": batch_id,
            "sheet": SHEET_NAME,
            "source_file": str(excel_path),
            "source_file_hash": source_hash,
            "created_at": datetime.now().isoformat(),
            "output_file": str(output_file),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "duration_seconds": round(duration, 2),
        }
        metadata_file = METADATA_DIR / f"batch_metadata_order_history_{batch_id}.json"

        manifest.append({
            "batch_id": batch_id,
            "source_file": str(excel_path),
            "source_file_hash": source_hash,
            "ingested_at": metadata["created_at"],
        })

        # Commit order matters here. Metadata is written FIRST; the
        # manifest entry and the CSV — the thing downstream stages
        # actually discover via glob — are committed together LAST, and
        # rolled back together on failure. That way:
        #   - a crash before this block means nothing downstream can
        #     see this batch at all yet (it just doesn't exist), and
        #   - a failure inside this block (manifest write or CSV
        #     publish) is rolled back completely: metadata is removed,
        #     and any manifest entry that did land is removed too — so
        #     a failed batch never permanently blocks a plain retry,
        #     and no metadata is ever left pointing at a CSV that
        #     doesn't exist.
        atomic_write_json(metadata, metadata_file)
        logger.info(f"Metadata saved -> {metadata_file}")

        try:
            save_manifest(manifest)
            atomic_write_csv(df, output_file)
        except Exception:
            logger.error(
                f"Failed to fully commit batch_id {batch_id} after metadata "
                f"was already written — rolling back so a plain retry isn't "
                f"permanently blocked."
            )
            rollback_failed_batch(batch_id, metadata_file)
            raise

        logger.info(f"Exported {len(df):,} rows -> {output_file}")

        logger.info(f"Completed in {duration / 60:.2f} minutes")
        return 0

    except Exception:
        logger.exception("Order History ingestion failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
