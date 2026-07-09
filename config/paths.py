"""
config/paths.py

Single source of truth for every directory used across the pipeline.
Every stage script should import its paths from here rather than
hardcoding its own BASE_DIR — that's what previously let different
scripts drift onto "data" vs "Data" (only harmless on Windows because
its filesystem is case-insensitive; it would break on Linux/S3).
"""

from pathlib import Path

PROJECT_ROOT = Path(r"C:\Users\ND\Desktop\Project No_Retreat")

if not PROJECT_ROOT.exists():
    raise FileNotFoundError(
        f"PROJECT_ROOT does not exist on this machine: {PROJECT_ROOT}. "
        "Update config/paths.py to point at the correct project location."
    )

DATA_DIR = PROJECT_ROOT / "data"

# --- Ingestion / standardization ---
RAW_EXCEL_DIR = DATA_DIR / "raw_excel"          # source Excel export
RAW_CSV_DIR = DATA_DIR / "raw_csv"              # Excel -> CSV, per sheet
STANDARDIZED_DIR = DATA_DIR / "standardized_csv"  # renamed, meaningful columns

# --- Privacy / pseudonymization ---
PSEUDONYMIZED_DIR = DATA_DIR / "pseudonymized"

# --- Quality ---
QUALITY_DIR = DATA_DIR / "quality_reports"

# --- Metadata (mapping files, batch metadata, run reports) ---
METADATA_DIR = DATA_DIR / "metadata"
PIPELINE_RUN_DIR = METADATA_DIR / "pipeline_runs"

# --- Logs (orchestrator + individual stage logs) ---
LOG_DIR = PROJECT_ROOT / "logs"

ALL_DATA_DIRS = [
    RAW_EXCEL_DIR,
    RAW_CSV_DIR,
    STANDARDIZED_DIR,
    PSEUDONYMIZED_DIR,
    QUALITY_DIR,
    METADATA_DIR,
    PIPELINE_RUN_DIR,
    LOG_DIR,
]


def ensure_all_dirs_exist() -> None:
    """
    Create every directory this pipeline writes to, if it doesn't already
    exist. Safe to call at the start of any stage, or once from the
    orchestrator before the first stage runs.
    """
    for d in ALL_DATA_DIRS:
        d.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    # Quick manual check: `python config/paths.py` prints every resolved
    # path and whether it currently exists, without creating anything.
    for d in ALL_DATA_DIRS:
        print(f"{'OK ' if d.exists() else 'MISSING '} {d}")