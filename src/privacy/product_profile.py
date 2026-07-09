"""
src/privacy/product_profile.py

Stage: build the current universe of unique products (product_mapping_base.csv)
and a category count profile (category_profile.csv) from the latest
standardized order history file.

Output contract (unchanged):
    data/metadata/product_mapping_base.csv
    data/metadata/category_profile.csv
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
logger = logging.getLogger("product_profile")

REQUIRED_COLUMNS = [
    "full_sku_code",
    "first_half_sku_code",
    "unique_sku_code",
    "product_description",
    "product_category",
]

CRITICAL_NULL_COLUMNS = ["full_sku_code", "product_description", "product_category"]


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
    parser = argparse.ArgumentParser(description="Build product mapping base and category profile.")
    parser.add_argument("--file", default=None, help="Explicit path to a standardized order history CSV.")
    args = parser.parse_args()

    METADATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        input_file = find_input_file(args.file)
        logger.info(f"Reading: {input_file}")

        df = pd.read_csv(input_file)

        if df.empty:
            raise ValueError(
                f"{input_file.name} has 0 rows. Refusing to build a product "
                "profile from an empty file."
            )

        logger.info(f"Loaded rows: {len(df):,}")
        logger.info(f"Loaded columns: {len(df.columns):,}")

        missing_columns = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        logger.info("Required column check passed.")

        null_check = df[REQUIRED_COLUMNS].isna().sum()
        logger.info(f"Null check:\n{null_check}")

        critical_nulls = df[CRITICAL_NULL_COLUMNS].isna().sum()
        if critical_nulls.sum() > 0:
            raise ValueError(f"Critical product fields contain nulls:\n{critical_nulls}")
        logger.info("Critical null check passed.")

        # Clean the join/dedup key the same way downstream stages
        # (pseudonymize_products.py, pseudonymize_order_history.py) do,
        # so a stray space here can't create phantom "new" or "duplicate"
        # products when matched against those stages.
        df["full_sku_code"] = df["full_sku_code"].astype(str).str.strip()

        duplicate_input_cols = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicate_input_cols:
            raise ValueError(f"Input file has duplicate column names: {duplicate_input_cols}")

        product_base = (
            df[REQUIRED_COLUMNS]
            .drop_duplicates(subset=["full_sku_code"])
            .sort_values(["product_category", "product_description"])
        )

        logger.info(f"Unique products based on full_sku_code: {len(product_base):,}")

        duplicate_skus = product_base["full_sku_code"].duplicated().sum()
        if duplicate_skus > 0:
            raise ValueError(f"Duplicate full_sku_code found: {duplicate_skus}")
        logger.info("Duplicate SKU check passed.")

        product_base_output = METADATA_DIR / "product_mapping_base.csv"
        atomic_write_csv(product_base, product_base_output)
        logger.info(f"Saved product mapping base -> {product_base_output}")

        category_profile = product_base["product_category"].value_counts().reset_index()
        category_profile.columns = ["product_category", "product_count"]
        logger.info(f"Category profile:\n{category_profile}")

        category_output = METADATA_DIR / "category_profile.csv"
        atomic_write_csv(category_profile, category_output)
        logger.info(f"Saved category profile -> {category_output}")

        return 0

    except Exception:
        logger.exception("Product profiling failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())