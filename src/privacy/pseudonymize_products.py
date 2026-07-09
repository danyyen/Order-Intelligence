"""
src/privacy/pseudonymize_products.py

Stage: merge category_mapping.csv into product_mapping_base.csv to assign
a pseudo_category and pseudo_product_description to every product,
preserving existing assignments and only generating new ones for products
not already in product_mapping_final.csv.

Output contract (unchanged):
    data/metadata/product_mapping_final.csv
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import METADATA_DIR


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("pseudonymize_products")

FINAL_COLUMNS = [
    "full_sku_code",
    "first_half_sku_code",
    "unique_sku_code",
    "product_description",
    "product_category",
    "pseudo_category",
    "pseudo_product_description",
]

REQUIRED_BASE_COLUMNS = [
    "full_sku_code",
    "first_half_sku_code",
    "unique_sku_code",
    "product_description",
    "product_category",
]

REQUIRED_CATEGORY_COLUMNS = ["product_category", "pseudo_category"]


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


def check_duplicate_columns(df: pd.DataFrame, label: str) -> None:
    dupes = df.columns[df.columns.duplicated()].unique().tolist()
    if dupes:
        raise ValueError(f"{label} has duplicate column names: {dupes}")


def check_required_columns(df: pd.DataFrame, required: list[str], label: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required column(s): {missing}")


def atomic_write_csv(df: pd.DataFrame, output_file: Path) -> None:
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_csv(tmp_file, index=False, encoding="utf-8-sig")
    os.replace(tmp_file, output_file)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    product_base_file = METADATA_DIR / "product_mapping_base.csv"
    category_mapping_file = METADATA_DIR / "category_mapping.csv"
    product_mapping_final_file = METADATA_DIR / "product_mapping_final.csv"

    try:
        if not product_base_file.exists():
            raise FileNotFoundError(
                f"{product_base_file} not found. Run product_profile.py first."
            )
        if not category_mapping_file.exists():
            raise FileNotFoundError(
                f"{category_mapping_file} not found. Create/maintain this file "
                "with a pseudo_category for every product_category."
            )

        product_base = pd.read_csv(product_base_file)
        category_mapping = pd.read_csv(category_mapping_file)

        product_base = clean_columns(product_base)
        category_mapping = clean_columns(category_mapping)

        check_duplicate_columns(product_base, "product_mapping_base.csv")
        check_duplicate_columns(category_mapping, "category_mapping.csv")
        check_required_columns(product_base, REQUIRED_BASE_COLUMNS, "product_mapping_base.csv")
        check_required_columns(category_mapping, REQUIRED_CATEGORY_COLUMNS, "category_mapping.csv")

        if product_base.empty:
            raise ValueError(
                "product_mapping_base.csv has 0 rows. This almost certainly "
                "means an upstream stage failed silently — refusing to proceed."
            )

        # --- Clean text values ---
        product_base["full_sku_code"] = product_base["full_sku_code"].astype(str).str.strip()
        product_base["product_category"] = (
            product_base["product_category"].astype(str).str.strip().str.upper()
        )
        product_base["product_description"] = (
            product_base["product_description"].astype(str).str.strip()
        )
        category_mapping["product_category"] = (
            category_mapping["product_category"].astype(str).str.strip().str.upper()
        )
        category_mapping["pseudo_category"] = (
            category_mapping["pseudo_category"].astype(str).str.strip()
        )

        # --- Guard against a many-to-many merge blowing up row counts ---
        duplicate_categories = (
            category_mapping["product_category"][category_mapping["product_category"].duplicated()]
            .unique()
            .tolist()
        )
        if duplicate_categories:
            raise ValueError(
                f"category_mapping.csv has duplicate product_category entries: "
                f"{duplicate_categories}. Each product_category must map to exactly "
                f"one row, or the merge will silently duplicate product rows."
            )

        # --- Merge category mapping ---
        current_products = product_base.merge(category_mapping, on="product_category", how="left")

        if len(current_products) != len(product_base):
            raise ValueError(
                f"Row count changed after merging category_mapping.csv "
                f"(before: {len(product_base):,}, after: {len(current_products):,}). "
                f"This should be impossible now that duplicate categories are "
                f"blocked — investigate before continuing."
            )

        # --- Check for new/unmapped categories ---
        unmapped_categories = (
            current_products[current_products["pseudo_category"].isna()]["product_category"]
            .drop_duplicates()
            .tolist()
        )
        if unmapped_categories:
            raise ValueError(
                f"New or unmapped categories found: {unmapped_categories}. "
                f"Update metadata/category_mapping.csv before continuing."
            )
        logger.info("All categories mapped successfully.")

        # --- Load existing final mapping if it exists ---
        if product_mapping_final_file.exists():
            existing_mapping = pd.read_csv(product_mapping_final_file)
            existing_mapping = clean_columns(existing_mapping)
            check_duplicate_columns(existing_mapping, "product_mapping_final.csv")
            check_required_columns(
                existing_mapping, ["full_sku_code", "pseudo_category", "pseudo_product_description"],
                "product_mapping_final.csv",
            )
            existing_mapping["full_sku_code"] = existing_mapping["full_sku_code"].astype(str).str.strip()
            logger.info(f"Loaded existing mapping: {len(existing_mapping):,} products")
        else:
            existing_mapping = pd.DataFrame(columns=FINAL_COLUMNS)
            logger.info("No existing mapping found. Creating first mapping.")

        # --- Identify new products ---
        existing_skus = set(existing_mapping["full_sku_code"])
        new_products = current_products[
            ~current_products["full_sku_code"].isin(existing_skus)
        ].copy()
        logger.info(f"New products found: {len(new_products):,}")

        # --- Generate pseudo product names only for new products ---
        if len(new_products) > 0:
            if len(existing_mapping) > 0:
                existing_mapping["existing_sequence"] = (
                    existing_mapping["pseudo_product_description"]
                    .str.extract(r"-(\d+)$")[0]
                    .astype(float)
                )
                max_sequence = (
                    existing_mapping
                    .groupby("pseudo_category")["existing_sequence"]
                    .max()
                    .fillna(0)
                    .to_dict()
                )
            else:
                max_sequence = {}

            new_products = new_products.sort_values(["pseudo_category", "product_description"])
            new_rows = []

            for pseudo_category, group in new_products.groupby("pseudo_category"):
                start_num = int(max_sequence.get(pseudo_category, 0)) + 1

                prefix_source = "".join(w[0] for w in pseudo_category.split()).upper()
                prefix = prefix_source if prefix_source else "X"

                group = group.copy()
                group["category_sequence"] = range(start_num, start_num + len(group))
                group["pseudo_product_description"] = (
                    group["pseudo_category"] + " Product " + prefix + "-"
                    + group["category_sequence"].astype(str).str.zfill(3)
                )
                new_rows.append(group)

            new_mapping = pd.concat(new_rows, ignore_index=True)
        else:
            new_mapping = pd.DataFrame(columns=existing_mapping.columns)

        # --- Combine existing + new ---
        final_mapping = pd.concat(
            [existing_mapping[FINAL_COLUMNS], new_mapping[FINAL_COLUMNS]],
            ignore_index=True,
        )

        # --- Quality checks ---
        duplicate_skus = final_mapping["full_sku_code"].duplicated().sum()
        if duplicate_skus > 0:
            raise ValueError(f"Duplicate full_sku_code found: {duplicate_skus}")

        missing_pseudo_names = final_mapping["pseudo_product_description"].isna().sum()
        if missing_pseudo_names > 0:
            raise ValueError(f"Missing pseudo product descriptions: {missing_pseudo_names}")

        # --- Export ---
        final_mapping = final_mapping.sort_values(["pseudo_category", "pseudo_product_description"])
        atomic_write_csv(final_mapping, product_mapping_final_file)

        logger.info(f"Final product mapping saved: {len(final_mapping):,} products")
        logger.info(str(product_mapping_final_file))

        return 0

    except Exception:
        logger.exception("Product pseudonymization failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())