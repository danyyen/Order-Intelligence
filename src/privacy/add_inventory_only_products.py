"""
src/privacy/add_inventory_only_products.py

Supplemental stage: some products may exist in warehouse inventory
(in stock) that have never appeared in order history OR open orders
(e.g. new stock not yet ordered). This stage finds exactly those SKUs
(after the same zero-pad/dash reconstruction pseudonymize_inventory.py
uses) and adds them to product_mapping_final.csv.

Reuses the SAME "Unknown Category" sentinel bucket (and continues its
SAME prefix/sequence) established by add_open_orders_only_products.py —
one consistent "we don't know the real category" bucket across every
source, not a separate one per source.

Run this AFTER standardize_inventory_columns and AFTER the order-history
product/SKU mapping stages, BEFORE pseudonymize_inventory.

Output contract:
    data/metadata/product_mapping_final.csv (updated in place)
"""

from __future__ import annotations

import logging
import os
import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import STANDARDIZED_DIR, METADATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("add_inventory_only_products")

UNKNOWN_CATEGORY = "Unknown Category"
UNKNOWN_PREFIX_BASE = "UC"

FINAL_COLUMNS = [
    "full_sku_code",
    "first_half_sku_code",
    "unique_sku_code",
    "product_description",
    "product_category",
    "pseudo_category",
    "pseudo_product_description",
    "pseudo_full_sku_code",
    "pseudo_first_half_sku_code",
    "pseudo_unique_sku_code",
]


def reconstruct_full_sku_code(raw_sku: str) -> str:
    digits = str(raw_sku).strip()
    padded = digits.zfill(11)
    return f"{padded[:6]}-{padded[6:]}"


def find_input_file() -> Path:
    candidates = sorted(STANDARDIZED_DIR.glob("inventory_standardized_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No files matching 'inventory_standardized_*.csv' found in "
            f"{STANDARDIZED_DIR}. Run standardize_inventory_columns.py first."
        )
    return candidates[-1]


def atomic_write_csv(df: pd.DataFrame, output_file: Path) -> None:
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_csv(tmp_file, index=False, encoding="utf-8-sig")
    os.replace(tmp_file, output_file)


def assign_stable_codes(new_keys: pd.Series, existing_mapping: dict, prefix: str, pad: int) -> dict:
    result = dict(existing_mapping)
    truly_new = sorted(k for k in new_keys.unique() if k not in result)
    if truly_new:
        existing_nums = [int(m.group(1)) for v in result.values() if (m := re.search(r"(\d+)$", v))]
        next_num = (max(existing_nums) + 1) if existing_nums else 1
        for i, key in enumerate(truly_new):
            result[key] = f"{prefix}{str(next_num + i).zfill(pad)}"
    return result


def main() -> int:
    product_mapping_file = METADATA_DIR / "product_mapping_final.csv"

    try:
        if not product_mapping_file.exists():
            raise FileNotFoundError(
                f"{product_mapping_file} not found. Run the order history "
                f"product/SKU mapping stages first."
            )

        input_file = find_input_file()
        inventory = pd.read_csv(input_file, dtype=str)
        inventory.columns = inventory.columns.str.strip().str.lower().str.replace(" ", "_")

        required = ["raw_sku", "product_description"]
        missing = [c for c in required if c not in inventory.columns]
        if missing:
            raise ValueError(f"Inventory file is missing required column(s): {missing}")

        inventory["raw_sku"] = inventory["raw_sku"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        inventory["full_sku_code"] = inventory["raw_sku"].apply(reconstruct_full_sku_code)

        existing = pd.read_csv(product_mapping_file, dtype=str)
        existing.columns = existing.columns.str.strip().str.lower().str.replace(" ", "_")

        existing_full_skus = set(existing["full_sku_code"])
        new_rows = (
            inventory[~inventory["full_sku_code"].isin(existing_full_skus)]
            [["full_sku_code", "product_description"]]
            .drop_duplicates(subset=["full_sku_code"])
            .sort_values("full_sku_code")
            .reset_index(drop=True)
        )

        if new_rows.empty:
            logger.info("No inventory-only products found. Nothing to add.")
            return 0

        logger.info(f"Found {len(new_rows):,} product(s) present in inventory but not in order history or open orders.")
        logger.info(f"SKUs: {new_rows['full_sku_code'].tolist()}")

        # Inventory has no first_half/unique SKU tiers of its own — derive
        # them from the SAME reconstructed full_sku_code split used
        # elsewhere, so these products fit the existing schema consistently.
        new_rows["first_half_sku_code"] = new_rows["full_sku_code"].str.split("-").str[0]
        new_rows["unique_sku_code"] = new_rows["full_sku_code"].str.split("-").str[1]
        new_rows["product_category"] = "UNKNOWN"
        new_rows["pseudo_category"] = UNKNOWN_CATEGORY

        existing_seq_for_unknown = (
            existing.loc[existing["pseudo_category"] == UNKNOWN_CATEGORY, "pseudo_product_description"]
            .str.extract(r"-(\d+)$")[0]
            .astype(float)
        )
        start_num = int(existing_seq_for_unknown.max()) + 1 if existing_seq_for_unknown.notna().any() else 1

        desc_prefix = "".join(w[0] for w in UNKNOWN_CATEGORY.split()).upper()
        new_rows["pseudo_product_description"] = [
            f"{UNKNOWN_CATEGORY} Product {desc_prefix}-{str(start_num + i).zfill(3)}"
            for i in range(len(new_rows))
        ]

        existing_unknown_rows = existing[existing["pseudo_category"] == UNKNOWN_CATEGORY]
        if len(existing_unknown_rows) > 0:
            established_prefixes = (
                existing_unknown_rows["pseudo_full_sku_code"]
                .dropna().str.extract(r"^SKU-([A-Z0-9]+)-\d+$")[0].dropna().unique()
            )
            if len(established_prefixes) == 0:
                raise ValueError("Existing Unknown Category rows have no valid pseudo_full_sku_code prefix.")
            if len(set(established_prefixes)) > 1:
                raise ValueError(f"Existing Unknown Category rows use multiple prefixes: {list(established_prefixes)}.")
            sku_prefix = established_prefixes[0]
        else:
            used_prefixes = set(
                existing["pseudo_full_sku_code"].dropna().str.extract(r"^SKU-([A-Z0-9]+)-\d+$")[0].dropna().unique()
            )
            sku_prefix = UNKNOWN_PREFIX_BASE
            suffix = 1
            while sku_prefix in used_prefixes:
                suffix += 1
                sku_prefix = f"{UNKNOWN_PREFIX_BASE}{suffix}"

        existing_sku_nums = (
            existing["pseudo_full_sku_code"].dropna()
            .str.extract(rf"^SKU-{re.escape(sku_prefix)}-(\d+)$")[0].dropna().astype(int)
        )
        sku_start = int(existing_sku_nums.max()) + 1 if len(existing_sku_nums) > 0 else 1
        new_rows["pseudo_full_sku_code"] = [f"SKU-{sku_prefix}-{str(sku_start + i).zfill(6)}" for i in range(len(new_rows))]

        existing_first_half_map = (
            existing[["first_half_sku_code", "pseudo_first_half_sku_code"]].dropna()
            .drop_duplicates(subset=["first_half_sku_code"])
            .set_index("first_half_sku_code")["pseudo_first_half_sku_code"].to_dict()
        )
        existing_unique_map = (
            existing[["unique_sku_code", "pseudo_unique_sku_code"]].dropna()
            .drop_duplicates(subset=["unique_sku_code"])
            .set_index("unique_sku_code")["pseudo_unique_sku_code"].to_dict()
        )
        first_half_result = assign_stable_codes(new_rows["first_half_sku_code"], existing_first_half_map, "SKU-GRP-", 5)
        unique_result = assign_stable_codes(new_rows["unique_sku_code"], existing_unique_map, "SKU-BASE-", 5)
        new_rows["pseudo_first_half_sku_code"] = new_rows["first_half_sku_code"].map(first_half_result)
        new_rows["pseudo_unique_sku_code"] = new_rows["unique_sku_code"].map(unique_result)

        for col in ["pseudo_full_sku_code", "pseudo_first_half_sku_code", "pseudo_unique_sku_code", "pseudo_product_description"]:
            if new_rows[col].isna().any():
                raise ValueError(f"Internal error: {col} was not assigned for some new row(s).")

        final_mapping = pd.concat([existing[FINAL_COLUMNS], new_rows[FINAL_COLUMNS]], ignore_index=True)

        if final_mapping["full_sku_code"].duplicated().sum() > 0:
            raise ValueError("Duplicate full_sku_code found after merge.")
        if final_mapping["pseudo_full_sku_code"].duplicated().sum() > 0:
            raise ValueError("Duplicate pseudo_full_sku_code found after merge.")

        for key_col, pseudo_col in [
            ("first_half_sku_code", "pseudo_first_half_sku_code"),
            ("unique_sku_code", "pseudo_unique_sku_code"),
        ]:
            check = final_mapping[[key_col, pseudo_col]].drop_duplicates()
            collisions = check[check[pseudo_col].duplicated(keep=False)]
            if len(collisions) > 0:
                raise ValueError(f"The same {pseudo_col} is assigned to multiple {key_col} values:\n{collisions}")

        atomic_write_csv(final_mapping, product_mapping_file)

        logger.info(f"Added {len(new_rows):,} inventory-only product(s) to product_mapping_final.csv")
        logger.info(f"Total products now: {len(final_mapping):,}")
        return 0

    except Exception:
        logger.exception("Adding inventory-only products failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())