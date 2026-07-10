"""
src/privacy/add_open_orders_only_products.py

Supplemental stage: some products appear in open orders (ordered, not
yet shipped) that have never appeared in a completed order history
extract — meaning product_profile.py / pseudonymize_products.py /
sku_mapping_pipeline.py (all sourced from order history) never assign
them a pseudo identity. This stage finds exactly those SKUs and adds
them to product_mapping_final.csv, using the same incremental,
stable-code logic those scripts already use — without modifying any of
them.

Open orders has no product_category field at all (unlike order
history), so these products are explicitly assigned pseudo_category =
"Unknown Category" — an honest sentinel, not a guess.

Run this AFTER standardize_open_orders and AFTER the order-history
product/SKU mapping stages, BEFORE pseudonymize_open_orders.

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
logger = logging.getLogger("add_open_orders_only_products")

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


def find_input_file() -> Path:
    candidates = sorted(STANDARDIZED_DIR.glob("open_orders_standardized_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No files matching 'open_orders_standardized_*.csv' found in "
            f"{STANDARDIZED_DIR}. Run standardize_open_orders_columns.py first."
        )
    return candidates[-1]


def atomic_write_csv(df: pd.DataFrame, output_file: Path) -> None:
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_csv(tmp_file, index=False, encoding="utf-8-sig")
    os.replace(tmp_file, output_file)


def pick_non_colliding_prefix(base_prefix: str, existing_pseudo_full_sku_codes: pd.Series) -> str:
    """
    Mirrors sku_mapping_pipeline.py's collision-avoidance: derive the set
    of prefixes already in use from existing pseudo_full_sku_code values
    (format "SKU-<prefix>-######"), and pick a non-colliding variant of
    base_prefix if needed.
    """
    used_prefixes = set(
        existing_pseudo_full_sku_codes.dropna().str.extract(r"^SKU-([A-Z0-9]+)-\d+$")[0].dropna().unique()
    )
    prefix = base_prefix
    suffix = 1
    while prefix in used_prefixes:
        suffix += 1
        prefix = f"{base_prefix}{suffix}"
    return prefix


def assign_stable_codes(df: pd.DataFrame, new_keys: pd.Series, existing_mapping: dict, prefix: str, pad: int) -> dict:
    """
    Given a set of key values needing a pseudo code, and a dict of
    already-assigned {key: pseudo_code} pairs (from the existing file),
    return the full {key: pseudo_code} mapping — reusing existing
    assignments, only minting new sequential codes for genuinely new keys.
    """
    result = dict(existing_mapping)
    truly_new = sorted(k for k in new_keys.unique() if k not in result)

    if truly_new:
        existing_nums = [
            int(m.group(1)) for v in result.values()
            if (m := re.search(r"(\d+)$", v))
        ]
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
        orders = pd.read_csv(input_file)
        orders.columns = orders.columns.str.strip().str.lower().str.replace(" ", "_")

        required = ["full_sku_code", "first_half_sku_code", "unique_sku_code", "product_description"]
        missing = [c for c in required if c not in orders.columns]
        if missing:
            raise ValueError(f"Open orders file is missing required column(s): {missing}")

        existing = pd.read_csv(product_mapping_file)
        existing.columns = existing.columns.str.strip().str.lower().str.replace(" ", "_")
        for col in ["full_sku_code", "first_half_sku_code", "unique_sku_code"]:
            existing[col] = existing[col].astype(str).str.strip()
            orders[col] = orders[col].astype(str).str.strip()

        existing_full_skus = set(existing["full_sku_code"])
        new_rows = (
            orders[~orders["full_sku_code"].isin(existing_full_skus)]
            [["full_sku_code", "first_half_sku_code", "unique_sku_code", "product_description"]]
            .drop_duplicates(subset=["full_sku_code"])
            .reset_index(drop=True)
        )

        if new_rows.empty:
            logger.info("No open-orders-only products found. Nothing to add.")
            return 0

        logger.info(f"Found {len(new_rows):,} product(s) present in open orders but not in order history.")
        logger.info(f"SKUs: {new_rows['full_sku_code'].tolist()}")

        new_rows["product_category"] = "UNKNOWN"
        new_rows["pseudo_category"] = UNKNOWN_CATEGORY

        # --- pseudo_product_description: continue the per-category sequence ---
        existing_seq_for_unknown = (
            existing.loc[existing["pseudo_category"] == UNKNOWN_CATEGORY, "pseudo_product_description"]
            .str.extract(r"-(\d+)$")[0]
            .astype(float)
        )
        start_num = int(existing_seq_for_unknown.max()) + 1 if existing_seq_for_unknown.notna().any() else 1

        desc_prefix = "".join(w[0] for w in UNKNOWN_CATEGORY.split()).upper()  # "UC"
        new_rows = new_rows.sort_values("full_sku_code").reset_index(drop=True)
        new_rows["pseudo_product_description"] = [
            f"{UNKNOWN_CATEGORY} Product {desc_prefix}-{str(start_num + i).zfill(3)}"
            for i in range(len(new_rows))
        ]

        # --- pseudo_full_sku_code: reuse the prefix already established for
        # Unknown Category, if this isn't the first time. Only pick a fresh,
        # non-colliding prefix on the very first run that ever adds one. ---
        existing_unknown_rows = existing[existing["pseudo_category"] == UNKNOWN_CATEGORY]
        if len(existing_unknown_rows) > 0:
            established_prefixes = (
                existing_unknown_rows["pseudo_full_sku_code"]
                .dropna().str.extract(r"^SKU-([A-Z0-9]+)-\d+$")[0].dropna().unique()
            )
            if len(established_prefixes) == 0:
                raise ValueError(
                    "Existing Unknown Category rows have no valid "
                    "pseudo_full_sku_code prefix — check for corruption."
                )
            if len(set(established_prefixes)) > 1:
                raise ValueError(
                    f"Existing Unknown Category rows use multiple different "
                    f"prefixes: {list(established_prefixes)} — check for corruption."
                )
            sku_prefix = established_prefixes[0]
        else:
            sku_prefix = pick_non_colliding_prefix(UNKNOWN_PREFIX_BASE, existing["pseudo_full_sku_code"])

        existing_sku_nums = (
            existing["pseudo_full_sku_code"]
            .dropna()
            .str.extract(rf"^SKU-{re.escape(sku_prefix)}-(\d+)$")[0]
            .dropna()
            .astype(int)
        )
        sku_start = int(existing_sku_nums.max()) + 1 if len(existing_sku_nums) > 0 else 1
        new_rows["pseudo_full_sku_code"] = [
            f"SKU-{sku_prefix}-{str(sku_start + i).zfill(6)}" for i in range(len(new_rows))
        ]

        # --- pseudo_first_half_sku_code / pseudo_unique_sku_code: stable, may
        # already be assigned if this SKU shares a group with an existing
        # order-history product. ---
        existing_first_half_map = (
            existing[["first_half_sku_code", "pseudo_first_half_sku_code"]]
            .dropna()
            .drop_duplicates(subset=["first_half_sku_code"])
            .set_index("first_half_sku_code")["pseudo_first_half_sku_code"]
            .to_dict()
        )
        existing_unique_map = (
            existing[["unique_sku_code", "pseudo_unique_sku_code"]]
            .dropna()
            .drop_duplicates(subset=["unique_sku_code"])
            .set_index("unique_sku_code")["pseudo_unique_sku_code"]
            .to_dict()
        )

        first_half_result = assign_stable_codes(
            new_rows, new_rows["first_half_sku_code"], existing_first_half_map, "SKU-GRP-", 5
        )
        unique_result = assign_stable_codes(
            new_rows, new_rows["unique_sku_code"], existing_unique_map, "SKU-BASE-", 5
        )

        new_rows["pseudo_first_half_sku_code"] = new_rows["first_half_sku_code"].map(first_half_result)
        new_rows["pseudo_unique_sku_code"] = new_rows["unique_sku_code"].map(unique_result)

        for col in ["pseudo_full_sku_code", "pseudo_first_half_sku_code", "pseudo_unique_sku_code", "pseudo_product_description"]:
            if new_rows[col].isna().any():
                raise ValueError(f"Internal error: {col} was not assigned for some new row(s).")

        final_mapping = pd.concat([existing[FINAL_COLUMNS], new_rows[FINAL_COLUMNS]], ignore_index=True)

        # --- Quality checks, same invariants as sku_mapping_pipeline.py ---
        duplicate_skus = final_mapping["full_sku_code"].duplicated().sum()
        if duplicate_skus > 0:
            raise ValueError(f"Duplicate full_sku_code found after merge: {duplicate_skus}")

        duplicate_pseudo_full = final_mapping["pseudo_full_sku_code"].duplicated().sum()
        if duplicate_pseudo_full > 0:
            raise ValueError(f"Duplicate pseudo_full_sku_code found after merge: {duplicate_pseudo_full}")

        for key_col, pseudo_col in [
            ("first_half_sku_code", "pseudo_first_half_sku_code"),
            ("unique_sku_code", "pseudo_unique_sku_code"),
        ]:
            mapping_check = final_mapping[[key_col, pseudo_col]].drop_duplicates()
            collisions = mapping_check[mapping_check[pseudo_col].duplicated(keep=False)]
            if len(collisions) > 0:
                raise ValueError(
                    f"The same {pseudo_col} is assigned to multiple distinct "
                    f"{key_col} values:\n{collisions}"
                )

        atomic_write_csv(final_mapping, product_mapping_file)

        logger.info(f"Added {len(new_rows):,} open-orders-only product(s) to product_mapping_final.csv")
        logger.info(f"Total products now: {len(final_mapping):,}")

        return 0

    except Exception:
        logger.exception("Adding open-orders-only products failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())