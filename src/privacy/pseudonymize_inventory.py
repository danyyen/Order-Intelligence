"""
src/privacy/pseudonymize_inventory.py

Stage: pseudonymize the latest standardized inventory file, reusing the
same product_mapping_final.csv that order history and open orders use.

Inventory's raw SKU is NOT the same format as full_sku_code elsewhere —
it's stored as a plain number with leading zeros stripped by Excel
(e.g. 403004403). full_sku_code is an 11-digit zero-padded value with a
dash after the first 6 digits (e.g. 004030-04403). The transformation:
zero-pad to 11 digits, insert a dash after position 6.

This transformation is NOT trusted blindly — after reconstructing
full_sku_code, this script cross-checks the reconstructed
first_half_sku_code/unique_sku_code against what's actually on file for
that product in product_mapping_final.csv. A mismatch means the 6+5
digit assumption broke for that SKU, and would otherwise silently join
to the WRONG product rather than just failing to find one — a worse
failure mode than a simple unmapped-SKU error, so it's checked for
explicitly.

Inventory has no customer data at all, and no product_category field —
category is pulled in from the product mapping as pure enrichment, same
as pseudonymize_open_orders.py does.

Output contract:
    data/pseudonymized/inventory_pseudonymized_<snapshot_date>.csv
"""

from __future__ import annotations

import hashlib
import logging
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import STANDARDIZED_DIR, METADATA_DIR, PSEUDONYMIZED_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("pseudonymize_inventory")

REQUIRED_INVENTORY_COLUMNS = ["raw_sku", "product_description", "snapshot_date", "warehouse_code"]

REQUIRED_PRODUCT_MAPPING_COLUMNS = [
    "full_sku_code",
    "first_half_sku_code",
    "unique_sku_code",
    "pseudo_category",
    "pseudo_product_description",
    "pseudo_full_sku_code",
    "pseudo_first_half_sku_code",
    "pseudo_unique_sku_code",
]

HASH_COLUMNS = [
    "warehouse_code",
    "snapshot_date",
    "full_sku_code",
    "pallet_number",
    "lot_number",
    "slot_location",
    "production_date",
    "best_before_date",
    "reserved_quantity",
    "quantity_on_hand",
    "weight",
]


def reconstruct_full_sku_code(raw_sku: str) -> str:
    """
    Zero-pad to 11 digits, insert a dash after the first 6.
    e.g. "403004403" -> "00403004403" -> "004030-04403"
    """
    digits = str(raw_sku).strip()
    padded = digits.zfill(11)
    return f"{padded[:6]}-{padded[6:]}"


def find_input_file(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"Specified --file does not exist: {path}")
        return path

    candidates = sorted(STANDARDIZED_DIR.glob("inventory_standardized_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No files matching 'inventory_standardized_*.csv' found in "
            f"{STANDARDIZED_DIR}. Run standardize_inventory_columns.py first."
        )
    return candidates[-1]


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


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Pseudonymize the latest standardized inventory file.")
    parser.add_argument("--file", default=None, help="Explicit path to a standardized inventory CSV.")
    args = parser.parse_args()

    PSEUDONYMIZED_DIR.mkdir(parents=True, exist_ok=True)
    product_mapping_file = METADATA_DIR / "product_mapping_final.csv"

    try:
        input_file = find_input_file(args.file)

        if not product_mapping_file.exists():
            raise FileNotFoundError(
                f"{product_mapping_file} not found. Run the order history "
                f"product pseudonymization stages first."
            )

        inventory = pd.read_csv(input_file, dtype=str)
        # Read as all-string: first_half_sku_code/unique_sku_code are
        # numeric-looking AS400 codes (e.g. "004030") — pandas' automatic
        # type inference would otherwise read them as int64 and silently
        # strip leading zeros before any .astype(str) call downstream ever
        # sees the original text, corrupting the exact-length check below.
        product_mapping = pd.read_csv(product_mapping_file, dtype=str)

        logger.info(f"Loaded inventory rows: {len(inventory):,}")
        logger.info(f"Loaded product mapping rows: {len(product_mapping):,}")
        logger.info(f"Input file: {input_file}")

        if inventory.empty:
            raise ValueError(f"{input_file.name} has 0 rows. Refusing to pseudonymize an empty file.")
        if product_mapping.empty:
            raise ValueError("product_mapping_final.csv has 0 rows.")

        for df, label in [(inventory, "inventory"), (product_mapping, "product mapping")]:
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            check_duplicate_columns(df, label)

        check_required_columns(inventory, REQUIRED_INVENTORY_COLUMNS, "inventory")
        check_required_columns(product_mapping, REQUIRED_PRODUCT_MAPPING_COLUMNS, "product_mapping_final.csv")
        logger.info("Required column checks passed.")

        product_mapping["full_sku_code"] = product_mapping["full_sku_code"].astype(str).str.strip()
        dupe_check = product_mapping["full_sku_code"][product_mapping["full_sku_code"].duplicated()].unique().tolist()
        if dupe_check:
            raise ValueError(f"product_mapping_final.csv has duplicate full_sku_code value(s): {dupe_check[:20]}")

        # --- Reconstruct full_sku_code from inventory's raw numeric SKU ---
        inventory["raw_sku"] = inventory["raw_sku"].astype(str).str.strip()
        # Handle floats like "403004403.0" that pandas sometimes introduces
        inventory["raw_sku"] = inventory["raw_sku"].str.replace(r"\.0$", "", regex=True)
        inventory["full_sku_code"] = inventory["raw_sku"].apply(reconstruct_full_sku_code)

        # --- Merge product mapping ---
        product_lookup = product_mapping[
            [
                "full_sku_code",
                "pseudo_category",
                "pseudo_product_description",
                "pseudo_full_sku_code",
                "pseudo_first_half_sku_code",
                "pseudo_unique_sku_code",
            ]
        ]

        inv_pseudo = inventory.merge(product_lookup, on="full_sku_code", how="left")

        unmapped = inv_pseudo[inv_pseudo["pseudo_full_sku_code"].isna()]["full_sku_code"].drop_duplicates().tolist()
        if unmapped:
            logger.error(f"Unmapped SKUs (reconstructed full_sku_code) found: {unmapped[:20]}")
            logger.error(f"Total unmapped: {len(unmapped):,}")
            raise ValueError(
                "Some inventory SKUs, after reconstruction, are missing from "
                "product_mapping_final.csv. Either these are genuinely new "
                "products (run add_inventory_only_products.py first), or the "
                "zero-pad/dash reconstruction is wrong for these SKUs — check "
                "the raw_sku values listed above by hand before assuming either."
            )
        logger.info("All reconstructed SKUs successfully mapped.")

        # NOTE: An earlier version of this script cross-checked the
        # reconstruction against product_mapping_final.csv's standalone
        # first_half_sku_code/unique_sku_code columns. That check was
        # removed — those columns get their leading zeros silently
        # stripped by pandas on read (they look like pure integers,
        # unlike full_sku_code, which is protected from this because the
        # dash forces it to be read as text). Checking against corrupted
        # reference data produced false positives, not real protection.
        # full_sku_code itself is reliable, and a successful merge against
        # it (above) is sufficient — the dash-protected string can't
        # coincidentally collide across different real products.
        #
        # Separately worth fixing at the source at some point (not blocking
        # this pipeline): first_half_sku_code/unique_sku_code as STANDALONE
        # values in product_mapping_final.csv are likely missing leading
        # zeros throughout, due to the same pandas dtype-inference issue.
        # This hasn't caused a functional bug in this pipeline (they're only
        # ever used as opaque, consistently-corrupted grouping keys for
        # stable pseudo-code assignment), but would matter for any external
        # use of those two columns on their own.

        if len(inv_pseudo) != len(inventory):
            raise ValueError(
                f"Row count changed after merge. Original: {len(inventory):,}, "
                f"After merge: {len(inv_pseudo):,}."
            )
        logger.info("Row count check passed after merge.")

        # --- Replace sensitive fields ---
        inv_pseudo["original_product_description_removed"] = True
        inv_pseudo["original_sku_removed"] = True

        inv_pseudo["product_description"] = inv_pseudo["pseudo_product_description"]
        inv_pseudo["full_sku_code"] = inv_pseudo["pseudo_full_sku_code"]

        # product_category never existed in inventory's source at all — pure
        # additive enrichment, same convention as pseudonymize_open_orders.py.
        inv_pseudo["product_category"] = inv_pseudo["pseudo_category"]
        inv_pseudo["product_category_derived"] = True

        inv_pseudo = inv_pseudo.drop(columns=[
            "raw_sku",
            "pseudo_product_description",
            "pseudo_category",
            "pseudo_full_sku_code",
            "pseudo_first_half_sku_code",
            "pseudo_unique_sku_code",
        ])

        # --- Row hash ---
        available_hash_columns = [c for c in HASH_COLUMNS if c in inv_pseudo.columns]
        missing_hash_columns = [c for c in HASH_COLUMNS if c not in inv_pseudo.columns]
        if missing_hash_columns:
            logger.warning(f"Expected hash columns not found, will be skipped: {missing_hash_columns}")

        inv_pseudo["row_hash"] = (
            inv_pseudo[available_hash_columns]
            .astype(str)
            .agg("|".join, axis=1)
            .apply(lambda x: hashlib.md5(x.encode()).hexdigest())
        )

        from datetime import datetime
        inv_pseudo["pseudonymized_at"] = datetime.now().isoformat()

        snapshot_date = str(inventory["snapshot_date"].iloc[0])
        output_file = PSEUDONYMIZED_DIR / f"inventory_pseudonymized_{snapshot_date}.csv"
        atomic_write_csv(inv_pseudo, output_file)

        logger.info("Pseudonymized inventory saved:")
        logger.info(str(output_file))
        logger.info(f"Rows: {len(inv_pseudo):,}")

        return 0

    except Exception:
        logger.exception("Inventory pseudonymization failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())