"""
src/privacy/pseudonymize_open_orders.py

Stage: pseudonymize the latest standardized open orders file, reusing
the SAME customer_mapping_final.csv and product_mapping_final.csv that
order history uses — no separate mapping-building stage needed, since
open orders shares the same customer/product/SKU universe.

IMPORTANT COUPLING: this builds the customer identity key the same way
the CURRENTLY LIVE pseudonymize_order_history.py does today
(source_customer_code + ship_to_customer_code + customer_name).

Open orders has no product_category field in the source at all (unlike
order history). pseudo_category from the product mapping is added as a
NEW, purely additive column here — not a replacement of anything that
existed in the raw data.

Output contract:
    data/pseudonymized/open_orders_pseudonymized_<batch_id>.csv
"""

from __future__ import annotations

import hashlib
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import STANDARDIZED_DIR, METADATA_DIR, PSEUDONYMIZED_DIR


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("pseudonymize_open_orders")

REQUIRED_ORDER_COLUMNS = [
    "full_sku_code",
    "first_half_sku_code",
    "unique_sku_code",
    "product_description",
    "source_customer_code",
    "ship_to_customer_code",
    "customer_name",
]

REQUIRED_PRODUCT_MAPPING_COLUMNS = [
    "full_sku_code",
    "pseudo_category",
    "pseudo_product_description",
    "pseudo_full_sku_code",
    "pseudo_first_half_sku_code",
    "pseudo_unique_sku_code",
]

REQUIRED_CUSTOMER_MAPPING_COLUMNS = ["customer_hash_key", "pseudo_customer_name"]

HASH_COLUMNS = [
    "company_code",
    "source_customer_code",
    "ship_to_customer_code",
    "customer_name",
    "purchase_order_number",
    "order_number",
    "order_type",
    "order_date",
    "scheduled_ship_date",
    "order_status",
    "full_sku_code",
    "first_half_sku_code",
    "unique_sku_code",
    "final_order_quantity",
    "shipped_order_quantity",
    "estimated_order_weight",
    "shipped_order_weight",
    "delivery_route",
    "customer_requested_date",
    "revised_delivery_date",
    "order_amount",
    "product_description",
]


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


def check_unique_key(df: pd.DataFrame, key_col: str, label: str) -> None:
    dupes = df[key_col][df[key_col].duplicated()].unique().tolist()
    if dupes:
        raise ValueError(
            f"{label} has duplicate {key_col} value(s): {dupes[:20]}"
            f"{' (truncated)' if len(dupes) > 20 else ''}. Every {key_col} "
            f"must be unique or the merge will silently multiply order rows."
        )


def find_input_file(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"Specified --file does not exist: {path}")
        return path

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


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Pseudonymize the latest standardized open orders file.")
    parser.add_argument("--file", default=None, help="Explicit path to a standardized open orders CSV.")
    args = parser.parse_args()

    PSEUDONYMIZED_DIR.mkdir(parents=True, exist_ok=True)

    product_mapping_file = METADATA_DIR / "product_mapping_final.csv"
    customer_mapping_file = METADATA_DIR / "customer_mapping_final.csv"

    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = PSEUDONYMIZED_DIR / f"open_orders_pseudonymized_{batch_id}.csv"

    try:
        input_file = find_input_file(args.file)

        if not product_mapping_file.exists():
            raise FileNotFoundError(
                f"{product_mapping_file} not found. Run the order history "
                f"product pseudonymization stages first — open orders "
                f"reuses that mapping, it doesn't build its own."
            )
        if not customer_mapping_file.exists():
            raise FileNotFoundError(
                f"{customer_mapping_file} not found. Run customer_mapping_pipeline.py first."
            )

        orders = pd.read_csv(input_file)
        product_mapping = pd.read_csv(product_mapping_file)
        customer_mapping = pd.read_csv(customer_mapping_file)

        logger.info(f"Loaded open orders rows: {len(orders):,}")
        logger.info(f"Loaded product mapping rows: {len(product_mapping):,}")
        logger.info(f"Loaded customer mapping rows: {len(customer_mapping):,}")
        logger.info(f"Input file: {input_file}")

        if orders.empty:
            raise ValueError(f"{input_file.name} has 0 rows. Refusing to pseudonymize an empty file.")
        if product_mapping.empty:
            raise ValueError("product_mapping_final.csv has 0 rows.")
        if customer_mapping.empty:
            raise ValueError("customer_mapping_final.csv has 0 rows.")

        for df, label in [(orders, "open orders"), (product_mapping, "product mapping"), (customer_mapping, "customer mapping")]:
            clean_columns(df)
            check_duplicate_columns(df, label)

        check_required_columns(orders, REQUIRED_ORDER_COLUMNS, "open orders")
        check_required_columns(product_mapping, REQUIRED_PRODUCT_MAPPING_COLUMNS, "product_mapping_final.csv")
        check_required_columns(customer_mapping, REQUIRED_CUSTOMER_MAPPING_COLUMNS, "customer_mapping_final.csv")
        logger.info("Required column checks passed.")

        check_unique_key(product_mapping, "full_sku_code", "product_mapping_final.csv")
        check_unique_key(customer_mapping, "customer_hash_key", "customer_mapping_final.csv")

        # --- Clean product join key ---
        orders["full_sku_code"] = orders["full_sku_code"].astype(str).str.strip()
        product_mapping["full_sku_code"] = product_mapping["full_sku_code"].astype(str).str.strip()

        # --- Clean customer fields + create customer hash key ---
        # NOTE: matches the CURRENTLY LIVE (pre-SCD2) key construction in
        # pseudonymize_order_history.py — source+ship+name. See module
        # docstring re: the coupling this creates.
        for col in ["source_customer_code", "ship_to_customer_code", "customer_name"]:
            orders[col] = orders[col].astype(str).str.strip().str.upper()

        rows_with_null_identity = orders[
            ["source_customer_code", "ship_to_customer_code", "customer_name"]
        ].isna().any(axis=1).sum()
        if rows_with_null_identity > 0:
            raise ValueError(
                f"{rows_with_null_identity:,} order row(s) have a null customer "
                f"identity field, which would corrupt customer hashing."
            )

        orders["customer_business_key"] = (
            orders["source_customer_code"] + "|" + orders["ship_to_customer_code"] + "|" + orders["customer_name"]
        )
        orders["customer_hash_key"] = orders["customer_business_key"].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest()
        )
        customer_mapping["customer_hash_key"] = customer_mapping["customer_hash_key"].astype(str).str.strip()

        # --- Merge product pseudonymization ---
        orders_pseudo = orders.merge(
            product_mapping[
                [
                    "full_sku_code",
                    "pseudo_category",
                    "pseudo_product_description",
                    "pseudo_full_sku_code",
                    "pseudo_first_half_sku_code",
                    "pseudo_unique_sku_code",
                ]
            ],
            on="full_sku_code",
            how="left",
        )

        unmapped_skus = (
            orders_pseudo[orders_pseudo["pseudo_full_sku_code"].isna()]["full_sku_code"]
            .drop_duplicates()
            .tolist()
        )
        if unmapped_skus:
            logger.error(f"Unmapped SKUs found: {unmapped_skus[:20]}")
            logger.error(f"Total unmapped SKUs: {len(unmapped_skus):,}")
            raise ValueError(
                "Some SKUs in open orders are missing from product_mapping_final.csv. "
                "This means open orders references a product that has never "
                "appeared in order history — the product pseudonymization "
                "stages need to run against a source that includes it."
            )
        logger.info("All SKUs successfully mapped.")

        # --- Merge customer pseudonymization ---
        orders_pseudo = orders_pseudo.merge(
            customer_mapping[["customer_hash_key", "pseudo_customer_name"]],
            on="customer_hash_key",
            how="left",
        )

        unmapped_customers = (
            orders_pseudo[orders_pseudo["pseudo_customer_name"].isna()]["customer_hash_key"]
            .drop_duplicates()
            .tolist()
        )
        if unmapped_customers:
            logger.error(f"Unmapped customers found: {unmapped_customers[:20]}")
            logger.error(f"Total unmapped customers: {len(unmapped_customers):,}")
            raise ValueError(
                "Some customers in open orders are missing from "
                "customer_mapping_final.csv. This means open orders "
                "references a customer that has never appeared in order "
                "history — run the customer mapping stages against a "
                "source that includes it."
            )
        logger.info("All customers successfully mapped.")

        if len(orders_pseudo) != len(orders):
            raise ValueError(
                f"Row count changed after joins. Original: {len(orders):,}, "
                f"After merge: {len(orders_pseudo):,}."
            )
        logger.info("Row count check passed after joins.")

        # --- Replace sensitive fields ---
        orders_pseudo["original_product_description_removed"] = True
        orders_pseudo["original_customer_name_removed"] = True
        orders_pseudo["original_sku_codes_removed"] = True

        orders_pseudo["product_description"] = orders_pseudo["pseudo_product_description"]
        orders_pseudo["customer_name"] = orders_pseudo["pseudo_customer_name"]
        orders_pseudo["full_sku_code"] = orders_pseudo["pseudo_full_sku_code"]
        orders_pseudo["first_half_sku_code"] = orders_pseudo["pseudo_first_half_sku_code"]
        orders_pseudo["unique_sku_code"] = orders_pseudo["pseudo_unique_sku_code"]

        # product_category never existed in the open orders source at all —
        # this is a NEW, purely additive enrichment from the product
        # mapping, not a replacement of real data.
        orders_pseudo["product_category"] = orders_pseudo["pseudo_category"]
        orders_pseudo["product_category_derived"] = True

        orders_pseudo = orders_pseudo.drop(columns=[
            "pseudo_product_description",
            "pseudo_category",
            "pseudo_customer_name",
            "pseudo_full_sku_code",
            "pseudo_first_half_sku_code",
            "pseudo_unique_sku_code",
            "customer_business_key",
            "customer_hash_key",
        ])

        # --- Row hash (for future incremental loading, same convention as order history) ---
        available_hash_columns = [c for c in HASH_COLUMNS if c in orders_pseudo.columns]
        missing_hash_columns = [c for c in HASH_COLUMNS if c not in orders_pseudo.columns]
        if missing_hash_columns:
            logger.warning(f"Expected hash columns not found, will be skipped: {missing_hash_columns}")

        orders_pseudo["row_hash"] = (
            orders_pseudo[available_hash_columns]
            .astype(str)
            .agg("|".join, axis=1)
            .apply(lambda x: hashlib.md5(x.encode()).hexdigest())
        )
        orders_pseudo["pseudonymized_at"] = datetime.now().isoformat()

        atomic_write_csv(orders_pseudo, output_file)

        logger.info("Pseudonymized open orders saved:")
        logger.info(str(output_file))
        logger.info(f"Original rows: {len(orders):,}")
        logger.info(f"Pseudonymized rows: {len(orders_pseudo):,}")

        return 0

    except Exception:
        logger.exception("Open orders pseudonymization failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())