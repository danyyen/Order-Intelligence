"""
src/privacy/pseudonymize_order_history.py

Stage: join customer_mapping_final.csv and product_mapping_final.csv onto
the latest standardized order history, replacing every sensitive/
identifying field (customer name, product description/category, and all
three SKU code fields) with their pseudo equivalents.

Output contract (unchanged):
    data/pseudonymized/order_history_pseudonymized_<batch_id>.csv
"""

from __future__ import annotations

import argparse
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
logger = logging.getLogger("pseudonymize_order_history")

REQUIRED_ORDER_COLUMNS = [
    "full_sku_code",
    "first_half_sku_code",
    "unique_sku_code",
    "product_description",
    "product_category",
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

REQUIRED_CUSTOMER_MAPPING_COLUMNS = ["customer_hash_key", "pseudo_customer_name", "is_current"]

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
    "shipped_date",
    "delivery_route",
    "order_status",
    "ordered_quantity",
    "full_sku_code",
    "first_half_sku_code",
    "unique_sku_code",
    "final_order_quantity",
    "shipped_order_quantity",
    "quantity_short",
    "shipped_order_weight",
    "order_amount",
    "product_category",
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


def parse_bool_column(series: pd.Series) -> pd.Series:
    """
    Robustly parse a boolean column that may come back from CSV as actual
    bools, or as the strings "True"/"False" — a naive .astype(bool) on
    strings is a classic trap, since the non-empty string "False" is
    truthy in Python.
    """
    if series.dtype == bool:
        return series
    return (
        series.astype(str).str.strip().str.lower()
        .map({"true": True, "false": False})
    )


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
    parser = argparse.ArgumentParser(description="Pseudonymize the latest standardized order history.")
    parser.add_argument("--file", default=None, help="Explicit path to a standardized order history CSV.")
    args = parser.parse_args()

    PSEUDONYMIZED_DIR.mkdir(parents=True, exist_ok=True)

    product_mapping_file = METADATA_DIR / "product_mapping_final.csv"
    customer_mapping_file = METADATA_DIR / "customer_mapping_final.csv"

    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = PSEUDONYMIZED_DIR / f"order_history_pseudonymized_{batch_id}.csv"

    try:
        input_file = find_input_file(args.file)

        if not product_mapping_file.exists():
            raise FileNotFoundError(
                f"{product_mapping_file} not found. Run pseudonymize_products.py "
                "and sku_mapping_pipeline.py first."
            )
        if not customer_mapping_file.exists():
            raise FileNotFoundError(
                f"{customer_mapping_file} not found. Run customer_mapping_pipeline.py first."
            )

        orders = pd.read_csv(input_file)
        product_mapping = pd.read_csv(product_mapping_file)
        customer_mapping = pd.read_csv(customer_mapping_file)

        logger.info(f"Loaded order history rows: {len(orders):,}")
        logger.info(f"Loaded product mapping rows: {len(product_mapping):,}")
        logger.info(f"Loaded customer mapping rows: {len(customer_mapping):,}")
        logger.info(f"Input file: {input_file}")

        if orders.empty:
            raise ValueError(f"{input_file.name} has 0 rows. Refusing to pseudonymize an empty file.")
        if product_mapping.empty:
            raise ValueError("product_mapping_final.csv has 0 rows.")
        if customer_mapping.empty:
            raise ValueError("customer_mapping_final.csv has 0 rows.")

        for df, label in [(orders, "order history"), (product_mapping, "product mapping"), (customer_mapping, "customer mapping")]:
            clean_columns(df)
            check_duplicate_columns(df, label)

        check_required_columns(orders, REQUIRED_ORDER_COLUMNS, "order history")
        check_required_columns(product_mapping, REQUIRED_PRODUCT_MAPPING_COLUMNS, "product_mapping_final.csv")
        check_required_columns(customer_mapping, REQUIRED_CUSTOMER_MAPPING_COLUMNS, "customer_mapping_final.csv")
        logger.info("Required column checks passed.")

        # customer_mapping_final.csv is an SCD Type 2 table — customer_hash_key
        # legitimately repeats across historical (non-current) versions of the
        # same customer. Only the CURRENT version of each customer is relevant
        # for pseudonymizing today's orders.
        customer_mapping["is_current"] = parse_bool_column(customer_mapping["is_current"])
        if customer_mapping["is_current"].isna().any():
            raise ValueError(
                "customer_mapping_final.csv has an is_current value that isn't "
                "True/False. Check for corruption or manual edits."
            )
        customer_mapping_current = customer_mapping[customer_mapping["is_current"]].copy()

        # --- Guard against join fan-out from a corrupted mapping file ---
        check_unique_key(product_mapping, "full_sku_code", "product_mapping_final.csv")
        check_unique_key(
            customer_mapping_current, "customer_hash_key",
            "customer_mapping_final.csv (current versions)",
        )

        # --- Clean product join key ---
        orders["full_sku_code"] = orders["full_sku_code"].astype(str).str.strip()
        product_mapping["full_sku_code"] = product_mapping["full_sku_code"].astype(str).str.strip()

        # --- Clean customer fields + create customer hash key ---
        # customer_hash_key is the DURABLE identity — built from the two
        # business codes only, NOT customer_name. A name change (typo fix,
        # legal rename) must not fork the order into a "new" customer; that's
        # exactly what SCD2 in customer_mapping_pipeline.py exists to handle.
        for col in ["source_customer_code", "ship_to_customer_code", "customer_name"]:
            orders[col] = orders[col].astype(str).str.strip().str.upper()

        rows_with_null_identity = orders[
            ["source_customer_code", "ship_to_customer_code"]
        ].isna().any(axis=1).sum()
        if rows_with_null_identity > 0:
            raise ValueError(
                f"{rows_with_null_identity:,} order row(s) have a null "
                f"source_customer_code or ship_to_customer_code, which would "
                f"corrupt customer hashing."
            )

        orders["customer_hash_key"] = orders.apply(
            lambda r: hashlib.md5(f"{r['source_customer_code']}|{r['ship_to_customer_code']}".encode()).hexdigest(),
            axis=1,
        )
        customer_mapping_current["customer_hash_key"] = (
            customer_mapping_current["customer_hash_key"].astype(str).str.strip()
        )

        # --- Merge product pseudonymization (category, description, all 3 SKU codes) ---
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
                "Some SKUs are missing from product_mapping_final.csv. "
                "Run pseudonymize_products.py and sku_mapping_pipeline.py first."
            )
        logger.info("All SKUs successfully mapped.")

        # --- Merge customer pseudonymization ---
        orders_pseudo = orders_pseudo.merge(
            customer_mapping_current[["customer_hash_key", "pseudo_customer_name"]],
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
                "Some customers are missing from customer_mapping_final.csv. "
                "Run customer_mapping_pipeline.py first."
            )
        logger.info("All customers successfully mapped.")

        # --- Row count must not change across either join ---
        if len(orders_pseudo) != len(orders):
            raise ValueError(
                f"Row count changed after joins. Original: {len(orders):,}, "
                f"After merge: {len(orders_pseudo):,}. This should be impossible "
                f"now that mapping key uniqueness is checked — investigate before continuing."
            )
        logger.info("Row count check passed after joins.")

        # --- Replace sensitive fields ---
        orders_pseudo["original_product_description_removed"] = True
        orders_pseudo["original_product_category_removed"] = True
        orders_pseudo["original_customer_name_removed"] = True
        orders_pseudo["original_sku_codes_removed"] = True

        orders_pseudo["product_description"] = orders_pseudo["pseudo_product_description"]
        orders_pseudo["product_category"] = orders_pseudo["pseudo_category"]
        orders_pseudo["customer_name"] = orders_pseudo["pseudo_customer_name"]
        orders_pseudo["full_sku_code"] = orders_pseudo["pseudo_full_sku_code"]
        orders_pseudo["first_half_sku_code"] = orders_pseudo["pseudo_first_half_sku_code"]
        orders_pseudo["unique_sku_code"] = orders_pseudo["pseudo_unique_sku_code"]

        # --- Drop temporary/sensitive helper columns ---
        orders_pseudo = orders_pseudo.drop(columns=[
            "pseudo_product_description",
            "pseudo_category",
            "pseudo_customer_name",
            "pseudo_full_sku_code",
            "pseudo_first_half_sku_code",
            "pseudo_unique_sku_code",
            "customer_hash_key",
        ])

        # --- Row hash for incremental loading ---
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

        # --- Export ---
        atomic_write_csv(orders_pseudo, output_file)

        logger.info("Pseudonymized order history saved:")
        logger.info(str(output_file))
        logger.info(f"Original rows: {len(orders):,}")
        logger.info(f"Pseudonymized rows: {len(orders_pseudo):,}")

        return 0

    except Exception:
        logger.exception("Order history pseudonymization failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())