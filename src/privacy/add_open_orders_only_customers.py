"""
src/privacy/add_open_orders_only_customers.py

Supplemental stage: some customers appear in open orders that have never
appeared in a completed order history extract (an order can be placed
before it ever ships/completes). This stage finds exactly those
customers and adds them to customer_mapping_final.csv, using the same
incremental sequential-numbering logic customer_mapping_pipeline.py
already uses — without modifying that script.

IMPORTANT: builds the customer identity key the same way the CURRENTLY
LIVE (pre-SCD2) pseudonymize_order_history.py / pseudonymize_open_orders.py
do — source_customer_code + ship_to_customer_code + customer_name. Must
stay in sync with those scripts, or newly-added customers here won't be
recognized by the pseudonymization join.

Run this AFTER standardize_open_orders and AFTER the order-history
customer mapping stages, BEFORE pseudonymize_open_orders.

Output contract:
    data/metadata/customer_mapping_final.csv (updated in place)
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

from config.paths import STANDARDIZED_DIR, METADATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("add_open_orders_only_customers")

FINAL_COLUMNS = [
    "customer_hash_key",
    "source_customer_code",
    "ship_to_customer_code",
    "customer_name",
    "pseudo_customer_name",
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


def main() -> int:
    customer_mapping_file = METADATA_DIR / "customer_mapping_final.csv"

    try:
        if not customer_mapping_file.exists():
            raise FileNotFoundError(
                f"{customer_mapping_file} not found. Run customer_mapping_pipeline.py first."
            )

        input_file = find_input_file()
        orders = pd.read_csv(input_file)
        orders.columns = orders.columns.str.strip().str.lower().str.replace(" ", "_")

        required = ["source_customer_code", "ship_to_customer_code", "customer_name"]
        missing = [c for c in required if c not in orders.columns]
        if missing:
            raise ValueError(f"Open orders file is missing required column(s): {missing}")

        rows_with_null_identity = orders[required].isna().any(axis=1).sum()
        if rows_with_null_identity > 0:
            raise ValueError(
                f"{rows_with_null_identity:,} open orders row(s) have a null "
                f"customer identity field, which would corrupt customer hashing."
            )

        for col in required:
            orders[col] = orders[col].astype(str).str.strip().str.upper()

        orders["customer_hash_key"] = orders.apply(
            lambda r: hashlib.md5(
                f"{r['source_customer_code']}|{r['ship_to_customer_code']}|{r['customer_name']}".encode()
            ).hexdigest(),
            axis=1,
        )

        existing = pd.read_csv(customer_mapping_file)
        existing.columns = existing.columns.str.strip().str.lower().str.replace(" ", "_")
        existing["customer_hash_key"] = existing["customer_hash_key"].astype(str).str.strip()

        required_existing = ["customer_hash_key", "pseudo_customer_name"]
        missing_existing = [c for c in required_existing if c not in existing.columns]
        if missing_existing:
            raise ValueError(f"customer_mapping_final.csv is missing required column(s): {missing_existing}")

        existing_keys = set(existing["customer_hash_key"])
        new_rows = (
            orders[~orders["customer_hash_key"].isin(existing_keys)]
            [["customer_hash_key", "source_customer_code", "ship_to_customer_code", "customer_name"]]
            .drop_duplicates(subset=["customer_hash_key"])
            .sort_values("customer_hash_key")
            .reset_index(drop=True)
        )

        if new_rows.empty:
            logger.info("No open-orders-only customers found. Nothing to add.")
            return 0

        logger.info(f"Found {len(new_rows):,} customer(s) present in open orders but not in order history.")

        existing_seq = existing["pseudo_customer_name"].str.extract(r"(\d+)$")[0].astype(float)
        if len(existing) > 0 and existing_seq.isna().all():
            raise ValueError(
                "Could not determine the next customer sequence number: every "
                "existing pseudo_customer_name failed to match the expected "
                "trailing-digit pattern. Check customer_mapping_final.csv for corruption."
            )
        start_num = int(existing_seq.max()) + 1 if existing_seq.notna().any() else 1

        new_rows["customer_sequence"] = range(start_num, start_num + len(new_rows))
        new_rows["pseudo_customer_name"] = (
            "Customer CUST-" + new_rows["customer_sequence"].astype(str).str.zfill(6)
        )

        final_mapping = pd.concat(
            [existing[FINAL_COLUMNS], new_rows[FINAL_COLUMNS]], ignore_index=True
        )

        duplicate_keys = final_mapping["customer_hash_key"].duplicated().sum()
        if duplicate_keys > 0:
            raise ValueError(f"Duplicate customer_hash_key found after merge: {duplicate_keys}")

        missing_names = final_mapping["pseudo_customer_name"].isna().sum()
        if missing_names > 0:
            raise ValueError(f"Missing pseudo customer names after merge: {missing_names}")

        atomic_write_csv(final_mapping, customer_mapping_file)

        logger.info(f"Added {len(new_rows):,} open-orders-only customer(s) to customer_mapping_final.csv")
        logger.info(f"Total customers now: {len(final_mapping):,}")

        return 0

    except Exception:
        logger.exception("Adding open-orders-only customers failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())