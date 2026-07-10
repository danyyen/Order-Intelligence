"""
src/privacy/customer_mapping_pipeline.py

Stage: assign stable pseudo customer names to every customer_hash_key in
customer_mapping_base.csv, preserving pseudo names already assigned in
customer_mapping_final.csv and only generating new ones for customers
that haven't been seen before.

Output contract (unchanged):
    data/metadata/customer_mapping_final.csv
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
logger = logging.getLogger("customer_mapping_pipeline")

FINAL_COLUMNS = [
    "customer_hash_key",
    "source_customer_code",
    "ship_to_customer_code",
    "customer_name",
    "pseudo_customer_name",
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


def atomic_write_csv(df: pd.DataFrame, output_file: Path) -> None:
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_csv(tmp_file, index=False, encoding="utf-8-sig")
    os.replace(tmp_file, output_file)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    customer_base_file = METADATA_DIR / "customer_mapping_base.csv"
    customer_mapping_final_file = METADATA_DIR / "customer_mapping_final.csv"

    try:
        if not customer_base_file.exists():
            raise FileNotFoundError(
                f"{customer_base_file} not found. Run profile_customers.py first."
            )

        customer_base = pd.read_csv(customer_base_file)
        customer_base = clean_columns(customer_base)
        check_duplicate_columns(customer_base, "customer_mapping_base.csv")

        if customer_base.empty:
            raise ValueError(
                "customer_mapping_base.csv has 0 rows. This almost certainly "
                "means an upstream stage failed silently — refusing to proceed."
            )

        if "customer_hash_key" not in customer_base.columns:
            raise ValueError(
                "customer_mapping_base.csv is missing required column: customer_hash_key"
            )

        customer_base["customer_hash_key"] = (
            customer_base["customer_hash_key"].astype(str).str.strip()
        )

        # --- Load existing mapping if it exists ---
        if customer_mapping_final_file.exists():
            existing_mapping = pd.read_csv(customer_mapping_final_file)
            existing_mapping = clean_columns(existing_mapping)
            check_duplicate_columns(existing_mapping, "customer_mapping_final.csv")

            required_existing = ["customer_hash_key", "pseudo_customer_name"]
            missing_existing = [c for c in required_existing if c not in existing_mapping.columns]
            if missing_existing:
                raise ValueError(
                    f"customer_mapping_final.csv is missing required column(s): {missing_existing}"
                )

            existing_mapping["customer_hash_key"] = (
                existing_mapping["customer_hash_key"].astype(str).str.strip()
            )
            logger.info(f"Loaded existing customer mapping: {len(existing_mapping):,}")
        else:
            existing_mapping = pd.DataFrame(columns=FINAL_COLUMNS)
            logger.info("No existing customer mapping found.")

        # --- Identify new customers ---
        existing_keys = set(existing_mapping["customer_hash_key"])
        new_customers = customer_base[
            ~customer_base["customer_hash_key"].isin(existing_keys)
        ].copy()
        logger.info(f"New customers found: {len(new_customers):,}")

        # --- Generate new pseudo customers ---
        if len(new_customers) > 0:
            if len(existing_mapping) > 0:
                existing_seq = (
                    existing_mapping["pseudo_customer_name"]
                    .str.extract(r"(\d+)$")[0]
                    .astype(float)
                )

                if existing_seq.isna().all():
                    raise ValueError(
                        "Could not determine the next customer sequence number: "
                        "every existing pseudo_customer_name failed to match the "
                        "expected trailing-digit pattern (e.g. 'Customer CUST-000123'). "
                        "Check customer_mapping_final.csv for corruption or manual "
                        "edits that broke the naming format."
                    )

                start_num = int(existing_seq.max()) + 1
            else:
                start_num = 1

            new_customers = new_customers.sort_values("customer_hash_key").reset_index(drop=True)
            new_customers["customer_sequence"] = range(start_num, start_num + len(new_customers))
            new_customers["pseudo_customer_name"] = (
                "Customer CUST-" + new_customers["customer_sequence"].astype(str).str.zfill(6)
            )
        else:
            new_customers["pseudo_customer_name"] = None

        # --- Combine existing + new ---
        final_mapping = pd.concat(
            [existing_mapping[FINAL_COLUMNS], new_customers[FINAL_COLUMNS]],
            ignore_index=True,
        )

        # --- Quality checks ---
        duplicate_keys = final_mapping["customer_hash_key"].duplicated().sum()
        if duplicate_keys > 0:
            raise ValueError(f"Duplicate customer keys found: {duplicate_keys}")

        missing_names = final_mapping["pseudo_customer_name"].isna().sum()
        if missing_names > 0:
            raise ValueError(f"Missing pseudo customer names: {missing_names}")

        # --- Export ---
        atomic_write_csv(final_mapping, customer_mapping_final_file)

        logger.info(f"Customer mapping saved: {len(final_mapping):,} customers")
        logger.info(str(customer_mapping_final_file))

        return 0

    except Exception:
        logger.exception("Customer mapping pipeline failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())