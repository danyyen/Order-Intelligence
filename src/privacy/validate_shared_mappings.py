"""
src/privacy/validate_shared_mappings.py

Stage: validate the integrity of customer_mapping_final.csv and
product_mapping_final.csv immediately after the shared foundation
finishes building them, before any of the three dataset tracks
(order_history, open_orders, inventory) start consuming them.

Every track already re-derives some of these checks independently
(see pseudonymize_order_history.py, pseudonymize_open_orders.py,
pseudonymize_inventory.py, add_*_only_*.py) because each one has to
defend itself against a corrupted mapping file. This stage doesn't
replace those defenses — it exists so a corrupted shared mapping is
caught ONCE, with one clear error, before three tracks independently
spend time discovering the same root cause three different ways.

This is the shared-track counterpart to the per-track quality gates:
critical, so a failure here stops the entire pipeline (see
run_pipeline.py) — nothing downstream can trust a broken foundation.

Checks performed:
    - Both mapping files exist and are non-empty.
    - Required columns are present in each.
    - Real key columns (customer_hash_key, full_sku_code,
      first_half_sku_code, unique_sku_code) contain no nulls — a mapping
      row with a null real key is never trustworthy.
    - customer_hash_key is unique in customer_mapping_final.csv, and
      full_sku_code is unique in product_mapping_final.csv (row-level
      join safety — pseudonymize_order_history.py depends on this).
    - No pseudo identifier is null.
    - Every real key <-> pseudo value pair is checked bidirectionally,
      for all four key/pseudo pairs (customer, full SKU, first-half
      SKU, unique SKU):
        - one real key never maps to two different pseudo values
          (a "split identity" — the same real entity quietly getting
          two different fake ones)
        - one pseudo value is never reused across two different real
          keys (an "identity collision" — the structural shape of the
          "428 duplicate identities" bug documented in docs/DECISIONS.md)
      A naive "is pseudo_col ever duplicated" check only catches the
      second direction, and is actively wrong for the two group-code
      columns (first_half_sku_code / unique_sku_code), where many rows
      legitimately share one pseudo value.
    - Every pseudo customer name / product description actually looks
      pseudonymized, not a passthrough of the real value.

Output contract:
    Writes nothing on success. Raises (non-zero exit) on any failure.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import METADATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("validate_shared_mappings")

CUSTOMER_MAPPING_FILE = METADATA_DIR / "customer_mapping_final.csv"
PRODUCT_MAPPING_FILE = METADATA_DIR / "product_mapping_final.csv"

REQUIRED_CUSTOMER_COLUMNS = [
    "customer_hash_key",
    "source_customer_code",
    "ship_to_customer_code",
    "customer_name",
    "pseudo_customer_name",
]

REQUIRED_PRODUCT_COLUMNS = [
    "full_sku_code",
    "first_half_sku_code",
    "unique_sku_code",
    "pseudo_category",
    "pseudo_product_description",
    "pseudo_full_sku_code",
    "pseudo_first_half_sku_code",
    "pseudo_unique_sku_code",
]


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def load_mapping(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"{label} not found at {path}. Run the shared mapping stages first."
        )
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    dupes = df.columns[df.columns.duplicated()].unique().tolist()
    if dupes:
        raise ValueError(f"{label} has duplicate column names: {dupes}")

    if df.empty:
        raise ValueError(f"{label} has 0 rows. Refusing to trust an empty shared mapping.")

    return df


def check_required_columns(df: pd.DataFrame, required: list[str], label: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required column(s): {missing}")


def check_unique_key(df: pd.DataFrame, key_col: str, label: str) -> None:
    dupes = df[key_col][df[key_col].duplicated()].unique().tolist()
    if dupes:
        raise ValueError(
            f"{label} has duplicate {key_col} value(s): {dupes[:20]}"
            f"{' (truncated)' if len(dupes) > 20 else ''}. Any downstream join "
            f"on this key will silently multiply rows."
        )


def check_no_nulls(df: pd.DataFrame, col: str, label: str) -> None:
    missing = int(df[col].isna().sum())
    if missing > 0:
        raise ValueError(f"{label} has {missing:,} null value(s) in {col}.")


def check_one_to_one_mapping(df: pd.DataFrame, key_col: str, pseudo_col: str, label: str) -> None:
    """
    Every distinct real key must map to exactly one pseudo value, AND
    every pseudo value must belong to exactly one real key — checked in
    BOTH directions. A naive "is pseudo_col ever duplicated" check only
    catches the second direction and would also be wrong applied
    row-wise for group codes (first_half_sku_code / unique_sku_code),
    where many rows legitimately share one pseudo value — deduping to
    distinct (key, pseudo) pairs first is what makes both directions
    checkable correctly.

    Catches both:
        real key A -> pseudo 001, real key B -> pseudo 001  (identity collision)
        real key A -> pseudo 001, real key A -> pseudo 002  (split identity)
    The second case is the one a pseudo-column-only duplicate check
    would silently miss. It's also the exact class of corruption
    assign_stable_codes() in sku_mapping_pipeline.py could paper over
    without ever raising, since it loads prior mappings via
    drop_duplicates(subset=[key_col]) — if a key ever already had two
    conflicting pseudo values on disk, that line just keeps one and
    discards the evidence.
    """
    mapping = df[[key_col, pseudo_col]].drop_duplicates()

    pseudo_per_key = mapping.groupby(key_col)[pseudo_col].nunique()
    inconsistent_keys = pseudo_per_key[pseudo_per_key > 1]
    if not inconsistent_keys.empty:
        raise ValueError(
            f"{label}: {key_col} value(s) map to multiple {pseudo_col} "
            f"values (split identity): {inconsistent_keys.index.tolist()[:20]}"
        )

    keys_per_pseudo = mapping.groupby(pseudo_col)[key_col].nunique()
    reused_pseudos = keys_per_pseudo[keys_per_pseudo > 1]
    if not reused_pseudos.empty:
        raise ValueError(
            f"{label}: {pseudo_col} value(s) are reused across multiple "
            f"{key_col} values (identity collision): {reused_pseudos.index.tolist()[:20]}"
        )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    try:
        # --- Customer mapping ---
        customers = load_mapping(CUSTOMER_MAPPING_FILE, "customer_mapping_final.csv")
        check_required_columns(customers, REQUIRED_CUSTOMER_COLUMNS, "customer_mapping_final.csv")
        check_no_nulls(customers, "customer_hash_key", "customer_mapping_final.csv")
        check_unique_key(customers, "customer_hash_key", "customer_mapping_final.csv")
        check_no_nulls(customers, "pseudo_customer_name", "customer_mapping_final.csv")
        check_one_to_one_mapping(
            customers, "customer_hash_key", "pseudo_customer_name", "customer_mapping_final.csv"
        )

        bad_customer_names = customers[
            ~customers["pseudo_customer_name"].astype(str).str.startswith("Customer CUST-")
        ]
        if len(bad_customer_names) > 0:
            raise ValueError(
                f"customer_mapping_final.csv has {len(bad_customer_names):,} "
                f"pseudo_customer_name value(s) that don't look pseudonymized "
                f"(expected the 'Customer CUST-######' format)."
            )

        logger.info(f"customer_mapping_final.csv: {len(customers):,} customers, all checks passed.")

        # --- Product / SKU mapping ---
        products = load_mapping(PRODUCT_MAPPING_FILE, "product_mapping_final.csv")
        check_required_columns(products, REQUIRED_PRODUCT_COLUMNS, "product_mapping_final.csv")

        for real_key_col in ["full_sku_code", "first_half_sku_code", "unique_sku_code"]:
            check_no_nulls(products, real_key_col, "product_mapping_final.csv")

        check_unique_key(products, "full_sku_code", "product_mapping_final.csv")

        for col in ["pseudo_full_sku_code", "pseudo_first_half_sku_code", "pseudo_unique_sku_code", "pseudo_category"]:
            check_no_nulls(products, col, "product_mapping_final.csv")

        check_one_to_one_mapping(
            products, "full_sku_code", "pseudo_full_sku_code", "product_mapping_final.csv"
        )
        check_one_to_one_mapping(
            products, "first_half_sku_code", "pseudo_first_half_sku_code", "product_mapping_final.csv"
        )
        check_one_to_one_mapping(
            products, "unique_sku_code", "pseudo_unique_sku_code", "product_mapping_final.csv"
        )

        bad_descriptions = products[
            ~products["pseudo_product_description"].astype(str).str.contains(" Product ", na=False)
        ]
        if len(bad_descriptions) > 0:
            raise ValueError(
                f"product_mapping_final.csv has {len(bad_descriptions):,} "
                f"pseudo_product_description value(s) that don't look "
                f"pseudonymized (expected to contain ' Product ')."
            )

        logger.info(f"product_mapping_final.csv: {len(products):,} products, all checks passed.")

        logger.info("Shared mapping validation passed. Safe for all three tracks to proceed.")
        return 0

    except Exception:
        logger.exception("Shared mapping validation failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
