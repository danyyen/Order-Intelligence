"""
src/privacy/customer_mapping_pipeline.py

Stage: assign stable pseudo customer names to every distinct customer
identity (customer_hash_key), implementing SCD Type 2 versioning for
customer_name changes.

Behavior:
- A customer_hash_key seen for the first time -> brand new customer,
  gets a new sequential pseudo_customer_name.
- A customer_hash_key already known, but with a customer_version_key
  not seen before -> the customer's real name changed. The existing
  pseudo_customer_name is REUSED (pseudo identity stays stable), the
  previously-current row is closed out (end_date set, is_current=False),
  and a new current row is inserted.
- Nothing changed -> file is left as-is.

Output contract (schema changed from the pre-SCD2 version):
    data/metadata/customer_mapping_final.csv
        columns: customer_version_key, customer_hash_key,
                 source_customer_code, ship_to_customer_code,
                 customer_name, pseudo_customer_name, effective_date,
                 end_date, is_current

NOTE: if you have an existing customer_mapping_final.csv from before this
SCD2 rewrite, run migrate_customer_mapping_to_scd2.py once against it
BEFORE running this script, or every existing customer will be
(incorrectly) treated as brand new and re-numbered from scratch.
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
    "customer_version_key",
    "customer_hash_key",
    "source_customer_code",
    "ship_to_customer_code",
    "customer_name",
    "pseudo_customer_name",
    "effective_date",
    "end_date",
    "is_current",
]

BASE_REQUIRED_COLUMNS = [
    "customer_version_key",
    "customer_hash_key",
    "source_customer_code",
    "ship_to_customer_code",
    "customer_name",
    "first_observed_date",
]

EXISTING_REQUIRED_COLUMNS = [
    "customer_version_key",
    "customer_hash_key",
    "pseudo_customer_name",
    "effective_date",
    "end_date",
    "is_current",
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
        check_required_columns(customer_base, BASE_REQUIRED_COLUMNS, "customer_mapping_base.csv")

        if customer_base.empty:
            raise ValueError(
                "customer_mapping_base.csv has 0 rows. This almost certainly "
                "means an upstream stage failed silently — refusing to proceed."
            )

        for col in ["customer_version_key", "customer_hash_key"]:
            customer_base[col] = customer_base[col].astype(str).str.strip()

        # --- Load existing mapping if it exists ---
        if customer_mapping_final_file.exists():
            existing_mapping = pd.read_csv(customer_mapping_final_file)
            existing_mapping = clean_columns(existing_mapping)
            check_duplicate_columns(existing_mapping, "customer_mapping_final.csv")
            check_required_columns(existing_mapping, EXISTING_REQUIRED_COLUMNS, "customer_mapping_final.csv")

            for col in ["customer_version_key", "customer_hash_key"]:
                existing_mapping[col] = existing_mapping[col].astype(str).str.strip()

            existing_mapping["is_current"] = parse_bool_column(existing_mapping["is_current"])
            if existing_mapping["is_current"].isna().any():
                raise ValueError(
                    "customer_mapping_final.csv has an is_current value that "
                    "isn't True/False. Check for corruption or manual edits."
                )
            existing_mapping["end_date"] = existing_mapping["end_date"].fillna("").astype(str)

            logger.info(f"Loaded existing customer mapping: {len(existing_mapping):,} version(s)")
        else:
            existing_mapping = pd.DataFrame(columns=FINAL_COLUMNS)
            existing_mapping["is_current"] = existing_mapping["is_current"].astype(bool)
            logger.info("No existing customer mapping found. Creating first mapping.")

        # --- Identify new VERSIONS (brand new customers OR name changes) ---
        existing_version_keys = set(existing_mapping["customer_version_key"])
        new_versions = customer_base[
            ~customer_base["customer_version_key"].isin(existing_version_keys)
        ].copy()
        logger.info(f"New customer version(s) found: {len(new_versions):,}")

        if len(new_versions) == 0:
            final_mapping = existing_mapping[FINAL_COLUMNS].copy()
        else:
            existing_hash_keys = set(existing_mapping["customer_hash_key"])
            brand_new_mask = ~new_versions["customer_hash_key"].isin(existing_hash_keys)

            brand_new = new_versions[brand_new_mask].copy()
            changed_name = new_versions[~brand_new_mask].copy()

            logger.info(f"  -> brand new customers: {len(brand_new):,}")
            logger.info(f"  -> existing customers with a changed name: {len(changed_name):,}")

            # --- Assign sequential pseudo names for brand new customers ---
            if len(brand_new) > 0:
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

                brand_new = brand_new.sort_values("customer_hash_key").reset_index(drop=True)
                brand_new["customer_sequence"] = range(start_num, start_num + len(brand_new))
                brand_new["pseudo_customer_name"] = (
                    "Customer CUST-" + brand_new["customer_sequence"].astype(str).str.zfill(6)
                )
                brand_new["effective_date"] = brand_new["first_observed_date"]
                brand_new["end_date"] = ""
                brand_new["is_current"] = True

            # --- Reuse existing pseudo name for customers with a changed name ---
            if len(changed_name) > 0:
                pseudo_lookup = (
                    existing_mapping[["customer_hash_key", "pseudo_customer_name"]]
                    .drop_duplicates(subset=["customer_hash_key"])
                    .set_index("customer_hash_key")["pseudo_customer_name"]
                )
                changed_name["pseudo_customer_name"] = changed_name["customer_hash_key"].map(pseudo_lookup)

                if changed_name["pseudo_customer_name"].isna().any():
                    bad_keys = changed_name.loc[
                        changed_name["pseudo_customer_name"].isna(), "customer_hash_key"
                    ].unique().tolist()
                    raise ValueError(
                        f"Internal error: could not find an existing pseudo_customer_name "
                        f"for customer_hash_key(s) flagged as a name change: {bad_keys}"
                    )

                changed_name["effective_date"] = changed_name["first_observed_date"]
                changed_name["end_date"] = ""
                changed_name["is_current"] = True

                # --- Close out the previously-current row for each changed customer ---
                new_effective_by_hash = changed_name.set_index("customer_hash_key")["effective_date"]
                close_mask = (
                    existing_mapping["customer_hash_key"].isin(set(changed_name["customer_hash_key"]))
                    & existing_mapping["is_current"]
                )
                existing_mapping.loc[close_mask, "end_date"] = (
                    existing_mapping.loc[close_mask, "customer_hash_key"].map(new_effective_by_hash)
                )
                existing_mapping.loc[close_mask, "is_current"] = False

            new_rows_parts = [df for df in [brand_new, changed_name] if len(df) > 0]
            new_rows = pd.concat(new_rows_parts, ignore_index=True) if new_rows_parts else pd.DataFrame(columns=FINAL_COLUMNS)

            final_mapping = pd.concat(
                [existing_mapping[FINAL_COLUMNS], new_rows[FINAL_COLUMNS]],
                ignore_index=True,
            )

        # --- Quality checks ---
        duplicate_versions = final_mapping["customer_version_key"].duplicated().sum()
        if duplicate_versions > 0:
            raise ValueError(f"Duplicate customer_version_key found: {duplicate_versions}")

        missing_names = final_mapping["pseudo_customer_name"].isna().sum()
        if missing_names > 0:
            raise ValueError(f"Missing pseudo customer names: {missing_names}")

        # Exactly one is_current=True row per customer_hash_key.
        current_rows = final_mapping[final_mapping["is_current"]]
        current_counts = current_rows.groupby("customer_hash_key").size()
        bad_counts = current_counts[current_counts != 1]
        if len(bad_counts) > 0:
            raise ValueError(
                f"Customer(s) with an invalid number of current versions "
                f"(expected exactly 1): {bad_counts.to_dict()}"
            )

        all_hash_keys = set(final_mapping["customer_hash_key"])
        current_hash_keys = set(current_rows["customer_hash_key"])
        missing_current = all_hash_keys - current_hash_keys
        if missing_current:
            raise ValueError(
                f"Customer(s) with NO current version at all: {missing_current}"
            )

        atomic_write_csv(final_mapping, customer_mapping_final_file)

        logger.info(f"Customer mapping saved: {len(final_mapping):,} version row(s), "
                    f"{final_mapping['customer_hash_key'].nunique():,} distinct customer(s)")
        logger.info(str(customer_mapping_final_file))

        return 0

    except Exception:
        logger.exception("Customer mapping pipeline failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())