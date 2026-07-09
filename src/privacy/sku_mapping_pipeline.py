"""
src/privacy/sku_mapping_pipeline.py

Stage: assign stable pseudo codes to full_sku_code, first_half_sku_code,
and unique_sku_code within product_mapping_final.csv, preserving every
pseudo code already assigned and only generating new ones for keys not
seen before.

This stage reads AND overwrites product_mapping_final.csv in place, so a
bad write here doesn't just produce bad output — it corrupts the file the
next run depends on. Every write goes through atomic_write_csv for that
reason.

Output contract (unchanged):
    data/metadata/product_mapping_final.csv (updated in place)
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
logger = logging.getLogger("sku_mapping_pipeline")

REQUIRED_COLUMNS = [
    "full_sku_code",
    "first_half_sku_code",
    "unique_sku_code",
    "pseudo_category",
    "pseudo_product_description",
]

VALUE_CLEAN_COLUMNS = [
    "full_sku_code",
    "first_half_sku_code",
    "unique_sku_code",
    "pseudo_category",
]


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def check_duplicate_columns(df: pd.DataFrame, label: str) -> None:
    dupes = df.columns[df.columns.duplicated()].unique().tolist()
    if dupes:
        raise ValueError(f"{label} has duplicate column names: {dupes}")


def atomic_write_csv(df: pd.DataFrame, output_file: Path) -> None:
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_csv(tmp_file, index=False, encoding="utf-8-sig")
    os.replace(tmp_file, output_file)


def safe_extract_int(series: pd.Series, pattern: str, label: str) -> pd.Series:
    """
    Extract a numeric suffix and cast to int, but fail with a clear message
    naming the offending values instead of a generic pandas cast error if
    any value doesn't match the expected pattern.
    """
    extracted = series.str.extract(pattern)[0]
    if extracted.isna().any():
        bad_values = series[extracted.isna()].unique().tolist()
        raise ValueError(
            f"Could not extract a numeric sequence from existing {label} "
            f"value(s): {bad_values}. Check product_mapping_final.csv for "
            f"corruption or manual edits that broke the expected format."
        )
    return extracted.astype(int)


def check_key_to_pseudo_consistency(df: pd.DataFrame, key_col: str, pseudo_col: str) -> None:
    """
    pseudo_first_half_sku_code and pseudo_unique_sku_code are GROUP codes —
    many rows are expected to share the same value, so a plain duplicate-row
    check would be wrong. The actual invariant is: every distinct key_col
    value must map to exactly one pseudo_col value. If the same pseudo code
    ever got assigned to two different real keys, that's a collision bug.
    """
    mapping = df[[key_col, pseudo_col]].drop_duplicates()
    collisions = mapping[mapping[pseudo_col].duplicated(keep=False)].sort_values(pseudo_col)
    if len(collisions) > 0:
        raise ValueError(
            f"The same {pseudo_col} value is assigned to multiple distinct "
            f"{key_col} values — this indicates a pseudo-code collision bug:\n"
            f"{collisions}"
        )


def assign_stable_codes(df, key_col, pseudo_col, prefix, pad=5, sort_key=None):
    """
    Assigns pseudo codes to unique values of key_col, preserving any
    existing key -> pseudo_col mapping already present in df, and only
    assigning new sequential codes to keys that don't have one yet.
    """
    if pseudo_col in df.columns:
        existing = (
            df[[key_col, pseudo_col]]
            .dropna(subset=[pseudo_col])
            .drop_duplicates(subset=[key_col])
        )
    else:
        existing = pd.DataFrame(columns=[key_col, pseudo_col])

    all_keys = df[[key_col]].drop_duplicates()
    if sort_key is not None:
        all_keys = all_keys.sort_values(sort_key)
    else:
        all_keys = all_keys.sort_values(key_col)
    all_keys = all_keys.reset_index(drop=True)

    new_keys = all_keys[~all_keys[key_col].isin(existing[key_col])]

    if len(existing) > 0:
        used_numbers = safe_extract_int(existing[pseudo_col], r"(\d+)$", pseudo_col)
        next_num = used_numbers.max() + 1
    else:
        next_num = 1

    new_keys = new_keys.copy()
    new_keys[pseudo_col] = [
        f"{prefix}{str(next_num + i).zfill(pad)}"
        for i in range(len(new_keys))
    ]

    mapping = pd.concat(
        [existing, new_keys[[key_col, pseudo_col]]],
        ignore_index=True,
    )

    df = df.drop(columns=[pseudo_col], errors="ignore").merge(
        mapping, on=key_col, how="left"
    )
    return df


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    product_mapping_file = METADATA_DIR / "product_mapping_final.csv"

    try:
        if not product_mapping_file.exists():
            raise FileNotFoundError(
                f"{product_mapping_file} not found. Run pseudonymize_products.py first."
            )

        df = pd.read_csv(product_mapping_file)

        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        check_duplicate_columns(df, "product_mapping_final.csv")

        if df.empty:
            raise ValueError(
                "product_mapping_final.csv has 0 rows. This almost certainly "
                "means an upstream stage failed silently — refusing to proceed."
            )

        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        for col in VALUE_CLEAN_COLUMNS:
            df[col] = df[col].astype(str).str.strip()

        if (df["pseudo_category"].str.lower() == "nan").any():
            raise ValueError(
                "Found row(s) with a null pseudo_category after cleaning. This "
                "should have been caught by pseudonymize_products.py — check "
                "for a manually edited or corrupted product_mapping_final.csv."
            )

        before = len(df)
        df = df.drop_duplicates(subset=["full_sku_code"]).reset_index(drop=True)
        after = len(df)
        logger.info(f"Removed duplicate product rows: {before - after:,}")
        logger.info(f"Products remaining: {after:,}")

        # --- pseudo_full_sku_code (category-scoped, needs sequence per category) ---
        if "pseudo_full_sku_code" in df.columns:
            logger.info("pseudo_full_sku_code already exists. Preserving existing values for known SKUs.")
            existing_full = df[["full_sku_code", "pseudo_full_sku_code"]].dropna(
                subset=["pseudo_full_sku_code"]
            )
        else:
            existing_full = pd.DataFrame(columns=["full_sku_code", "pseudo_full_sku_code"])

        new_rows_mask = ~df["full_sku_code"].isin(existing_full["full_sku_code"])

        if new_rows_mask.any():
            categories = sorted(df["pseudo_category"].unique())
            prefix_map = {}
            used_prefixes = set()
            for category in categories:
                base_prefix = "".join(w[0] for w in category.split()).upper()
                prefix = base_prefix
                suffix = 1
                while prefix in used_prefixes:
                    suffix += 1
                    prefix = f"{base_prefix}{suffix}"
                used_prefixes.add(prefix)
                prefix_map[category] = prefix

            df["category_prefix"] = df["pseudo_category"].map(prefix_map)

            if len(existing_full) > 0:
                # Only bring full_sku_code/pseudo_category from df into this
                # merge — df may already have its own pseudo_full_sku_code
                # column (with NaNs for new rows) from the file we just read.
                # Merging that against existing_full's pseudo_full_sku_code
                # column would make pandas silently suffix both as
                # pseudo_full_sku_code_x/_y, breaking the next line.
                prior = df[["full_sku_code", "pseudo_category"]].merge(
                    existing_full, on="full_sku_code", how="inner"
                )
                prior_seq = safe_extract_int(
                    prior["pseudo_full_sku_code"], r"-(\d+)$", "pseudo_full_sku_code"
                )
                prior_max_by_cat = (
                    prior.assign(seq=prior_seq)
                    .groupby("pseudo_category")["seq"]
                    .max()
                )
            else:
                prior_max_by_cat = pd.Series(dtype=int)

            df = df.sort_values(["pseudo_category", "pseudo_product_description"]).reset_index(drop=True)

            def next_seq(group):
                cat = group.name
                start = int(prior_max_by_cat.get(cat, 0))
                is_new = ~group["full_sku_code"].isin(existing_full["full_sku_code"])
                seq = pd.Series(index=group.index, dtype="object")
                counter = start
                for idx in group.index:
                    if is_new.loc[idx]:
                        counter += 1
                        seq.loc[idx] = counter
                return seq

            new_seq = df.groupby("pseudo_category", group_keys=False).apply(
                next_seq, include_groups=False
            )

            df["pseudo_full_sku_code"] = df["full_sku_code"].map(
                existing_full.set_index("full_sku_code")["pseudo_full_sku_code"]
            ).astype("object")

            fill_mask = df["pseudo_full_sku_code"].isna()
            df.loc[fill_mask, "pseudo_full_sku_code"] = (
                "SKU-" + df.loc[fill_mask, "category_prefix"] + "-"
                + new_seq[fill_mask].astype(int).astype(str).str.zfill(6)
            )

            df = df.drop(columns=["category_prefix"], errors="ignore")
        else:
            df = df.drop(columns=["pseudo_full_sku_code"], errors="ignore").merge(
                existing_full, on="full_sku_code", how="left"
            )

        # --- pseudo_first_half_sku_code (stable across runs) ---
        df = assign_stable_codes(
            df, key_col="first_half_sku_code", pseudo_col="pseudo_first_half_sku_code",
            prefix="SKU-GRP-", pad=5,
        )

        # --- pseudo_unique_sku_code (stable across runs) ---
        df = assign_stable_codes(
            df, key_col="unique_sku_code", pseudo_col="pseudo_unique_sku_code",
            prefix="SKU-BASE-", pad=5,
        )

        # --- Quality checks ---
        # pseudo_full_sku_code is one-to-one with full_sku_code (which is
        # already unique per row at this point) — a plain duplicate check
        # is correct here.
        duplicate_pseudo_skus = df[
            df["pseudo_full_sku_code"].duplicated(keep=False)
        ].sort_values("pseudo_full_sku_code")

        if len(duplicate_pseudo_skus) > 0:
            dup_file = METADATA_DIR / "duplicate_pseudo_sku_codes.csv"
            atomic_write_csv(duplicate_pseudo_skus, dup_file)
            raise ValueError(
                f"Duplicate pseudo_full_sku_code values found: "
                f"{len(duplicate_pseudo_skus):,}. See {dup_file}"
            )

        # pseudo_first_half_sku_code / pseudo_unique_sku_code are GROUP
        # codes — many rows legitimately share one value. The invariant to
        # check is collision at the key->pseudo mapping level, not row-level
        # duplication.
        check_key_to_pseudo_consistency(df, "first_half_sku_code", "pseudo_first_half_sku_code")
        check_key_to_pseudo_consistency(df, "unique_sku_code", "pseudo_unique_sku_code")

        missing_checks = {
            "pseudo_full_sku_code": int(df["pseudo_full_sku_code"].isna().sum()),
            "pseudo_first_half_sku_code": int(df["pseudo_first_half_sku_code"].isna().sum()),
            "pseudo_unique_sku_code": int(df["pseudo_unique_sku_code"].isna().sum()),
        }
        failed_missing = {col: count for col, count in missing_checks.items() if count > 0}
        if failed_missing:
            raise ValueError(f"Missing pseudo SKU values found: {failed_missing}")

        # --- Export (in place — must be atomic) ---
        atomic_write_csv(df, product_mapping_file)

        logger.info("SKU pseudonymization completed successfully.")
        logger.info(f"Updated product mapping saved: {product_mapping_file}")
        logger.info(f"Products: {len(df):,}")

        return 0

    except Exception:
        logger.exception("SKU pseudonymization failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())