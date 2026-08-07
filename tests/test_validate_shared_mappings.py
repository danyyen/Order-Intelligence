"""
tests/test_validate_shared_mappings.py

Focused unit tests for validate_shared_mappings.py's collision logic.
It passing against whatever real mappings happen to be on disk today
doesn't prove the checks are correct — these tests construct the
specific corrupted shapes the validator exists to catch (and the
legitimate shapes it must NOT flag) directly, independent of any real
data.
"""

from __future__ import annotations

import pandas as pd
import pytest

from conftest import load_module

VALID_CUSTOMER_ROWS = [
    {
        "customer_hash_key": "hash-a",
        "source_customer_code": "A1",
        "ship_to_customer_code": "ST1",
        "customer_name": "ACME INC",
        "pseudo_customer_name": "Customer CUST-000001",
    },
    {
        "customer_hash_key": "hash-b",
        "source_customer_code": "B1",
        "ship_to_customer_code": "ST2",
        "customer_name": "BETA LLC",
        "pseudo_customer_name": "Customer CUST-000002",
    },
]


def _product_row(full_sku, first_half, unique_sku, pseudo_full, pseudo_first_half, pseudo_unique, category="Widgets"):
    return {
        "full_sku_code": full_sku,
        "first_half_sku_code": first_half,
        "unique_sku_code": unique_sku,
        "pseudo_category": category,
        "pseudo_product_description": f"{category} Product {full_sku}",
        "pseudo_full_sku_code": pseudo_full,
        "pseudo_first_half_sku_code": pseudo_first_half,
        "pseudo_unique_sku_code": pseudo_unique,
    }


# Two rows deliberately share first_half_sku_code ("SKU-001" -> group
# pseudo "SKU-GRP-00001") and two rows deliberately share unique_sku_code
# ("A" -> group pseudo "SKU-BASE-00001") — this is the normal, legitimate
# shape for those two columns, not a corruption.
VALID_PRODUCT_ROWS = [
    _product_row("SKU-001-A", "SKU-001", "A", "SKU-W-000001", "SKU-GRP-00001", "SKU-BASE-00001"),
    _product_row("SKU-001-B", "SKU-001", "B", "SKU-W-000002", "SKU-GRP-00001", "SKU-BASE-00002"),
    _product_row("SKU-002-A", "SKU-002", "A", "SKU-W-000003", "SKU-GRP-00002", "SKU-BASE-00001"),
]


def _to_df(rows, template):
    # An explicitly empty list still needs the real header row (to
    # exercise the "0 rows" check specifically, rather than pandas'
    # own "no columns to parse" error on a totally blank file).
    if not rows:
        return pd.DataFrame(columns=list(template[0].keys()))
    return pd.DataFrame(rows)


@pytest.fixture
def validator_env(tmp_path, monkeypatch):
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir()
    customer_file = metadata_dir / "customer_mapping_final.csv"
    product_file = metadata_dir / "product_mapping_final.csv"

    module = load_module(
        "src/privacy/validate_shared_mappings.py", "validate_shared_mappings_under_test"
    )
    monkeypatch.setattr(module, "CUSTOMER_MAPPING_FILE", customer_file)
    monkeypatch.setattr(module, "PRODUCT_MAPPING_FILE", product_file)

    def write(customer_rows=None, product_rows=None):
        customer_rows = VALID_CUSTOMER_ROWS if customer_rows is None else customer_rows
        product_rows = VALID_PRODUCT_ROWS if product_rows is None else product_rows
        _to_df(customer_rows, VALID_CUSTOMER_ROWS).to_csv(customer_file, index=False)
        _to_df(product_rows, VALID_PRODUCT_ROWS).to_csv(product_file, index=False)

    return module, write


def test_valid_mappings_pass(validator_env):
    module, write = validator_env
    write()
    assert module.main() == 0


def test_valid_repeated_group_keys_across_rows_pass(validator_env):
    """
    Many product rows legitimately sharing one first_half_sku_code /
    unique_sku_code group pseudo value must NOT be flagged as a
    collision — VALID_PRODUCT_ROWS already exercises exactly that
    shape, so this pins down the negative case explicitly.
    """
    module, write = validator_env
    write()
    assert module.main() == 0


def test_split_identity_is_rejected_for_customer(validator_env):
    """One real customer key mapped to two different pseudo names."""
    module, write = validator_env
    rows = [
        {**VALID_CUSTOMER_ROWS[0], "pseudo_customer_name": "Customer CUST-000001"},
        {**VALID_CUSTOMER_ROWS[0], "pseudo_customer_name": "Customer CUST-000099"},
    ]
    write(customer_rows=rows)
    assert module.main() == 1


def test_identity_collision_is_rejected_for_customer(validator_env):
    """Two different real customer keys mapped to the same pseudo name."""
    module, write = validator_env
    rows = [
        {**VALID_CUSTOMER_ROWS[0], "pseudo_customer_name": "Customer CUST-000001"},
        {**VALID_CUSTOMER_ROWS[1], "pseudo_customer_name": "Customer CUST-000001"},
    ]
    write(customer_rows=rows)
    assert module.main() == 1


def test_split_identity_is_rejected_for_sku_group_key(validator_env):
    """
    Same first_half_sku_code assigned two different pseudo group codes
    across rows — the exact corruption a naive "is pseudo_col ever
    duplicated" check would miss entirely.
    """
    module, write = validator_env
    rows = [
        _product_row("SKU-001-A", "SKU-001", "A", "SKU-W-000001", "SKU-GRP-00001", "SKU-BASE-00001"),
        _product_row("SKU-001-B", "SKU-001", "B", "SKU-W-000002", "SKU-GRP-00002", "SKU-BASE-00002"),
    ]
    write(product_rows=rows)
    assert module.main() == 1


def test_identity_collision_is_rejected_for_full_sku(validator_env):
    """Two different full_sku_code values assigned the same pseudo_full_sku_code."""
    module, write = validator_env
    rows = [
        _product_row("SKU-001-A", "SKU-001", "A", "SKU-W-000001", "SKU-GRP-00001", "SKU-BASE-00001"),
        _product_row("SKU-001-B", "SKU-001", "B", "SKU-W-000001", "SKU-GRP-00001", "SKU-BASE-00002"),
    ]
    write(product_rows=rows)
    assert module.main() == 1


def test_null_real_key_is_rejected(validator_env):
    module, write = validator_env
    rows = [
        VALID_CUSTOMER_ROWS[0],
        {**VALID_CUSTOMER_ROWS[1], "customer_hash_key": None},
    ]
    write(customer_rows=rows)
    assert module.main() == 1


def test_null_pseudo_identifier_is_rejected(validator_env):
    module, write = validator_env
    rows = [
        VALID_CUSTOMER_ROWS[0],
        {**VALID_CUSTOMER_ROWS[1], "pseudo_customer_name": None},
    ]
    write(customer_rows=rows)
    assert module.main() == 1


def test_missing_required_column_is_rejected(validator_env):
    module, write = validator_env
    rows = [{k: v for k, v in row.items() if k != "pseudo_customer_name"} for row in VALID_CUSTOMER_ROWS]
    write(customer_rows=rows)
    assert module.main() == 1


def test_empty_mapping_file_is_rejected(validator_env):
    module, write = validator_env
    write(customer_rows=[])
    assert module.main() == 1


def test_missing_mapping_file_is_rejected(tmp_path, monkeypatch):
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir()
    module = load_module(
        "src/privacy/validate_shared_mappings.py", "validate_shared_mappings_under_test_missing"
    )
    monkeypatch.setattr(module, "CUSTOMER_MAPPING_FILE", metadata_dir / "customer_mapping_final.csv")
    monkeypatch.setattr(module, "PRODUCT_MAPPING_FILE", metadata_dir / "product_mapping_final.csv")
    assert module.main() == 1
