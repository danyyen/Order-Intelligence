"""
tests/test_ingestion_idempotency.py

Integration-style test: ingest_order_history.py and ingest_open_orders.py
both read the SAME workbook, independently. This verifies:
    - Each script can ingest that workbook exactly once without --force.
    - A second run of the SAME script on the SAME file is refused
      (idempotency guard).
    - Ingesting Order History first does not block Open Orders from
      ingesting the same file for the first time, and vice versa —
      proving the two scripts don't share a manifest.

Builds its own tiny two-sheet .xlsx fixture rather than depending on a
committed sample file, since nothing under data/raw_excel/ is actually
tracked in the repo today.
"""

from __future__ import annotations

import pandas as pd
import pytest

from conftest import load_module, make_incrementing_datetime


@pytest.fixture
def sample_workbook(tmp_path):
    raw_excel_dir = tmp_path / "raw_excel"
    raw_excel_dir.mkdir()
    workbook_path = raw_excel_dir / "orH_test_0001.xlsx"

    order_history = pd.DataFrame({
        "Order Number": ["1001", "1002"],
        "Customer Name": ["Acme Inc", "Beta LLC"],
    })
    open_orders = pd.DataFrame({
        "Order Number": ["2001"],
        "Customer Name": ["Acme Inc"],
    })

    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        order_history.to_excel(writer, sheet_name="Order History", index=False)
        open_orders.to_excel(writer, sheet_name="Open Orders", index=False)

    return raw_excel_dir


@pytest.fixture
def ingestion_env(tmp_path, sample_workbook, monkeypatch):
    raw_csv_dir = tmp_path / "raw_csv"
    metadata_dir = tmp_path / "metadata"
    raw_csv_dir.mkdir()
    metadata_dir.mkdir()

    order_history_module = load_module(
        "src/ingestion/ingest_order_history.py", "ingest_order_history_under_test"
    )
    open_orders_module = load_module(
        "src/ingestion/ingest_open_orders.py", "ingest_open_orders_under_test"
    )

    for module in (order_history_module, open_orders_module):
        monkeypatch.setattr(module, "RAW_EXCEL_DIR", sample_workbook)
        monkeypatch.setattr(module, "RAW_CSV_DIR", raw_csv_dir)
        monkeypatch.setattr(module, "METADATA_DIR", metadata_dir)

    monkeypatch.setattr(
        order_history_module, "MANIFEST_FILE",
        metadata_dir / "ingestion_manifest_order_history.json",
    )
    monkeypatch.setattr(
        open_orders_module, "MANIFEST_FILE",
        metadata_dir / "ingestion_manifest_open_orders.json",
    )

    return order_history_module, open_orders_module, raw_csv_dir


def test_each_script_ingests_the_shared_workbook_once(ingestion_env, monkeypatch):
    order_history_module, open_orders_module, raw_csv_dir = ingestion_env

    monkeypatch.setattr("sys.argv", ["ingest_order_history.py"])
    assert order_history_module.main() == 0
    assert len(list(raw_csv_dir.glob("order_history_*.csv"))) == 1

    monkeypatch.setattr("sys.argv", ["ingest_open_orders.py"])
    assert open_orders_module.main() == 0
    assert len(list(raw_csv_dir.glob("open_orders_*.csv"))) == 1


def test_rerunning_the_same_script_on_the_same_file_is_refused(ingestion_env, monkeypatch):
    order_history_module, _, raw_csv_dir = ingestion_env

    monkeypatch.setattr("sys.argv", ["ingest_order_history.py"])
    assert order_history_module.main() == 0
    assert order_history_module.main() == 1  # refused without --force

    assert len(list(raw_csv_dir.glob("order_history_*.csv"))) == 1


def test_force_allows_reingesting_the_same_file(ingestion_env, monkeypatch):
    order_history_module, _, raw_csv_dir = ingestion_env
    monkeypatch.setattr(order_history_module, "datetime", make_incrementing_datetime())

    monkeypatch.setattr("sys.argv", ["ingest_order_history.py"])
    assert order_history_module.main() == 0

    monkeypatch.setattr("sys.argv", ["ingest_order_history.py", "--force"])
    assert order_history_module.main() == 0

    assert len(list(raw_csv_dir.glob("order_history_*.csv"))) == 2


def test_ingesting_order_history_does_not_block_open_orders(ingestion_env, monkeypatch):
    order_history_module, open_orders_module, raw_csv_dir = ingestion_env

    monkeypatch.setattr("sys.argv", ["ingest_order_history.py"])
    assert order_history_module.main() == 0

    # Same physical file, first time for THIS script — must not be
    # blocked by order_history's manifest entry for the same file hash.
    monkeypatch.setattr("sys.argv", ["ingest_open_orders.py"])
    assert open_orders_module.main() == 0

    assert len(list(raw_csv_dir.glob("open_orders_*.csv"))) == 1


@pytest.mark.parametrize("module_key, csv_glob, metadata_glob", [
    ("order_history_module", "order_history_*.csv", "batch_metadata_order_history_*.json"),
    ("open_orders_module", "open_orders_*.csv", "batch_metadata_open_orders_*.json"),
])
def test_csv_publish_failure_rolls_back_metadata_and_manifest(
    ingestion_env, monkeypatch, module_key, csv_glob, metadata_glob
):
    """
    If atomic_write_csv() fails after metadata/manifest were already
    written, the commit must roll back completely: no committed
    manifest entry, no metadata pointing at a nonexistent CSV, and a
    plain retry (no --force) must be allowed afterward.
    """
    order_history_module, open_orders_module, raw_csv_dir = ingestion_env
    module = order_history_module if module_key == "order_history_module" else open_orders_module
    script_name = "ingest_order_history.py" if module_key == "order_history_module" else "ingest_open_orders.py"

    original_atomic_write_csv = module.atomic_write_csv

    def failing_write_csv(df, output_file):
        raise OSError("simulated disk failure during CSV publish")

    monkeypatch.setattr(module, "atomic_write_csv", failing_write_csv)
    monkeypatch.setattr("sys.argv", [script_name])

    assert module.main() == 1

    # Nothing should have landed: no CSV, no committed manifest entry,
    # no metadata pointing at a CSV that doesn't exist.
    assert list(raw_csv_dir.glob(csv_glob)) == []
    assert module.load_manifest() == []
    assert list(module.METADATA_DIR.glob(metadata_glob)) == []

    # Restore the real CSV writer and confirm a plain retry (no
    # --force) is allowed now, since nothing was actually committed.
    monkeypatch.setattr(module, "atomic_write_csv", original_atomic_write_csv)
    assert module.main() == 0
    assert len(list(raw_csv_dir.glob(csv_glob))) == 1
