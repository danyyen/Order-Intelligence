"""
tests/test_run_pipeline.py

Orchestration tests for run_pipeline.py's track-isolation model:
    - A shared-foundation failure stops every track.
    - A critical failure inside one dataset track stops only that
      track; the other tracks run to completion independently.
    - --track runs exactly the requested track, nothing else.
    - overall_status correctly distinguishes "completed" /
      "partially_completed" / "failed".

No real stage script is executed — subprocess.run is faked per test
via the `run_with_failures` fixture (see conftest.py), so these run in
milliseconds and need no data, AWS credentials, or Excel file.

Prerequisite: ingest_order_history.py, ingest_open_orders.py, and
validate_shared_mappings.py must exist under src/ for
verify_scripts_exist() to pass — this suite assumes they're already in
place.
"""

from __future__ import annotations


def stage_status(run_report: dict, stage_name: str) -> str | None:
    for s in run_report["stages"]:
        if s["name"] == stage_name:
            return s["status"]
    return None


def test_foundation_failure_blocks_all_tracks(run_with_failures):
    exit_code, report = run_with_failures([], failures={"map_skus": 1})

    assert exit_code == 1
    assert report["overall_status"] == "failed"

    assert stage_status(report, "map_skus") == "failed"
    for downstream in [
        "validate_shared_mappings",
        "pseudonymize_orders", "quality_gate", "upload_s3",
        "standardize_open_orders", "quality_gate_open_orders",
        "ingest_inventory", "quality_gate_inventory",
    ]:
        assert stage_status(report, downstream) == "skipped_upstream_failure", downstream

    assert report["track_status"]["shared"] == "failed"


def test_order_history_failure_does_not_block_other_tracks(run_with_failures):
    exit_code, report = run_with_failures([], failures={"pseudonymize_orders": 1})

    assert exit_code == 1
    assert report["overall_status"] == "partially_completed"

    assert stage_status(report, "pseudonymize_orders") == "failed"
    assert stage_status(report, "validate") == "skipped_upstream_failure"
    assert stage_status(report, "quality_gate") == "skipped_upstream_failure"
    assert stage_status(report, "upload_s3") == "skipped_upstream_failure"

    # Open orders and inventory never depend on order_history's output —
    # both should run to completion untouched.
    assert stage_status(report, "upload_open_orders_s3") == "success"
    assert stage_status(report, "upload_inventory_s3") == "success"

    assert report["track_status"]["order_history"] == "failed"
    assert report["track_status"]["open_orders"] == "completed"
    assert report["track_status"]["inventory"] == "completed"


def test_quality_gate_failure_blocks_only_that_dataset_upload(run_with_failures):
    """
    quality_gate is critical and independently re-derives most of
    'validate's own checks — this confirms it blocks upload on its own,
    even when 'validate' itself passed cleanly.
    """
    exit_code, report = run_with_failures([], failures={"quality_gate": 1})

    assert exit_code == 1
    assert stage_status(report, "quality_gate") == "failed"
    assert stage_status(report, "upload_s3") == "skipped_upstream_failure"

    assert stage_status(report, "upload_open_orders_s3") == "success"
    assert stage_status(report, "upload_inventory_s3") == "success"
    assert report["overall_status"] == "partially_completed"


def test_validate_failure_now_blocks_only_that_dataset_track(run_with_failures):
    """
    The three privacy validators (validate, validate_open_orders,
    validate_inventory) are critical: a failure there stops that
    dataset's own quality_gate/upload, same as any other critical
    stage in that track — but doesn't touch the other two tracks.
    """
    exit_code, report = run_with_failures([], failures={"validate": 1})

    assert exit_code == 1
    assert stage_status(report, "validate") == "failed"
    assert stage_status(report, "quality_gate") == "skipped_upstream_failure"
    assert stage_status(report, "upload_s3") == "skipped_upstream_failure"

    assert stage_status(report, "upload_open_orders_s3") == "success"
    assert stage_status(report, "upload_inventory_s3") == "success"

    assert report["track_status"]["order_history"] == "failed"
    assert report["overall_status"] == "partially_completed"


def test_open_orders_failure_does_not_block_inventory(run_with_failures):
    exit_code, report = run_with_failures([], failures={"pseudonymize_open_orders": 1})

    assert stage_status(report, "pseudonymize_open_orders") == "failed"
    assert stage_status(report, "quality_gate_open_orders") == "skipped_upstream_failure"
    assert stage_status(report, "upload_open_orders_s3") == "skipped_upstream_failure"

    assert stage_status(report, "upload_s3") == "success"
    assert stage_status(report, "upload_inventory_s3") == "success"

    assert report["track_status"]["open_orders"] == "failed"
    assert report["track_status"]["inventory"] == "completed"
    assert report["overall_status"] == "partially_completed"


def test_track_flag_runs_only_the_requested_track(run_with_failures):
    """
    --track inventory should run inventory's own stages plus an
    auto-prepended validate_shared_mappings (shared track) — it must
    NOT pull in order_history or open_orders.
    """
    exit_code, report = run_with_failures(["--track", "inventory"], failures={})

    assert exit_code == 0
    assert report["overall_status"] == "completed"

    stage_names_run = {s["name"] for s in report["stages"]}
    expected_stage_names = {
        "validate_shared_mappings",
        "ingest_inventory", "standardize_inventory", "add_inventory_products",
        "pseudonymize_inventory", "validate_inventory", "quality_gate_inventory",
        "upload_inventory_s3",
    }
    assert stage_names_run == expected_stage_names
    assert set(report["track_status"].keys()) == {"shared", "inventory"}


def test_track_flag_skips_auto_prepend_when_explicitly_skipped(run_with_failures):
    exit_code, report = run_with_failures(
        ["--track", "inventory", "--skip", "validate_shared_mappings"], failures={}
    )

    assert exit_code == 0
    stage_names_run = {s["name"] for s in report["stages"]}
    assert "validate_shared_mappings" not in stage_names_run


def test_track_flag_auto_prepend_failure_blocks_the_track(run_with_failures):
    """
    If the re-validated shared mappings are broken, the standalone
    track must not proceed against them — same as a normal shared
    failure, just scoped to this run's selection.
    """
    exit_code, report = run_with_failures(
        ["--track", "inventory"], failures={"validate_shared_mappings": 1}
    )

    assert exit_code == 1
    assert report["overall_status"] == "failed"
    assert stage_status(report, "ingest_inventory") == "skipped_upstream_failure"
    assert stage_status(report, "upload_inventory_s3") == "skipped_upstream_failure"


def test_all_tracks_succeed_reports_completed(run_with_failures):
    exit_code, report = run_with_failures([], failures={})

    assert exit_code == 0
    assert report["overall_status"] == "completed"
    assert all(status == "completed" for status in report["track_status"].values())


def test_all_dataset_tracks_fail_reports_failed_not_partial(run_with_failures):
    """
    Every dataset track fails, but the shared foundation itself
    succeeded. Zero datasets shipped — that's a full failure, not a
    partial one, even though 'shared' technically completed.
    """
    exit_code, report = run_with_failures([], failures={
        "pseudonymize_orders": 1,
        "pseudonymize_open_orders": 1,
        "pseudonymize_inventory": 1,
    })

    assert exit_code == 1
    assert report["overall_status"] == "failed"
