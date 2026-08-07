"""
tests/conftest.py

Shared fixtures for the orchestration and ingestion test suites.

These tests load the pipeline's stage scripts directly from their file
paths (via importlib), rather than relying on package-style imports,
because src/ isn't set up as installable packages — matching how the
scripts already import each other via a PROJECT_ROOT sys.path bootstrap
at the top of each file.

Note: config/paths.py resolves PROJECT_ROOT from its own file location
(Path(__file__).resolve().parents[1]), same as every stage script's own
bootstrap — so these tests, and the pipeline itself, work from any
checkout location, not just one specific machine path.
"""

from __future__ import annotations

import importlib.util
import itertools
import sys
from datetime import datetime as _real_datetime, timedelta as _timedelta
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_module(rel_path: str, module_name: str):
    """Load a stage script as a module by file path, bypassing the lack
    of __init__.py files under src/."""
    path = PROJECT_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def make_incrementing_datetime(start: _real_datetime | None = None):
    """
    A drop-in replacement for `datetime.datetime` whose .now() advances
    by one second on every call, so a test calling a script's main()
    more than once in quick succession gets distinct batch_id/timestamp
    values instead of colliding on the same wall-clock second.
    """
    start = start or _real_datetime(2026, 1, 1, 0, 0, 0)
    counter = itertools.count()

    class FakeDateTime(_real_datetime):
        @classmethod
        def now(cls, tz=None):
            return start + _timedelta(seconds=next(counter))

    return FakeDateTime


@pytest.fixture
def run_pipeline_module():
    """A fresh import of run_pipeline.py per test, so module-level state
    (STAGES, etc.) can't leak between tests."""
    return load_module("src/run_script/run_pipeline.py", "run_pipeline_under_test")


@pytest.fixture
def isolated_pipeline_dirs(tmp_path, run_pipeline_module, monkeypatch):
    """Redirect the orchestrator's log/report directories into a temp
    dir, and make ensure_all_dirs_exist a no-op, so tests never touch
    the real logs/ or data/metadata/pipeline_runs/ directories."""
    log_dir = tmp_path / "logs"
    run_dir = tmp_path / "pipeline_runs"
    log_dir.mkdir()
    run_dir.mkdir()

    monkeypatch.setattr(run_pipeline_module, "LOG_DIR", log_dir)
    monkeypatch.setattr(run_pipeline_module, "PIPELINE_RUN_DIR", run_dir)
    monkeypatch.setattr(run_pipeline_module, "ensure_all_dirs_exist", lambda: None)
    # No scenario in this suite depends on retry timing, only on the
    # pass/fail/skip outcome — never actually sleep during backoff.
    monkeypatch.setattr(run_pipeline_module.time, "sleep", lambda seconds: None)

    return {"log_dir": log_dir, "run_dir": run_dir}


def _make_fake_subprocess_run(run_pipeline_module, failures: dict[str, int]):
    """
    Build a fake subprocess.run() that resolves which Stage is being
    invoked by matching the script path back against run_pipeline's own
    STAGES list, and returns the exit code configured for it in
    `failures` (default: success). No real stage script ever runs.
    """
    import subprocess as real_subprocess

    script_to_stage = {
        str(run_pipeline_module.SRC_DIR / s.script): s.name
        for s in run_pipeline_module.STAGES
    }

    def fake_run(command, **kwargs):
        script_path = command[1]
        stage_name = script_to_stage.get(script_path, "<unknown>")
        returncode = failures.get(stage_name, 0)
        stderr = f"simulated failure in {stage_name}" if returncode else ""
        return real_subprocess.CompletedProcess(command, returncode, stdout="", stderr=stderr)

    return fake_run


@pytest.fixture
def run_with_failures(run_pipeline_module, isolated_pipeline_dirs, monkeypatch):
    """
    Returns a function: run_with_failures(argv, failures) -> (exit_code, run_report)
    that executes run_pipeline.main() with subprocess.run faked out per
    the `failures` map ({stage_name: returncode}), and returns the exit
    code plus the parsed run report it wrote.
    """
    import json

    def _run(argv: list[str], failures: dict[str, int] | None = None):
        fake_run = _make_fake_subprocess_run(run_pipeline_module, failures or {})
        monkeypatch.setattr(run_pipeline_module.subprocess, "run", fake_run)
        monkeypatch.setattr(sys, "argv", ["run_pipeline.py"] + argv)

        exit_code = run_pipeline_module.main()

        report_files = list(isolated_pipeline_dirs["run_dir"].glob("pipeline_run_*.json"))
        assert len(report_files) == 1, "expected exactly one run report to be written"
        run_report = json.loads(report_files[0].read_text(encoding="utf-8"))

        return exit_code, run_report

    return _run
