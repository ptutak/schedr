"""The paired speed page: verdicts from runs of two builds in turn, read from their reports."""

from __future__ import annotations

import importlib.util
import json
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def paired() -> ModuleType:
    """Load scripts/stress_paired.py."""
    path = ROOT / "scripts" / "stress_paired.py"
    spec = importlib.util.spec_from_file_location("stress_paired", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def write(directory: Path, side: str, run: int, metrics: dict[str, float], **extra: object) -> None:
    """A report of `01_throughput` for `side`'s run `run`."""
    path = directory / side / str(run) / "01_throughput.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    report = {"scenario": "01_throughput", "failures": [], "metrics": metrics, **extra}
    path.write_text(json.dumps(report))


def test_its_own_check_passes() -> None:
    paired().check()


def test_a_slower_build_fails_the_page_with_its_metric_named(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for run, (base, new) in enumerate([(50.0, 40.0), (52.0, 41.0), (49.0, 42.0)], 1):
        write(tmp_path, "base", run, {"tasks_per_s": base, "slot_run_s": 0.5})
        write(tmp_path, "new", run, {"tasks_per_s": new, "slot_run_s": 0.5})
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert paired().main(["x", str(tmp_path)]) == 1
    text = summary.read_text()
    assert "### `01_throughput`: 3 pairs" in text
    assert "**Slower:** `tasks_per_s`" in text
    assert "| `slot_run_s` | 0.50 s | 0.50 s | +0.0 % |" in text


def test_the_same_speed_passes_and_a_change_under_the_threshold_is_noise(tmp_path: Path) -> None:
    for run, (base, new) in enumerate([(50.0, 49.0), (52.0, 51.0)], 1):
        write(tmp_path, "base", run, {"tasks_per_s": base})
        write(tmp_path, "new", run, {"tasks_per_s": new})
    text, failed = paired().page(tmp_path, 5.0)
    assert not failed, text
    assert "within noise" in text
    text, failed = paired().page(tmp_path, 1.0)
    assert failed
    assert "slower" in text


def test_a_run_without_its_report_fails_the_page(tmp_path: Path) -> None:
    write(tmp_path, "base", 1, {"tasks_per_s": 50.0})
    text, failed = paired().page(tmp_path, 5.0)
    assert failed
    assert "❌ new run 1: report missing or unreadable" in text


def test_a_stalled_runner_is_said(tmp_path: Path) -> None:
    write(tmp_path, "base", 1, {"wall_s": 10.0}, noisy_runner=True)
    write(tmp_path, "new", 1, {"wall_s": 10.0})
    text, _ = paired().page(tmp_path, 5.0)
    assert "1 runs report a runner stalled on I/O" in text


def test_no_pairs_is_said_and_fails(tmp_path: Path) -> None:
    text, failed = paired().page(tmp_path, 5.0)
    assert failed
    assert "No pairs" in text


def test_the_threshold_is_read_from_the_arguments(tmp_path: Path) -> None:
    for run, (base, new) in enumerate([(50.0, 48.0), (50.0, 48.5)], 1):
        write(tmp_path, "base", run, {"tasks_per_s": base})
        write(tmp_path, "new", run, {"tasks_per_s": new})
    assert paired().main(["x", str(tmp_path)]) == 0
    assert paired().main(["x", str(tmp_path), "--threshold", "2"]) == 1
    assert paired().main(["x"]) == 2
