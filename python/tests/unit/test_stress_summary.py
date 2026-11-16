"""The stress results page: the table and the folded numbers from the scenarios' reports."""

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
def summary() -> ModuleType:
    """Load scripts/stress_summary.py."""
    path = ROOT / "scripts" / "stress_summary.py"
    spec = importlib.util.spec_from_file_location("stress_summary", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_its_own_check_passes() -> None:
    summary().check()


def test_a_directory_of_reports_is_one_table_failures_first_in_their_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "01_throughput.json").write_text(
        json.dumps(
            {
                "scenario": "01_throughput",
                "profile": "quick",
                "seed": None,
                "failures": [],
                "metrics": {"tasks": 2501.0, "tasks_per_s": 49.88, "wall_s": 57.5},
            }
        )
    )
    (tmp_path / "21_random_chaos.json").write_text(
        json.dumps(
            {
                "scenario": "21_random_chaos",
                "profile": "quick",
                "seed": 42,
                "failures": ["a run never ended"],
                "metrics": {"wall_s": 300.0},
            }
        )
    )
    (tmp_path / "22_broken.json").write_text("{not json")
    (tmp_path / "01_throughput").mkdir()  # a scenario's own directory: not a report
    page = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(page))
    assert summary().main(["stress_summary.py", str(tmp_path)]) == 0
    text = page.read_text()
    assert "3 scenarios, 2 failed; profile quick." in text
    assert "| `01_throughput` | ✅ | 2,501 | 49.9 |  |  |  |  | 57.5 s |" in text
    assert "| `21_random_chaos` (seed 42) | ❌ a run never ended |" in text
    assert "| `22_broken` | ❌ report unreadable:" in text
    assert "<details><summary><code>01_throughput</code></summary>" in text
    assert text.index("## Stress results") < text.index("### All numbers")


def test_no_reports_says_so(tmp_path: Path) -> None:
    assert "No scenario reports in" in summary().table(tmp_path)
