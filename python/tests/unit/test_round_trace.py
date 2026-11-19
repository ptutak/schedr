"""The master's round trace as a table (scripts/round_trace.py)."""

from __future__ import annotations

import importlib.util
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def round_trace() -> ModuleType:
    """Load scripts/round_trace.py."""
    path = ROOT / "scripts" / "round_trace.py"
    spec = importlib.util.spec_from_file_location("round_trace", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_its_own_check_passes() -> None:
    round_trace().check()


def test_each_points_count_mean_median_p99_and_total_by_part(tmp_path: Path) -> None:
    trace = tmp_path / "round.trace"
    lines = [f"assign/claim\t{ms * 1_000_000}" for ms in range(1, 101)]
    lines += ["round/start_to_start\t5000000", "assign/claim\tbroken"]
    trace.write_text("\n".join(lines) + "\n")
    with trace.open() as read:
        found = round_trace().points(read)
    assert [p.name for p in found] == ["assign/claim", "round/start_to_start"]
    claim = found[0]
    assert (claim.count, claim.mean, claim.median, claim.p99) == (100, 50.5, 50.0, 99.0)
    assert claim.total == 5050.0
    table = round_trace().table(found)
    assert "| `assign/claim` | 100 | 50.500 | 50.000 | 99.000 | 5.050 |" in table


def test_without_a_file_it_says_how_to_use_it() -> None:
    assert round_trace().main(["round_trace.py"]) == 2
