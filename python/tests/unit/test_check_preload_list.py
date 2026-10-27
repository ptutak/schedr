"""The chart's preload list must be the one the image's build checks."""

from __future__ import annotations

import importlib.util
from functools import lru_cache
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def checker() -> ModuleType:
    """Load scripts/check-preload-list.py."""
    path = ROOT / "scripts" / "check-preload-list.py"
    spec = importlib.util.spec_from_file_location("check_preload_list", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write(tmp_path: Path, chart: list[str], listed: str) -> tuple[Path, Path]:
    values = tmp_path / "values.yaml"
    items = "".join(f"      - {m}\n" for m in chart)
    values.write_text(f"worker:\n  forkServer:\n    preload:\n{items}")
    preload = tmp_path / "preload.txt"
    preload.write_text(listed)
    return values, preload


def test_the_same_list_passes_comments_and_blanks_aside(tmp_path: Path) -> None:
    values, preload = write(tmp_path, ["requests", "yaml"], "# why\n\nrequests\n  yaml  \n")
    assert checker().main(values, preload) == 0


@pytest.mark.parametrize(
    ("chart", "listed"),
    [(["requests"], "requests\nyaml\n"), (["yaml", "requests"], "requests\nyaml\n")],
)
def test_a_different_list_or_order_fails_and_says_both(
    tmp_path: Path, chart: list[str], listed: str, capsys: pytest.CaptureFixture[str]
) -> None:
    values, preload = write(tmp_path, chart, listed)
    assert checker().main(values, preload) == 1
    assert "worker.forkServer.preload is" in capsys.readouterr().err


def test_this_repos_chart_and_image_agree() -> None:
    values = ROOT / "charts" / "schedr" / "values.yaml"
    assert checker().main(values, ROOT / "images" / "schedr" / "preload.txt") == 0
