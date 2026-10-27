"""The chart's preload list must be the one the image's build checks."""

from __future__ import annotations

import importlib.util
import json
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


def manifest(*lists: list[str], name: str = "PYTHON") -> str:
    """A rendered chart setting each list on a container, as `helm template` writes it."""
    env = "".join(
        f"            - name: SCHEDR_CONFIG__INTERPRETERS__{name}__PRELOAD\n"
        f"              value: {json.dumps(json.dumps(modules, separators=(',', ':')))}\n"
        for modules in lists
    )
    return f"kind: StatefulSet\nspec:\n  containers:\n          env:\n{env}"


def listed(tmp_path: Path, text: str) -> Path:
    preload = tmp_path / "preload.txt"
    preload.write_text(text)
    return preload


def test_the_same_list_passes_comments_and_blanks_aside(tmp_path: Path) -> None:
    preload = listed(tmp_path, "# why\n\nrequests\n  yaml  \n  # indented\n")
    assert checker().main(manifest(["requests", "yaml"]), preload) == 0
    # Every container that gets it, under a renamed DAG interpreter too.
    two = manifest(["requests", "yaml"], ["requests", "yaml"], name="ETL")
    assert checker().main(two, preload) == 0


@pytest.mark.parametrize(
    ("chart", "says"),
    [
        (manifest(["requests"]), "worker.forkServer.preload is ['requests']"),
        (manifest(["yaml", "requests"]), "worker.forkServer.preload is ['yaml', 'requests']"),
        (manifest(["requests", "yaml"], ["requests"]), "is ['requests']"),
        ("kind: StatefulSet\n", "sets no preload list"),
    ],
)
def test_a_different_list_order_or_none_fails_and_says_why(
    tmp_path: Path, chart: str, says: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert checker().main(chart, listed(tmp_path, "requests\nyaml\n")) == 1
    assert says in capsys.readouterr().err
