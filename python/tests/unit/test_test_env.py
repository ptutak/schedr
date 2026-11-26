"""The integration tests' environment check run by nextest's setup step."""

from __future__ import annotations

import importlib.util
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def env_check() -> ModuleType:
    """Load scripts/test_env.py."""
    path = ROOT / "scripts" / "test_env.py"
    spec = importlib.util.spec_from_file_location("test_env_script", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_its_own_check_passes() -> None:
    env_check().check()


def test_an_unset_variable_stops_the_run_saying_what_to_run(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("SCHEDR_TEST_POSTGRES_URL", raising=False)
    assert env_check().main(["x", "SCHEDR_TEST_POSTGRES_URL"]) == 1
    err = capsys.readouterr().err
    assert "SCHEDR_TEST_POSTGRES_URL is not set" in err
    assert "make test-integration" in err


def test_a_set_variable_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCHEDR_TEST_PYTHON", sys.executable)
    assert env_check().main(["x", "SCHEDR_TEST_PYTHON"]) == 0


def test_a_python_that_isnt_there_is_said(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SCHEDR_TEST_PYTHON", "/nowhere/python")
    assert env_check().main(["x", "SCHEDR_TEST_PYTHON"]) == 1
    assert "isn't a file" in capsys.readouterr().err


def test_an_unknown_variable_is_a_usage_error() -> None:
    assert env_check().main(["x", "OTHER"]) == 2
    assert env_check().main(["x"]) == 2
