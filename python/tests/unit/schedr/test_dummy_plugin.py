"""The dummy extension's plugin (examples/extensions/dummy): its steps read their values as
typed JSON, refusing a mistyped one, accepting what they always took."""

import asyncio
import importlib.machinery
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

PLUGIN = Path(__file__).parents[4] / "examples" / "extensions" / "dummy" / "schedr-ext-dummy"


def load() -> ModuleType:
    """The plugin as a module (it has no `.py`)."""
    loader = importlib.machinery.SourceFileLoader("schedr_ext_dummy", str(PLUGIN))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[loader.name] = module
    loader.exec_module(module)
    return module


class FakeChannel:
    """Records what a step asks."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    async def secret(self, path: str, key: str | None = None) -> object:
        self.calls.append(("secret", (path, key)))
        return {"token": "t"} if key is None else "v"

    async def status(
        self, phase: str, message: str | None = None, *, progress: float | None
    ) -> None:
        self.calls.append(("status", (phase, progress)))

    async def metric(self, name: str, value: float, **labels: str) -> None:
        self.calls.append(("metric", (name, value, labels)))


def step(name: str, value: object) -> list[tuple[str, Any]]:
    channel = FakeChannel()
    asyncio.run(load().STEPS[name](channel, value))
    return channel.calls


def test_a_whole_secret_a_status_without_progress_and_a_labelled_metric() -> None:
    # agy's review of M5.2i7: once typed, a null key and a null progress were refused.
    assert step("secret", ["etl/api", None]) == [("secret", ("etl/api", None))]
    assert step("secret", ["etl/api", "token"]) == [("secret", ("etl/api", "token"))]
    assert step("status", ["running", None]) == [("status", ("running", None))]
    assert step("status", ["pulling", 0.25]) == [("status", ("pulling", 0.25))]
    assert step("metric", ["jobs", 2, {"outcome": "ok"}]) == [
        ("metric", ("jobs", 2.0, {"outcome": "ok"}))
    ]


@pytest.mark.parametrize(
    ("name", "value"),
    [("secret", "etl/api"), ("status", ["running", "fast"]), ("metric", ["jobs", 1, ["x"]])],
)
def test_a_mistyped_step_is_refused(name: str, value: object) -> None:
    with pytest.raises(ValueError, match=f"`{name}`"):
        step(name, value)
