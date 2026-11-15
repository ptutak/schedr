"""The upgrade check's own logic: which release it upgrades from, what counts as filled or lost."""

from __future__ import annotations

import importlib.util
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def upgrade() -> ModuleType:
    """Load scripts/upgrade_check.py."""
    path = ROOT / "scripts" / "upgrade_check.py"
    spec = importlib.util.spec_from_file_location("upgrade_check", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("released", "current", "expected"),
    [
        # Between releases the tree says the last one: its new migrations tested from it.
        (["0.1.0", "1.0.0"], "1.0.0", "1.0.0"),
        # At a release commit: the one before.
        (["0.1.0", "1.0.0"], "1.1.0", "1.0.0"),
        # Pre-releases and later versions are never the base.
        (["1.0.0", "1.1.0rc1", "1.2.0"], "1.1.0", "1.0.0"),
        (["1.0.0", "1.0.10", "1.0.9"], "1.1.0", "1.0.10"),
    ],
)
def test_the_release_upgraded_from_is_the_newest_final_at_or_below_the_tree(
    released: list[str], current: str, expected: str
) -> None:
    assert upgrade().previous_release(released, current) == expected


def test_no_release_to_upgrade_from_is_an_error() -> None:
    with pytest.raises(ValueError, match=r"no release at or below 1\.0\.0"):
        upgrade().previous_release(["1.1.0", "2.0.0rc1"], "1.0.0")


def test_the_trees_version_is_the_workspaces_with_an_rc_dropped() -> None:
    cargo = (
        '[package]\nversion = "9.9.9"\n\n'
        '[workspace.package]\nedition = "2024"\nversion = "1.1.0-rc1"\n'
    )
    assert upgrade().tree_version(cargo) == "1.1.0"


def test_an_empty_table_is_missing_unless_excused_and_sqlxs_own_never_counts() -> None:
    counts = {"runs": 2, "events": 0, "masters": 0, "_sqlx_migrations": 0}
    assert upgrade().missing_tables(counts, {}) == ["events", "masters"]
    assert upgrade().missing_tables(counts, {"masters": "why"}) == ["events"]


def test_a_table_gone_or_losing_a_row_or_a_column_loses_data_unless_excused() -> None:
    before = {
        "runs": (["id", "state"], [(1, "success"), (2, "failed")]),
        "events": (["id"], [(1,), (2,)]),
        "secrets": (["path"], [("etl/db",)]),
        "outputs": (["id", "value"], [(1, "3")]),
    }
    after = {
        "runs": (["id", "state"], [(1, "success")]),
        "events": (["id"], [(1,), (2,)]),
        "outputs": (["id"], [(1,)]),
    }
    assert upgrade().lost_rows(before, after, {}) == {
        "runs": "1 of 2 rows gone or changed",
        "secrets": "the table is gone",
        "outputs": "columns gone: value",
    }
    assert set(upgrade().lost_rows(before, after, {"runs": "why", "outputs": "why"})) == {"secrets"}


def test_a_changed_value_is_lost_but_added_rows_and_columns_are_not() -> None:
    before = {"outputs": (["id", "value"], [(1, '{"a": 1}'), (1, '{"a": 1}')])}
    changed = {"outputs": (["id", "value"], [(1, '{"a": 1}'), (1, "null")])}
    grown = {
        "outputs": (
            ["note", "id", "value"],
            [("x", 1, '{"a": 1}'), ("y", 1, '{"a": 1}'), ("z", 2, "2")],
        )
    }
    assert upgrade().lost_rows(before, changed, {}) == {"outputs": "1 of 2 rows gone or changed"}
    assert upgrade().lost_rows(before, grown, {}) == {}


def test_json_and_bytes_compare_as_values() -> None:
    comparable = upgrade().comparable
    assert comparable({"b": 1, "a": [2]}) == comparable({"a": [2], "b": 1})
    assert comparable(memoryview(b"ab")) == b"ab"
    assert comparable(3) == 3


def test_the_lists_of_excused_tables_say_why() -> None:
    for excused in (upgrade().NOT_FILLED, upgrade().NOT_KEPT):
        assert all(reason.strip() for reason in excused.values())
