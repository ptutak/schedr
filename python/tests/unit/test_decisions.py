"""The decision files are checked and rendered; this repository's DECISIONS.md is current."""

from __future__ import annotations

import importlib.util
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]
TOPICS = {"runs", "chained-runs"}


@lru_cache(maxsize=1)
def decisions() -> ModuleType:
    """Load scripts/decisions.py."""
    path = ROOT / "scripts" / "decisions.py"
    spec = importlib.util.spec_from_file_location("decisions", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    # Registered first: its dataclass looks its module up while it is created.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def decision(status: str = "active", topic: str = "runs", **extra: str) -> str:
    """A decision's file."""
    fields = {
        "title": '"a title"',
        "date": "2026-11-08",
        "by": '"user"',
        "status": f'"{status}"',
        "topic": f'"{topic}"',
        "summary": "'''\nwhat holds.\n'''",
        "history": '"PROJECT.md: Runs"',
        **extra,
    }
    return "".join(f"{key} = {value}\n" for key, value in fields.items())


def folder(tmp_path: Path, files: dict[int, str], next_id: int | None = None) -> Path:
    """A decisions folder with `files` by number."""
    for number, text in files.items():
        (tmp_path / f"D{number}.toml").write_text(text)
    upto = next_id if next_id is not None else max(files, default=0) + 1
    (tmp_path / "next-id").write_text(f"D{upto}\n")
    return tmp_path


def test_a_folder_loads_by_number_and_renders_newest_first(tmp_path: Path) -> None:
    d = decisions()
    path = folder(
        tmp_path,
        {
            1: decision(),
            2: decision(status="superseded", superseded_by="3"),
            3: decision(topic="chained-runs", step='"M4.12a"'),
        },
    )
    loaded = d.load(path, TOPICS)
    assert [x.number for x in loaded] == [1, 2, 3]
    assert loaded[1].shown_status == "superseded by D3"
    text = d.render(path, TOPICS)
    assert text.index("| D3 |") < text.index("| D1 |")
    # Only active ones under their topic.
    assert "### chained-runs\n\n- D3: a title" in text
    assert "- D2:" not in text


@pytest.mark.parametrize(
    ("files", "message"),
    [
        ({1: decision(status="maybe")}, "`status` is one of"),
        ({1: decision(topic="nowhere")}, "topic `nowhere`"),
        ({1: decision(status="superseded")}, "exactly when the status is `superseded`"),
        ({1: decision(superseded_by="1")}, "exactly when"),
        ({1: decision(status="superseded", superseded_by="9")}, "superseded by D9, unknown"),
        ({1: decision(by='"someone"')}, "`by` is one of"),
        ({1: decision(date='"2026-11-08"')}, "`date` is a date"),
        ({1: decision(colour='"red"')}, "unknown ['colour']"),
        ({1: decision(code='"a/path"')}, "`code` is a list"),
        ({1: decision(), 3: decision()}, "no file for D2"),
    ],
)
def test_a_wrong_file_is_refused_saying_why(
    tmp_path: Path, files: dict[int, str], message: str
) -> None:
    with pytest.raises(decisions().DecisionError, match=message.replace("[", r"\[")):
        decisions().load(folder(tmp_path, files), TOPICS)


def test_next_id_must_be_past_every_file(tmp_path: Path) -> None:
    with pytest.raises(decisions().DecisionError, match="next-id says D1"):
        decisions().load(folder(tmp_path, {1: decision()}, next_id=1), TOPICS)


def test_mentions_read_lists_and_ranges_but_not_years() -> None:
    found = decisions().mentions(
        "decision 12; decisions 326-328 and 330, decision 38A'; decision 2026 was a year"
    )
    assert found == {12, 326, 327, 328, 330, 38, 2026}


def test_a_mention_past_next_id_is_reported(tmp_path: Path) -> None:
    text = tmp_path / "PLAN.md"
    text.write_text("built in decision 9 and decision 12; the year 2026 (decision 2026)\n")
    assert decisions().unknown_mentions([text], 10) == ["PLAN.md: decision 12"]


def test_an_active_decisions_code_must_exist(tmp_path: Path) -> None:
    d = decisions()
    (tmp_path / "here.rs").write_text("")
    (tmp_path / "d").mkdir()
    files = {
        1: decision(code='["here.rs", "gone.rs"]'),
        # A dropped decision's code may be gone.
        2: decision(status="dropped", code='["gone.rs"]'),
    }
    path = folder(tmp_path / "d", files)
    assert d.missing_code(d.load(path, TOPICS), tmp_path) == ["D1: gone.rs"]


def test_new_takes_the_next_number(tmp_path: Path) -> None:
    d = decisions()
    path = folder(tmp_path, {1: decision()})
    created = d.new(path, "a new one")
    assert created.name == "D2.toml"
    assert (path / "next-id").read_text() == "D3\n"
    assert 'title = "a new one"' in created.read_text()


def test_the_topics_are_read_from_the_design_readme(tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    readme.write_text("| Topic | Covers |\n|---|---|\n| `runs` | runs |\n| `chained-runs` | x |\n")
    assert decisions().topics(readme) == {"runs", "chained-runs"}
