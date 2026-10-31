"""The bug files are checked, ranked and rendered; this repository's BUGS.md is current."""

from __future__ import annotations

import importlib.util
import re
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def bugs() -> ModuleType:
    """Load scripts/bugs.py."""
    path = ROOT / "scripts" / "bugs.py"
    spec = importlib.util.spec_from_file_location("bugs", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    # Registered first: its dataclass looks its module up while it is created.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def bug(
    severity: str = "high", probability: str = "possible", kind: str = "data", **extra: str
) -> str:
    """A bug's file."""
    fields = {
        "title": '"a title"',
        "severity": f'"{severity}"',
        "probability": f'"{probability}"',
        "kind": f'"{kind}"',
        "summary": '"what happens."',
        "found": "2026-10-30",
        "suspected": "false",
        "details": "'''\n- **Found:** today.\n'''",
        **extra,
    }
    return "".join(f"{key} = {value}\n" for key, value in fields.items())


@pytest.fixture
def folder(tmp_path: Path) -> Path:
    """A bugs folder with its README and next-id, no bugs yet."""
    (tmp_path / "README.md").write_text("# Known bugs\n")
    (tmp_path / "next-id").write_text("B10\n")
    return tmp_path


def error_of(folder: Path) -> str:
    """The error loading `folder` gives."""
    with pytest.raises(bugs().BugError) as raised:
        bugs().load(folder)
    return str(raised.value)


def test_security_ranks_first_then_score_and_the_score_is_computed(folder: Path) -> None:
    (folder / "B1.toml").write_text(bug("critical", "rare", "security"))
    (folder / "B2.toml").write_text(bug("critical", "possible"))
    (folder / "B3.toml").write_text(bug("low", "likely", "tests", suspected="true"))
    text = bugs().render(folder)
    ranking = re.findall(r"^\| \d+ \| (B\d+):", text, re.MULTILINE)
    assert ranking == ["B1", "B2", "B3"]
    assert "| 1 | B1: a title | 4 | critical | rare | **security** |" in text
    assert "## B3: a title (Suspected)" in text
    assert (
        "- **Rating:** severity **critical**, probability **possible**, data (score 8): "
        "what happens.\n" in text
    )
    # Entries newest first, after the ranking.
    assert text.index("## B3:") < text.index("## B2:") < text.index("## B1:")


@pytest.mark.parametrize(
    ("contents", "message"),
    [
        (bug(severity="severe"), "`severity` is one of critical, high, medium, low"),
        (bug(kind="ux"), "`kind` is one of security"),
        (bug(found='"2026-10-30"'), "`found` is a date"),
        (bug(summary='""'), "`summary` is a non-empty string"),
        (bug(suspected='"no"'), "`suspected` is true or false"),
        (bug(score="8"), "unknown ['score']"),
        ("title = 'a'\n", "missing ["),
        ("title = \n", "B5.toml: "),
    ],
)
def test_a_bad_file_is_refused_with_its_name(folder: Path, contents: str, message: str) -> None:
    (folder / "B5.toml").write_text(contents)
    error = error_of(folder)
    assert error.startswith("B5.toml: "), error
    assert message in error, error


def test_numbers_and_stray_files_are_checked(folder: Path) -> None:
    (folder / "B10.toml").write_text(bug())
    assert error_of(folder) == "next-id says B10, but B10 exists"
    (folder / "B10.toml").rename(folder / "b10.toml")
    assert "named `B<number>.toml`" in error_of(folder)
    (folder / "b10.toml").unlink()
    (folder / "notes.txt").write_text("")
    assert error_of(folder) == "not a bug's file: notes.txt"


def test_a_new_bug_takes_the_next_number_and_must_be_filled_in(folder: Path) -> None:
    path = bugs().new(folder, 'a "quoted" title')
    assert path.name == "B10.toml"
    assert (folder / "next-id").read_text() == "B11\n"
    # Its rating is empty until someone fills it in.
    assert "`severity` is one of" in error_of(folder)
    filled = (
        path.read_text()
        .replace('severity = ""', 'severity = "low"', 1)
        .replace('probability = ""', 'probability = "rare"', 1)
        .replace('kind = ""', 'kind = "tests"', 1)
        .replace('summary = ""', 'summary = "it flakes"', 1)
    )
    path.write_text(filled)
    (loaded,) = bugs().load(folder)
    assert (loaded.id, loaded.title, loaded.score) == ("B10", 'a "quoted" title', 1)


def test_this_repositorys_bugs_md_is_current() -> None:
    assert (ROOT / "BUGS.md").read_text() == bugs().render(ROOT / "bugs"), (
        "BUGS.md is out of date: run `make bugs`"
    )
