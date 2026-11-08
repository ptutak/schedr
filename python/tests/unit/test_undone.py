"""The check that the work about to be committed doesn't undo the last commit (B190)."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def undone() -> ModuleType:
    """Load scripts/undone.py."""
    path = ROOT / "scripts" / "undone.py"
    spec = importlib.util.spec_from_file_location("undone", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


PREVIOUS = """+++ b/a.py
+def delay(wait):
+    return sleep(wait)
+)
+# a comment
"""


def test_lines_the_last_commit_added_and_these_changes_remove_are_found() -> None:
    current = """--- a/a.py
-def delay(wait):
-    return sleep(wait)
-)
-# a comment
"""
    # Brackets and comments say nothing on their own.
    assert undone().undone(PREVIOUS, current) == ["def delay(wait):", "return sleep(wait)"]


def test_moved_or_reindented_lines_are_not_undone() -> None:
    current = """--- a/a.py
-def delay(wait):
-    return sleep(wait)
+++ b/b.py
+        return sleep(wait)
+def delay(wait):
"""
    assert undone().undone(PREVIOUS, current) == []


def test_lines_older_than_the_last_commit_may_go() -> None:
    current = """--- a/a.py
-def older():
"""
    assert undone().undone(PREVIOUS, current) == []


def test_a_diffs_file_headers_are_not_lines() -> None:
    assert undone().lines("--- a/x\n+++ b/x\n+y = 1\n-z = 2\n", "+") == {"y = 1"}


FEATURE = "".join(f"value_{n} = compute({n})\n" for n in range(30))


def commit(repo: Path, files: dict[str, str], message: str) -> None:
    """Write `files` under `repo/python/` and commit them."""
    for name, text in files.items():
        path = repo / "python" / name
        if text:
            path.write_text(text)
        else:
            path.unlink()
    run = ["git", "-C", str(repo)]
    subprocess.run([*run, "add", "-A"], check=True, capture_output=True)
    subprocess.run([*run, "commit", "-q", "-m", message], check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A git repository with a first commit; the current directory."""
    for args in (
        ["init", "-q"],
        ["config", "user.email", "test@example.invalid"],
        ["config", "user.name", "test"],
        ["config", "commit.gpgsign", "false"],
    ):
        subprocess.run(["git", "-C", str(tmp_path), *args], check=True, capture_output=True)
    (tmp_path / "python").mkdir()
    commit(tmp_path, {"base.py": "x = 1\n"}, "first")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_a_commit_undoing_work_two_commits_back_fails(repo: Path) -> None:
    commit(repo, {"feature.py": FEATURE}, "the feature")
    commit(repo, {"other.py": "y = 2\n"}, "something else")
    commit(repo, {"feature.py": ""}, "a step copied over main")
    assert undone().check_commits("-1") == 1


def test_a_commit_saying_what_it_undoes_passes(repo: Path) -> None:
    commit(repo, {"feature.py": FEATURE}, "the feature")
    commit(repo, {"feature.py": ""}, "drop it\n\nUndoes: the feature, replaced by the plan")
    assert undone().check_commits("-1") == 0


def test_moved_code_passes_and_so_do_uncommitted_changes_that_keep_it(repo: Path) -> None:
    commit(repo, {"feature.py": FEATURE}, "the feature")
    commit(repo, {"feature.py": "", "moved.py": FEATURE}, "moved")
    assert undone().check_commits("HEAD~2..HEAD") == 0
    (repo / "python" / "moved.py").write_text(FEATURE + "more = 1\n")
    assert undone().check_working_tree() == 0
    (repo / "python" / "moved.py").unlink()
    assert undone().check_working_tree() == 1
