"""The check that the work about to be committed doesn't undo the last commit (B190)."""

from __future__ import annotations

import importlib.util
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

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
