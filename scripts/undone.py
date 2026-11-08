#!/usr/bin/env python3
"""Fail when the work about to be committed undoes the previous commit (B190).

M4.11f's commit removed all of M4.11g, the commit before it, with its tests, and CI stayed
green. This check compares the changes not yet committed (`git diff HEAD`) with the last
commit (`git show HEAD`): lines that commit added which the new changes remove, and don't add
back elsewhere (moved or reindented code is fine), are listed. More than `LIMIT` of them fail
the check; a change that means it (a step replacing the last one's work) passes with
`SCHEDR_UNDO_OK=1`.

Usage:
    python3 scripts/undone.py          # check the working tree against HEAD
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

# A few lines of the last commit may well go (a fix, a rename): more is suspicious.
LIMIT = 20
# Lines that say nothing on their own: brackets, blank, comments.
TRIVIAL = re.compile(r"^\s*(?:[})\]]*[,;]?|//.*|#.*|\"\"\"|''')\s*$")
PATHS = ("crates", "python", "scripts", "docs", "examples", "schema", "proto")


def lines(diff: str, sign: str) -> set[str]:
    """The added (`+`) or removed (`-`) lines of a unified diff, stripped, trivial ones left out."""
    found = set()
    for line in diff.splitlines():
        if line.startswith(sign) and not line.startswith(sign * 3):
            text = line[1:].strip()
            if text and not TRIVIAL.fullmatch(text):
                found.add(text)
    return found


def undone(previous: str, current: str) -> list[str]:
    """Lines `previous` (a commit's diff) added that `current` removes without adding back."""
    return sorted((lines(previous, "+") & lines(current, "-")) - lines(current, "+"))


def git(*args: str) -> str:
    """A git command's output."""
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout


def main() -> int:
    """Check the working tree against HEAD."""
    previous = git("show", "--format=", "--unified=0", "HEAD", "--", *PATHS)
    current = git("diff", "--unified=0", "HEAD", "--", *PATHS)
    found = undone(previous, current)
    if len(found) <= LIMIT or os.environ.get("SCHEDR_UNDO_OK") == "1":
        return 0
    subject = git("log", "-1", "--format=%h %s", "HEAD").strip()
    print(
        f"error: these changes remove {len(found)} lines the last commit ({subject}) added, "
        "not moved elsewhere. Was the work started before that commit and copied over it? "
        "Bring it in with git (merge, rebase) instead; if the removal is meant, run with "
        "SCHEDR_UNDO_OK=1. The first of them:",
        file=sys.stderr,
    )
    for line in found[:15]:
        print(f"  {line}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
