#!/usr/bin/env python3
"""Fail when a change undoes recent commits without saying so (B190).

M4.11f's commit removed all of M4.11g, the commit before it, with its tests, and CI stayed
green: work done in a worktree started before M4.11g had been copied over main. This check
looks for lines that the last `DEPTH` commits added and a change removes without adding them
back elsewhere (moved or reindented code is fine). More than `LIMIT` of them fail it.

- Before committing (`make undone-check`, in `make check`): the uncommitted changes against
  the last `DEPTH` commits. A removal that is meant passes with `SCHEDR_UNDO_OK=1`.
- In CI (`--range BEFORE..AFTER`, the commits a push or pull request brings): each of them
  against the `DEPTH` before it. A commit that means it says so in its message with a line
  `Undoes: <what and why>`.

Usage:
    python3 scripts/undone.py               # the working tree against HEAD and before
    python3 scripts/undone.py --range A..B  # each commit in A..B (CI)
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

# A few lines of recent commits may well go (a fix, a rename): more is suspicious.
LIMIT = 20
# How many commits back a copy-over is looked for: a worktree lives a step or two.
DEPTH = 10
# Lines that say nothing on their own: brackets, blank, comments.
TRIVIAL = re.compile(r"^\s*(?:[})\]]*[,;]?|//.*|#.*|\"\"\"|''')\s*$")
PATHS = ("crates", "python", "scripts", "docs", "examples", "schema")
DECLARED = re.compile(r"^Undoes: \S", re.MULTILINE)


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
    """Lines `previous` (recent commits' diffs) added that `current` removes, not added back."""
    return sorted((lines(previous, "+") & lines(current, "-")) - lines(current, "+"))


def git(*args: str) -> str:
    """A git command's output."""
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout


def recent(commit: str) -> str:
    """The diffs of `commit` and the `DEPTH - 1` before it (fewer near the first commit)."""
    return git("log", f"-{DEPTH}", "--format=", "--unified=0", "-p", commit, "--", *PATHS)


def report(found: list[str], what: str, how: str) -> None:
    """Say what was undone and how to go on."""
    print(
        f"error: {what} removes {len(found)} lines the last {DEPTH} commits added, not moved "
        "elsewhere. Was work started before those commits copied over them? Bring it in with "
        f"git (merge, rebase) instead; if the removal is meant, {how}. The first of them:",
        file=sys.stderr,
    )
    for line in found[:15]:
        print(f"  {line}", file=sys.stderr)


def check_working_tree() -> int:
    """The uncommitted changes against the recent commits."""
    found = undone(recent("HEAD"), git("diff", "--unified=0", "HEAD", "--", *PATHS))
    if len(found) <= LIMIT or os.environ.get("SCHEDR_UNDO_OK") == "1":
        return 0
    report(
        found,
        "the uncommitted change",
        "run with SCHEDR_UNDO_OK=1 and give the commit message a line `Undoes: <what, why>` "
        "(CI checks for that line, not the variable)",
    )
    return 1


def check_commits(span: str) -> int:
    """Each commit of `span` (`A..B`, or `-N` for the last N) against the ones before it."""
    commits = (
        git("rev-list", span).split() if ".." in span else git("log", span, "--format=%H").split()
    )
    failed = 0
    for commit in commits:
        parents = git("log", "-1", "--format=%P", commit).split()
        if len(parents) != 1 or DECLARED.search(git("log", "-1", "--format=%B", commit)):
            continue  # the first commit, a merge, or one that says what it undoes
        current = git("show", "--format=", "--unified=0", commit, "--", *PATHS)
        found = undone(recent(parents[0]), current)
        if len(found) > LIMIT:
            subject = git("log", "-1", "--format=%h %s", commit).strip()
            report(found, f"commit {subject}", "say so in its message: `Undoes: <what, why>`")
            failed = 1
    return failed


def main(argv: list[str]) -> int:
    """The working tree, or `--commits N`."""
    if argv[1:] == []:
        return check_working_tree()
    if argv[1:2] == ["--range"] and len(argv[2:]) == 1 and ".." in argv[2]:
        return check_commits(argv[2])
    print(__doc__.split("Usage:")[1], file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
