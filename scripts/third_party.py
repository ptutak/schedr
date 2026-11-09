#!/usr/bin/env python3
"""Write LICENSE-THIRD-PARTY: the licences of what the `schedr` binary carries.

    third_party.py            write LICENSE-THIRD-PARTY
    third_party.py --check    fail if it isn't what this tree would write

The binary links the crates the `schedr` package depends on (normal dependencies, not dev or
build ones, nor proc macros, which run only at compile time), on the platforms the wheels
are built for. Most of their licences (MIT, Apache-2.0, BSD, ISC) ask that their notices go
with a binary copy: each crate's own licence files are copied in, a text shared by several
crates printed once. The UI's vendored JavaScript is listed too. The wheels and the images
carry the file next to LICENSE. Standard library only; it reads the crates' sources from
cargo's registry (`cargo fetch` first).
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "LICENSE-THIRD-PARTY"
# The wheels' platforms (ci.yml: wheel, wheel-native).
TARGETS = (
    "x86_64-unknown-linux-gnu",
    "x86_64-unknown-linux-musl",
    "aarch64-unknown-linux-gnu",
    "aarch64-apple-darwin",
)
LICENSE_FILE = re.compile(r"^(LICEN[CS]E|COPYING|NOTICE|COPYRIGHT|UNLICENSE)", re.IGNORECASE)
# Vendored into crates/schedr-ui/assets (archive/PROJECT.md, Libraries); 0BSD asks for no notice.
VENDORED = ("htmx 2.0.11 (https://htmx.org), `crates/schedr-ui/assets/htmx.min.js`: 0BSD",)


@dataclass(frozen=True)
class Crate:
    """A crate the binary links."""

    name: str
    version: str
    license: str
    repository: str
    directory: Path


def metadata(target: str) -> dict[str, object]:
    """`cargo metadata` for one target (only the dependencies built for it)."""
    done = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--locked", "--filter-platform", target],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    loaded: dict[str, object] = json.loads(done.stdout)
    return loaded


def linked(meta: dict[str, object], root: str = "schedr") -> list[Crate]:
    """The crates outside the workspace that `root` links: normal dependencies, no macros."""
    packages = {p["id"]: p for p in meta["packages"]}  # type: ignore[attr-defined]
    members = set(meta["workspace_members"])  # type: ignore[call-overload]
    nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}  # type: ignore[index]
    start = next(i for i in members if packages[i]["name"] == root)
    seen: set[str] = set()
    todo = [start]
    while todo:
        current = todo.pop()
        if current in seen:
            continue
        seen.add(current)
        for dep in nodes[current]["deps"]:
            if any(kind["kind"] is None for kind in dep["dep_kinds"]):
                todo.append(dep["pkg"])
    crates = []
    for package_id in seen - members:
        package = packages[package_id]
        if any("proc-macro" in target["kind"] for target in package["targets"]):
            continue
        crates.append(
            Crate(
                package["name"],
                package["version"],
                package["license"] or "(no licence stated)",
                package["repository"] or "",
                Path(package["manifest_path"]).parent,
            )
        )
    return crates


def licence_texts(crate: Crate) -> list[tuple[str, str]]:
    """The crate's licence files, by name: (file name, text)."""
    found = []
    for path in sorted(crate.directory.iterdir()):
        if path.is_file() and LICENSE_FILE.match(path.name):
            text = path.read_text(encoding="utf-8", errors="replace").strip()
            if text:
                found.append((path.name, text))
    return found


def render(crates: list[Crate]) -> str:
    """The file: each crate with its licence, then each distinct text with its crates."""
    crates = sorted(set(crates), key=lambda c: (c.name, c.version))
    # Texts the same but for their white space are one (the Apache-2.0 text, rewrapped).
    by_text: dict[str, list[str]] = defaultdict(list)
    first: dict[str, str] = {}
    without: list[str] = []
    lines = [
        "Third-party licences",
        "====================",
        "",
        "The `schedr` binary (in the wheels and the images) is built from the crates below,",
        "each under its own licence; schedr itself is under LICENSE (MIT). Written by",
        "scripts/third_party.py from Cargo.lock: don't edit it by hand.",
        "",
        "Crates",
        "------",
        "",
    ]
    for crate in crates:
        label = f"{crate.name} {crate.version}"
        where = f" ({crate.repository})" if crate.repository else ""
        lines.append(f"- {label}: {crate.license}{where}")
        texts = licence_texts(crate)
        if not texts:
            without.append(label)
        for name, text in texts:
            key = " ".join(text.split())
            first.setdefault(key, text)
            by_text[key].append(f"{label} ({name})")
    lines += ["", "Vendored in the UI", "------------------", ""]
    lines += [f"- {entry}" for entry in VENDORED]
    if without:
        lines += [
            "",
            "Crates without a licence file",
            "-----------------------------",
            "",
            "These crates ship no licence text; the standard text of the licence they state",
            "applies.",
            "",
        ]
        lines += [f"- {label}" for label in without]
    lines += ["", "Licence texts", "-------------"]
    for key, users in sorted(by_text.items(), key=lambda item: (item[1][0], item[0])):
        lines += ["", "=" * 78, "", "Used by:"]
        lines += [f"  {user}" for user in users]
        lines += ["", first[key]]
    # No trailing white space (some licence files have it; the repository's hooks strip it).
    return "\n".join(line.rstrip() for line in "\n".join(lines).splitlines()) + "\n"


def main(argv: list[str]) -> int:
    """The command line (module docstring)."""
    if argv not in ([], ["--check"]):
        print(__doc__, file=sys.stderr)
        return 2
    crates = [crate for target in TARGETS for crate in linked(metadata(target))]
    text = render(crates)
    if argv == ["--check"]:
        if not OUT.exists() or OUT.read_text() != text:
            print(
                "third_party: LICENSE-THIRD-PARTY is out of date: run `make third-party` "
                "and commit it",
                file=sys.stderr,
            )
            return 1
        return 0
    OUT.write_text(text)
    print(f"wrote {OUT.name}: {len(set(crates))} crates")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
