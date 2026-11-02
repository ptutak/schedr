#!/usr/bin/env python3
"""What the release workflow checks and reads (decisions 153-158, 160-163).

    release.py check TAG          the tag against the version; prints the workflow's outputs
    release.py notes TAG          the release notes: the version's CHANGELOG.md section
    release.py wheels TAG DIR     DIR holds exactly the four wheels of that version

A tag is ``vX.Y.Z`` or, for a rehearsal on TestPyPI, ``vX.Y.ZrcN``. It must name the version in
the tree: the Cargo workspace's ``X.Y.Z`` or ``X.Y.Z-rcN`` (the Python package takes its
version from there, written ``X.Y.ZrcN`` by maturin). Standard library only: the workflow runs
it before anything is installed.
"""

from __future__ import annotations

import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_NUMBER = r"(?:0|[1-9][0-9]*)"
TAG = re.compile(
    rf"^v(?P<base>(?P<series>{_NUMBER}\.{_NUMBER})\.{_NUMBER})(?:rc(?P<rc>[1-9][0-9]*))?$"
)
# The platforms CI builds (ci.yml: wheel, wheel-native), one wheel each.
PLATFORMS = {
    # A manylinux wheel may carry two tags: `manylinux_2_17_x86_64.manylinux2014_x86_64`.
    "Linux x86_64 (manylinux)": re.compile(
        r"-manylinux[0-9_]*_x86_64(?:\.manylinux[0-9_]*_x86_64)?\.whl$"
    ),
    "Linux x86_64 (musllinux)": re.compile(r"-musllinux_[0-9]+_[0-9]+_x86_64\.whl$"),
    "Linux aarch64 (manylinux)": re.compile(
        r"-manylinux[0-9_]*_aarch64(?:\.manylinux[0-9_]*_aarch64)?\.whl$"
    ),
    "macOS arm64": re.compile(r"-macosx_[0-9]+_[0-9]+_arm64\.whl$"),
}


class ReleaseError(Exception):
    """Why the release can't go on."""


@dataclass(frozen=True)
class Release:
    """What a tag releases."""

    tag: str
    version: str  # as Python writes it: 0.1.0, 0.1.0rc1
    base: str  # without the rc: 0.1.0
    series: str  # 0.1
    rc: int | None

    @property
    def cargo_version(self) -> str:
        """The version Cargo.toml must hold for this tag."""
        return self.base if self.rc is None else f"{self.base}-rc{self.rc}"


def parse_tag(tag: str) -> Release:
    """The release a tag names; an error for anything but ``vX.Y.Z`` or ``vX.Y.ZrcN``."""
    found = TAG.match(tag)
    if found is None:
        raise ReleaseError(f"`{tag}` isn't a release tag: use vX.Y.Z, or vX.Y.ZrcN for a rehearsal")
    rc = int(found["rc"]) if found["rc"] else None
    return Release(tag, tag[1:], found["base"], found["series"], rc)


def check(tag: str, root: Path | None = None) -> Release:
    """The tag matches the tree's version, and CHANGELOG.md has its section."""
    root = root or ROOT
    release = parse_tag(tag)
    cargo = tomllib.loads((root / "Cargo.toml").read_text())
    version = cargo.get("workspace", {}).get("package", {}).get("version")
    if version != release.cargo_version:
        raise ReleaseError(
            f'`{tag}` needs version = "{release.cargo_version}" in Cargo.toml\'s '
            f"[workspace.package], which has {version!r}: set it, commit, and tag that commit"
        )
    project = tomllib.loads((root / "pyproject.toml").read_text()).get("project", {})
    if "version" not in project.get("dynamic", []):
        raise ReleaseError(
            'pyproject.toml must take its version from Cargo (`dynamic = ["version"]`), '
            "so the wheels carry the tag's version"
        )
    notes(release, root)
    return release


def notes(release: Release, root: Path | None = None) -> str:
    """The release notes: CHANGELOG.md's ``## X.Y.Z`` section, an rc's marked as a rehearsal."""
    path = (root or ROOT) / "CHANGELOG.md"
    lines = path.read_text().splitlines() if path.exists() else []
    heading = re.compile(rf"^## {re.escape(release.base)}(?:\s|$)")
    start = next((i for i, line in enumerate(lines) if heading.match(line)), None)
    if start is None:
        raise ReleaseError(f"CHANGELOG.md has no `## {release.base}` section: write it first")
    end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith("## ")), len(lines))
    body = "\n".join(lines[start + 1 : end]).strip()
    if not body:
        raise ReleaseError(f"CHANGELOG.md's `## {release.base}` section is empty")
    if release.rc is not None:
        body = (
            f"A release candidate of {release.base}, a rehearsal: on TestPyPI, not PyPI.\n\n{body}"
        )
    return body + "\n"


def wheels(release: Release, directory: Path) -> list[Path]:
    """The directory's wheels: one per platform CI builds, all of the tag's version."""
    found = sorted(directory.glob("*.whl"))
    prefix = f"schedr-{release.version}-"
    wrong = [path.name for path in found if not path.name.startswith(prefix)]
    if wrong:
        raise ReleaseError(f"wheels of another version than {release.version}: {', '.join(wrong)}")
    for platform, pattern in PLATFORMS.items():
        matching = [path.name for path in found if pattern.search(path.name)]
        if len(matching) != 1:
            raise ReleaseError(f"{platform}: expected one wheel, found {matching or 'none'}")
    if len(found) != len(PLATFORMS):
        known = [p.name for p in found if any(r.search(p.name) for r in PLATFORMS.values())]
        extra = sorted({p.name for p in found} - set(known))
        raise ReleaseError(f"wheels for no platform CI builds: {', '.join(extra)}")
    return found


def main(argv: list[str]) -> int:
    """The command line (module docstring)."""
    try:
        match argv:
            case ["check", tag]:
                release = check(tag)
                prerelease = "true" if release.rc is not None else "false"
                print(f"version={release.version}")
                print(f"series={release.series}")
                print(f"prerelease={prerelease}")
            case ["notes", tag]:
                print(notes(parse_tag(tag)), end="")
            case ["wheels", tag, directory]:
                for path in wheels(parse_tag(tag), Path(directory)):
                    print(path.name)
            case _:
                print(__doc__, file=sys.stderr)
                return 2
    except ReleaseError as error:
        print(f"release: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
