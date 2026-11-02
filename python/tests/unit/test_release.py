"""The release workflow's checks: the tag against the tree, the notes, the wheels."""

from __future__ import annotations

import importlib.util
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def release() -> ModuleType:
    """Load scripts/release.py."""
    path = ROOT / "scripts" / "release.py"
    spec = importlib.util.spec_from_file_location("release", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    # Registered first: its dataclass looks its module up while it is created.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


CHANGELOG = """# Changelog

## 0.2.0

Later.

## 0.1.0

The first preview.

- one
"""


def tree(
    tmp_path: Path,
    version: str,
    *,
    dynamic: bool = True,
    changelog: str = CHANGELOG,
    app_version: str | None = None,
) -> Path:
    (tmp_path / "Cargo.toml").write_text(f'[workspace.package]\nversion = "{version}"\n')
    pyproject = '[project]\nname = "schedr"\n'
    pyproject += 'dynamic = ["version"]\n' if dynamic else 'version = "0.1.0"\n'
    (tmp_path / "pyproject.toml").write_text(pyproject)
    (tmp_path / "CHANGELOG.md").write_text(changelog)
    (tmp_path / "charts" / "schedr").mkdir(parents=True, exist_ok=True)
    app = version.replace("-rc", "rc") if app_version is None else app_version
    (tmp_path / "charts" / "schedr" / "Chart.yaml").write_text(
        f'apiVersion: v2\nname: schedr\nversion: 0.1.0\nappVersion: "{app}"\n'
    )
    return tmp_path


@pytest.mark.parametrize(
    ("tag", "version", "series", "rc", "cargo"),
    [
        ("v0.1.0", "0.1.0", "0.1", None, "0.1.0"),
        ("v0.1.0rc1", "0.1.0rc1", "0.1", 1, "0.1.0-rc1"),
        ("v12.30.4rc10", "12.30.4rc10", "12.30", 10, "12.30.4-rc10"),
    ],
)
def test_a_tag_names_its_version_and_series(
    tag: str, version: str, series: str, rc: int | None, cargo: str
) -> None:
    found = release().parse_tag(tag)
    assert (found.version, found.series, found.rc, found.cargo_version) == (
        version,
        series,
        rc,
        cargo,
    )


@pytest.mark.parametrize(
    "tag", ["0.1.0", "v0.1", "v0.1.0-rc.1", "v0.1.0rc0", "v01.1.0", "v0.1.0a1", "v0.1.0rc1x"]
)
def test_other_tags_are_refused(tag: str) -> None:
    with pytest.raises(release().ReleaseError, match="isn't a release tag"):
        release().parse_tag(tag)


def test_the_tag_must_match_the_tree(tmp_path: Path) -> None:
    root = tree(tmp_path, "0.1.0-rc1")
    assert release().check("v0.1.0rc1", root).version == "0.1.0rc1"
    with pytest.raises(
        release().ReleaseError, match=r'needs version = "0\.1\.0" .* has \'0\.1\.0-rc1\''
    ):
        release().check("v0.1.0", root)
    with pytest.raises(release().ReleaseError, match="dynamic"):
        release().check("v0.1.0", tree(tmp_path, "0.1.0", dynamic=False))
    with pytest.raises(
        release().ReleaseError, match=r'appVersion: "0\.1\.0rc1" .* has \'0\.1\.0\''
    ):
        release().check("v0.1.0rc1", tree(tmp_path, "0.1.0-rc1", app_version="0.1.0"))
    with pytest.raises(release().ReleaseError, match=r"no `## 0\.3\.0` section"):
        release().check("v0.3.0", tree(tmp_path, "0.3.0"))


def test_the_notes_are_the_versions_section(tmp_path: Path) -> None:
    root = tree(tmp_path, "0.1.0")
    final = release().notes(release().parse_tag("v0.1.0"), root)
    assert final == "The first preview.\n\n- one\n"
    rc = release().notes(release().parse_tag("v0.1.0rc2"), root)
    assert rc.startswith("A release candidate of 0.1.0, a rehearsal")
    assert rc.endswith("The first preview.\n\n- one\n")
    # `## 0.1.0` doesn't take `## 0.1.0.1` or `## 0.1.01`, and an empty section is refused.
    root = tree(tmp_path, "0.1.0", changelog="## 0.1.01\n\nNo.\n\n## 0.1.0\n\n## 0.0.9\n\nx\n")
    with pytest.raises(release().ReleaseError, match="is empty"):
        release().notes(release().parse_tag("v0.1.0"), root)


FOUR = [
    "schedr-0.1.0-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
    "schedr-0.1.0-py3-none-musllinux_1_2_x86_64.whl",
    "schedr-0.1.0-py3-none-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
    "schedr-0.1.0-py3-none-macosx_11_0_arm64.whl",
]


def wheels(tmp_path: Path, names: list[str]) -> Path:
    directory = tmp_path / "dist"
    directory.mkdir(exist_ok=True)
    for name in names:
        (directory / name).write_bytes(b"")
    return directory


def test_the_four_wheels_are_found(tmp_path: Path) -> None:
    found = release().wheels(release().parse_tag("v0.1.0"), wheels(tmp_path, FOUR))
    assert sorted(path.name for path in found) == sorted(FOUR)


@pytest.mark.parametrize(
    ("names", "message"),
    [
        (FOUR[:3], r"macOS arm64: expected one wheel, found none"),
        (
            [*FOUR, "schedr-0.1.0-py3-none-manylinux_2_28_x86_64.whl"],
            r"Linux x86_64 \(manylinux\): expected one wheel",
        ),
        ([*FOUR, "schedr-0.1.0-py3-none-win_amd64.whl"], "for no platform CI builds: .*win_amd64"),
        ([*FOUR[:3], "schedr-0.0.9-py3-none-macosx_11_0_arm64.whl"], "another version"),
    ],
)
def test_a_wheel_missing_doubled_or_unknown_is_refused(
    tmp_path: Path, names: list[str], message: str
) -> None:
    with pytest.raises(release().ReleaseError, match=message):
        release().wheels(release().parse_tag("v0.1.0"), wheels(tmp_path, names))


def test_the_command_line_prints_the_outputs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(release(), "ROOT", tree(tmp_path, "0.1.0-rc1"))
    assert release().main(["check", "v0.1.0rc1"]) == 0
    assert capsys.readouterr().out == "version=0.1.0rc1\nseries=0.1\nprerelease=true\n"
    assert release().main(["check", "v0.1.0"]) == 1
    assert "needs version" in capsys.readouterr().err
    assert release().main(["notes", "v0.1.0"]) == 0
    assert capsys.readouterr().out == "The first preview.\n\n- one\n"
    assert release().main(["check", "v0.1"]) == 1
    assert "isn't a release tag" in capsys.readouterr().err
    assert release().main(["wheels", "v0.1.0", str(wheels(tmp_path, FOUR))]) == 0
    assert len(capsys.readouterr().out.splitlines()) == 4
    assert release().main(["nonsense"]) == 2
