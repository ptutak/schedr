"""LICENSE-THIRD-PARTY lists what the binary links, with each licence text once."""

from __future__ import annotations

import importlib.util
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def third_party() -> ModuleType:
    """Load scripts/third_party.py."""
    path = ROOT / "scripts" / "third_party.py"
    spec = importlib.util.spec_from_file_location("third_party", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    # Registered first: its dataclass looks its module up while it is created.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def package(
    tmp_path: Path, name: str, *, kind: str = "lib", files: dict[str, str] | None = None
) -> dict[str, object]:
    directory = tmp_path / name
    directory.mkdir()
    (directory / "Cargo.toml").write_text("")
    for file, text in (files or {}).items():
        (directory / file).write_text(text)
    return {
        "id": name,
        "name": name,
        "version": "1.0.0",
        "license": "MIT",
        "repository": f"https://example.com/{name}",
        "manifest_path": str(directory / "Cargo.toml"),
        "targets": [{"kind": [kind]}],
    }


def dep(name: str, *kinds: str | None) -> dict[str, object]:
    return {"pkg": name, "dep_kinds": [{"kind": kind, "target": None} for kind in kinds]}


@pytest.fixture
def meta(tmp_path: Path) -> dict[str, object]:
    """schedr -> a (normal) -> b (normal); schedr -> dev (dev only), build (build only),
    derive (a proc macro); schedr -> schedr-model (a workspace member) -> c."""
    mit = "MIT License\n\nCopyright (c) someone\n"
    packages = [
        package(tmp_path, "schedr"),
        package(tmp_path, "schedr-model"),
        package(tmp_path, "a", files={"LICENSE-MIT": mit, "LICENSE-APACHE": "Apache\n  text"}),
        package(tmp_path, "b", files={"LICENSE": "Apache  text\n", "README.md": "not a licence"}),
        package(tmp_path, "c"),
        package(tmp_path, "dev", files={"LICENSE": "dev"}),
        package(tmp_path, "build", files={"LICENSE": "build"}),
        package(tmp_path, "derive", kind="proc-macro", files={"LICENSE": "derive"}),
    ]
    nodes = [
        {
            "id": "schedr",
            "deps": [
                dep("a", None),
                dep("dev", "dev"),
                dep("build", "build"),
                dep("derive", None),
                dep("schedr-model", None),
            ],
        },
        {"id": "schedr-model", "deps": [dep("c", None, "dev")]},
        {"id": "a", "deps": [dep("b", None)]},
        {"id": "b", "deps": []},
        {"id": "c", "deps": []},
        {"id": "dev", "deps": []},
        {"id": "build", "deps": []},
        {"id": "derive", "deps": []},
    ]
    return {
        "packages": packages,
        "workspace_members": ["schedr", "schedr-model"],
        "resolve": {"nodes": nodes},
    }


def test_only_what_the_binary_links_is_listed(meta: dict[str, object]) -> None:
    names = sorted(crate.name for crate in third_party().linked(meta))
    # Not the workspace, not dev or build dependencies, not a proc macro; through a member.
    assert names == ["a", "b", "c"]


def test_each_text_is_printed_once_with_its_crates(meta: dict[str, object]) -> None:
    text = third_party().render(third_party().linked(meta))
    assert "- a 1.0.0: MIT (https://example.com/a)" in text
    # The same Apache text but for its white space: printed once, both crates under it.
    assert text.count("Apache") == 1
    assert "Used by:\n  a 1.0.0 (LICENSE-APACHE)\n  b 1.0.0 (LICENSE)\n" in text
    assert "Copyright (c) someone" in text
    assert "not a licence" not in text
    # A crate without a file is named as such; the vendored files are listed.
    assert "Crates without a licence file" in text
    assert "\n- c 1.0.0\n" in text
    assert "htmx 2.0.11" in text
    # The same crate from two targets' metadata is listed once.
    crates = third_party().linked(meta)
    assert third_party().render(crates + crates) == text


def test_the_committed_file_is_current() -> None:
    # The check the Makefile runs (`make third-party-check`), against this repository.
    if not (ROOT / "LICENSE-THIRD-PARTY").exists():
        pytest.fail("LICENSE-THIRD-PARTY is missing: run `make third-party`")
    assert third_party().main(["--check"]) == 0


def test_the_command_line_refuses_other_arguments(capsys: pytest.CaptureFixture[str]) -> None:
    assert third_party().main(["--write"]) == 2
    assert "third_party.py --check" in capsys.readouterr().err
