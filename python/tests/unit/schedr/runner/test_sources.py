"""Code under a DAG root is imported from its source, never from stale bytecode (B59)."""

from __future__ import annotations

import importlib
import importlib.machinery
import importlib.util
import os
import py_compile
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest

from schedr.runner import sources
from schedr.runner.parse import extend_sys_path, import_dag_entries, module_name


@pytest.fixture(autouse=True)
def restore_imports() -> Iterator[None]:
    """Each test's path hooks, importer cache, sys.path and modules are undone."""
    hooks, path, modules = list(sys.path_hooks), list(sys.path), set(sys.modules)
    yield
    sys.path_hooks[:] = hooks
    sys.path[:] = path
    sys.path_importer_cache.clear()
    for name in set(sys.modules) - modules:
        del sys.modules[name]


def stale(path: Path, old: str, new: str) -> None:
    """`path` holds `new`, with bytecode of `old` that Python's checks would take as current.

    The same size and modification time, as git-sync switching revisions within a second.
    """
    assert len(old) == len(new)
    path.write_text(old)
    py_compile.compile(str(path), doraise=True)
    stat = path.stat()
    path.write_text(new)
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))


def standard_import(name: str, path: Path) -> ModuleType:
    """`path` imported by Python's own source loader, which trusts the bytecode."""
    loader = importlib.machinery.SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_loader(name, loader)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def test_a_dag_file_with_stale_bytecode_runs_its_new_source(tmp_path: Path) -> None:
    dag_file = tmp_path / "sync.py"
    stale(dag_file, "VALUE = 'old'\n", "VALUE = 'new'\n")
    # Python's loader runs the old code: the bug.
    assert standard_import("control_sync", dag_file).VALUE == "old"
    import_dag_entries("etl", tmp_path, "sync.py")
    assert sys.modules[module_name("etl", "sync.py")].VALUE == "new"


def test_a_module_found_through_a_root_runs_its_new_source(tmp_path: Path) -> None:
    package = tmp_path / "shared"
    package.mkdir()
    (package / "__init__.py").write_text("")
    stale(tmp_path / "helper.py", "VALUE = 'old'\n", "VALUE = 'new'\n")
    stale(package / "names.py", "NAME = 'old'\n", "NAME = 'new'\n")
    assert standard_import("control_helper", tmp_path / "helper.py").VALUE == "old"
    extend_sys_path([str(tmp_path)])
    helper = importlib.import_module("helper")
    names = importlib.import_module("shared.names")
    assert (helper.VALUE, names.NAME) == ("new", "new")


def test_importing_from_a_root_writes_no_bytecode(tmp_path: Path) -> None:
    (tmp_path / "fresh.py").write_text("VALUE = 1\n")
    extend_sys_path([str(tmp_path)])
    assert importlib.import_module("fresh").VALUE == 1
    assert not (tmp_path / "__pycache__").exists()


def test_other_directories_keep_their_bytecode(tmp_path: Path) -> None:
    root, elsewhere = tmp_path / "etl", tmp_path / "etl-old"
    root.mkdir()
    elsewhere.mkdir()
    stale(elsewhere / "library.py", "VALUE = 'old'\n", "VALUE = 'new'\n")
    sources.import_from_source([str(root)])
    # Not a root: a sibling whose name starts like one.
    sys.path.insert(0, str(elsewhere))
    assert importlib.import_module("library").VALUE == "old"
    assert (elsewhere / "__pycache__").exists()


def test_registering_again_adds_no_second_hook(tmp_path: Path) -> None:
    sources.import_from_source([str(tmp_path)])
    hooks = len(sys.path_hooks)
    sources.import_from_source([str(tmp_path / "other")])
    assert len(sys.path_hooks) == hooks


def test_an_extension_module_in_a_root_is_still_found(tmp_path: Path) -> None:
    # Not a real one: found, it fails to load (where an unfound one is ModuleNotFoundError).
    suffix = importlib.machinery.EXTENSION_SUFFIXES[0]
    (tmp_path / f"native{suffix}").write_bytes(b"not an extension module")
    extend_sys_path([str(tmp_path)])
    with pytest.raises(ImportError) as raised:
        importlib.import_module("native")
    assert not isinstance(raised.value, ModuleNotFoundError)
    assert str(tmp_path) in str(raised.value)
