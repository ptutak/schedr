"""DAG files are imported as Python imports them, and a clashing name fails only what uses it."""

import subprocess
import sys
from pathlib import Path

import pytest

from schedr.runner import sources
from schedr.runner.parse import extend_sys_path, file_result, import_dag_entries, module_name

DAG = """
from schedr import Dag, task

@task
def t() -> None:
    pass

with Dag("{name}"):
    t()
"""


def _root(tmp_path: Path, files: dict[str, str]) -> Path:
    for name, text in files.items():
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_text(text)
    return tmp_path


def _dags(root: Path, path: str) -> list[str]:
    result = file_result("etl", root, path)
    assert result["error"] is None, result["error"]
    return [dag["id"] for dag in result["dags"]]


def _error(root: Path, path: str) -> str:
    error = file_result("etl", root, path)["error"]
    assert error is not None
    return str(error["message"])


@pytest.mark.parametrize(
    ("path", "name"),
    [
        ("sync.py", "sync"),
        ("team/a/sync.py", "team.a.sync"),
        ("team/__init__.py", "team"),
        ("__init__.py", "__init__"),
        ("my-team/sync.py", "my-team.sync"),
    ],
)
def test_a_file_has_its_ordinary_name(path: str, name: str) -> None:
    assert module_name("etl", path) == name


@pytest.mark.parametrize("path", ["a.b.py", "a.b/sync.py"])
def test_a_dot_in_a_name_is_refused(path: str) -> None:
    with pytest.raises(ImportError, match="rename it"):
        module_name("etl", path)


def test_a_helper_with_a_relative_import_is_no_error_and_its_parent_runs(tmp_path: Path) -> None:
    # B172.
    root = _root(
        tmp_path,
        {
            "mylib/__init__.py": "RAN = True\n",
            "mylib/util.py": "def clean() -> int:\n    return 1\n",
            "mylib/factory.py": "from schedr import Dag\nfrom .util import clean\n",
            "main.py": "import mylib\nassert mylib.RAN\n"
            "from mylib.factory import clean\n" + DAG.format(name="main"),
        },
    )
    extend_sys_path([str(root)])
    assert _dags(root, "mylib/factory.py") == []
    assert _dags(root, "main.py") == ["main"]


def test_a_dag_built_by_a_helper_belongs_to_the_module_calling_it(tmp_path: Path) -> None:
    # B170.
    root = _root(
        tmp_path,
        {
            "lib.py": "from schedr import Dag, task\n\n@task\ndef t() -> None:\n    pass\n\n"
            "def build(name: str) -> None:\n    with Dag(name):\n        t()\n",
            "main.py": "# schedr\nfrom lib import build\nbuild('built')\n",
        },
    )
    extend_sys_path([str(root)])
    assert _dags(root, "main.py") == ["built"]
    assert _dags(root, "lib.py") == []


def test_a_dag_a_module_defines_itself_stays_its_own(tmp_path: Path) -> None:
    # B110.
    root = _root(
        tmp_path,
        {"other.py": DAG.format(name="other"), "main.py": "import other\n" + DAG.format(name="m")},
    )
    extend_sys_path([str(root)])
    assert _dags(root, "main.py") == ["m"]


def test_a_stray_library_name_fails_only_itself_and_what_imports_it(tmp_path: Path) -> None:
    root = _root(
        tmp_path,
        {
            "queue.py": DAG.format(name="clashing"),
            "good.py": DAG.format(name="good"),
            "user.py": "import queue\n" + DAG.format(name="user"),
            "later.py": "def f() -> None:\n    from queue import Queue\n" + DAG.format(name="l"),
        },
    )
    extend_sys_path([str(root)])
    assert _dags(root, "good.py") == ["good"]
    for path in ("queue.py", "user.py", "later.py"):
        assert "has the name of a library" in _error(root, path), path


def test_a_library_gets_its_own_module_whatever_a_root_holds(tmp_path: Path) -> None:
    source = """
import sys
from pathlib import Path
from schedr.runner.parse import extend_sys_path
root = sys.argv[1]
sys.modules.pop("colorsys", None)
extend_sys_path([root])
import importlib
found = importlib.import_module("colorsys")
assert not found.__file__.startswith(root), found.__file__
print("library")
"""
    root = _root(tmp_path, {"colorsys.py": "CUSTOM = True\n"})
    ran = subprocess.run(
        [sys.executable, "-P", "-c", source, str(root)], capture_output=True, text=True, check=False
    )
    assert ran.stdout.strip() == "library", ran


def test_the_same_name_in_two_roots_fails_what_uses_it(tmp_path: Path) -> None:
    one = _root(
        tmp_path / "one", {"pkg/sync.py": DAG.format(name="a"), "a.py": DAG.format(name="x")}
    )
    two = _root(tmp_path / "two", {"pkg/sync.py": DAG.format(name="b"), "b.py": "import pkg\n"})
    extend_sys_path([str(one), str(two)])
    assert _dags(one, "a.py") == ["x"]
    assert "in several DAG roots" in _error(one, "pkg/sync.py")
    assert "in several DAG roots" in _error(two, "b.py")


def test_files_outside_the_root_are_not_its_dags(tmp_path: Path) -> None:
    # B177.
    outside = _root(tmp_path / "outside", {"foreign.py": DAG.format(name="foreign")})
    root = _root(
        tmp_path / "local",
        {"main.py": f"import sys\nsys.path.insert(0, {str(outside)!r})\nimport foreign\n"},
    )
    extend_sys_path([str(root)])
    assert import_dag_entries("local", root, "main.py") == []


def test_only_real_pydantic_counts_as_pydantic(tmp_path: Path) -> None:
    # B180.
    from schedr._serialization.models import is_model  # noqa: PLC0415

    root = _root(tmp_path, {"pydanticish/__init__.py": "class BaseModel:\n    pass\n"})
    extend_sys_path([str(root)])
    import pydanticish  # type: ignore[import-not-found]  # noqa: PLC0415  # pyright: ignore[reportMissingImports]

    assert not is_model(pydanticish.BaseModel)


def test_only_names_imported_are_looked_at(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every file and task starts a process: its start can't grow with the size of the root."""
    root = _root(tmp_path, {f"d{n}.py": "X = 1\n" for n in range(50)})
    looked_at: list[str] = []
    real = sources._top_place

    def counting(place: str, name: str) -> str | None:
        looked_at.append(name)
        return real(place, name)

    monkeypatch.setattr(sources, "_top_place", counting)
    extend_sys_path([str(root)])
    assert looked_at == []
    assert sources.clash("d1") is None
    assert sources.clash("d1") is None
    assert looked_at == ["d1"]


def test_a_folder_of_modules_leaves_a_library_of_its_name_alone(tmp_path: Path) -> None:
    """Without ``__init__.py`` it is no package: a library of that name wins, as in Python."""
    root = _root(
        tmp_path,
        {"wave/note.py": "NOTE = 1\n", "main.py": "import wave\n" + DAG.format(name="m")},
    )
    extend_sys_path([str(root)])
    assert sources.clash("wave") is None
    assert _dags(root, "main.py") != []


def test_a_package_takes_a_librarys_name(tmp_path: Path) -> None:
    root = _root(tmp_path, {"wave/__init__.py": "NOTE = 1\n"})
    extend_sys_path([str(root)])
    assert "has the name of a library" in (sources.clash("wave") or "")


def test_a_folder_of_modules_merging_with_a_namespace_library_clashes(tmp_path: Path) -> None:
    """Both are portions of one package: which ``ns.policy`` you get depends on how it starts."""
    root = _root(tmp_path / "root", {"review_ns/policy.py": "WHO = 'root'\n"})
    library = _root(tmp_path / "lib", {"review_ns/policy.py": "WHO = 'library'\n"})
    sys.path.append(str(library))
    extend_sys_path([str(root)])
    assert "has the name of a library" in (sources.clash("review_ns") or "")


def test_a_module_beside_a_folder_of_its_name_is_what_counts(tmp_path: Path) -> None:
    """Python takes a package, then a module, then a folder of modules."""
    root = _root(tmp_path, {"wave.py": "X = 1\n", "wave/note.py": "NOTE = 1\n"})
    extend_sys_path([str(root)])
    assert sources._top_place(str(root), "wave") == str(root / "wave.py")
    assert "has the name of a library" in (sources.clash("wave") or "")
