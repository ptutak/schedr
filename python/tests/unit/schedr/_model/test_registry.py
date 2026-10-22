from pathlib import Path

from schedr._model.dag import Dag, DagLocation
from schedr._model.registry import (
    LOCAL_ROOT,
    DefinedDag,
    defined_in,
    location_for,
    parse_root,
    register,
    take_defined,
    take_entries,
)


def test_without_a_parse_root_dags_are_local_by_file_name(tmp_path: Path) -> None:
    assert location_for(str(tmp_path / "team_a" / "sync.py")) == DagLocation(LOCAL_ROOT, "sync.py")


def test_inside_a_parse_root_paths_are_relative_with_slashes(tmp_path: Path) -> None:
    with parse_root("etl-repo", tmp_path):
        location = location_for(str(tmp_path / "team_a" / "sync.py"))
    assert location == DagLocation("etl-repo", "team_a/sync.py")


def test_files_outside_the_parse_root_are_local(tmp_path: Path) -> None:
    with parse_root("etl-repo", tmp_path / "root"):
        location = location_for(str(tmp_path / "elsewhere" / "x.py"))
    assert location == DagLocation(LOCAL_ROOT, "x.py")


def test_the_parse_root_ends_with_the_block(tmp_path: Path) -> None:
    with parse_root("etl-repo", tmp_path):
        pass
    assert location_for(str(tmp_path / "a.py")).root == LOCAL_ROOT


def test_take_defined_returns_registered_dags_once_in_order() -> None:
    first = Dag("a", DagLocation("r", "a.py"))
    second = Dag("b", DagLocation("r", "b.py"))
    register(first)
    register(second)
    assert take_defined() == [first, second]
    assert take_defined() == []


def test_take_entries_keeps_the_task_functions() -> None:
    def extract() -> None: ...

    dag = Dag("a", DagLocation("r", "a.py"))
    register(dag, {"extract": extract})
    assert take_entries() == [DefinedDag(dag, {"extract": extract})]
    assert take_entries() == []


def test_defined_in_remembers_dags_by_file_after_they_are_taken(tmp_path: Path) -> None:
    def extract() -> None: ...

    first = Dag("a", DagLocation("r", "a.py"))
    second = Dag("a", DagLocation("r", "a.py"), max_active_runs=2)
    register(first, {"extract": extract}, str(tmp_path / "a.py"))
    register(second, {"extract": extract}, str(tmp_path / "a.py"))
    take_entries()
    assert defined_in(tmp_path / "x" / ".." / "a.py", "a") == DefinedDag(
        second, {"extract": extract}
    )
    assert defined_in(tmp_path / "a.py", "b") is None
    assert defined_in(tmp_path / "b.py", "a") is None
