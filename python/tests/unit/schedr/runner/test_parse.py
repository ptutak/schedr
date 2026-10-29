import socket
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from schedr.runner.parse import (
    TRACEBACK_LINES,
    extend_sys_path,
    file_result,
    import_dag_entries,
    import_dag_file,
    import_error,
    module_name,
    run_parse,
)
from schedr.runner.protocol import Channel

SYNC = """
from schedr import Dag, EmptyOperator, task

with Dag("sync"):
    EmptyOperator("start")
"""

TWO_DAGS = """
from schedr import Dag, EmptyOperator

for name in ("a", "b"):
    with Dag(name):
        EmptyOperator("t")
"""

WITH_DATACLASS = """
from dataclasses import dataclass
from schedr import Dag, task

@dataclass
class Rows:
    rows: list[str]

with Dag("typed"):
    @task
    def extract() -> Rows: ...
    extract()
"""

HELPER = "VALUE = 42\n"

USES_HELPER = """
from helpers import VALUE
from schedr import Dag, EmptyOperator

with Dag(f"helped_{VALUE}"):
    EmptyOperator("t")
"""

BROKEN = "import does_not_exist_anywhere\n"

PARTIAL = """
from schedr import Dag, EmptyOperator

with Dag("first"):
    EmptyOperator("t")

raise RuntimeError("boom")
"""


@pytest.fixture
def root(tmp_path: Path) -> Path:
    files = {
        "team_a/sync.py": SYNC,
        "two.py": TWO_DAGS,
        "typed.py": WITH_DATACLASS,
        "helpers.py": HELPER,
        "uses_helper.py": USES_HELPER,
        "broken.py": BROKEN,
        "partial.py": PARTIAL,
        "exits.py": "raise SystemExit(3)\n",
    }
    for path, text in files.items():
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_text(text)
    return tmp_path


@pytest.fixture(autouse=True)
def restore_sys_path() -> Iterator[None]:
    saved = list(sys.path)
    yield
    sys.path[:] = saved


def test_module_names_are_valid_stable_and_distinct() -> None:
    name = module_name("etl-repo", "team_a/sync.py")
    assert name.isidentifier()
    assert name.startswith("_schedr_dag_etl_repo_team_a_sync_")
    assert name == module_name("etl-repo", "team_a/sync.py")
    assert name != module_name("etl_repo", "team_a/sync.py")


def test_import_dag_file_returns_the_dags_located_in_their_root(root: Path) -> None:
    (dag,) = import_dag_file("etl", root, "team_a/sync.py")
    assert dag.id == "sync"
    assert (dag.location.root, dag.location.path) == ("etl", "team_a/sync.py")
    assert module_name("etl", "team_a/sync.py") in sys.modules


def test_a_file_can_define_several_dags(root: Path) -> None:
    assert [d.id for d in import_dag_file("etl", root, "two.py")] == ["a", "b"]


def test_dataclasses_in_dag_files_resolve(root: Path) -> None:
    (dag,) = import_dag_file("etl", root, "typed.py")
    assert dag.tasks[0].output_type.to_json()["name"] == "Rows"


def test_a_failing_file_leaves_nothing_behind(root: Path) -> None:
    with pytest.raises(RuntimeError, match="boom"):
        import_dag_file("etl", root, "partial.py")
    assert module_name("etl", "partial.py") not in sys.modules
    assert import_dag_file("etl", root, "two.py")[0].id == "a"


def test_import_dag_entries_keeps_the_task_functions(root: Path) -> None:
    (entry,) = import_dag_entries("etl", root, "typed.py")
    assert entry.model.id == "typed"
    assert list(entry.functions) == ["extract"]
    assert entry.functions["extract"].__name__ == "extract"


def test_extend_sys_path_puts_entries_first_in_order_once() -> None:
    extend_sys_path(["/x", "/y", "/x"])
    assert sys.path[:2] == ["/x", "/y"]
    extend_sys_path(["/y"])
    assert sys.path.count("/y") == 1


def test_a_non_python_file_is_an_import_error(root: Path) -> None:
    (root / "data.txt").write_text("x")
    with pytest.raises(ImportError, match="not a Python source file"):
        import_dag_file("etl", root, "data.txt")


def test_import_error_keeps_the_type_message_and_traceback_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="bad value") as info:
        raise ValueError("bad value")
    error = import_error(info.value)
    assert error["exception"] == "ValueError"
    assert error["message"] == "bad value"
    lines = error["traceback"].splitlines()
    assert lines[0] == "Traceback (most recent call last):"
    assert lines[-1] == "ValueError: bad value"
    assert len(lines) <= TRACEBACK_LINES

    monkeypatch.setattr("schedr.runner.parse.TRACEBACK_LINES", 2)
    assert len(import_error(info.value)["traceback"].splitlines()) == 2


def test_an_import_traceback_starts_at_the_dag_file(root: Path) -> None:
    lines = file_result("etl", root, "partial.py")["error"]["traceback"].splitlines()
    assert lines[0] == "Traceback (most recent call last):"
    assert lines[1].startswith(f'  File "{root / "partial.py"}", line ')
    assert all("schedr/runner" not in line for line in lines)


def test_an_error_inside_schedr_keeps_its_whole_traceback(root: Path) -> None:
    (root / "data.txt").write_text("x")
    with pytest.raises(ImportError) as info:
        import_dag_file("etl", root, "data.txt")
    lines = import_error(info.value)["traceback"].splitlines()
    assert any("schedr/runner/parse.py" in line for line in lines)


def test_file_result_for_success_and_failure(
    root: Path, validate_runner_message: Callable[[Any], None]
) -> None:
    ok = file_result("etl", root, "team_a/sync.py")
    assert ok["type"] == "file_parsed"
    assert ok["error"] is None
    assert ok["dags"][0]["id"] == "sync"
    validate_runner_message(ok)

    failed = file_result("etl", root, "broken.py")
    assert failed["dags"] == []
    assert failed["error"]["exception"] == "ModuleNotFoundError"
    validate_runner_message(failed)

    exited = file_result("etl", root, "exits.py")
    assert exited["error"]["exception"] == "SystemExit"


PLAIN_OUTCOMES = """
import warnings
from dataclasses import dataclass
from schedr import Dag, task

@dataclass
class Rows:
    rows: list[str]

@dataclass
class Gone:
    reason: str

warnings.warn("custom note", UserWarning, stacklevel=1)

with Dag("plain"):
    @task
    def fetch() -> Rows | Gone: ...
    fetch()
"""


def test_the_warnings_importing_a_file_raised_come_with_its_result(
    root: Path, validate_runner_message: Callable[[Any], None]
) -> None:
    (root / "plain.py").write_text(PLAIN_OUTCOMES)
    result = file_result("etl", root, "plain.py")
    assert result["error"] is None
    assert result["dags"][0]["id"] == "plain"
    assert result["warnings"] == [
        "UserWarning: custom note",
        "TypingHint: task `fetch`: outcome class `Rows` derives from neither schedr.Data nor "
        "schedr.pydantic.Model, so type checkers reject `.on(Rows)`; derive it from "
        "schedr.Data, or write `on(output, Rows)`",
        "TypingHint: task `fetch`: outcome class `Gone` derives from neither schedr.Data nor "
        "schedr.pydantic.Model, so type checkers reject `.on(Gone)`; derive it from "
        "schedr.Data, or write `on(output, Gone)`",
    ]
    validate_runner_message(result)
    # A file without any leaves the key out.
    assert "warnings" not in file_result("etl", root, "team_a/sync.py")


def test_a_helper_module_reports_no_dags(root: Path) -> None:
    assert file_result("etl", root, "helpers.py")["dags"] == []


def received(peer: socket.socket) -> list[dict[str, Any]]:
    peer.settimeout(5)
    channel = Channel(peer)
    messages: list[dict[str, Any]] = []
    while not messages or messages[-1]["type"] != "parse_finished":
        messages.append(channel.receive())
    return messages


def test_run_parse_reports_each_file_then_finishes(
    root: Path, validate_runner_message: Callable[[Any], None]
) -> None:
    ours, theirs = socket.socketpair()
    request = {
        "type": "parse",
        "root": "etl",
        "root_dir": str(root),
        "files": ["uses_helper.py", "broken.py"],
        "sys_path": [str(root)],
    }
    validate_runner_message(request)
    run_parse(Channel(ours), request)
    messages = received(theirs)
    assert [m["type"] for m in messages] == [
        "file_started",
        "file_parsed",
        "file_started",
        "file_parsed",
        "parse_finished",
    ]
    assert messages[1]["dags"][0]["id"] == "helped_42"
    assert messages[3]["error"]["exception"] == "ModuleNotFoundError"
    for message in messages:
        validate_runner_message(message)
    assert sys.path[0] == str(root)
    ours.close()
    theirs.close()


def test_run_parse_doesnt_add_sys_path_entries_twice(root: Path) -> None:
    ours, theirs = socket.socketpair()
    sys.path.insert(0, str(root))
    run_parse(
        Channel(ours), {"root": "etl", "root_dir": str(root), "files": [], "sys_path": [str(root)]}
    )
    assert sys.path.count(str(root)) == 1
    assert received(theirs) == [{"type": "parse_finished"}]
    ours.close()
    theirs.close()
