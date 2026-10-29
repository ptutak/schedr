import socket
import sys
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from schedr.context import RunValues
from schedr.runner.parse import import_dag_file
from schedr.runner.protocol import Channel
from schedr.runner.task import (
    SUCCESS,
    TaskInputError,
    TaskSetupError,
    build,
    execute,
    failure,
    find_function,
    render,
    resolve,
    run_task,
    run_values,
)

DAG = """
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from schedr import Dag, Data, Outcome, dag_ctx, task

class Rows(Data):
    rows: list[int]

class Gone(Data):
    reason: str

class Check(Outcome):
    CHANGED = 0
    UNCHANGED = 3

class Primary(Data):
    n: int

class Mirror(Data):
    n: int

@task
def pick(value: Primary | Mirror) -> str:
    return type(value).__name__

@task
def fetch(k: int) -> Rows | Gone | None:
    return Rows([k]) if k > 0 else Gone("down") if k < 0 else None

@task
def check(k: int) -> Check:
    return Check.CHANGED if k else Check.UNCHANGED

@task
def liar() -> Check:
    return 3  # type: ignore[return-value]

@task
def take(value: Rows | Gone | None) -> str:
    return type(value).__name__

@task
def react(outcome: Check) -> str:
    return outcome.name

@task
def one() -> dict[str, int]:
    return {"n": 3}

@task
def two(x: int, when: datetime, label: str) -> dict[str, Any]:
    return {"x": x + 1, "when": when.isoformat(), "label": label, "kind": dag_ctx.run.kind}

@task
def coll(xs: list[int], m: dict[str, int]) -> int:
    return sum(xs) + sum(m.values())

@task
def boom() -> None:
    raise ValueError("no rows")

@task
def leaves() -> None:
    raise SystemExit(4)

@task
def opaque() -> Any:
    return object()

@task
def big() -> str:
    return "x" * 100

with Dag("d"):
    f = fetch(1)
    take(f)
    pick(Primary(1))
    react(check(1).outcome)
    liar()
    r = one()
    two(r["n"], dag_ctx.run.planned_for, t"at {dag_ctx.run.planned_for:%Y} by {dag_ctx.dag_id}")
    coll([r["n"], 2], {"k": r["n"]})
    boom()
    leaves()
    opaque()
    big()
"""

RUN = {
    "dag_id": "d",
    "run_id": "0190a3e5-0000-7000-8000-000000000000",
    "kind": "manual",
    "planned_for": "2026-03-04T05:06:07Z",
    "since": "2026-03-03T05:06:07Z",
    "try_number": 1,
    "map_index": None,
}

PLANNED = datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    (tmp_path / "d.py").write_text(DAG)
    (tmp_path / "broken.py").write_text("raise RuntimeError('import boom')\n")
    return tmp_path


@pytest.fixture(autouse=True)
def restore_sys_path() -> Iterator[None]:
    saved = list(sys.path)
    yield
    sys.path[:] = saved


def request_for(
    root: Path, task_id: str, inputs: dict[str, Any] | None = None, **changes: Any
) -> dict[str, Any]:
    (dag,) = import_dag_file("etl", root, "d.py")
    (task,) = [t for t in dag.to_json()["tasks"] if t["id"] == task_id]
    request = {
        "type": "task",
        "dag_file": {"root": "etl", "root_dir": str(root), "path": "d.py"},
        "dag_id": "d",
        "task": task,
        "inputs": inputs or {},
        "run": RUN,
        "sys_path": [str(root)],
    }
    request.update(changes)
    return request


def test_resolve_gives_run_fields_and_output_paths() -> None:
    inputs = {"a": {"rows": [1, {"$tuple": [2, 3]}]}}
    output = {"kind": "output", "task": "a", "path": ["rows", 1, 0]}
    assert resolve(output, inputs, RUN) == 2
    assert resolve({"kind": "output", "task": "a"}, inputs, RUN) == inputs["a"]
    assert resolve({"kind": "dag_ctx", "field": "run_kind"}, inputs, RUN) == "manual"
    assert resolve({"kind": "dag_ctx", "field": "since"}, inputs, RUN) == {
        "$datetime": "2026-03-03T05:06:07Z"
    }


def test_a_reference_whose_edge_wasnt_taken_is_none() -> None:
    inputs = {"fetch": {"rows": [1, 2]}}
    outcomes = {"fetch": "Rows", "gone": None}
    narrowed = {"kind": "output", "task": "fetch", "path": ["rows"], "on": ["Rows"]}
    # Taken: the value.
    assert resolve(narrowed, inputs, RUN, outcomes) == [1, 2]
    # Another outcome: None, without reading the output.
    assert resolve({**narrowed, "on": ["Unavailable"]}, inputs, RUN, outcomes) is None
    # A skipped upstream: None, narrowed or not.
    assert resolve({"kind": "output", "task": "gone"}, inputs, RUN, outcomes) is None
    assert resolve({**narrowed, "task": "gone"}, inputs, RUN, outcomes) is None
    # Neither known: still a missing input.
    with pytest.raises(TaskInputError, match="the output of `other` isn't among"):
        resolve({"kind": "output", "task": "other"}, inputs, RUN, outcomes)


def test_an_outcome_reference_is_the_outcomes_name() -> None:
    outcomes = {"check": "CHANGED"}
    assert resolve({"kind": "outcome", "task": "check"}, {}, RUN, outcomes) == "CHANGED"
    with pytest.raises(TaskInputError, match="the outcome of `other` isn't among"):
        resolve({"kind": "outcome", "task": "other"}, {}, RUN, outcomes)


@pytest.mark.parametrize(
    ("reference", "message"),
    [
        ({"kind": "output", "task": "b"}, "the output of `b` isn't among"),
        ({"kind": "output", "task": "a", "path": ["nope"]}, "`nope` doesn't exist"),
        ({"kind": "output", "task": "a", "path": ["rows", 5]}, "index 5 doesn't exist"),
        ({"kind": "output", "task": "a", "path": [0]}, "index 0 doesn't exist"),
        ({"kind": "output", "task": "a", "path": ["stdout"]}, "`a.stdout` was truncated"),
    ],
)
def test_resolve_refuses_what_isnt_there(reference: dict[str, Any], message: str) -> None:
    inputs = {"a": {"rows": [1], "stdout": "...", "stdout_truncated": True}}
    with pytest.raises(TaskInputError, match=message):
        resolve(reference, inputs, RUN)


def test_render_writes_values_as_text_and_dates_with_a_pattern() -> None:
    parts = [
        {"text": "n="},
        {"ref": {"kind": "output", "task": "a", "path": ["n"]}},
        {"text": " f="},
        {"ref": {"kind": "output", "task": "a", "path": ["f"]}},
        {"text": " day="},
        {"ref": {"kind": "dag_ctx", "field": "planned_for", "format": "%Y-%m-%d"}},
        {"text": " raw="},
        {"ref": {"kind": "output", "task": "a", "path": ["s"], "format": "raw"}},
    ]
    inputs = {"a": {"n": 3, "f": True, "s": "a b"}}
    assert render(parts, inputs, RUN) == "n=3 f=True day=2026-03-04 raw=a b"


@pytest.mark.parametrize("value", [None, [1], {"k": 1}])
def test_render_refuses_values_without_a_text_form(value: object) -> None:
    parts = [{"ref": {"kind": "output", "task": "a"}}]
    with pytest.raises(TaskInputError, match="can't be rendered into text"):
        render(parts, {"a": value}, RUN)


def test_build_decodes_against_the_parameter_type() -> None:
    inputs = {"a": {"n": 3}}
    ref = {"ref": {"kind": "output", "task": "a", "path": ["n"]}}
    assert build({"literal": {"$tuple": [1, 2]}}, tuple[int, int], inputs, RUN) == (1, 2)
    assert build(ref, int, inputs, RUN) == 3
    assert build({"ref": {"kind": "dag_ctx", "field": "planned_for"}}, datetime, {}, RUN) == (
        PLANNED
    )
    assert build({"template": [{"text": "x"}]}, str, {}, RUN) == "x"
    assert build({"list": [ref, {"literal": 2}]}, list[int], inputs, RUN) == [3, 2]
    assert build({"map": {"k": ref}}, dict[str, int], inputs, RUN) == {"k": 3}
    assert build({"list": [{"literal": 1}]}, Any, {}, RUN) == [1]
    assert build({"map": {"k": {"literal": 1}}}, Any, {}, RUN) == {"k": 1}


def test_build_refuses_a_value_that_doesnt_fit() -> None:
    with pytest.raises(TaskInputError, match="doesn't fit the parameter's type"):
        build({"literal": "three"}, int, {}, RUN)


def test_run_values_parses_the_times() -> None:
    assert run_values(RUN) == RunValues(
        "d", RUN["run_id"], "manual", PLANNED, datetime(2026, 3, 3, 5, 6, 7, tzinfo=UTC)
    )


def test_find_function_imports_the_file_and_finds_the_task(root: Path) -> None:
    function = find_function(request_for(root, "one"))
    assert function() == {"n": 3}


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"dag_id": "other"}, "doesn't define DAG `other`"),
        ({"task": {"id": "gone"}}, "has no Python task `gone`"),
    ],
)
def test_find_function_explains_what_is_missing(
    root: Path, changes: dict[str, Any], message: str
) -> None:
    with pytest.raises(TaskSetupError, match=message):
        find_function(request_for(root, "one", **changes))


def test_failure_summarises_the_exception() -> None:
    message = failure(ValueError("bad"), "input_mismatch", "replaced")
    assert message["type"] == "task_failed"
    assert message["kind"] == "input_mismatch"
    assert message["error"]["exception"] == "ValueError"
    assert message["error"]["message"] == "replaced"
    assert failure(ValueError("bad"), None)["error"]["message"] == "bad"


def test_execute_passes_inputs_and_run_values(
    root: Path, validate_runner_message: Callable[[Any], None]
) -> None:
    result = execute(request_for(root, "two", {"one": {"n": 3}}))
    assert result == {
        "type": "task_succeeded",
        "output": {
            "x": 4,
            "when": "2026-03-04T05:06:07+00:00",
            "label": "at 2026 by d",
            "kind": "manual",
        },
        "outcome": SUCCESS,
    }
    validate_runner_message(result)


def test_execute_builds_collections(root: Path) -> None:
    result = execute(request_for(root, "coll", {"one": {"n": 3}}))
    assert result["output"] == 8


@pytest.mark.parametrize(
    ("task_id", "inputs", "kind", "message"),
    [
        ("two", {}, "input_mismatch", "the output of `one` isn't among"),
        ("two", {"one": {"n": "x"}}, "input_mismatch", "doesn't fit"),
        ("boom", {}, None, "no rows"),
        ("leaves", {}, None, "4"),
        ("opaque", {}, "output_not_serializable", "the output of `opaque` can't be stored"),
    ],
)
def test_execute_reports_failures_by_kind(
    root: Path,
    validate_runner_message: Callable[[Any], None],
    task_id: str,
    inputs: dict[str, Any],
    kind: str | None,
    message: str,
) -> None:
    result = execute(request_for(root, task_id, inputs))
    assert result["type"] == "task_failed"
    assert result["kind"] == kind
    assert message in result["error"]["message"]
    validate_runner_message(result)


def test_execute_checks_the_output_size(root: Path) -> None:
    request = request_for(root, "big")
    request["task"]["settings"]["max_output_size"] = 10
    result = execute(request)
    assert result["kind"] == "output_too_large"
    assert "over the limit of 10 bytes" in result["error"]["message"]


def test_execute_reports_a_missing_task_and_a_broken_file(root: Path) -> None:
    imported: list[None] = []
    missing = execute(request_for(root, "one", dag_id="other"), lambda: imported.append(None))
    assert missing["kind"] == "task_not_found"
    broken = request_for(root, "one")
    broken["dag_file"]["path"] = "broken.py"
    result = execute(broken, lambda: imported.append(None))
    assert result["kind"] == "import_failed"
    assert result["error"]["message"] == "import boom"
    assert imported == []


@pytest.mark.parametrize(
    ("task_id", "code", "kind"), [("one", 0, "task_succeeded"), ("boom", 1, "task_failed")]
)
def test_run_task_sends_the_result_and_exits_with_its_code(
    root: Path, task_id: str, code: int, kind: str
) -> None:
    ours, theirs = socket.socketpair()
    try:
        request = request_for(root, task_id, sys_path=["/dags/shared"])
        assert run_task(Channel(ours), request) == code
        received = Channel(theirs)
        assert received.receive() == {"type": "task_imported"}
        assert received.receive()["type"] == kind
        assert sys.path[0] == "/dags/shared"
    finally:
        ours.close()
        theirs.close()


@pytest.mark.parametrize(
    ("k", "outcome", "output"),
    [(1, "Rows", {"rows": [1]}), (-1, "Gone", {"reason": "down"}), (0, "None", None)],
)
def test_an_outcome_union_member_is_the_outcome_and_its_value_the_output(
    root: Path, k: int, outcome: str, output: object
) -> None:
    request = request_for(root, "fetch")
    request["task"]["arguments"] = {"k": {"literal": k}}
    assert execute(request) == {"type": "task_succeeded", "output": output, "outcome": outcome}


def test_an_outcome_enum_member_is_the_outcome_with_no_output(root: Path) -> None:
    request = request_for(root, "check")
    request["task"]["arguments"] = {"k": {"literal": 0}}
    assert execute(request) == {"type": "task_succeeded", "output": None, "outcome": "UNCHANGED"}
    result = execute(request_for(root, "liar"))
    assert (result["type"], result["kind"]) == ("task_failed", "output_not_serializable")


def test_a_union_input_is_decoded_into_the_member_its_outcome_names(root: Path) -> None:
    # {"reason": ...} alone: the stored outcome says it is a Gone.
    request = request_for(root, "take", {"fetch": {"reason": "down"}}, outcomes={"fetch": "Gone"})
    assert execute(request)["output"] == "Gone"
    request = request_for(root, "take", {"fetch": {"rows": [1]}}, outcomes={"fetch": "Rows"})
    assert execute(request)["output"] == "Rows"


def test_an_outcome_reaches_an_outcome_parameter_as_its_member(root: Path) -> None:
    request = request_for(root, "react", outcomes={"check": "UNCHANGED"})
    assert execute(request)["output"] == "UNCHANGED"


def test_the_outcome_tells_apart_members_the_json_would_fit_alike(root: Path) -> None:
    # {"n": 1} fits Primary and Mirror alike: only the stored outcome says it is a Mirror.
    request = request_for(root, "pick", {"src": {"n": 1}}, outcomes={"src": "Mirror"})
    request["task"]["arguments"] = {"value": {"ref": {"kind": "output", "task": "src"}}}
    assert execute(request)["output"] == "Mirror"
