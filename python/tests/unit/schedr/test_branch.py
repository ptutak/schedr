from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from schedr import Dag, EmptyOperator, Task, branch, task
from schedr._model import registry
from schedr._serialization import CodecError
from schedr.runner.parse import import_dag_file
from schedr.runner.task import chosen_ids, execute

type Validate = Callable[[str, Any], None]


@branch
def pick(fast: bool) -> list[Task]:
    return []


@task
def after(ids: list[str] | None) -> None:
    pass


def only_dag() -> Any:
    (dag,) = registry.take_defined()
    return dag


def test_a_branchs_edges_are_selection_edges(validate: Validate) -> None:
    with Dag("d"):
        chooser = pick(True)
        chooser >> [EmptyOperator("a"), EmptyOperator("b")]
    model = only_dag()
    assert [(e.from_, e.to, e.kind.value) for e in model.edges] == [
        ("pick", "a", "selection"),
        ("pick", "b", "selection"),
    ]
    branch_task = model.tasks[0].to_json()
    assert branch_task["branch"] is True
    assert branch_task["output_type"] == {"type": "list", "item": {"type": "str"}}
    validate("Dag", model.to_json())


def test_a_branch_routes_by_choice_not_outcome() -> None:
    with Dag("d"), pytest.raises(TypeError, match="taken when it chose their task"):
        pick(True).on()
    registry.take_defined()


def test_a_branch_target_may_be_skipped(validate: Validate) -> None:
    @task
    def take(n: int, m: int) -> None:
        pass

    @task
    def count() -> int:
        return 1

    def define() -> None:
        with Dag("d"):
            chooser = pick(True)
            target = count.override(task_id="target")()
            chooser >> target
            take(target, count())

    # `target` may be skipped (not chosen) while `take` runs through `count`: None.
    with pytest.raises(TypeError, match=r"argument `n`: `target` is None"):
        define()
    registry.take_defined()


DAG = """
from schedr import Dag, EmptyOperator, Task, branch

@branch
def targets(which: str) -> list[Task]:
    choices = {"a": [to_a], "ab": [to_a, to_b], "none": [], "one": to_b, "outside": [other],
               "bad": ["to_a"], "nothing": None}
    return choices[which]

with Dag("d"):
    other = EmptyOperator("other")
    chooser = targets("a")
    to_a = EmptyOperator("to_a")
    to_b = EmptyOperator("to_b")
    chooser >> [to_a, to_b]
"""


@pytest.fixture
def request_for(tmp_path: Path) -> Callable[[str], dict[str, Any]]:
    (tmp_path / "d.py").write_text(DAG)

    def make(which: str) -> dict[str, Any]:
        (dag,) = import_dag_file("etl", tmp_path, "d.py")
        (targets,) = [t for t in dag.to_json()["tasks"] if t["id"] == "targets"]
        targets["arguments"] = {"which": {"literal": which}}
        return {
            "type": "task",
            "dag_file": {"root": "etl", "root_dir": str(tmp_path), "path": "d.py"},
            "dag_id": "d",
            "task": targets,
            "inputs": {},
            "run": {
                "dag_id": "d",
                "run_id": "0190a3e5-0000-7000-8000-000000000000",
                "kind": "manual",
                "planned_for": "2026-03-04T05:06:07Z",
                "since": "2026-03-03T05:06:07Z",
                "try_number": 1,
                "map_index": None,
            },
            "sys_path": [str(tmp_path)],
        }

    return make


@pytest.mark.parametrize(
    ("which", "chosen"),
    [("a", ["to_a"]), ("ab", ["to_a", "to_b"]), ("none", []), ("one", ["to_b"])],
)
def test_a_branchs_handles_become_the_chosen_ids(
    request_for: Callable[[str], dict[str, Any]], which: str, chosen: list[str]
) -> None:
    assert execute(request_for(which)) == {
        "type": "task_succeeded",
        "output": chosen,
        "outcome": "SUCCESS",
    }


@pytest.mark.parametrize(
    ("which", "message"),
    [
        ("outside", "`other` isn't right after the branch"),
        ("bad", "returns task handles, got str"),
        ("nothing", r"or \[\] for none \(not None\)"),
    ],
)
def test_a_branch_can_choose_only_its_own_targets(
    request_for: Callable[[str], dict[str, Any]], which: str, message: str
) -> None:
    result = execute(request_for(which))
    assert (result["type"], result["kind"]) == ("task_failed", "output_not_serializable")
    with pytest.raises(CodecError, match=message):
        chosen_ids(
            {
                "outside": [type("H", (), {"_schedr_task_id": "other"})()],
                "bad": ["to_a"],
                "nothing": None,
            }[which],
            frozenset({"to_a", "to_b"}),
        )
