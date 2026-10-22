from collections.abc import Callable as _Callable
from datetime import UTC, datetime
from typing import Any

import pytest

from schedr._model.dag import Dag, DagLocation, Edge, EdgeKind
from schedr._model.ids import IdError
from schedr._model.tasks import CommandKind, EmptyKind, ShellCommand, Task
from schedr._model.trigger import CronTrigger
from schedr._model.types import ScalarType
from schedr._model.values import Template
from schedr._model.version import SchedrVersion

Validate = _Callable[[str, Any], None]

LOCATION = DagLocation("etl-repo", "team_a/sync.py")


def empty(task_id: str) -> Task:
    return Task(task_id, EmptyKind(), ScalarType("none"))


def sync_dag(**overrides: object) -> Dag:
    fields: dict[str, object] = {
        "id": "sync",
        "location": LOCATION,
        "tasks": [empty("a"), empty("b")],
        "edges": [Edge("a", "b", EdgeKind.ORDER)],
        "trigger": CronTrigger("0 3 * * *", "Europe/Warsaw"),
        "start": datetime(2026, 1, 1, tzinfo=UTC),
        "produced_by": SchedrVersion(0, 1),
    }
    fields.update(overrides)
    return Dag(**fields)  # type: ignore[arg-type]


def test_a_dag_serializes_to_the_schema(validate: Validate) -> None:
    dag = sync_dag()
    json = dag.to_json()
    assert json == {
        "produced_by": "0.1",
        "id": "sync",
        "location": {"root": "etl-repo", "path": "team_a/sync.py"},
        "trigger": {
            "kind": "cron",
            "expression": "0 3 * * *",
            "timezone": "Europe/Warsaw",
            "catchup": False,
        },
        "start": "2026-01-01T00:00:00Z",
        "end": None,
        "initial_since": None,
        "max_active_runs": 1,
        "tasks": [t.to_json() for t in dag.tasks],
        "edges": [{"from": "a", "to": "b", "kind": "order"}],
    }
    validate("Dag", json)


def test_description_and_tags_appear_only_when_set(validate: Validate) -> None:
    json = sync_dag(description="Daily sync", tags=["sync"], max_active_runs=None).to_json()
    assert json["description"] == "Daily sync"
    assert json["tags"] == ["sync"]
    assert json["max_active_runs"] is None
    validate("Dag", json)


def test_a_full_dag_with_commands_validates(validate: Validate) -> None:
    notify = Task(
        "notify", CommandKind(ShellCommand(Template(["echo done"]))), ScalarType("process_result")
    )
    dag = sync_dag(
        tasks=[empty("a"), notify],
        edges=[Edge("a", "notify", EdgeKind.DATA)],
        end=datetime(2026, 12, 31, tzinfo=UTC),
        initial_since=datetime(2020, 1, 1, tzinfo=UTC),
        trigger=CronTrigger("@daily", catchup=True),
    )
    validate("Dag", dag.to_json())


def test_produced_by_defaults_to_the_installed_version() -> None:
    assert Dag("x", LOCATION).produced_by == SchedrVersion.current()


def test_rejects_an_invalid_dag_id() -> None:
    with pytest.raises(IdError):
        sync_dag(id="bad id")


def test_rejects_duplicate_task_ids() -> None:
    with pytest.raises(IdError, match="task ID `a` is defined twice in DAG `sync`"):
        sync_dag(tasks=[empty("a"), empty("a")], edges=[])


def test_rejects_edges_to_unknown_tasks() -> None:
    with pytest.raises(ValueError, match="edge a -> c: no task `c`"):
        sync_dag(edges=[Edge("a", "c", EdgeKind.ORDER)])


@pytest.mark.parametrize("limit", [0, -1])
def test_rejects_a_non_positive_max_active_runs(limit: int) -> None:
    with pytest.raises(ValueError, match="max_active_runs must be positive"):
        sync_dag(max_active_runs=limit)


def test_rejects_naive_timestamps_when_serializing() -> None:
    dag = sync_dag(start=datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="naive datetime"):
        dag.to_json()


def test_edges_validate_their_task_ids_and_serialize(validate: Validate) -> None:
    with pytest.raises(IdError):
        Edge("a", "bad id", EdgeKind.DATA)
    assert Edge("a", "b", EdgeKind.DATA).to_json() == {"from": "a", "to": "b", "kind": "data"}
    validate("Edge", Edge("a", "b", EdgeKind.DATA).to_json())


def test_location_serializes(validate: Validate) -> None:
    assert LOCATION.to_json() == {"root": "etl-repo", "path": "team_a/sync.py"}
    validate("DagLocation", LOCATION.to_json())
