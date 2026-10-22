from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from schedr import Dag
from schedr._model import DagCtxField, DagCtxRef, registry
from schedr.context import DagCtxReference, RunValues, dag_ctx, running

FIELDS: list[tuple[Callable[[], Any], DagCtxField]] = [
    (lambda: dag_ctx.run.id, DagCtxField.RUN_ID),
    (lambda: dag_ctx.run.kind, DagCtxField.RUN_KIND),
    (lambda: dag_ctx.run.planned_for, DagCtxField.PLANNED_FOR),
    (lambda: dag_ctx.run.since, DagCtxField.SINCE),
    (lambda: dag_ctx.dag_id, DagCtxField.DAG_ID),
]


@pytest.fixture
def dag_body() -> Iterator[None]:
    with Dag("d"):
        yield
    registry.take_entries()


@pytest.mark.parametrize(("read", "field"), FIELDS)
@pytest.mark.usefixtures("dag_body")
def test_in_a_dag_body_fields_are_references(read: Callable[[], Any], field: DagCtxField) -> None:
    reference = read()
    assert reference == DagCtxReference(field)
    assert reference._schedr_reference() == DagCtxRef(field)


@pytest.mark.parametrize(("read", "field"), FIELDS)
def test_outside_a_task_and_a_dag_body_fields_fail(
    read: Callable[[], Any], field: DagCtxField
) -> None:
    with pytest.raises(RuntimeError, match="`dag_ctx` must be run inside a schedr task") as e:
        read()
    assert "task_context" in str(e.value), field


@pytest.mark.usefixtures("dag_body")
def test_a_reference_cant_be_used_as_a_value() -> None:
    with pytest.raises(TypeError, match="can't be used as a value in the DAG file"):
        bool(dag_ctx.run.planned_for)
    with pytest.raises(TypeError, match="can't be used as a value"):
        str(dag_ctx.run.since)


def test_params_come_later() -> None:
    with pytest.raises(NotImplementedError, match="milestone M4"):
        _ = dag_ctx.params


def test_inside_a_running_task_fields_are_values() -> None:
    planned = datetime(2026, 1, 2, tzinfo=UTC)
    values = RunValues("sync", "0190-run", "manual", planned, planned - timedelta(days=1))
    with running(values):
        assert dag_ctx.dag_id == "sync"
        assert dag_ctx.run.id == "0190-run"
        assert dag_ctx.run.kind == "manual"
        assert dag_ctx.run.planned_for == planned
        assert dag_ctx.run.since == planned - timedelta(days=1)
    with pytest.raises(RuntimeError, match="must be run inside a schedr task"):
        _ = dag_ctx.run.planned_for


def test_running_resets_after_an_exception() -> None:
    planned = datetime(2026, 1, 2, tzinfo=UTC)
    with (
        pytest.raises(ValueError, match="inside"),
        running(RunValues("a", "r", "manual", planned, planned)),
    ):
        raise ValueError("inside")
    with pytest.raises(RuntimeError, match="must be run inside a schedr task"):
        _ = dag_ctx.dag_id
