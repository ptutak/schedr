"""`RunOf` and `wait_for`: what a DAG file says."""

from datetime import timedelta
from typing import Any

import pytest

from schedr import Dag, Failed, RunOf, TimedOut, dag_ctx, task, wait_for
from schedr._model import registry


@task
def build(run_id: str) -> str:
    """Reads the other run's ID."""
    return run_id


@task
def notify(failed: Failed) -> str:
    """Says what failed."""
    return failed.state


def _wait_task() -> dict[str, Any]:
    (dag,) = registry.take_defined()
    tasks: list[dict[str, Any]] = dag.to_json()["tasks"]
    return next(t for t in tasks if t["kind"] == "wait_for")


def test_wait_for_says_which_run_and_how_long() -> None:
    with Dag("report"):
        waited = wait_for(
            RunOf("orders", task="load", lag=timedelta(hours=6)), max_wait=timedelta(hours=2)
        )
        build(waited.on(str))
        notify(waited.on(Failed))
    shown = _wait_task()
    assert shown["id"] == "wait_for_load"
    assert shown["run_of"] == {"dag": "orders", "task": "load", "lag_ms": 21_600_000}
    assert shown["max_wait_ms"] == 7_200_000
    assert [o["name"] for o in shown["outcomes"]] == ["SUCCESS", "Failed", "TimedOut"]


def test_without_a_lag_or_a_task_only_the_dag_is_said() -> None:
    with Dag("report"):
        wait_for(RunOf("orders")).on(TimedOut) >> build("late")
    shown = _wait_task()
    assert shown["id"] == "wait_for_orders"
    assert shown["run_of"] == {"dag": "orders"}
    assert "max_wait_ms" not in shown


def test_a_second_wait_for_the_same_run_is_numbered() -> None:
    with Dag("report"):
        wait_for(RunOf("orders"))
        wait_for(RunOf("orders"))
        wait_for(RunOf("orders"), task_id="mine")
    (dag,) = registry.take_defined()
    ids = {t["id"] for t in dag.to_json()["tasks"]}
    assert {"wait_for_orders", "wait_for_orders_2", "mine"} <= ids


@pytest.mark.parametrize(
    ("given", "error"),
    [
        (lambda: RunOf("no spaces"), ValueError),
        (lambda: RunOf("orders", task="bad task"), ValueError),
        (lambda: RunOf("orders", lag=timedelta(microseconds=5)), ValueError),
    ],
)
def test_a_run_of_that_cant_be_stored_is_refused(given: Any, error: type[Exception]) -> None:
    with pytest.raises(error):
        given()


def test_what_isnt_a_run_of_and_a_wait_of_nothing_are_refused() -> None:
    with Dag("report"):
        with pytest.raises(TypeError, match="give it a RunOf"):
            wait_for("orders")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="more than zero"):
            wait_for(RunOf("orders"), max_wait=timedelta(0))
        with pytest.raises(TypeError, match="or `Failed`"):
            wait_for(RunOf("orders")).on(int)
    registry.take_defined()


def test_a_task_taking_only_the_value_is_refused() -> None:
    @task
    def reads(run_id: str) -> str:
        return run_id

    with Dag("report"), pytest.raises(TypeError, match="TimedOut"):
        reads(wait_for(RunOf("orders")))  # type: ignore[arg-type]
    registry.take_defined()


def test_it_says_its_value_failed_and_timed_out_as_its_output_type() -> None:
    with Dag("report"):
        wait_for(RunOf("orders"))
    shown = _wait_task()
    assert shown["output_type"]["type"] == "union"


def test_as_a_trigger_it_starts_a_run_per_run_of_the_other_dag() -> None:
    with Dag("report", trigger=RunOf("orders", task="load", lag=timedelta(hours=6))):
        build(dag_ctx.event.key)
    (dag,) = registry.take_defined()
    assert dag.to_json()["trigger"] == {
        "kind": "run_of",
        "run_of": {"dag": "orders", "task": "load", "lag_ms": 21_600_000},
    }


def test_a_dag_started_by_its_own_runs_or_before_them_is_refused() -> None:
    with pytest.raises(ValueError, match="run of itself"), Dag("orders", trigger=RunOf("orders")):
        pass
    with (
        pytest.raises(ValueError, match="can't be negative"),
        Dag("report", trigger=RunOf("orders", lag=timedelta(hours=-1))),
    ):
        pass
    with pytest.raises(ValueError, match="at most 10 years"):
        RunOf("orders", lag=timedelta(days=3651))
    with pytest.raises(ValueError, match="at most 10 years"):
        RunOf("orders", lag=-timedelta(days=3651))
    assert RunOf("orders", lag=timedelta(days=3650)).lag_ms == 315_360_000_000


def test_a_dag_started_by_runs_reads_which_run_as_its_event() -> None:
    @task
    def consume(data: dict[str, str]) -> str:
        return data["run_id"]

    with Dag("report", trigger=RunOf("orders")):
        consume(dag_ctx.event.data)
    (dag,) = registry.take_defined()
    assert dag.to_json()["trigger"]["kind"] == "run_of"
