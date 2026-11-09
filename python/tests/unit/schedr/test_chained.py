"""`TriggerDagOperator`: what a DAG file says (decisions 357-362, 365)."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from schedr import Chained, Dag, Failed, TimedOut, TriggerDagOperator, dag_ctx, task
from schedr._model import registry


@task
def day() -> str:
    """The day to load."""
    return "2026-10-21"


@task
def after(run_id: str) -> str:
    """Reads the started run's ID."""
    return run_id


@task
def told(failed: Failed) -> str:
    """Says how the run ended."""
    return failed.state


@task
def late(timed_out: TimedOut) -> None:
    """Notes the wait ran out."""


def _shown() -> dict[str, Any]:
    (dag,) = registry.take_defined()
    return dag.to_json()


def _start(shown: dict[str, Any]) -> dict[str, Any]:
    return next(t for t in shown["tasks"] if t["kind"] == "trigger_dag")


def test_it_says_the_target_its_params_and_how_long_it_waits() -> None:
    with Dag("sync"):
        loaded = day()
        started = TriggerDagOperator(
            "load",
            dag="dwh_load",
            params={"day": loaded, "region": "eu", "run": dag_ctx.run.id},
            max_wait=timedelta(hours=2),
            retries=1,
        )
        after(started.on(str))
        told(started.on(Failed))
        late(started.on(TimedOut))
    shown = _shown()
    start = _start(shown)
    assert start["id"] == "load"
    assert start["dag"] == "dwh_load"
    assert start["params"] == {
        "day": {"ref": {"kind": "output", "task": "day"}},
        "region": {"literal": "eu"},
        "run": {"ref": {"kind": "dag_ctx", "field": "run_id"}},
    }
    assert "wait" not in start, "waiting is the default"
    assert start["max_wait_ms"] == 7_200_000
    assert "planned_for" not in start
    assert start["settings"]["retries"] == 1
    assert [o["name"] for o in start["outcomes"]] == ["SUCCESS", "Failed", "TimedOut"]
    edges = {(e["from"], e["to"], e["kind"], tuple(e.get("on", []))) for e in shown["edges"]}
    assert ("day", "load", "data", ()) in edges, "its params read `day`"
    assert ("load", "after", "data", ("SUCCESS",)) in edges
    assert ("load", "told", "data", ("Failed",)) in edges
    assert ("load", "late", "data", ("TimedOut",)) in edges


def test_without_waiting_it_has_only_its_value() -> None:
    with Dag("sync"):
        started = TriggerDagOperator("load", dag="dwh_load", wait=False)
        after(started.output)
    start = _start(_shown())
    assert start["wait"] is False
    assert "max_wait_ms" not in start
    assert [o["name"] for o in start["outcomes"]] == ["SUCCESS"]


@pytest.mark.parametrize(
    ("planned_for", "shown"),
    [
        (
            lambda: datetime(2026, 10, 21, 8, tzinfo=UTC),
            {"literal": {"$datetime": "2026-10-21T08:00:00+00:00"}},
        ),
        # A reference is made inside `with Dag(...)`.
        (lambda: dag_ctx.run.planned_for, {"ref": {"kind": "dag_ctx", "field": "planned_for"}}),
    ],
)
def test_planned_for_is_a_datetime_or_a_reference(
    planned_for: Callable[[], object], shown: object
) -> None:
    with Dag("sync"):
        TriggerDagOperator("load", dag="dwh_load", planned_for=planned_for())
    assert _start(_shown())["planned_for"] == shown


@pytest.mark.parametrize(
    ("given", "error"),
    [
        ({"dag": "no spaces allowed"}, "`dag`"),
        ({"dag": "dwh_load", "wait": False, "max_wait": timedelta(minutes=1)}, "`max_wait`"),
        ({"dag": "dwh_load", "params": ["day"]}, "`params` is a dict"),
        ({"dag": "dwh_load", "params": {1: "day"}}, "keys are strings"),
        ({"dag": "dwh_load", "planned_for": "2026-10-21"}, "`planned_for`"),
    ],
)
def test_what_it_cant_be_is_refused_in_the_dag_file(given: dict[str, object], error: str) -> None:
    with Dag("sync"), pytest.raises(TypeError, match=error):
        TriggerDagOperator("load", **given)  # type: ignore[arg-type]  # the mistake tested
    registry.take_defined()


def test_outside_a_dag_it_is_refused() -> None:
    with pytest.raises(RuntimeError, match="TriggerDagOperator"):
        TriggerDagOperator("load", dag="dwh_load")


@pytest.mark.parametrize(
    ("trigger", "shown"),
    [
        (Chained(), {"kind": "chained"}),
        (Chained(by=["sync", "orders"]), {"kind": "chained", "by": ["sync", "orders"]}),
    ],
)
def test_a_chained_dag_says_who_may_start_it(trigger: Chained, shown: object) -> None:
    with Dag("dwh_load", trigger=trigger):
        day()
    assert _shown()["trigger"] == shown


@pytest.mark.parametrize(
    ("by", "error", "message"),
    [
        ([], ValueError, r"Chained\(by=\[\]\) names no DAG"),
        ("sync", TypeError, "a list of DAG IDs"),
        ([1], TypeError, "holds DAG IDs"),
        (["no spaces"], ValueError, "DAG ID"),
    ],
)
def test_a_chained_trigger_names_dags(by: object, error: type[Exception], message: str) -> None:
    with pytest.raises(error, match=message):
        Chained(by=by)  # type: ignore[arg-type]  # the mistake tested


@dataclass
class Load:
    """A chained DAG's params: no default needed, its runs take the operator's."""

    region: str


def test_a_chained_dags_params_need_no_defaults() -> None:
    with Dag("dwh_load", trigger=Chained(), params=Load):
        day()
    assert _shown()["trigger"] == {"kind": "chained"}
