from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any

import pytest

from schedr import (
    CommandOperator,
    Cron,
    Dag,
    EmptyOperator,
    Every,
    Once,
    TaskDefaults,
    dag_ctx,
    task,
)
from schedr._model import IdError, NoSchedule, TaskSettings, registry

Validate = Callable[[str, Any], None]


def only_dag() -> Any:
    dags = registry.take_defined()
    assert len(dags) == 1
    return dags[0]


def test_a_dag_is_recorded_with_its_tasks(validate: Validate) -> None:
    with Dag("sync", schedule=Cron("0 3 * * *"), description="Daily", tags=["sync"]) as dag:
        EmptyOperator("start")
    model = only_dag()
    assert dag.model is model
    assert [t.id for t in model.tasks] == ["start"]
    json = model.to_json()
    assert json["id"] == "sync"
    assert json["location"] == {"root": "local", "path": "test_dag.py"}
    assert json["description"] == "Daily"
    assert json["tags"] == ["sync"]
    validate("Dag", json)


def test_defaults_without_a_schedule() -> None:
    with Dag("manual"):
        pass
    model = only_dag()
    assert model.schedule == NoSchedule()
    assert (model.start, model.end, model.initial_since) == (None, None, None)
    assert model.catchup is False
    assert model.max_active_runs == 1


@pytest.mark.parametrize(
    ("start", "expected"),
    [
        ("2026-01-01", datetime(2025, 12, 31, 23, tzinfo=UTC)),
        (date(2026, 1, 1), datetime(2025, 12, 31, 23, tzinfo=UTC)),
        ("2026-07-01T03:00", datetime(2026, 7, 1, 1, tzinfo=UTC)),
        (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
        ("2026-01-01T00:00+02:00", datetime(2025, 12, 31, 22, tzinfo=UTC)),
    ],
)
def test_naive_moments_are_in_the_schedules_time_zone(start: object, expected: datetime) -> None:
    with Dag("d", schedule=Cron("0 3 * * *", tz="Europe/Warsaw"), start=start):  # type: ignore[arg-type]
        pass
    assert only_dag().start == expected


def test_naive_moments_are_utc_without_a_cron_schedule() -> None:
    with Dag("d", schedule=Every(timedelta(hours=1)), end="2026-01-01", initial_since="2020-01-01"):
        pass
    model = only_dag()
    assert model.end == datetime(2026, 1, 1, tzinfo=UTC)
    assert model.initial_since == datetime(2020, 1, 1, tzinfo=UTC)
    assert model.start is None


def test_naive_moments_follow_a_once_schedules_zone() -> None:
    with Dag("d", schedule=Once("2026-12-24T23:00", tz="Europe/Warsaw"), end="2026-12-25"):
        pass
    model = only_dag()
    assert model.end == datetime(2026, 12, 24, 23, tzinfo=UTC)
    assert model.to_json()["schedule"] == {"kind": "once", "at": "2026-12-24T22:00:00Z"}


@pytest.mark.parametrize(
    ("schedule", "what"), [(None, "no schedule"), (Once("2026-12-24"), "a Once schedule")]
)
def test_since_without_a_previous_fire_time_needs_initial_since(
    schedule: Once | None, what: str
) -> None:
    with (
        pytest.raises(ValueError, match=f"DAG `d` has {what}, so its first run has no previous"),
        Dag("d", schedule=schedule),
    ):
        CommandOperator("load", t"load --since {dag_ctx.run.since}")
    with Dag("d", schedule=schedule, initial_since="2020-01-01"):
        CommandOperator("load", t"load --since {dag_ctx.run.since}")
    with Dag("e", schedule=schedule):
        CommandOperator("load", t"load --until {dag_ctx.run.planned_for}")
    with Dag("f", schedule=Every(timedelta(hours=1))):
        CommandOperator("load", t"load --since {dag_ctx.run.since}")
    assert [d.id for d in registry.take_defined()] == ["d", "e", "f"]


def test_bad_moments_are_rejected() -> None:
    with pytest.raises(ValueError, match="start: `soon` is not an ISO 8601"):
        Dag("d", start="soon")
    with pytest.raises(TypeError, match="start must be a datetime, a date or an ISO string"):
        Dag("d", start=5)  # type: ignore[arg-type]


def test_aware_moments_keep_their_instant() -> None:
    start = datetime(2026, 1, 1, 5, tzinfo=timezone(timedelta(hours=5)))
    with Dag("d", start=start):
        pass
    assert only_dag().to_json()["start"] == "2026-01-01T00:00:00Z"


@pytest.mark.parametrize(
    ("kwargs", "error", "message"),
    [
        ({"schedule": "0 3 * * *"}, TypeError, "schedule must be Cron"),
        ({"tags": "sync"}, TypeError, "tags must be a list"),
        ({"max_active_runs": 0}, ValueError, "max_active_runs must be positive"),
    ],
)
def test_bad_arguments_are_rejected(
    kwargs: dict[str, Any], error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        Dag("d", **kwargs)


def test_unknown_arguments_are_a_type_error() -> None:
    with pytest.raises(TypeError):
        Dag("d", owner="me")  # type: ignore[call-arg]


def test_an_invalid_dag_id_is_rejected() -> None:
    with pytest.raises(IdError):
        Dag("bad id")


def test_no_limit_on_runs() -> None:
    with Dag("d", max_active_runs=None, catchup=True):
        pass
    model = only_dag()
    assert model.max_active_runs is None
    assert model.catchup is True


def test_task_defaults_apply_and_task_arguments_win() -> None:
    defaults = TaskDefaults(
        retries=2,
        retry_delay=timedelta(minutes=1),
        timeout=timedelta(hours=1),
        queue="q",
        max_output_size=10,
    )
    with Dag("d", defaults=defaults):
        CommandOperator("a", "ls")
        CommandOperator("b", "ls", retries=5, queue="other")
    a, b = only_dag().tasks
    assert a.settings == TaskSettings(2, 60_000, 3_600_000, "q", 10)
    assert b.settings == TaskSettings(5, 60_000, 3_600_000, "other", 10)


def test_dags_cant_be_nested() -> None:
    with Dag("outer"), pytest.raises(RuntimeError, match="DAGs can't be nested"):
        Dag("inner").__enter__()
    assert [d.id for d in registry.take_defined()] == ["outer"]


def test_a_failed_block_records_nothing() -> None:
    with pytest.raises(ZeroDivisionError), Dag("d"):
        _ = 1 / 0
    assert registry.take_defined() == []


def define_cycle() -> None:
    with Dag("d"):
        a = EmptyOperator("a")
        b = EmptyOperator("b")
        a >> b >> a


def test_cycles_are_rejected() -> None:
    with pytest.raises(ValueError, match="DAG `d` has a dependency cycle: a -> b -> a"):
        define_cycle()
    assert registry.take_defined() == []


def test_the_dag_is_usable_after_the_block_for_further_dags() -> None:
    with Dag("one"):
        EmptyOperator("x")
    with Dag("two"):
        EmptyOperator("x")
    assert [d.id for d in registry.take_defined()] == ["one", "two"]


def test_tasks_reference_only_their_own_dag() -> None:
    @task
    def produce() -> int:
        return 1

    @task
    def consume(n: int) -> None: ...

    with Dag("one"):
        out = produce()
    with pytest.raises(ValueError, match="task `produce` is not part of DAG `two`"), Dag("two"):
        consume(out)
