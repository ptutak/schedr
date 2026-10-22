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
from schedr._model import IdError, NoTrigger, TaskSettings, registry

Validate = Callable[[str, Any], None]


def only_dag() -> Any:
    dags = registry.take_defined()
    assert len(dags) == 1
    return dags[0]


def test_a_dag_is_recorded_with_its_tasks(validate: Validate) -> None:
    with Dag("sync", trigger=Cron("0 3 * * *"), description="Daily", tags=["sync"]) as dag:
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


def test_defaults_without_a_trigger() -> None:
    with Dag("manual"):
        pass
    model = only_dag()
    assert model.trigger == NoTrigger()
    assert model.initial_since is None
    assert model.max_active_runs == 1


@pytest.mark.parametrize(
    ("since", "expected"),
    [
        ("2026-01-01", datetime(2025, 12, 31, 23, tzinfo=UTC)),
        (date(2026, 1, 1), datetime(2025, 12, 31, 23, tzinfo=UTC)),
        ("2026-07-01T03:00", datetime(2026, 7, 1, 1, tzinfo=UTC)),
        (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
        ("2026-01-01T00:00+02:00", datetime(2025, 12, 31, 22, tzinfo=UTC)),
    ],
)
def test_a_naive_initial_since_is_in_the_triggers_time_zone(
    since: object, expected: datetime
) -> None:
    with Dag("d", trigger=Cron("0 3 * * *", tz="Europe/Warsaw"), initial_since=since):  # type: ignore[arg-type]
        pass
    assert only_dag().initial_since == expected


def test_a_naive_initial_since_is_utc_without_a_cron_trigger() -> None:
    with Dag("d", trigger=Every(timedelta(hours=1)), initial_since="2020-01-01"):
        pass
    assert only_dag().initial_since == datetime(2020, 1, 1, tzinfo=UTC)
    with Dag("e", initial_since="2020-01-01"):
        pass
    assert only_dag().initial_since == datetime(2020, 1, 1, tzinfo=UTC)


def test_a_naive_initial_since_follows_a_once_triggers_zone() -> None:
    with Dag("d", trigger=Once("2026-12-24T23:00", tz="Europe/Warsaw"), initial_since="2026-12-01"):
        pass
    model = only_dag()
    assert model.initial_since == datetime(2026, 11, 30, 23, tzinfo=UTC)
    assert model.to_json()["trigger"] == {"kind": "once", "at": "2026-12-24T22:00:00Z"}


@pytest.mark.parametrize(
    ("trigger", "what"), [(None, "no trigger"), (Once("2026-12-24"), "a Once trigger")]
)
def test_since_without_a_previous_fire_time_needs_initial_since(
    trigger: Once | None, what: str
) -> None:
    with (
        pytest.raises(ValueError, match=f"DAG `d` has {what}, so its first run has no previous"),
        Dag("d", trigger=trigger),
    ):
        CommandOperator("load", t"load --since {dag_ctx.run.since}")
    with Dag("d", trigger=trigger, initial_since="2020-01-01"):
        CommandOperator("load", t"load --since {dag_ctx.run.since}")
    with Dag("e", trigger=trigger):
        CommandOperator("load", t"load --until {dag_ctx.run.planned_for}")
    with Dag("f", trigger=Every(timedelta(hours=1))):
        CommandOperator("load", t"load --since {dag_ctx.run.since}")
    assert [d.id for d in registry.take_defined()] == ["d", "e", "f"]


def test_a_bad_initial_since_is_rejected() -> None:
    with pytest.raises(ValueError, match="initial_since: `soon` is not an ISO 8601"):
        Dag("d", initial_since="soon")
    with pytest.raises(TypeError, match="initial_since must be a datetime, a date or an ISO"):
        Dag("d", initial_since=5)  # type: ignore[arg-type]


def test_an_aware_initial_since_keeps_its_instant() -> None:
    since = datetime(2026, 1, 1, 5, tzinfo=timezone(timedelta(hours=5)))
    with Dag("d", initial_since=since):
        pass
    assert only_dag().to_json()["initial_since"] == "2026-01-01T00:00:00Z"


def test_start_and_end_are_set_on_the_trigger() -> None:
    with pytest.raises(TypeError):
        Dag("d", start="2026-01-01")  # type: ignore[call-arg]
    with Dag("d", trigger=Cron("@daily", start="2026-01-01", end="2026-12-31")):
        pass
    trigger = only_dag().to_json()["trigger"]
    assert (trigger["start"], trigger["end"]) == ("2026-01-01T00:00:00Z", "2026-12-31T00:00:00Z")


@pytest.mark.parametrize(
    ("kwargs", "error", "message"),
    [
        ({"trigger": "0 3 * * *"}, TypeError, "trigger must be Cron"),
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
    with Dag("d", max_active_runs=None):
        pass
    model = only_dag()
    assert model.max_active_runs is None


def test_catchup_is_set_on_the_trigger() -> None:
    with Dag("d", trigger=Cron("@daily", catchup=True)):
        pass
    assert only_dag().to_json()["trigger"]["catchup"] is True


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
