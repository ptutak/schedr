import warnings
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any

import pytest

from schedr import (
    CommandOperator,
    Cron,
    Dag,
    DagHint,
    EmptyOperator,
    Every,
    GiB,
    MiB,
    Once,
    StepDefaults,
    chain,
    dag_ctx,
    group,
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
        CommandOperator("load", ["load", t"--since={dag_ctx.run.since}"])
    with Dag("d", trigger=trigger, initial_since="2020-01-01"):
        CommandOperator("load", ["load", t"--since={dag_ctx.run.since}"])
    with Dag("e", trigger=trigger):
        CommandOperator("load", ["load", t"--until={dag_ctx.run.planned_for}"])
    with Dag("f", trigger=Every(timedelta(hours=1))):
        CommandOperator("load", ["load", t"--since={dag_ctx.run.since}"])
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
        ({"max_active_tasks": 0}, ValueError, "max_active_tasks must be positive"),
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
    defaults = StepDefaults(
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


def test_how_python_tasks_start_is_a_default_and_a_tasks_own_argument_wins() -> None:
    @task(preload=True)
    def keeps() -> int:
        return 1

    @task
    def follows() -> int:
        return 2

    @task(fresh_process=False)
    def forks() -> int:
        return 3

    with Dag("d", defaults=StepDefaults(preload=False, fresh_process=True)):
        keeps()
        follows()
        forks()
    settings = {t.id: (t.settings.preload, t.settings.fresh_process) for t in only_dag().tasks}
    assert settings == {"keeps": (True, True), "follows": (False, True), "forks": (False, False)}


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


def test_a_long_chain_is_checked_for_cycles_without_recursion() -> None:
    # B115: the check recursed once per task of a path, past Python's recursion limit.
    with Dag("long"):
        tasks = [EmptyOperator(f"t{i}") for i in range(1500)]
        chain(*tasks)
    registry.take_entries()

    def loop() -> None:
        with Dag("loop"):
            tasks = [EmptyOperator(f"t{i}") for i in range(1500)]
            chain(*tasks)
            tasks[-1] >> tasks[0]

    with pytest.raises(ValueError, match="t1499 -> t0"):
        loop()
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
        start = EmptyOperator("start")
    with (
        pytest.raises(ValueError, match="task `produce` belongs to DAG `one`, not `two`"),
        Dag("two"),
    ):
        consume(out)
    registry.take_entries()
    # B107: a task of the same ID in this DAG took the other DAG's handle silently.
    with Dag("three"):
        produce()
        EmptyOperator("start")
        for use in (
            lambda: consume(out),
            lambda: consume(out.on(None)),  # type: ignore[arg-type]
            lambda: start >> produce.override(task_id="p2")(),
            lambda: consume(t"{out}"),  # type: ignore[arg-type]
            lambda: consume(start.output),  # type: ignore[arg-type]
        ):
            with pytest.raises(ValueError, match="belongs to DAG `one`, not `three`"):
                use()
    registry.take_entries()


def test_groups_reference_only_their_own_dag() -> None:
    # B125: a group kept from one DAG stood for this DAG's tasks of the same IDs.
    with Dag("one"), group("g") as foreign:
        EmptyOperator("a")
    with Dag("two"):
        with group("g") as own:
            EmptyOperator("a")
        finish = EmptyOperator("finish")
        for use in (
            lambda: foreign >> finish,
            lambda: finish >> foreign,
            lambda: [foreign, own] >> finish,
            lambda: chain(foreign, finish),
        ):
            with pytest.raises(ValueError, match="group `g` belongs to DAG `one`, not `two`"):
                use()
    registry.take_entries()


def test_concurrency_limits_reach_the_dag_json_and_follow_the_defaults() -> None:
    @task(max_active=1)
    def locked() -> int:
        return 1

    @task
    def item(n: int) -> int:
        return n

    @task(max_active_per_run=2)
    def own(n: int) -> int:
        return n

    with Dag("d", max_active_tasks=5, defaults=StepDefaults(max_active=3, max_active_per_run=10)):
        locked()
        item.expand(n=[1, 2])
        own.expand(n=[1, 2])
    document = only_dag().to_json()
    assert document["max_active_tasks"] == 5
    limits = {
        t["id"]: (t["settings"].get("max_active"), t["settings"].get("max_active_per_run"))
        for t in document["tasks"]
    }
    assert limits == {"locked": (1, 10), "item": (3, 10), "own": (3, 2)}


def test_no_limits_leave_the_dag_json_as_it_was() -> None:
    @task
    def one() -> int:
        return 1

    with Dag("d"):
        one()
    document = only_dag().to_json()
    assert "max_active_tasks" not in document
    assert set(document["tasks"][0]["settings"]) & {"max_active", "max_active_per_run"} == set()


def test_a_tasks_own_memory_budget_and_hard_cap_reach_the_dag_json() -> None:
    @task(memory="2GiB", hard_memory_limit=True)
    def capped() -> int:
        return 1

    @task
    def plain() -> int:
        return 1

    with Dag("d"):
        capped()
        plain.override(memory=512 * MiB)()
        CommandOperator("dump", ["pg_dump"], memory="1024MiB")
    settings = {t["id"]: t["settings"] for t in only_dag().to_json()["tasks"]}
    assert (settings["capped"]["memory_bytes"], settings["capped"]["hard_memory_limit"]) == (
        2 * GiB,
        True,
    )
    assert settings["plain"]["memory_bytes"] == 512 * MiB
    assert "hard_memory_limit" not in settings["plain"]
    assert settings["dump"]["memory_bytes"] == GiB


@pytest.mark.parametrize(
    ("memory", "hard", "error", "says"),
    [
        ("2 GiB", None, ValueError, "isn't a size"),
        ("2G", None, ValueError, "isn't a size"),
        (1.5, None, TypeError, "takes bytes or a size"),
        (True, None, TypeError, "takes bytes or a size"),
        ("1MiB", None, ValueError, "at least 16 MiB"),
        (None, True, ValueError, "set memory= too"),
    ],
)
def test_a_memory_budget_that_isnt_one_is_an_error(
    memory: Any, hard: bool | None, error: type[Exception], says: str
) -> None:
    @task
    def one() -> int:
        return 1

    with Dag("d"), pytest.raises(error, match=says):
        one.override(memory=memory, hard_memory_limit=hard)()


@pytest.mark.parametrize("limit", ["max_active", "max_active_per_run"])
def test_a_task_limit_below_one_is_an_error(limit: str) -> None:
    @task
    def one(n: int) -> int:
        return n

    with Dag("d"), pytest.raises(ValueError, match=f"{limit} must be at least 1"):
        one.override(**{limit: 0}).expand(n=[1])


def test_max_active_per_run_on_an_unmapped_task_is_hinted_unless_a_default() -> None:
    # Decision 90A: it limits nothing there; from `StepDefaults` it is meant for the mapped
    # tasks, so no hint.
    @task(max_active_per_run=2)
    def single() -> int:
        return 1

    @task
    def plain() -> int:
        return 1

    @task(max_active_per_run=2)
    def mapped(n: int) -> int:
        return n

    hint = "task `single`: `max_active_per_run` limits nothing"
    with pytest.warns(DagHint, match=hint), Dag("d"):
        single()
    with warnings.catch_warnings():
        warnings.simplefilter("error", DagHint)
        with Dag("e", defaults=StepDefaults(max_active_per_run=2)):
            plain()
            mapped.expand(n=[1, 2])
    with pytest.warns(DagHint, match="task `cmd`"), Dag("f"):
        CommandOperator("cmd", "true", max_active_per_run=1)


def test_a_resource_s_cleanup_has_its_setup_s_memory_a_sensor_none() -> None:
    from schedr import resource, sensor  # noqa: PLC0415 (only this test)

    @resource(memory="64MiB", hard_memory_limit=True)
    def area() -> int:
        return 1

    @area.cleanup
    def drop(value: int) -> None:
        del value

    @task
    def use(a: int) -> int:
        return a

    with Dag("d"):
        use(area())
    settings = {t["id"]: t["settings"] for t in only_dag().to_json()["tasks"]}
    assert settings["area.cleanup"]["memory_bytes"] == 64 * MiB
    assert settings["area.cleanup"]["hard_memory_limit"] is True

    @sensor
    async def ready() -> bool:
        return True

    with Dag("e"), pytest.raises(TypeError, match="no memory budget of its own"):
        ready.override(memory="64MiB")()
