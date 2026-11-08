"""`RunOf` and `wait_for`: what a DAG file says."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from schedr import (
    Dag,
    Event,
    Failed,
    RunOf,
    RunResult,
    TimedOut,
    dag_ctx,
    sensor,
    task,
    trigger,
    wait_for,
)
from schedr._model import registry
from schedr._serialization import decode
from schedr.sensors import NOT_YET, NotYet
from schedr.testing import run_trigger as run_trigger_for_test


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


@sensor(wait_for=RunOf("orders", task="load", lag=timedelta(0)))
async def enough(run: RunResult[int], least: int) -> int | NotYet:
    """Takes the run's rows once there are enough, else waits for a redo."""
    if run.output is None or run.output < least:
        return NOT_YET
    return run.output


@task
def report(rows: int) -> str:
    """Reports the rows."""
    return str(rows)


def test_a_sensor_waiting_for_a_run_is_given_it_by_the_master() -> None:
    with Dag("reports"):
        report(enough(least=10).on(int))
    (dag,) = registry.take_defined()
    wait = next(t for t in dag.to_json()["tasks"] if t["id"] == "enough")
    assert wait["kind"] == "sensor"
    assert wait["arguments"] == {"least": {"literal": 10}}, "the run isn't an argument"
    assert wait["wait_for"] == {
        "run_of": {"dag": "orders", "task": "load", "lag_ms": 0},
        "param": "run",
    }
    assert wait["output_type"]["variants"][0] == {"type": "int"}, "NOT_YET isn't its output"


def test_a_sensor_waiting_for_a_run_takes_it_first_and_no_poke_takes_one() -> None:
    async def untyped(run: str) -> int:
        """Its first parameter isn't the run."""
        del run
        return 1

    with pytest.raises(TypeError, match=r"annotate it `RunResult\[\.\.\.\]`"):
        sensor(wait_for=RunOf("orders"))(untyped)  # type: ignore[arg-type]

    async def nothing() -> int:
        """Has no parameter for the run."""
        return 1

    with pytest.raises(TypeError, match="its first parameter is the run"):
        sensor(wait_for=RunOf("orders"))(nothing)  # type: ignore[arg-type]

    async def poked(run: RunResult[int]) -> int | NotYet:
        """A poke can't wait for a run."""
        del run
        return NOT_YET

    with pytest.raises(TypeError, match="a poke is called on a timer"):
        sensor.poke(every=timedelta(seconds=5), wait_for=RunOf("orders"))(poked)  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="takes a RunOf"):
        sensor(wait_for="orders")(poked)  # type: ignore[call-overload]


def test_the_run_handed_over_reads_as_its_result_with_the_tasks_output() -> None:
    stored = {
        "run_id": "r1",
        "dag_id": "orders",
        "planned_for": {"$datetime": "2026-11-01T00:00:00Z"},
        "state": "running",
        "ended_at": None,
        "task_id": "load",
        "task_state": "success",
        "outcome": "SUCCESS",
        "output": 12,
    }
    run = decode(stored, RunResult[int])
    assert (run.run_id, run.output, run.planned_for) == (
        "r1",
        12,
        datetime(2026, 11, 1, tzinfo=UTC),
    )
    whole = decode({**stored, "output": None, "task_id": None}, RunResult)
    assert whole.output is None


@trigger(wait_for=RunOf("orders", task="load", lag=timedelta(hours=1)), run_retries=2)
async def on_orders(run: RunResult[int], least: int) -> Event[int] | None:
    """Starts a run for a load with enough orders."""
    if run.output is None or run.output < least:
        return None
    return Event(data=run.output)


@task
def tally(orders: int) -> str:
    """Counts the orders."""
    return str(orders)


def _ended_run(output: int | None) -> RunResult[int]:
    return RunResult(
        run_id="r1",
        dag_id="orders",
        planned_for=datetime(2026, 11, 1, tzinfo=UTC),
        state="success",
        ended_at=datetime(2026, 11, 1, 0, 5, tzinfo=UTC),
        task_id="load",
        task_state="success",
        outcome="SUCCESS",
        output=output,
    )


def test_a_trigger_waiting_for_a_run_is_called_with_it_once_per_run() -> None:
    with Dag("tally", trigger=on_orders(least=5)):
        tally(dag_ctx.event.data)
    (dag,) = registry.take_defined()
    shown = dag.to_json()["trigger"]
    assert shown["run_retries"] == 2
    assert shown["task"]["wait_for"]["timeout_ms"] == 600_000, "10 minutes unless it says"
    assert shown["task"]["mode"] == "loop"
    assert shown["task"]["arguments"] == {"least": {"literal": 5}}, "the run isn't an argument"
    assert shown["task"]["wait_for"] == {
        "run_of": {"dag": "orders", "task": "load", "lag_ms": 3_600_000},
        "param": "run",
        "timeout_ms": 600_000,
    }


def test_run_trigger_calls_a_waiting_trigger_for_the_run_and_fills_its_event() -> None:
    (event,) = run_trigger_for_test(on_orders(least=5), run=_ended_run(7))
    assert (event.key, event.data) == ("r1", 7), "keyed by the run's ID unless it says"
    assert event.at == datetime(2026, 11, 1, 1, tzinfo=UTC), "planned_for plus lag"
    assert run_trigger_for_test(on_orders(least=10), run=_ended_run(7)) == []
    with pytest.raises(ValueError, match="give it as run="):
        run_trigger_for_test(on_orders(least=5))


def test_a_waiting_trigger_is_async_returns_an_event_and_takes_the_run_first() -> None:
    def plain(run: RunResult[int]) -> Event[int] | None:
        """Not `async def`."""
        del run
        return None

    with pytest.raises(TypeError, match="it is `async def`"):
        trigger(wait_for=RunOf("orders"))(plain)  # type: ignore[arg-type]

    async def unannotated(run: RunResult[int]) -> int:
        """Doesn't return an event."""
        del run
        return 1

    with pytest.raises(TypeError, match=r"annotate it `-> Event\[T\] \| None`"):
        trigger(wait_for=RunOf("orders"))(unannotated)  # type: ignore[arg-type]

    async def fine(run: RunResult[int]) -> Event[int] | None:
        """Fine but for its options."""
        del run
        return None

    with pytest.raises(TypeError, match="at least 0"):
        trigger(wait_for=RunOf("orders"), run_retries=-1)(fine)
    with pytest.raises(TypeError, match="can't be negative"):
        trigger(wait_for=RunOf("orders", lag=timedelta(hours=-1)))(fine)


def test_run_trigger_gives_the_run_by_name_as_the_runner_does() -> None:
    @trigger(wait_for=RunOf("orders"))
    async def keyword_only(*, run: RunResult[int]) -> Event[int] | None:
        """Takes the run by keyword only."""
        return Event(data=run.output or 0)

    (event,) = run_trigger_for_test(keyword_only, run=_ended_run(3))
    assert (event.key, event.data) == ("r1", 3)


def test_a_waiting_trigger_s_call_has_a_timeout_cancelling_a_call_that_hangs() -> None:
    from schedr.context import RunValues, TaskValues  # noqa: PLC0415
    from schedr.runner.task import (  # noqa: PLC0415 (private helpers)
        _Begun,  # pyright: ignore[reportPrivateUsage]
        _called_for_run,  # pyright: ignore[reportPrivateUsage]
    )

    @trigger(wait_for=RunOf("orders"), timeout=timedelta(milliseconds=50))
    async def hangs(run: RunResult[int]) -> Event[int] | None:
        """Awaits for ever: the loop turns, the call never ends."""
        del run
        await asyncio.Event().wait()
        return None

    with Dag("hanging", trigger=hangs()):
        tally(dag_ctx.event.data)
    (dag,) = registry.take_defined()
    shown = dag.to_json()["trigger"]["task"]
    assert shown["wait_for"]["timeout_ms"] == 50
    begun = _Begun(
        function=hangs.fn,
        arguments={"run": _ended_run(1)},
        hints={},
        values=RunValues(
            "hanging", "r", "event", _ended_run(1).planned_for, _ended_run(1).planned_for
        ),
        own=TaskValues("hangs", 1, None),
        downstream=frozenset(),
    )
    ended = asyncio.run(_called_for_run({"task": shown}, begun, _NeverTold(), 1024))
    assert ended["type"] == "task_failed"
    assert ended["kind"] == "trigger_timed_out", ended

    async def fine(run: RunResult[int]) -> Event[int] | None:
        """Fine but for its options."""
        del run
        return None

    with pytest.raises(TypeError, match="above zero"):
        trigger(wait_for=RunOf("orders"), timeout=timedelta(0))(fine)
    with pytest.raises(TypeError, match="`timeout` is for a trigger with `wait_for=`"):
        trigger(timeout=timedelta(seconds=1))(fine)  # type: ignore[call-overload]


class _NeverTold:
    """A channel a call that times out tells nothing on."""

    def ask(self, message: dict[str, object]) -> dict[str, object]:
        raise AssertionError(f"told {message}")

    def send(self, message: dict[str, object]) -> None:
        raise AssertionError(f"sent {message}")


def test_a_plain_sensor_has_no_not_yet() -> None:
    @sensor
    async def plain(n: int) -> int | NotYet:
        """Waits by awaiting: NOT_YET means nothing here."""
        del n
        return NOT_YET

    with (
        pytest.raises(TypeError, match="a @sensor waits by awaiting"),
        Dag("plain_not_yet"),
    ):
        build(plain(n=1).on(int))  # type: ignore[arg-type]
    registry.take_defined()


def test_a_sensor_waiting_for_a_run_may_limit_each_call() -> None:
    from schedr.runner.waiting import CallTimedOutError, waited  # noqa: PLC0415

    @sensor(wait_for=RunOf("orders"), timeout=timedelta(milliseconds=50))
    async def looks(run: RunResult[int]) -> int:
        """Hangs on an await: the loop turns, the call never ends."""
        del run
        await asyncio.Event().wait()
        return 1

    with Dag("looking"):
        report(looks().on(int))
    (dag,) = registry.take_defined()
    shown = next(t for t in dag.to_json()["tasks"] if t["id"] == "looks")
    assert shown["wait_for"]["timeout_ms"] == 50
    arguments = {"run": _ended_run(1)}
    # Its own limit first: a failure its retries cover, not the end of the wait.
    with pytest.raises(CallTimedOutError, match="its `timeout`"):
        asyncio.run(waited(looks.fn, arguments, {**shown, "max_wait_ms": 60_000}))
    # What is left of `max_wait` first: the wait ends, `TimedOut`.
    ended = asyncio.run(waited(looks.fn, arguments, {**shown, "max_wait_ms": 10}))
    assert isinstance(ended, TimedOut)

    async def plain(n: int) -> int:
        """A plain wait step: its call is its wait."""
        return n

    with pytest.raises(TypeError, match="a wait step has `max_wait`"):
        sensor(timeout=timedelta(seconds=1))(plain)  # type: ignore[call-overload]
    with pytest.raises(TypeError, match="above zero"):
        sensor(wait_for=RunOf("orders"), timeout=timedelta(0))(looks.fn)
