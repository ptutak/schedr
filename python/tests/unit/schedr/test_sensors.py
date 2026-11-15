"""``@sensor`` and ``@sensor.poke`` in a DAG body and in a test (M4.11a1)."""

import asyncio
import functools
import threading
import time
from collections.abc import Callable, Coroutine
from contextvars import ContextVar
from datetime import timedelta
from typing import Annotated, Any, NewType, cast

import pytest

from schedr import (
    NOT_YET,
    Dag,
    DefaultOutcome,
    EmptyOperator,
    NotYet,
    Output,
    StepDefaults,
    TimedOut,
    chain,
    group,
    sensor,
    task,
)
from schedr._model import registry
from schedr.runner import waiting
from schedr.runner.waiting import PokeTooSlowError, waited


def _defined() -> dict[str, Any]:
    (dag,) = registry.take_defined()
    return dag.to_json()


def _kind(dag: dict[str, Any], task_id: str) -> dict[str, Any]:
    return next(t for t in dag["tasks"] if t["id"] == task_id)


def test_each_form_is_recorded_as_a_sensor_with_what_it_waits_by(
    validate: Callable[[str, object], None],
) -> None:
    @sensor(max_wait=timedelta(hours=6), retries=2)
    async def loop(n: int) -> int:
        return n

    @sensor.poke(every=timedelta(minutes=1))
    async def poke() -> str | NotYet:
        return NOT_YET

    with Dag("d"):
        loop(1)
        poke()
    dag = _defined()
    validate("Dag", dag)
    written = _kind(dag, "loop")
    assert written["kind"] == "sensor"
    assert written["mode"] == "loop"
    assert written["max_wait_ms"] == 6 * 3600 * 1000
    assert "every_ms" not in written
    # `timeout=` is how long it waits: no process limit, which would be a failure.
    assert written["settings"].get("max_wait_ms") is None
    assert written["settings"]["retries"] == 2
    poked = _kind(dag, "poke")
    assert (poked["mode"], poked["every_ms"]) == ("poke", 60_000)
    assert "max_wait_ms" not in poked
    assert [o["name"] for o in poked["outcomes"]] == ["SUCCESS", "TimedOut"]


def test_a_plain_order_edge_is_on_its_value_and_a_reference_says_what_it_expects() -> None:
    @sensor
    async def wait() -> int:
        return 1

    @task
    def use(value: int) -> None:
        del value

    @task
    def expects(value: int | TimedOut) -> None:
        del value

    @task
    def alert() -> None:
        pass

    @task
    def after() -> None:
        pass

    @task
    def always() -> None:
        pass

    with Dag("d"):
        waited = wait()
        use(waited.on(int))
        expects(waited)  # type: ignore[arg-type]
        waited >> after()
        waited.on(TimedOut) >> alert()
        chain(waited.on(int, TimedOut), always())
    dag = _defined()
    edges = {e["to"]: e.get("on") for e in dag["edges"]}
    assert edges == {
        "use": ["SUCCESS"],
        "expects": None,
        "after": ["SUCCESS"],
        "alert": ["TimedOut"],
        "always": ["SUCCESS", "TimedOut"],
    }


def test_a_task_taking_only_the_value_is_refused_without_on() -> None:
    @sensor
    async def wait() -> int:
        return 1

    @task
    def use(value: int) -> None:
        del value

    with pytest.raises(TypeError, match="TimedOut"), Dag("d"):
        use(wait())  # type: ignore[arg-type]


def test_on_names_only_its_value_or_timed_out_and_outcome_is_refused() -> None:
    @sensor
    async def wait() -> int:
        return 1

    with Dag("d"), pytest.raises(TypeError, match="ends with its value, `int`, or `TimedOut`"):
        wait().on(str)
    registry.take_defined()
    with Dag("e"), pytest.raises(TypeError, match=r"routes with `.on\(TimedOut\)`"):
        _ = cast("Output[int]", wait()).outcome
    registry.take_defined()


def test_on_success_works_and_an_enum_or_timed_out_in_the_type_is_refused() -> None:
    @sensor
    async def wait() -> int:
        return 1

    @task
    def after() -> None:
        pass

    with Dag("d"):
        chain(wait().on(DefaultOutcome.SUCCESS), after())
    dag = _defined()
    assert next(e for e in dag["edges"])["on"] == ["SUCCESS"]

    @sensor
    async def both() -> TimedOut | None:
        return None

    with pytest.raises(TypeError, match="schedr's to return"), Dag("e"):
        both()


def test_a_plain_def_is_refused_but_for_a_poke() -> None:
    def plain() -> int:
        return 1

    with pytest.raises(TypeError, match="must be `async def`"):
        sensor(plain)  # type: ignore[call-overload]
    # Decision 489: a poke may be a plain `def`, called on the host's threads.
    poke = sensor.poke(every=timedelta(seconds=1))(plain)  # type: ignore[arg-type]
    assert asyncio.run(poke.fn()) == 1

    def lines() -> object:
        yield 1

    with pytest.raises(TypeError, match="yields"):
        sensor.poke(every=timedelta(seconds=1))(lines)  # type: ignore[arg-type]


def test_an_async_generator_is_refused() -> None:
    async def events() -> object:
        yield 1

    with pytest.raises(TypeError, match="trigger"):
        sensor(events)  # type: ignore[arg-type]


def test_a_poke_must_say_not_yet_in_its_type() -> None:
    @sensor.poke(every=timedelta(seconds=1))  # type: ignore[arg-type]
    async def poke() -> int:
        return 1

    with pytest.raises(TypeError, match="NotYet"), Dag("d"):
        poke()


def test_every_and_timeout_must_be_above_zero() -> None:
    async def fn() -> int | NotYet:
        return 1

    with pytest.raises(TypeError, match="`every` must be more than zero"):
        sensor.poke(every=timedelta(0))(fn)
    with pytest.raises(TypeError, match="`max_wait` must be more than zero"):
        sensor(max_wait=timedelta(seconds=-1))(fn)


def test_cleanup_is_refused_saying_to_use_finally() -> None:
    def undo() -> None:
        pass

    async def fn() -> int:
        return 1

    with pytest.raises(TypeError, match="use `finally`"):
        sensor(cleanup=undo)(fn)  # type: ignore[call-overload]
    with pytest.raises(TypeError, match="use `finally`"):
        sensor(fn).override(cleanup=undo)
    with pytest.raises(TypeError, match="isn't an option"):
        sensor(preload=True)(fn)  # type: ignore[call-overload]


def test_override_keeps_it_a_sensor_and_its_timeout_is_how_long_it_waits() -> None:
    @sensor.poke(every=timedelta(seconds=5), max_wait=timedelta(hours=1))
    async def poke() -> int | NotYet:
        return 1

    with Dag("d"):
        poke.override(task_id="short", max_wait=timedelta(seconds=9), retries=1)()
        poke.override(task_id="same")()
    dag = _defined()
    short = _kind(dag, "short")
    assert (short["kind"], short["every_ms"], short["max_wait_ms"]) == ("sensor", 5000, 9000)
    assert short["settings"].get("max_wait_ms") is None
    assert _kind(dag, "same")["max_wait_ms"] == 3_600_000


def test_outside_a_dag_fn_is_the_function_itself() -> None:
    @sensor.poke(every=timedelta(seconds=1))
    async def poke(value: int) -> int | NotYet:
        return value if value else NOT_YET

    assert asyncio.run(poke.fn(3)) == 3
    assert asyncio.run(poke.fn(0)) is NOT_YET


def test_a_wait_step_cant_be_mapped_for_now() -> None:
    @sensor
    async def wait(name: str) -> int:
        return len(name)

    with pytest.raises(TypeError, match="can't be mapped"), Dag("d"):
        wait.expand(name=["a", "b"])


def test_a_value_that_can_be_missing_must_allow_none_with_another_upstream() -> None:
    @sensor
    async def wait() -> int:
        return 1

    @task
    def other() -> None:
        pass

    @task
    def use(value: int) -> None:
        del value

    with pytest.raises(TypeError, match="None"), Dag("d"):
        other() >> use(wait().on(int))


def test_a_dependency_on_a_group_ending_with_a_wait_step_is_on_its_value() -> None:
    @sensor
    async def wait() -> int:
        return 1

    @task
    def publish() -> None:
        pass

    with Dag("d"):
        with group("g") as waiting:
            wait()
        waiting >> publish()
    dag = _defined()
    assert [e["on"] for e in dag["edges"]] == [["SUCCESS"]]


def test_step_defaults_give_a_wait_step_max_wait_and_a_task_timeout() -> None:
    @sensor
    async def wait() -> int:
        return 1

    @task
    def typed() -> None:
        pass

    second = timedelta(seconds=1)
    with Dag("d", defaults=StepDefaults(timeout=second, max_wait=timedelta(hours=1))):
        wait()
        with group("g", defaults=StepDefaults(max_wait=timedelta(minutes=1))):
            wait.override(task_id="grouped")()
        wait.override(task_id="own", max_wait=timedelta(seconds=9))()
        typed()
    dag = _defined()
    written = _kind(dag, "wait")
    assert written["max_wait_ms"] == 3_600_000
    assert written["settings"].get("timeout_ms") is None
    assert _kind(dag, "g.grouped")["max_wait_ms"] == 60_000
    assert _kind(dag, "own")["max_wait_ms"] == 9000
    # A task has `timeout`, and no `max_wait`.
    assert "max_wait_ms" not in _kind(dag, "typed")
    assert _kind(dag, "typed")["settings"]["timeout_ms"] == 1000


type Maybe = str | NotYet


def test_a_poke_may_name_its_type_with_an_alias() -> None:
    @sensor.poke(every=timedelta(seconds=1))
    async def aliased() -> Maybe:
        return NOT_YET

    with Dag("d"):
        aliased()
    assert _kind(_defined(), "aliased")["kind"] == "sensor"


async def _forever() -> None:
    await asyncio.Event().wait()


def _waited(function: Callable[[], Coroutine[object, object, object]], **task: int) -> object:
    return asyncio.run(waited(function, {}, task))


def test_the_limit_decides_however_the_function_takes_its_cancel() -> None:
    async def fallback() -> str:
        try:
            await _forever()
        except asyncio.CancelledError:
            return "too late"
        return "never"

    assert _waited(fallback, max_wait_ms=20) == TimedOut()


def test_a_function_raising_cancelled_itself_is_no_timeout() -> None:
    async def own() -> str:
        await asyncio.sleep(0)
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        _waited(own, max_wait_ms=60_000)


def test_no_poke_starts_after_the_deadline() -> None:
    calls: list[int] = []

    async def poke() -> object:
        calls.append(1)
        # Its wait for the next call ends past the deadline, as when the machine stalls.
        asyncio.get_running_loop().call_soon(time.sleep, 0.05)
        return NOT_YET

    assert _waited(poke, max_wait_ms=20, every_ms=1) == TimedOut()
    assert calls == [1]


def test_a_call_that_blocks_past_its_limit_is_late_whatever_it_returns() -> None:
    # Blocking the loop, its limit can't fire: the time it took decides (Grok's review).
    async def blocks() -> str:
        time.sleep(0.05)
        return "late"

    assert _waited(blocks, max_wait_ms=10) == TimedOut()
    assert _waited(blocks, max_wait_ms=10, every_ms=1000) == TimedOut()
    with pytest.raises(PokeTooSlowError, match="longer than 3 times `every`"):
        _waited(blocks, max_wait_ms=60_000, every_ms=10)


def _overrunning(
    takes: list[float], then: object = NOT_YET
) -> Callable[[], Coroutine[object, object, object]]:
    """A poke whose calls take ``takes`` seconds in turn, returning ``then`` after the last."""
    left = list(takes)

    async def poke() -> object:
        await asyncio.sleep(left.pop(0))
        return then if not left else NOT_YET

    return poke


def test_a_poke_fails_after_poke_overruns_calls_in_a_row_past_every() -> None:
    # every 100 ms: 150 ms calls run past it, well under the hard limit of 300 ms.
    with pytest.raises(PokeTooSlowError, match="3 calls in a row took longer than `every`"):
        _waited(_overrunning([0.15] * 5, "never"), max_wait_ms=60_000, every_ms=100)


def test_a_call_within_every_starts_the_count_again_and_a_late_value_counts() -> None:
    takes = [0.15, 0.15, 0.0, 0.15, 0.15]
    assert _waited(_overrunning(takes, "ready"), max_wait_ms=60_000, every_ms=100) == "ready"


def test_a_stall_after_a_call_by_something_else_on_the_loop_isnt_an_overrun() -> None:
    # Another poke on the host blocks the loop after each call ends (Grok's review of D458).
    left = [1, 1, 1, 1, 0]

    async def poke() -> object:
        asyncio.get_running_loop().call_soon(time.sleep, 0.05)
        return "ready" if not left.pop(0) else NOT_YET

    assert _waited(poke, max_wait_ms=60_000, every_ms=20) == "ready"


def test_poke_overruns_1_fails_at_the_first_overrun(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(waiting, "_poke_overruns", 1)
    with pytest.raises(PokeTooSlowError, match="1 calls in a row"):
        _waited(_overrunning([0.15, 0.0], "ready"), max_wait_ms=60_000, every_ms=100)
    waiting.set_poke_overruns(0)
    assert waiting._poke_overruns == 1  # what the host's setting became


def test_a_stop_during_the_cancel_at_the_limit_lets_its_finally_finish() -> None:
    finished: list[str] = []

    async def closing() -> None:
        try:
            await _forever()
        finally:
            await asyncio.sleep(0.05)
            finished.append("closed")

    async def stopped() -> None:
        waiting = asyncio.ensure_future(waited(closing, {}, {"max_wait_ms": 5}))
        await asyncio.sleep(0.02)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

    asyncio.run(stopped())
    assert finished == ["closed"]


def test_timeout_is_refused_on_a_wait_step_and_max_wait_on_a_task() -> None:
    async def fn() -> int:
        return 1

    @task
    def plain() -> None:
        pass

    with pytest.raises(TypeError, match="has `max_wait`"):
        sensor(timeout=timedelta(seconds=1))(fn)  # type: ignore[call-overload]
    with pytest.raises(TypeError, match="has `max_wait`"):
        sensor(fn).override(timeout=timedelta(seconds=1))
    with pytest.raises(TypeError, match="only a wait step has `max_wait`"):
        plain.override(max_wait=timedelta(seconds=1))


type Alias = str
Noted = Annotated[str, "a path"]
Kind = NewType("Kind", str)


@pytest.mark.parametrize("annotation", [Alias, Noted, Kind, str])
def test_on_names_a_value_by_what_its_type_stands_for(annotation: object) -> None:
    async def wait() -> None:
        pass

    wait.__annotations__ = {"return": annotation}
    with Dag("d"):
        chain(sensor(wait)().on(str), EmptyOperator("after"))
    assert _defined()["edges"][0]["on"] == ["SUCCESS"]


def test_a_plain_poke_runs_on_a_thread_with_the_steps_context_until_it_has_a_value() -> None:
    # Decision 489.
    marker: ContextVar[str] = ContextVar("marker", default="none")
    seen: list[tuple[bool, str]] = []

    def poke() -> object:
        seen.append((threading.current_thread() is threading.main_thread(), marker.get()))
        return "ready" if len(seen) == 3 else NOT_YET

    async def step() -> object:
        marker.set("the step's")
        # 50 ms a call: a thread's start under load (Codex's review: 1 ms flaked).
        return await waited(poke, {}, {"every_ms": 50, "max_wait_ms": 60_000})

    assert asyncio.run(step()) == "ready"
    assert seen == [(False, "the step's")] * 3


def test_a_plain_poke_past_its_limit_is_stopped_and_fails_as_an_async_one() -> None:
    # A blocking call past three times `every` is stopped (here: an ordinary pool's thread is
    # left to end; on a host its call is stopped, decision 337) and the poke fails.
    def slow() -> object:
        time.sleep(0.2)
        return "late"

    with pytest.raises(PokeTooSlowError, match="longer than 3 times `every`"):
        _waited(slow, max_wait_ms=60_000, every_ms=10)  # type: ignore[arg-type]
    # Past `max_wait`: `TimedOut`, as for an async poke.
    assert _waited(slow, max_wait_ms=30, every_ms=1000) == TimedOut()  # type: ignore[arg-type]


def test_a_plain_poke_returning_a_coroutine_is_awaited() -> None:
    async def later() -> str:
        await asyncio.sleep(0)
        return "ready"

    def wraps() -> object:
        return later()

    assert _waited(wraps, max_wait_ms=60_000, every_ms=10) == "ready"  # type: ignore[arg-type]
    poke = sensor.poke(every=timedelta(seconds=1))(wraps)  # type: ignore[arg-type]
    assert asyncio.run(poke.fn()) == "ready"


def test_a_plain_wrapper_of_an_async_def_runs_on_a_thread_too() -> None:
    # Codex's review of M5.1o: `functools.wraps` made it look async, and it ran on the loop.
    threads: list[bool] = []

    async def inner() -> str:
        return "ready"

    @functools.wraps(inner)
    def wrapper() -> Coroutine[object, object, str | NotYet]:
        threads.append(threading.current_thread() is threading.main_thread())
        return inner()

    assert _waited(wrapper, max_wait_ms=60_000, every_ms=50) == "ready"
    assert threads == [False]


def test_a_plain_poke_annotated_as_returning_a_coroutine_is_a_wait_step() -> None:
    # Codex's review of M5.1o: its coroutine's value is its output, `NOT_YET` in it.
    async def later() -> str:
        return "ready"

    @sensor.poke(every=timedelta(seconds=1))
    def wraps_it() -> Coroutine[object, object, str | NotYet]:
        return later()

    @sensor.poke(every=timedelta(seconds=1))
    async def plainly_async() -> str | NotYet:
        return "ready"

    with Dag("d"):
        wraps_it()
        plainly_async()
    dag = _defined()
    assert _kind(dag, "wraps_it")["output_type"] == _kind(dag, "plainly_async")["output_type"]
    assert asyncio.run(wraps_it.fn()) == "ready"
