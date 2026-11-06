"""``@sensor`` and ``@sensor.poke`` in a DAG body and in a test (M4.11a1)."""

import asyncio
import time
from collections.abc import Callable, Coroutine
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


def test_a_plain_def_is_refused() -> None:
    def plain() -> int:
        return 1

    with pytest.raises(TypeError, match="must be `async def`"):
        sensor(plain)  # type: ignore[call-overload]
    with pytest.raises(TypeError, match="must be `async def`"):
        sensor.poke(every=timedelta(seconds=1))(plain)  # type: ignore[arg-type]


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
    with pytest.raises(PokeTooSlowError, match="longer than `every`"):
        _waited(blocks, max_wait_ms=60_000, every_ms=10)


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
