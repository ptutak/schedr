"""Event triggers: what a DAG file says, and what a trigger does when it runs."""

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta

import pytest

from schedr import Dag, Event, Every, dag_ctx, task, trigger
from schedr.runner.waiting import run_trigger
from schedr.triggers import Trigger


@trigger
async def yields(after: str | None, prefix: str = "p") -> AsyncIterator[Event[int]]:
    """Yields one event."""
    yield Event(f"{prefix}{after}", 1)


_SAME_NAME = yields


@trigger.poke(every=timedelta(milliseconds=1))
async def pokes(after: str | None) -> list[Event[int]]:
    """Returns one event per call."""
    return [Event(f"{after}x", 1)]


def _defined(name: str = "d") -> dict[str, object]:
    with Dag(name, trigger=yields(prefix="q")) as dag:

        @task
        def reads(key: str, data: int, at: datetime) -> str:
            return f"{key}{data}{at}"

        reads(dag_ctx.event.key, dag_ctx.event.data, dag_ctx.event.at)
    return dag.model.to_json()


def test_a_dag_with_a_trigger_is_valid_json_with_its_function_as_a_hidden_sensor(
    validate: Callable[[str, object], None],
) -> None:
    written = _defined()
    validate("Dag", written)
    trigger_json = written["trigger"]
    assert isinstance(trigger_json, dict)
    assert trigger_json["kind"] == "event"
    assert trigger_json["task"]["id"] == "yields"
    assert trigger_json["task"]["kind"] == "sensor"
    assert trigger_json["task"]["mode"] == "loop"
    assert [t["id"] for t in written["tasks"]] == ["reads"]


def test_a_poke_says_how_often() -> None:
    written = pokes().to_model().to_json()
    assert written["task"]["mode"] == "poke"
    assert written["task"]["every_ms"] == 1


def test_options_reach_the_json() -> None:
    start = datetime(2030, 1, 1, tzinfo=UTC)
    written = (
        yields()
        .override(queue="io", restart_if_idle=timedelta(seconds=2), start=start)
        .to_model()
        .to_json()
    )
    assert written["task"]["settings"]["queue"] == "io"
    assert written["restart_if_idle_ms"] == 2000
    assert written["start"].startswith("2030-01-01")


def test_a_function_has_to_be_async_and_take_after_first() -> None:
    def plain(after: str | None) -> list[Event[int]]:
        return [Event(str(after), 1)]

    with pytest.raises(TypeError, match="async generator"):
        trigger(plain)

    async def no_after(prefix: str) -> AsyncIterator[Event[int]]:
        yield Event(prefix, 1)

    with pytest.raises(TypeError, match="first parameter is `after"):
        trigger(no_after)


def test_arguments_are_checked_and_after_is_schedr_s() -> None:
    with pytest.raises(TypeError, match="schedr fills in `after`"):
        yields(after="a")
    with pytest.raises(TypeError, match="nope"):
        yields(nope=1)


def test_an_event_needs_a_key_and_an_aware_time() -> None:
    with pytest.raises(TypeError, match="non-empty string"):
        Event("")
    with pytest.raises(TypeError, match="time-zone aware"):
        Event("a", at=datetime(2030, 1, 1))


def _define(starts: Trigger, body: Callable[[], object]) -> None:
    with Dag("d", trigger=starts):
        body()


def test_a_key_too_long_for_storing_is_refused() -> None:
    assert Event("k" * 1024).key
    for key in ("k" * 1025, "a\0b", "ż" * 513):
        with pytest.raises(TypeError, match="at most 1024 bytes"):
            Event(key)


def test_a_task_named_as_the_function_is_refused() -> None:
    @task
    def yields() -> int:
        return 1

    with pytest.raises(ValueError, match="has the name of its trigger's function"):
        _define(_SAME_NAME(), yields)


def test_dag_ctx_event_needs_an_event_trigger() -> None:
    @task
    def reads(key: str) -> str:
        return key

    with pytest.raises(ValueError, match=r"dag_ctx\.event"):
        _define(Every(timedelta(minutes=1)), lambda: reads(dag_ctx.event.key))


def test_the_datas_type_is_checked_against_the_parameter() -> None:
    @task
    def reads(data: str) -> str:
        return data

    with pytest.raises(TypeError):
        _define(yields(), lambda: reads(dag_ctx.event.data))


def _no_poke(error: str | None) -> None:
    raise AssertionError(error)


def test_a_generator_tells_what_it_yields_in_order_and_starts_after_the_bookmark() -> None:
    told: list[str] = []

    async def tell(event: Event[object]) -> None:
        told.append(event.key)

    async def gives(after: str | None, n: int) -> AsyncIterator[Event[int]]:
        for i in range(n):
            yield Event(f"{after}-{i}", i)

    asyncio.run(run_trigger(gives, {"n": 3}, {}, "b", tell, _no_poke))
    assert told == ["b-0", "b-1", "b-2"]


class _Enough(BaseException):
    """Ends a poke's calls."""


def _poking(function: Callable[..., object], calls: int = 3) -> tuple[list[str], list[str | None]]:
    told: list[str] = []
    poked: list[str | None] = []

    async def tell(event: Event[object]) -> None:
        told.append(event.key)

    def said(error: str | None) -> None:
        poked.append(error)
        if len(poked) == calls:
            raise _Enough

    with pytest.raises(_Enough):
        asyncio.run(run_trigger(function, {}, {"every_ms": 1}, None, tell, said))
    return told, poked


def test_a_poke_is_called_again_from_the_last_key_told() -> None:
    seen: list[str | None] = []

    async def gives(after: str | None) -> list[Event[int]]:
        seen.append(after)
        return [Event(f"{len(seen)}", 0)]

    told, poked = _poking(gives)
    assert seen == [None, "1", "2"]
    assert told == ["1", "2", "3"]
    assert poked == [None, None, None]


def test_a_raising_poke_keeps_its_bookmark_says_why_and_is_called_again() -> None:
    seen: list[str | None] = []

    async def fails(after: str | None) -> list[Event[int]]:
        seen.append(after)
        if len(seen) == 2:
            raise ValueError("not now")
        return [Event(f"{len(seen)}", 0)]

    told, poked = _poking(fails)
    assert seen == [None, "1", "1"]
    assert told == ["1", "3"]
    assert poked == [None, "ValueError: not now", None]


def test_a_poke_past_its_time_tells_nothing_and_says_so() -> None:
    async def slow(after: str | None) -> list[Event[int]]:
        await asyncio.sleep(60)
        return [Event(str(after), 0)]

    async def blocks(after: str | None) -> list[Event[int]]:
        time.sleep(0.02)
        return [Event(str(after), 0)]

    for function in (slow, blocks):
        told, poked = _poking(function, calls=1)
        assert told == []
        assert "PokeTooSlowError" in str(poked[0])


def test_a_poke_with_nothing_to_say_tells_nothing() -> None:
    async def quiet(after: str | None) -> list[Event[int]]:
        return [] if after is None else [Event(after, 0)]

    assert _poking(quiet) == ([], [None, None, None])
