"""`@trigger.poke(batch=...)`: a poke's items gathered into one event per window (decision 352)."""

import asyncio
from datetime import timedelta
from typing import Any

import pytest

from schedr import Dag, Event, dag_ctx, task, trigger
from schedr._model import registry
from schedr.runner.waiting import Batch, run_trigger
from schedr.testing import run_trigger as run_trigger_for_test


@trigger.poke(every=timedelta(milliseconds=10), batch=timedelta(milliseconds=50))
async def files(after: str | None) -> list[Event[str]]:
    """One file a call, named by the call."""
    number = 0 if after is None else int(after) + 1
    return [Event(str(number), f"file-{number}")]


@task
def load(paths: list[Event[str]]) -> int:
    """Counts the files."""
    return len(paths)


def test_a_batch_says_its_window_and_the_run_reads_the_list_of_items() -> None:
    with Dag("loads", trigger=files()):
        load(dag_ctx.event.data)
    (dag,) = registry.take_defined()
    shown = dag.to_json()
    assert shown["trigger"]["task"]["batch_ms"] == 50
    assert shown["trigger"]["task"]["output_type"]["type"] == "list"
    assert shown["trigger"]["task"]["output_type"]["item"]["name"] == "Event"


class _EnoughError(Exception):
    pass


def _batches(
    function: Any,
    every: float,
    window: float,
    limit: int = 1_000_000,
    calls: int = 3,
    when: list[float] | None = None,
) -> list[Event[Any]]:
    told: list[Event[Any]] = []

    async def tell(event: Event[Any]) -> None:
        told.append(event)
        if when is not None:
            when.append(asyncio.get_running_loop().time())
        if len(told) >= calls:
            raise _EnoughError

    task_json = {"every_ms": round(every * 1000)}
    with pytest.raises(_EnoughError):
        asyncio.run(
            run_trigger(
                function,
                {},
                task_json,
                None,
                tell,
                lambda _e: None,
                Batch(window, lambda _item: 10, limit),
            )
        )
    return told


def test_the_items_of_a_window_go_as_one_event_keyed_by_the_last() -> None:
    # A window of about 60 pokes: a pause of the host's can't leave it under three.
    told = _batches(files.fn, every=0.005, window=0.3, calls=2)
    first, second = told
    assert [item.key for item in first.data] == [str(n) for n in range(len(first.data))]
    assert len(first.data) >= 3, "several pokes in one window"
    assert first.key == first.data[-1].key, "after keeps its meaning"
    assert second.data[0].key == str(int(first.key) + 1), "the next window goes on from it"


def test_a_window_near_its_size_goes_early() -> None:
    told = _batches(files.fn, every=0.001, window=60.0, limit=2 + 10 + 1 + 10, calls=2)
    assert [[item.key for item in event.data] for event in told] == [["0", "1"], ["2", "3"]]


def test_a_window_ending_during_a_poke_goes_then_and_its_items_open_the_next() -> None:
    # Codex's review: it waited for the poke, which put a later item in an ended window.
    async def slow(after: str | None) -> list[Event[str]]:
        await asyncio.sleep(0.3)
        number = 0 if after is None else int(after) + 1
        return [Event(str(number), "x")]

    when: list[float] = []
    told = _batches(slow, every=0.4, window=0.2, calls=2, when=when)
    assert [[item.key for item in event.data] for event in told] == [["0"], ["1"]]
    # Its window ended at about 0.5 s, during the second poke (0.4 to 0.7 s); the next at 0.9 s.
    assert when[1] - when[0] >= 0.3, "each sent at its own window's end"


def test_pokes_with_nothing_send_nothing() -> None:
    calls: list[str | None] = []

    async def quiet(after: str | None) -> list[Event[str]]:
        calls.append(after)
        return [Event("x", "found")] if len(calls) == 5 else []

    (told,) = _batches(quiet, every=0.001, window=0.01, calls=1)
    assert [item.data for item in told.data] == ["found"]


def test_run_trigger_gives_each_poke_s_items_as_one_event() -> None:
    @trigger.poke(every=timedelta(milliseconds=1), batch=timedelta(minutes=5))
    async def two(after: str | None) -> list[Event[int]]:
        return [Event("a", 1), Event("b", 2)]

    (found,) = run_trigger_for_test(two)
    assert found.key == "b"
    assert found.data == [Event("a", 1), Event("b", 2)]


@pytest.mark.parametrize(
    ("make", "match"),
    [
        (lambda: trigger.poke(every=timedelta(seconds=1), batch=timedelta(0)), "above zero"),
        (lambda: trigger.poke(every=timedelta(seconds=1), batch=5), "timedelta"),  # type: ignore[call-overload]
    ],
)
def test_a_batch_that_cant_be_one_is_refused(make: Any, match: str) -> None:
    async def poke(after: str | None) -> list[Event[int]]:
        return []

    with pytest.raises(TypeError, match=match):
        make()(poke)


def test_an_item_is_sized_with_room_for_masking_its_numbers() -> None:
    # Grok's review: a secret `12345` masked as `"****"` grows by a byte; a full window went over.
    from schedr.runner.task import _size  # noqa: PLC0415 (the sizer itself)

    plain = Event("a", "12345")
    numbers = Event("a", {"n": 12345, "m": [1.5, True]})
    assert _size(plain, Event[str], 100) == len('{"key":"a","data":"12345","at":null}')
    assert (
        _size(numbers, Event[Any], 100)
        == len('{"key":"a","data":{"n":12345,"m":[1.5,true]},"at":null}') + 2
    )


def test_an_item_without_a_key_fails_the_trigger_as_alone() -> None:
    async def keyless(after: str | None) -> list[Event[str]]:
        return [Event("ok", "kept"), Event(data="no key")]

    with pytest.raises(TypeError, match="non-empty"):
        _batches(keyless, every=0.001, window=60.0, calls=1)


def test_an_event_the_agent_refuses_fails_as_data_that_cant_be_stored() -> None:
    # Grok's review of M4.11f: a key holding a secret is refused by the agent.
    from schedr._serialization import CodecError  # noqa: PLC0415
    from schedr.runner.task import _tell_event  # noqa: PLC0415 (the teller itself)

    class Refusing:
        def ask(self, message: dict[str, Any]) -> dict[str, Any]:
            return {"type": "event_refused", "message": "an event's key holds a secret"}

    with pytest.raises(CodecError, match="holds a secret"):
        asyncio.run(_tell_event(Refusing(), Event("k", 1), int, 1000))  # type: ignore[arg-type]
