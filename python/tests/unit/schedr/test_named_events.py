"""`EventNamed`: what a DAG file says (decision 354)."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from schedr import (
    Dag,
    Event,
    EventNamed,
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
from schedr.sensors import NotYet
from schedr.testing import run_trigger
from schedr.types import Json


@task
def ship(event: Event[Json]) -> str:
    """Reads the event."""
    return event.key


@task
def echo(key: str) -> str:
    """Reads a key."""
    return key


@task
def order_of() -> str:
    """An upstream key."""
    return "o-1"


def _wait_task() -> dict[str, Any]:
    (dag,) = registry.take_defined()
    tasks: list[dict[str, Any]] = dag.to_json()["tasks"]
    return next(t for t in tasks if t["kind"] == "wait_for_event")


def test_a_wait_for_an_event_says_its_name_its_key_and_how_long() -> None:
    with Dag("ship"):
        waited = wait_for(EventNamed("paid", key="o-1"), max_wait=timedelta(hours=1))
        ship(waited.on(Event[Json]))
        ship.override(task_id="ship_2")(waited.on(Event))  # the same; typed `Event[None]`
    shown = _wait_task()
    assert shown["id"] == "wait_for_paid"
    assert shown["event"] == {"name": "paid", "key": {"literal": "o-1"}}
    assert shown["max_wait_ms"] == 3_600_000
    assert [o["name"] for o in shown["outcomes"]] == ["SUCCESS", "TimedOut"]


@pytest.mark.parametrize(
    ("key", "stored"),
    [
        (lambda: None, None),
        (lambda: dag_ctx.run.id, {"ref": {"kind": "dag_ctx", "field": "run_id"}}),
        (lambda: dag_ctx.event.key, {"ref": {"kind": "dag_ctx", "field": "event_key"}}),
    ],
)
def test_its_key_is_any_or_a_reference(key: Any, stored: Any) -> None:
    trigger = EventNamed("seen") if stored and "event_key" in str(stored) else None
    with Dag("ship", trigger=trigger):
        wait_for(EventNamed("paid", key=key())).on(TimedOut) >> ship(Event("x"))
    event = _wait_task()["event"]
    assert event.get("key") == stored


def test_a_key_from_an_upstream_output_makes_it_downstream_of_that_task() -> None:
    with Dag("ship"):
        wait_for(EventNamed("paid", key=order_of()))
    (dag,) = registry.take_defined()
    shown = dag.to_json()
    assert {"from": "order_of", "to": "wait_for_paid", "kind": "data"}.items() <= next(
        e for e in shown["edges"] if e["to"] == "wait_for_paid"
    ).items()


@pytest.mark.parametrize(
    ("given", "match"),
    [
        (lambda: EventNamed("no spaces"), "ID"),
        (lambda: EventNamed("paid", key=""), "1 to 1024 bytes"),
        (lambda: EventNamed("paid", key="a\nb"), "control"),
        (lambda: EventNamed("paid", key="a\u2028b"), "line-separator"),
        (lambda: EventNamed("paid", key="x" * 1025), "1 to 1024 bytes"),
        (lambda: EventNamed("paid", key=5), "a string or a reference"),  # type: ignore[arg-type]
    ],
)
def test_an_event_that_cant_be_one_is_refused(given: Any, match: str) -> None:
    with pytest.raises((TypeError, ValueError), match=match):
        given()


def test_as_a_trigger_it_starts_a_run_per_key_and_takes_no_key() -> None:
    with Dag("ship", trigger=EventNamed("paid")):
        echo(dag_ctx.event.key)
    (dag,) = registry.take_defined()
    assert dag.to_json()["trigger"] == {"kind": "event_named", "event": {"name": "paid"}}
    with (
        pytest.raises(ValueError, match="takes no `key`"),
        Dag("ship", trigger=EventNamed("paid", key="o-1")),
    ):
        pass


def test_the_event_waited_for_reads_as_an_event() -> None:
    stored = {
        "key": "o-1",
        "data": {"amount": 40},
        "at": {"$datetime": "2026-10-21T08:00:00Z"},
    }
    event = decode(stored, Event[Any])
    assert event == Event("o-1", {"amount": 40}, datetime(2026, 10, 21, 8, tzinfo=UTC))


def _sensor_wait_for() -> dict[str, Any]:
    (dag,) = registry.take_defined()
    tasks: list[dict[str, Any]] = dag.to_json()["tasks"]
    return next(t for t in tasks if t["kind"] == "sensor")["wait_for"]


def test_a_sensor_waiting_for_an_event_is_given_it_first() -> None:
    with Dag("ship"):
        # Defined in the DAG: `dag_ctx` gives references there.
        @sensor(wait_for=EventNamed("packed", key=dag_ctx.run.id), timeout=timedelta(seconds=5))
        async def packed(event: Event[Json], least: int) -> int | NotYet:
            return least

        packed(least=2)
    assert _sensor_wait_for() == {
        "event": {"name": "packed", "key": {"ref": {"kind": "dag_ctx", "field": "run_id"}}},
        "param": "event",
        "timeout_ms": 5000,
    }


def test_a_sensor_s_event_key_from_upstream_makes_it_downstream_of_that_task() -> None:
    with Dag("ship"):

        @sensor(wait_for=EventNamed("packed", key=order_of()))
        async def packed(event: Event[Json]) -> str:
            return event.key

        packed()
    (dag,) = registry.take_defined()
    edges = dag.to_json()["edges"]
    assert any(e["from"] == "order_of" and e["to"] == "packed" for e in edges), edges


def test_a_sensor_waiting_for_an_event_takes_an_event_first() -> None:
    with pytest.raises(TypeError, match="annotate it `Event"):

        @sensor(wait_for=EventNamed("packed"))
        async def packed(run: RunResult[int]) -> int:
            return 1


def test_a_trigger_waiting_for_events_is_called_with_each_and_fills_its_event() -> None:
    @trigger(wait_for=EventNamed("paid"))
    async def on_paid(event: Event[Json], least: int) -> Event[int] | None:
        amount = event.data["amount"] if isinstance(event.data, dict) else 0
        return Event(data=amount) if amount >= least else None

    with Dag("ship", trigger=on_paid(least=10)):
        echo(dag_ctx.event.key)
    (dag,) = registry.take_defined()
    shown = dag.to_json()["trigger"]["task"]["wait_for"]
    assert shown == {"event": {"name": "paid"}, "param": "event", "timeout_ms": 600_000}
    at = datetime(2026, 11, 8, 9, tzinfo=UTC)
    (found,) = run_trigger(on_paid(least=10), run=Event("o-1", {"amount": 40}, at))
    assert found == Event("o-1", 40, at), "the event's key and moment"
    assert run_trigger(on_paid(least=10), run=Event("o-2", {"amount": 4}, at)) == []
    with pytest.raises(ValueError, match=r"run=schedr\.Event"):
        run_trigger(on_paid(least=10))


def test_a_trigger_waiting_for_events_takes_no_key() -> None:
    with pytest.raises(TypeError, match="takes no `key`"):

        @trigger(wait_for=EventNamed("paid", key="o-1"))
        async def on_paid(event: Event[Json]) -> Event[int] | None:
            return None
