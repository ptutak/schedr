"""`EventNamed`: what a DAG file says (decision 354)."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from schedr import Dag, Event, EventNamed, TimedOut, dag_ctx, task, wait_for
from schedr._model import registry
from schedr._serialization import decode
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
