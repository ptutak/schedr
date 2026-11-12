"""A payment starts a run; that run waits for its package to be packed.

`ship_paid` starts once per order paid (`POST /api/v1/events` or `schedr events post paid
--key order-123`), and waits for that order's `packed` event. Run alone, as `dags test` does,
nothing posts it and the wait runs out: `TimedOut`, which `late` handles.
"""

from datetime import timedelta

from schedr import Dag, Event, EventNamed, TimedOut, dag_ctx, task, wait_for
from schedr.types import Json


@task
def ship(event: Event[Json]) -> str:
    """Ship the package identified by the packing event."""
    return f"ship {event.key}: {event.data}"


@task
def late(outcome: TimedOut) -> str:
    """Report a package that did not arrive in time."""
    del outcome
    return "packing timed out"


with Dag(
    "ship_paid",
    description="Ships each paid order once its package is packed.",
    trigger=EventNamed("paid"),
):
    packed = wait_for(EventNamed("packed", key=dag_ctx.event.key), max_wait=timedelta(seconds=3))
    ship(packed.on(Event[Json]))
    late(packed.on(TimedOut))
