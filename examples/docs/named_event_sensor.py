"""Wait for a packed event for the order that started this run."""

from datetime import timedelta

from schedr import Dag, Event, EventNamed, TimedOut, dag_ctx, sensor, task
from schedr.sensors import NOT_YET, NotYet
from schedr.types import Json


@task
def report(value: str) -> str:
    """Report the package."""
    return value


@task
def late(outcome: TimedOut) -> str:
    """Report a timeout."""
    del outcome
    return "packing timed out"


with Dag("inspect_packing", trigger=EventNamed("paid")):

    @sensor(wait_for=EventNamed("packed", key=dag_ctx.event.key), max_wait=timedelta(seconds=3))
    async def packed(event: Event[Json]) -> str | NotYet:
        """Wait for a packed event with data."""
        if event.data is None:
            return NOT_YET
        return f"packed {event.key}: {event.data}"

    result = packed()
    report(result.on(str))
    late(result.on(TimedOut))
