"""Inspect paid events before starting a run."""

from schedr import Dag, Event, EventNamed, dag_ctx, task, trigger
from schedr.types import Json


@trigger(wait_for=EventNamed("paid"))
async def paid_with_data(event: Event[Json]) -> Event[Json] | None:
    """Start a run only when the payment has data."""
    if event.data is None:
        return None
    return Event(data=event.data)


@task
def handle(key: str) -> str:
    """Handle the payment."""
    return f"handling {key}"


with Dag(
    "inspect_payment",
    description="Starts a run for each paid event whose data passes a check.",
    trigger=paid_with_data(),
):
    handle(dag_ctx.event.key)
