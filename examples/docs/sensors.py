"""Wait steps: a value, or the `TimedOut` outcome downstream tasks route on."""

import asyncio
from datetime import datetime, timedelta

from schedr import NOT_YET, Dag, NotYet, TimedOut, dag_ctx, sensor, task


async def exists(path: str) -> bool:
    """Stands in for a call to a storage system."""
    await asyncio.sleep(0)
    return bool(path)


@sensor.poke(every=timedelta(seconds=1), max_wait=timedelta(hours=6))
async def partner_file(day: datetime) -> str | NotYet:
    """Called every second until it returns a value, for at most six hours."""
    path = f"s3://in/orders_{day:%Y-%m-%d}.csv"
    return path if await exists(path) else NOT_YET


@sensor(max_wait=timedelta(milliseconds=200))
async def approval() -> str:
    """Full control: it writes its own waiting. This one never comes."""
    await asyncio.Event().wait()
    return "approved"


@task
def load(path: str) -> None:
    print(f"loading {path}")


@task
def alert() -> None:
    print("no approval in time")


with Dag("sensors_demo"):
    load(partner_file(dag_ctx.run.planned_for).on(str))
    answer = approval()
    load.override(task_id="after_approval")(answer.on(str))
    answer.on(TimedOut) >> alert()
