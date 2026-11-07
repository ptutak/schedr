"""Wait steps: a value, or the `TimedOut` outcome downstream tasks route on.

Two wait steps ask schedr's REST API about a task of their own run: one that succeeds after five
seconds, and one the run doesn't have. `full_report` runs when both have ended. A task always waits
for all its upstream tasks, so `fast_report` follows a third wait step, which watches the other two
and ends with the first of them. Point `schedr_api` at your webserver; with sign-in on, the calls
would need a token too.
"""

import asyncio
import json
import time
import urllib.request
from collections.abc import AsyncIterator
from datetime import timedelta
from http.client import HTTPResponse
from typing import cast

from schedr import NOT_YET, Dag, NotYet, TimedOut, connection, dag_ctx, sensor, task

type Run = dict[str, list[dict[str, str | None]]]
"""What `GET /api/v1/runs/{run}` answers, as far as this example reads it."""


@connection(base_url="http://localhost:8080")
async def schedr_api(base_url: str) -> AsyncIterator[str]:
    """Stands in for an async client, opened with `async with`."""
    yield base_url


def read_run(url: str) -> Run:
    """A blocking call: the wait steps make it in a thread, leaving their event loop free."""
    with cast("HTTPResponse", urllib.request.urlopen(url, timeout=5)) as answer:
        return cast("Run", json.load(answer))


async def has_succeeded(task_id: str) -> bool:
    """Whether a task of this run has succeeded, as the REST API says."""
    async with schedr_api() as base_url:
        url = f"{base_url}/api/v1/runs/{dag_ctx.run.id}"
    run = await asyncio.to_thread(read_run, url)
    return any(t["task_id"] == task_id and t["state"] == "success" for t in run["tasks"])


@task
def slow_task() -> None:
    """Succeeds after five seconds, well inside what the wait steps allow."""
    time.sleep(5)


@sensor.poke(every=timedelta(seconds=1), max_wait=timedelta(seconds=10), connections=[schedr_api])
async def sees_slow_task() -> str | NotYet:
    """Called every second until it returns a value, for at most ten seconds."""
    return "Task slow_task success" if await has_succeeded("slow_task") else NOT_YET


@sensor(max_wait=timedelta(seconds=10), connections=[schedr_api])
async def sees_absent_task() -> str:
    """Full control: it writes its own waiting. The run has no such task, so its time runs out."""
    while not await has_succeeded("absent_task"):
        await asyncio.sleep(1)
    return "Task absent_task success"


@sensor.poke(every=timedelta(seconds=1), max_wait=timedelta(seconds=15), connections=[schedr_api])
async def sees_either() -> str | NotYet:
    """Ends with the first of the other two to end, whichever way it ended."""
    for name in ("sees_slow_task", "sees_absent_task"):
        if await has_succeeded(name):  # a timeout is a success, with the `TimedOut` outcome
            return name
    return NOT_YET


@task
def full_report(first: str | TimedOut, second: str | TimedOut) -> str:
    """Runs on either outcome of each, and says what came of both."""
    said = [
        f"{name}: {'timed out' if isinstance(value, TimedOut) else value}"
        for name, value in (("sees_slow_task", first), ("sees_absent_task", second))
    ]
    print("\n".join(said))
    return "; ".join(said)


@task
def fast_report(first: str) -> str:
    """Runs as soon as one of them has ended, the other perhaps still waiting."""
    said = f"{first} ended first"
    print(said)
    return said


with Dag("sensors_demo"):
    slow_task()
    full_report(sees_slow_task().on(str, TimedOut), sees_absent_task().on(str, TimedOut))
    fast_report(sees_either().on(str))
