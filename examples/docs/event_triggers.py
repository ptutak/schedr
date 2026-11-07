"""Files arriving in a folder start a run each: a trigger, and what a run reads of its event.

The bookmark works here because the file names carry their time (``2026-11-07T10.csv``), so a later
file sorts after an earlier one. With names that don't, look at all of them: a key seen before
starts no run.
"""

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path

from schedr import Dag, Data, Event, dag_ctx, task, trigger


class Arrived(Data):
    """A file that came."""

    path: str
    size: int


def _names(folder: Path, after: str | None) -> list[str]:
    return sorted(name for name in os.listdir(folder) if after is None or name > after)


@trigger
async def new_files(after: str | None, folder: str) -> AsyncIterator[Event[Arrived]]:
    """Yields each file not seen yet, in name order, and looks again every few seconds."""
    place = Path(folder)
    while True:
        names = await asyncio.to_thread(_names, place, after)
        for name in names:
            yield Event(name, Arrived(str(place / name), (place / name).stat().st_size))
            after = name
        await asyncio.sleep(5)


@task
def load(key: str, file: Arrived) -> str:
    """Loads one file."""
    return f"{key}: {file.size} bytes in {file.path}"


with Dag("event_triggers", trigger=new_files(folder="/tmp/schedr-incoming")):
    load(dag_ctx.event.key, dag_ctx.event.data)  # pyright: ignore[reportAny]
