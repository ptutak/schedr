"""Batches of device files: each checked, stored or quarantined, then scanned for anomalies.

A poke trigger looks at the inbox every five seconds and gathers what came within thirty into
one run. Each file gets its own `validate`; a file that parses goes on to `store` in its lane, a
corrupt one to `quarantine`, and one bad file stops nothing else. `detect` runs under a hard
memory cap and posts an `anomaly` event for each device that ran hot: `anomaly_response` starts
a run per device from it.
"""

import asyncio
import json
import os
import shutil
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

from greenhouse import (
    INBOX,
    Anomaly,
    Corrupt,
    Reading,
    Readings,
    pager,
    readings_db,
    zone_of,
)

from schedr import Dag, Data, Event, Notify, api, dag_ctx, resource, task, trigger


class Arrived(Data, frozen=True):
    """A file found in the inbox."""

    path: str


@dataclass
class IngestParams:
    """A run's thresholds; an event run uses the defaults."""

    hot_celsius: float = 32.0


def _new_names(after: str | None) -> list[str]:
    INBOX.mkdir(parents=True, exist_ok=True)
    return sorted(
        n for n in os.listdir(INBOX) if n.endswith(".jsonl") and (after is None or n > after)
    )


@trigger.poke(every=timedelta(seconds=5), batch=timedelta(seconds=30))
async def inbox_files(after: str | None) -> list[Event[Arrived]]:
    """The files not seen yet. Their names start with a timestamp, so later ones sort after."""
    names = await asyncio.to_thread(_new_names, after)
    return [Event(name, Arrived(str(INBOX / name))) for name in names]


@task
def paths(batch: list[Event[Arrived]]) -> list[str]:
    """The batch's files, for one `validate` each."""
    return [item.data.path for item in batch]


@resource
def workspace() -> str:
    """A scratch folder for the run's parsing, removed by the cleanup however the run ends."""
    return tempfile.mkdtemp(prefix=f"greenhouse-{dag_ctx.run.id}-")


@workspace.cleanup(retries=2)
def remove_workspace(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)
    print(f"removed {path}")


def _reading(line: str) -> Reading:
    """One line, its fields checked: a `ValueError` or `TypeError` for a bad one."""
    fields = cast("dict[str, object]", json.loads(line))
    device, at, celsius, humidity = (fields[k] for k in ("device", "at", "celsius", "humidity"))
    if not (isinstance(device, str) and isinstance(at, str)):
        raise TypeError(f"device and at must be strings: {line!r}")
    if not (isinstance(celsius, (int, float)) and isinstance(humidity, (int, float))):
        raise TypeError(f"celsius and humidity must be numbers: {line!r}")
    datetime.fromisoformat(at)
    return Reading(device, at, float(celsius), float(humidity))


@task
def validate(work: str, path: str) -> Readings | Corrupt:
    """Parses one file into the workspace first, so a half-read file is never stored."""
    copy = Path(work) / Path(path).name
    try:
        shutil.copyfile(path, copy)
        lines = copy.read_text().splitlines()
        readings = [_reading(line) for line in lines if line.strip()]
    except (OSError, KeyError, ValueError, TypeError) as error:
        return Corrupt(path, f"{type(error).__name__}: {error}")
    if not readings:
        return Corrupt(path, "no readings")
    return Readings(path, readings)


@task(connections=[readings_db])
def store(batch: Readings) -> int:
    """Writes one file's readings; a file sent again changes nothing (its rows are keyed)."""
    with readings_db() as db:
        db.executemany(
            "insert or ignore into readings values (?, ?, ?, ?, ?)",
            [(r.device, zone_of(r.device), r.at, r.celsius, r.humidity) for r in batch.readings],
        )
    Path(batch.path).unlink(missing_ok=True)
    return len(batch.readings)


@task
def quarantine(bad: Corrupt) -> str:
    """Moves a corrupt file aside, with why next to it."""
    folder = Path(bad.path).parent.parent / "quarantine"  # beside the inbox it came from
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / Path(bad.path).name
    if Path(bad.path).exists():
        shutil.move(bad.path, target)
    target.with_suffix(".why").write_text(bad.reason + "\n")
    print(f"quarantined {target.name}: {bad.reason}")
    return str(target)


# A hard cap: past 256 MiB the task's allocations fail and it fails, instead of the worker
# killing it, or another task, when its pod runs short.
@task(memory="256MiB", hard_memory_limit=True)
def detect(parsed: list[Readings | Corrupt], hot_celsius: float) -> list[Anomaly]:
    """The hottest reading of each device over the threshold, one event per device."""
    hottest: dict[str, Reading] = {}
    for result in parsed:
        if isinstance(result, Readings):
            for r in result.readings:
                if r.celsius > hot_celsius and (
                    r.device not in hottest or r.celsius > hottest[r.device].celsius
                ):
                    hottest[r.device] = r
    found = [Anomaly(r.device, r.celsius, r.at) for r in hottest.values()]
    for anomaly in found:
        # Keyed by device and time: a trigger starts one run per key ever, so the device's next
        # alarm needs a key of its own; posted again (a retry), it is the same event.
        key = f"{anomaly.device}@{anomaly.at}"
        api.post_event(
            "anomaly",
            key=key,
            data={"device": anomaly.device, "celsius": anomaly.celsius, "at": anomaly.at},
            idempotency_key=key,
        )
    return found


@task
def summary(
    stored: Sequence[int | None] | None,
    quarantined: Sequence[str | None] | None,
    found: list[Anomaly],
) -> str:
    """One line on the batch.

    In a lane, `store` or `quarantine` was skipped (`None`); a whole mapped task is `None` when
    no file took its way (a batch of only good files).
    """
    text = (
        f"{sum(n for n in stored or [] if n is not None)} readings stored, "
        f"{sum(q is not None for q in quarantined or [])} files quarantined, "
        f"{len(found)} anomalies"
    )
    print(text)
    return text


with Dag(
    "readings_ingest",
    trigger=inbox_files(),
    params=IngestParams,
    tags=["showcase"],
    notify={pager: [Notify.RUN_FAILED]},
):
    work = workspace()
    batch: list[Event[Arrived]] = dag_ctx.event.data  # pyright: ignore[reportAny]
    parsed = validate.partial(work=work).expand(path=paths(batch))
    stored = store.expand(batch=parsed.on(Readings))
    moved = quarantine.expand(bad=parsed.on(Corrupt))
    hot: float = dag_ctx.params.hot_celsius  # pyright: ignore[reportAny]
    summary(stored, moved, detect(parsed, hot))
