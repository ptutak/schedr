"""What the greenhouse DAGs share: where the files are, the readings database, the models.

The devices of two greenhouse zones write JSON-lines files of readings into the inbox
(`examples/showcase/simulate.py` plays them). Everything lives under `/tmp/schedr-greenhouse`:
the inbox, the quarantine beside it, and `greenhouse.db`, a SQLite file standing in for the
readings store.
"""

import os
import sqlite3
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import BaseModel

from schedr import Data, WebhookChannel, connection, notify

HOME = Path("/tmp/schedr-greenhouse")
"""The scenario's folder; `simulate.py` writes into the same one."""
INBOX = HOME / "inbox"
FAST = os.environ.get("GREENHOUSE_FAST") == "1"
"""Waits of seconds instead of minutes, read where the files are parsed: the test sets it."""


def wait(minutes: float) -> timedelta:
    """A wait of `minutes`, or of three seconds with `GREENHOUSE_FAST=1`."""
    return timedelta(seconds=3) if FAST else timedelta(minutes=minutes)


ZONES = ("zone_a", "zone_b")
"""The greenhouse zones; each device belongs to one (`dev-a1` to zone_a, `dev-b2` to zone_b)."""

SCHEMA = """
create table if not exists readings (
    device text not null,
    zone text not null,
    at text not null,
    celsius real not null,
    humidity real not null,
    primary key (device, at)
)
"""


@connection(path=str(HOME / "greenhouse.db"))
def readings_db(path: str) -> Iterator[sqlite3.Connection]:
    """The readings store: a SQLite file, its table made on first use."""
    db = sqlite3.connect(path, timeout=30)
    try:
        db.execute(SCHEMA)
        yield db
        db.commit()
    finally:
        db.close()


def zone_of(device: str) -> str:
    """`dev-a1` → `zone_a`."""
    return f"zone_{device.removeprefix('dev-')[:1]}"


class Reading(Data, frozen=True):
    """One line of a device's file."""

    device: str
    at: str
    celsius: float
    humidity: float


class Readings(Data, frozen=True):
    """A file that parsed: where it was, and its readings."""

    path: str
    readings: list[Reading]


class Corrupt(Data, frozen=True):
    """A file that didn't: an expected problem, so an outcome, not an exception."""

    path: str
    reason: str


class Anomaly(Data, frozen=True):
    """A device that ran too hot in a batch."""

    device: str
    celsius: float
    at: str


class ZoneSummary(BaseModel, frozen=True):
    """A zone over one rollup window, a pydantic model as an output."""

    zone: str
    readings: int
    mean_celsius: float | None
    max_celsius: float | None
    since: datetime
    until: datetime


# A DAG's own webhook: its URL must be inside the server's `[notifications] dag_urls`
# (examples/standalone.toml allows the local receiver `simulate.py receive` runs).
pager = WebhookChannel(
    "pager",
    url="http://127.0.0.1:9099/pager",
    body={
        "text": notify.text,
        "dag": notify.dag,
        "run": notify.run,
        "event": notify.event,
        "data": notify.data,
    },
)
