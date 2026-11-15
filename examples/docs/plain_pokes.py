"""Pokes as plain `def`s: a blocking client, called on the host's threads (decision 489).

`orders_ready` waits for a marker file another system writes, with a blocking read; `new_reports`
starts a run for each report file that appears in a folder. Each call has a timeout of its own,
shorter than `every`, so it ends by itself. Point the `orders_inbox` connection at a folder of
your own and at your webserver.
"""

import urllib.request
from collections.abc import Iterator
from datetime import timedelta
from http.client import HTTPResponse
from pathlib import Path
from typing import cast

from schedr import NOT_YET, Dag, Event, NotYet, connection, dag_ctx, sensor, task, trigger


@connection(folder="/tmp/schedr-plain-pokes", status_url="http://localhost:8080/api/v1/version")
def orders_inbox(folder: str, status_url: str) -> Iterator[tuple[Path, str]]:
    """A plain connection, opened with `with` in a plain `def`: the folder and a URL to ask."""
    yield Path(folder), status_url


@sensor.poke(every=timedelta(seconds=30), max_wait=timedelta(hours=2), connections=[orders_inbox])
def orders_ready() -> int | NotYet:
    """A plain `def`: blocking calls are fine here, each with a timeout below `every`."""
    with orders_inbox() as (folder, status_url):
        marker = folder / "orders.done"
        if not marker.exists():
            return NOT_YET
        # A blocking HTTP call, its own timeout (10 s) well inside the call's window (`every`).
        with cast("HTTPResponse", urllib.request.urlopen(status_url, timeout=10)) as answer:
            answer.read()
        return int(marker.read_text() or "0")


@task
def load_orders(count: int) -> str:
    """Runs once the marker is there."""
    return f"{count} orders"


with Dag("plain_poke_demo", description="A plain `def` poke waits for a marker file."):
    load_orders(orders_ready().on(int))


@trigger.poke(every=timedelta(minutes=1), connections=[orders_inbox])
def new_reports(after: str | None) -> list[Event[str]]:
    """A run for each report file newer than the last one acted on, oldest first."""
    with orders_inbox() as (folder, _status_url):
        if not folder.is_dir():
            return []
        names = sorted(p.name for p in folder.glob("*.csv"))
    return [Event(name, name) for name in names if after is None or name > after]


@task
def read_report(name: str) -> str:
    """Reads the report the run is for."""
    return name


with Dag("plain_trigger_demo", trigger=new_reports()):
    read_report(dag_ctx.event.key)
