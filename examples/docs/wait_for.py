"""Waiting for another DAG's run: `report` builds on the `orders` of the same day.

`wait_for(RunOf(...))` is a step the master ends when that run's `load` has ended: no process, no
slot, no polling. Its value is that run's ID. `.on(Failed)` handles a run that didn't succeed;
without it the step fails, and the run with it. Run alone, as `dags test` does, `report` finds no
`orders` run and its wait runs out: `TimedOut`, which `late` handles. `tally_orders` is started by
each run of `orders` that succeeds. `checked_report` reads the run itself: an `@sensor` with
`wait_for=` is called once that `load` has ended, with the run as its first parameter.
`big_orders` is started by an `@trigger` with `wait_for=`: called once for each run of `orders`
that ends, given it, it decides whether that run starts one.
"""

from datetime import timedelta

from schedr import (
    Dag,
    Event,
    Failed,
    RunOf,
    RunResult,
    TimedOut,
    dag_ctx,
    sensor,
    task,
    trigger,
    wait_for,
)
from schedr.sensors import NOT_YET, NotYet


@task
def load() -> int:
    """The day's orders."""
    return 42


@task
def build(run_id: str) -> str:
    """Builds the report from that run's orders."""
    return f"report from {run_id}"


@task
def say_failed(failed: Failed) -> str:
    """Says the orders weren't loaded."""
    return f"no report: orders ended {failed.state} in {failed.run_id}"


@task
def late(timed_out: TimedOut) -> str:
    """Says the orders didn't come in time."""
    del timed_out
    return "no orders in time"


with Dag("orders", description="Collects the day's orders."):
    load()


@task
def tally() -> str:
    """Counts the day's orders once they are loaded."""
    return "tallied"


# A run of `tally_orders` for each run of `orders` that succeeds, planned for the same moment.
with Dag(
    "tally_orders",
    description="Tallies the orders after each orders run.",
    trigger=RunOf("orders", lag=timedelta(0)),
):
    tally()

with Dag(
    "report",
    description="Builds a report on the orders of the same day, waiting for that orders run.",
):
    loaded = wait_for(RunOf("orders", task="load", lag=timedelta(0)), max_wait=timedelta(seconds=3))
    build(loaded.on(str))
    say_failed(loaded.on(Failed))
    late(loaded.on(TimedOut))


@sensor(wait_for=RunOf("orders", task="load", lag=timedelta(0)), max_wait=timedelta(seconds=3))
async def loaded_orders(run: RunResult[int], least: int) -> int | NotYet:
    """The orders once `load` has ended with enough; a failed or short run waits for a redo."""
    if run.task_state != "success" or run.output is None or run.output < least:
        return NOT_YET
    return run.output


@task
def build_from(orders: int) -> str:
    """Builds the report from that many orders."""
    return f"report of {orders} orders"


with Dag(
    "checked_report", description="Builds a report once the same day's orders run has succeeded."
):
    orders = loaded_orders(least=1)
    build_from(orders.on(int))
    late(orders.on(TimedOut))


@trigger(wait_for=RunOf("orders", task="load", lag=timedelta(0)), run_retries=3)
async def on_big_orders(run: RunResult[int], least: int) -> Event[int] | None:
    """A run of `big_orders` for a load of at least `least` orders; keyed by that run's ID."""
    if run.task_state != "success" or run.output is None or run.output < least:
        return None
    return Event(data=run.output)


@task
def handle_big(orders: int) -> str:
    """Handles a big day."""
    return f"{orders} orders: a big day"


with Dag(
    "big_orders",
    description="Starts a run for each orders run with at least 40 orders.",
    trigger=on_big_orders(least=40),
):
    handle_big(dag_ctx.event.data)  # pyright: ignore[reportAny]
