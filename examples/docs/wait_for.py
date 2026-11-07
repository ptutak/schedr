"""Waiting for another DAG's run: `report` builds on the `orders` of the same day.

`wait_for(RunOf(...))` is a step the master ends when that run's `load` has ended: no process, no
slot, no polling. Its value is that run's ID. `.on(Failed)` handles a run that didn't succeed;
without it the step fails, and the run with it. Run alone, as `dags test` does, `report` finds no
`orders` run and its wait runs out: `TimedOut`, which `late` handles. `tally_orders` is started by
each run of `orders` that succeeds. `checked_report` reads the run itself: an `@sensor` with
`wait_for=` is called once that `load` has ended, with the run as its first parameter.
"""

from datetime import timedelta

from schedr import Dag, Failed, RunOf, RunResult, TimedOut, sensor, task, wait_for
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


with Dag("orders"):
    load()


@task
def tally() -> str:
    """Counts the day's orders once they are loaded."""
    return "tallied"


# A run of `tally_orders` for each run of `orders` that succeeds, planned for the same moment.
with Dag("tally_orders", trigger=RunOf("orders", lag=timedelta(0))):
    tally()

with Dag("report"):
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


with Dag("checked_report"):
    orders = loaded_orders(least=1)
    build_from(orders.on(int))
    late(orders.on(TimedOut))
