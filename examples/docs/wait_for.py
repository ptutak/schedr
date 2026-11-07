"""Waiting for another DAG's run: `report` builds on the `orders` of the same day.

`wait_for(RunOf(...))` is a step the master ends when that run's `load` has ended: no process, no
slot, no polling. Its value is that run's ID. `.on(Failed)` handles a run that didn't succeed;
without it the step fails, and the run with it. Run alone, as `dags test` does, `report` finds no
`orders` run and its wait runs out: `TimedOut`, which `late` handles.
"""

from datetime import timedelta

from schedr import Dag, Failed, RunOf, TimedOut, task, wait_for


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

with Dag("report"):
    loaded = wait_for(RunOf("orders", task="load", lag=timedelta(0)), max_wait=timedelta(seconds=3))
    build(loaded.on(str))
    say_failed(loaded.on(Failed))
    late(loaded.on(TimedOut))
