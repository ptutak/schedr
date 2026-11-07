# Waiting for another DAG

`wait_for(RunOf(...))` is a step that waits for a run of another DAG, or for one of its tasks. The
master ends it when that run or task ends, as it does `EmptyOperator`: no process, no slot and no
polling.

```{literalinclude} ../../examples/docs/wait_for.py
:language: python
:caption: examples/docs/wait_for.py
```

## Which run

`RunOf("orders", task="load", lag=timedelta(hours=6))` names the DAG, and of it the task; without
`task`, the whole run counts. `lag` says how long before this run's `planned_for` that run was
planned for: `timedelta(0)` is the run for the same moment, so a rerun or a backfill of `report` for
2026-11-01 waits for the `orders` of 2026-11-01. Without `lag`, the step waits for the next run of
`orders` to end after it started.
`lag` cannot exceed 10 years, in either direction.

## How it ends

| That run, or its task | The step |
|---|---|
| succeeded | its value, the run's ID: `build(loaded.on(str))` |
| ended `failed`, `upstream_failed`, `skipped` or `cancelled` | `Failed`, with `.state` and `.run_id`, when the DAG takes `.on(Failed)`; otherwise the step fails and the run with it |
| nothing matching ended within `max_wait` | `TimedOut`, as any [wait step](18-sensors.md); `max_wait` defaults to the master's `[sensors] default_max_wait` |

When several runs are planned for the same moment, only the newest counts: schedr has no clear,
so redoing `orders` for 2026-11-01 (`schedr runs trigger orders --for ...`, or a backfill) starts
a new run for that moment, and a `report` run waiting or rerun afterwards waits for that one,
queued or running, and takes its result. An older run that succeeded no longer counts. A `report`
run that already ended is not run again by it: rerun it by hand, or start `report` with
`trigger=RunOf(...)`, which starts a run for each run of `orders` that succeeds.

A failure upstream is never quietly skipped past: without `.on(Failed)`, the step fails and what
follows it is `upstream_failed`. `TimedOut` stays an outcome, as for any wait step: nothing broke.

While it waits, the step is `queued` with a reason, `waiting for task `load` of a run of `orders``.
A DAG that isn't loaded is said there too; the step waits all the same, as that DAG may come from a
folder that loads later.

## Reading the run: `@sensor(wait_for=...)`

When the step has to look at that run, an `@sensor` takes `wait_for=RunOf(...)`. The master waits
as for `wait_for(...)`, with no call and no slot, and once a matching run (or its task) has ended,
whatever its state, calls the function once, with that run as its first parameter:

```python
@sensor(wait_for=RunOf("orders", task="load", lag=timedelta(0)), max_wait=timedelta(hours=6))
async def loaded_orders(run: RunResult[int], least: int) -> int | NotYet:
    if run.task_state != "success" or run.output is None or run.output < least:
        return NOT_YET
    return run.output
```

`RunResult[T]` has the run's `run_id`, `dag_id`, `planned_for`, `state` and `ended_at`, and with
`task=` that task's `task_state`, `outcome` and `output`, read as `T` (a mapped task's is the list
of its elements'). The output is masked as the API masks it for anyone reading another DAG's runs:
secret fields and passwords in URLs come as `****`. The DAG gives the other parameters as usual:
`loaded_orders(least=1)`. A run that ended within `max_wait` is handed though little of it is left:
the call then gets at least a minute.

What it returns is its value. `NOT_YET` says "not this run": the step waits for a later matching
one, until `max_wait`. With `lag` that is a newer run for the same moment, as when `orders` is run
again for that day; without it, the next run of `orders` to end. So it fits two cases: waiting for
a redo after a run that failed or was short, and picking the next run by what it holds. It is not
"ask again later": to check the same run again and again, write a `@sensor.poke` that reads it
from the API. A poke can't take `wait_for=`: it is called on a timer, never per run.

## Starting a DAG after another

`Dag("tally_orders", trigger=RunOf("orders", lag=timedelta(0)))` starts a run of `tally_orders` for
each run of `orders` that succeeds, or whose `task` succeeds. With `lag`, the new run is planned for
that run's `planned_for` plus `lag`, which can't be negative; without it, for the moment the master
sees it. Each run of `orders` starts one run, however often the master looks, and the new run's
`dag_ctx.event` says which: its key is that run's ID, its data `{"run_id", "dag_id",
"planned_for"}`.

Only runs that end while the DAG is live (unpaused, present and not in conflict) count. Runs from
before it was live or while it was paused are never caught up. A DAG can't be started by its own
runs, as it would start itself for ever. Queued runs count
against `[triggers] max_queued_events`, as an event trigger's do.

Any DAG may wait for any other's runs, as anyone signed in sees them.
