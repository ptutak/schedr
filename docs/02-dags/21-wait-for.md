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
| ended `failed`, `upstream_failed`, `skipped` or `cancelled`, with no run for that moment still going | `Failed`, with `.state` and `.run_id`, when the DAG takes `.on(Failed)`; otherwise the step fails and the run with it |
| nothing matching ended within `max_wait` | `TimedOut`, as any [wait step](18-sensors.md); `max_wait` defaults to the master's `[sensors] default_max_wait` |

When several runs are planned for the same moment, as after a rerun, one that succeeded is enough
at once, even while a rerun still goes; with none succeeded, the step waits while one of them still
runs, a rerun queued too.

A failure upstream is never quietly skipped past: without `.on(Failed)`, the step fails and what
follows it is `upstream_failed`. `TimedOut` stays an outcome, as for any wait step: nothing broke.

While it waits, the step is `queued` with a reason, `waiting for task `load` of a run of `orders``.
A DAG that isn't loaded is said there too; the step waits all the same, as that DAG may come from a
folder that loads later.

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
