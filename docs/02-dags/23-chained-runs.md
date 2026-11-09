# Starting another DAG

`TriggerDagOperator` starts a run of another DAG from a task. The target opts in with
`trigger=Chained()` or names the DAGs allowed to start it with `Chained(by=[...])`.
`by=[]` is an error. Here `chained_report` starts `chained_load` and waits for it:

```{literalinclude} ../../examples/docs/chained_runs.py
:language: python
:caption: examples/docs/chained_runs.py
```

The master runs the operator without a worker or slot. It creates a `chained` run linked to
its parent run. The new run's `planned_for` is the parent's unless the operator gives an
aware `datetime` or a reference as `planned_for=`. Its `params=` can contain literals,
upstream outputs and `dag_ctx` references. They overlay the target's defaults and are checked
against its schema before the run is created. A `Chained` DAG's parameters need no defaults;
the operator or a manual run must supply required fields.

## How the task ends

With `wait=True` (the default), the task waits for the target run:

| Target run | Operator task |
|---|---|
| succeeds | Its value is the run's ID (`str`). |
| fails, is cancelled or skipped | `Failed`, with `state` and `run_id`, if the DAG takes `.on(Failed)`; otherwise the task fails and its retries apply. |
| does not end within `max_wait` | `TimedOut`; the target run continues. `max_wait` defaults to the master's `[sensors] default_max_wait`. |

With `wait=False`, the task ends with the run's ID as soon as the run is created; it does
not wait for its result and cannot take `max_wait`. Read the target's output with a
[`RunOf` sensor](21-wait-for.md#reading-the-run-sensorwait_for) or the API.

A retry starts a new target run only after the previous attempt failed. The master records
its intended run ID before creating the run, so a restart between those writes does not
create two. Cancelling the operator or its parent run cancels a target run it is still
waiting for; a run started with `wait=False` continues.

## Refusals

A missing target, a target in an ID conflict, or a paused target fails the task and its
retries apply. A target may be missing while its root has not been parsed yet. These failures
are not retried:

- The target is not `Chained`, or its `by` does not name the parent DAG.
- The target refuses the parameters or `planned_for`.

DAGs may start each other in a cycle (`a` starts `b`, which starts `a`): it is allowed, as
between `RunOf` triggers. Only a DAG starting its own runs is refused, when its file loads.

## One way to start a DAG

A DAG gets automatic runs from its own trigger, or from other DAGs' operators with
`trigger=Chained()`. `trigger=None` is for manual runs only. Any DAG can also be run
manually, including a scheduled or `Chained` DAG. Pausing a `Chained` DAG makes operators
fail until it is unpaused; manual runs remain available.

Operators cannot be mapped yet. To start one run per item, create an operator per item in
a `for` loop in the DAG file, giving each a distinct task ID.

## Testing

`schedr dags test` and `run_dag` run the target for real when its DAG file is in the path
tested. Include the target in that path; otherwise the operator fails. The test unpauses
`Chained` DAGs in the path. `schedr dags check` fails the operator's file if a target in
the root refuses it; a target in no file of the root gives a warning.

## From Airflow

`TriggerDagRunOperator` maps to `TriggerDagOperator`, and `logical_date` to `planned_for`.
For a target with Airflow's `schedule=None`, use `trigger=Chained()` when another DAG's
operator starts it; use `trigger=None` for manual runs only.
