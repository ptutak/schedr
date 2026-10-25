# The DAG context

`dag_ctx` is the current run. In the DAG body each field is a reference, filled in when
the task runs. Inside a running task the same expression is the value. `examples/daily_report.py`
reads the value inside its tasks and fills a command from the reference.
`examples/docs/context.py` also passes the window into a task.

```{literalinclude} ../../examples/daily_report.py
:language: python
:caption: examples/daily_report.py
```

```{literalinclude} ../../examples/docs/context.py
:language: python
:caption: examples/docs/context.py
```

| | In the DAG body | Inside a running task |
|---|---|---|
| `dag_ctx.run.planned_for` | A reference. Pass it to a task, or write it in a t-string. | An aware UTC `datetime`. |
| Calling a method on it (`.date()`, `.isoformat()`) | `AttributeError`. Do that inside the task, as `label` does. | The datetime's method. |
| `if dag_ctx.run.planned_for` or `str(...)` or an f-string | `TypeError`. It is not a value yet. | The value. |
| Anywhere else, including a test with no setup | `RuntimeError` naming `schedr.testing.task_context(...)`. | |

| Field | What it is |
|---|---|
| `dag_ctx.dag_id` | The DAG's ID, a `str`. |
| `dag_ctx.run.id` | The run's ID, a UUIDv7 string. |
| `dag_ctx.run.kind` | `scheduled`, `manual`, `backfill`, `chained` or `event`. |
| `dag_ctx.run.planned_for` | The moment the run is for. A scheduled run's is its fire time. |
| `dag_ctx.run.since` | The start of the window `(since, planned_for]`. |

`{dag_ctx.run.planned_for:%Y-%m-%d}` in a command formats that moment when the command
runs. The window itself, and when `initial_since` is required, is
[Schedules](04-schedules.md#run-window). `bounds` records a use of `since` in its
arguments, so a `Once` or a DAG with no trigger must set `initial_since` or the import
fails. `label` reads `dag_ctx` only while it runs, which the import does not see. A `Once`
or a DAG with no trigger, without `initial_since`, has `since` equal to `planned_for`
when the run has no earlier run and no previous fire time. `task_context` defaults
`since` to `planned_for`
([Testing a DAG](07-testing.md#one-task-and-its-context)).

The datetimes inside a task are aware and UTC. A task that returned a naive `datetime`
would fail, because a naive datetime cannot be stored
([Tasks and operators](02-tasks.md#what-a-task-may-return)).

`dag_ctx.params` raises `NotImplementedError`. Run parameters are
[Run parameters and the DAG context](../_generated/05-design/07-python-api-structure.md#run-parameters-and-the-dag-context).
Declared secrets are [Secret storage](../_generated/05-design/05-secret-storage.md); an
`env` entry on a command is an ordinary environment variable
([Tasks and operators](02-tasks.md#commands)). From a command, a t-string is how the run
is read. Reading it with `schedr dag-ctx` is
[In-task CLI: `schedr task-ctx` and `schedr dag-ctx`](../_generated/05-design/03-architecture.md#in-task-cli-schedr-task-ctx-and-schedr-dag-ctx).

A test gives `dag_ctx` its values with `task_context`
([Testing a DAG](07-testing.md#one-task-and-its-context)).
