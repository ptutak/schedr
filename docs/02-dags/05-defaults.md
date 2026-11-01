# Defaults and retries

`defaults` on the DAG applies to every task. A task's own arguments replace the fields they
set; a field left unset keeps the DAG's value, then the built-in one.

```{literalinclude} ../../examples/docs/defaults.py
:language: python
:caption: examples/docs/defaults.py
```

`usual` keeps the DAG's timeout of 30 minutes and two retries. `quick` sets its own
timeout and no retries, and keeps the DAG's `retry_delay` and `max_output_size`.

| Setting | Built-in | What it does |
|---|---|---|
| `retries` | `0` | How many times a failed attempt is retried. |
| `retry_delay` | 5 minutes | How long the task waits before each retry. |
| `timeout` | none | How long one attempt may run. When it is reached the attempt ends `timed_out`. |
| `queue` | `"default"` | Which workers may run the task. A worker with no `[worker] queues` serves `default`. |
| `max_output_size` | 1 MiB | How large the stored output may be. `KiB` and `MiB` are the sizes in bytes (`8 * MiB`). For a command the limit is per stream. |
| `preload` | `True` | A Python task forks from the preloaded fork server. `False` forks only from the bare one, for code that misbehaves next to the preloaded libraries. |
| `fresh_process` | `False` | A Python task is always a new process, never forked, for code that misbehaves after a fork. `True` wins when `preload` is also set. |
| `run_if` | `RunIf.NONE_FAILED` | When the task runs, from its upstream edges. `RunIf.ALL_SUCCEEDED` runs only if every edge succeeded. [Routing by outcome](08-outcomes.md). |
| `max_active` | none | At most this many of the task's instances run at once, across all the DAG's runs. [Concurrency limits](#concurrency-limits). |
| `max_active_per_run` | none | At most this many of the task's instances run at once within one run: mainly a mapped task's fan-out. [Concurrency limits](#concurrency-limits). |

A duration is a `timedelta` of whole milliseconds. `retry_delay` must not be negative.
`timeout` must be positive. `retries` and `max_output_size` must not be negative, and
`queue` must not be empty. `retries`, `retry_delay`, `timeout`, `queue` and
`max_output_size` exist on `@task`, `.override(...)` and `CommandOperator`. `preload` and
`fresh_process` exist on `@task`, `.override(...)` and `TaskDefaults(...)`; they are stored
on the task and matter for a Python task only. A command always starts a new process, and
`CommandOperator` does not take them. `run_if` exists on all of those and on
`EmptyOperator`. A task's own argument wins over the DAG's `defaults`. A `TaskDefaults`
value is still stored on every task, including a command and an `EmptyOperator`.
`EmptyOperator` takes its ID and `run_if`; the master completes it, and no worker is
involved.

A task runs on a worker that serves its queue
([Configuration](../_generated/04-reference/01-configuration.md), `[worker] queues`). With
no queues configured, that is `default`. `schedr dags test` starts one worker from that
configuration, and a task whose queue it does not serve waits until the command's hour
limit.

## Concurrency limits

Worker slots and queues say how much can run. The limits say how much **should**, whatever
the slots:

- **`Dag(max_active_tasks=N)`**: at most `N` tasks of the DAG run at once, across all its
  runs. It keeps one big DAG from filling every worker, or limits the load it puts on another
  system.
- **`@task(max_active=N)`**: at most `N` instances of the task at once, across all runs (a
  task holding an external lock: `max_active=1`).
- **`@task(max_active_per_run=N)`**: at most `N` instances of the task at once within one
  run: a mapped task over 1,000 elements, 10 at a time.

```python
with Dag("sync", max_active_tasks=20):

    @task(max_active_per_run=10)
    def fetch(url: str) -> bytes: ...

    fetch.expand(url=urls)
```

`max_active` and `max_active_per_run` exist on `@task`, `.override(...)`, `TaskDefaults` and
`CommandOperator`. `None`, the default, means no limit, and a value below 1 is a
`ValueError` (the master refuses it too). Each instance of a mapped task counts toward all
three. A task counts while its attempt is open: assigned and not yet started, running, or
cleaning up. A task waiting for its retry, an `EmptyOperator` and a task the master
completes never count. `max_active_per_run` set on a task that isn't mapped limits nothing,
since it has one instance a run: `schedr dags check` warns (a `DagHint`), except for a
value from `TaskDefaults`, which is meant for the mapped ones.

A ready task whose limit is reached stays `queued`, with the reason shown in the UI and the
API: "waiting: `max_active_per_run` (10) reached". When a place frees, the oldest ready task
the limits allow goes first. Lowering a limit lets what runs finish, and holds new ones
until under it. The limits across runs (`max_active_tasks`, `max_active`) are always the
DAG's current version's; `max_active_per_run` is the run's own version's. The DAG's page in
the UI, and `GET /api/v1/dags/{dag_id}`, show each limit with how many run now. The metric
`schedr_tasks_waiting`, with `reason` `max_active_tasks`, `max_active` or
`max_active_per_run`, counts the waiting ones.

Limits shared by several DAGs (Airflow's pools) don't exist: a queue with a dedicated
worker group of fixed slots does that.

## What a failure does

The master decides retries. The default of `retries=0` means the first failure of these
kinds fails the task.

| The attempt ended | What happens |
|---|---|
| `failed` (the function raised, or a command exited with a code that is not a declared success), `timed_out`, `oom_killed`, `lost` | Retried while `retries` last, after `retry_delay`. Then the task fails. |
| `interrupted` (the worker was drained), `assignment_expired` | Run again, without using a retry. |
| `cancelled` | Not retried. |
| An error certain to repeat: inputs or an output that is too large, a template that cannot be rendered, a return value that cannot be stored, inputs that do not match the task | Not retried. |

A task downstream of a failure ends `upstream_failed` and does not run. An edge that
wasn't taken counts as skipped; [Routing by outcome](08-outcomes.md) is when that skips
the downstream task or still runs it. Tasks that do not depend on the failure keep
running. The run ends `failed`.

A task that succeeds has an outcome in the report (`schedr dags test` prints it in the
`OUTCOME` column): `SUCCESS` when the task has no outcomes of its own, otherwise the
name it declared. [Routing by outcome](08-outcomes.md) is how a DAG routes on that name.
[State and failures](../03-guide/07-failures.md#retries) is the full list of what each
stopped process becomes.
