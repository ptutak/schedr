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

A duration is a `timedelta` of whole milliseconds. `retry_delay` must not be negative.
`timeout` must be positive. `retries` and `max_output_size` must not be negative, and
`queue` must not be empty. `retries`, `retry_delay`, `timeout`, `queue` and
`max_output_size` exist on `@task`, `.override(...)` and `CommandOperator`. `preload` and
`fresh_process` exist on `@task`, `.override(...)` and `TaskDefaults(...)`. A task's own
argument wins over the DAG's `defaults`. Both are stored on the task as `preload` and
`fresh_process`. They matter for a Python task only. A command always starts a new process,
and `CommandOperator` does not take them. A `TaskDefaults` value is still stored on every
task, including a command and an `EmptyOperator`. `EmptyOperator` takes only its ID; the
master completes it, and no worker is involved.

A task runs on a worker that serves its queue
([Configuration](../_generated/04-reference/01-configuration.md), `[worker] queues`). With
no queues configured, that is `default`. `schedr dags test` starts one worker from that
configuration, and a task whose queue it does not serve waits until the command's hour
limit.

## What a failure does

The master decides retries. The default of `retries=0` means the first failure of these
kinds fails the task.

| The attempt ended | What happens |
|---|---|
| `failed` (the function raised, or a command exited with something other than 0), `timed_out`, `oom_killed`, `lost` | Retried while `retries` last, after `retry_delay`. Then the task fails. |
| `interrupted` (the worker was drained), `assignment_expired` | Run again, without using a retry. |
| `cancelled` | Not retried. |
| An error certain to repeat: inputs or an output that is too large, a template that cannot be rendered, a return value that cannot be stored, inputs that do not match the task | Not retried. |

A task downstream of a failure ends `upstream_failed` and does not run. Tasks that do not
depend on it keep running. The run ends `failed`.

A task that succeeds has the outcome `SUCCESS` in the report (`schedr dags test` prints it
in the `OUTCOME` column). Routing a DAG by outcome is
[Outcomes, failures and joins](../_generated/05-design/07-python-api-structure.md#outcomes-failures-and-joins).
[State and failures](../03-guide/07-failures.md#retries) is the full list of what each
stopped process becomes.
