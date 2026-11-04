# Cleaning up after a task

A task can start work outside its own process, such as a warehouse job, or make a temporary
file. A cleanup removes what the attempt leaves behind, even when schedr stops the task for a
timeout or cancellation and its own `finally` block cannot run.

For something shared by several tasks, see [Resources](16-resources.md).

```{literalinclude} ../../examples/docs/cleanup.py
:language: python
:caption: examples/docs/cleanup.py
```

## A Python task

Give `@task(cleanup=undo_export)` a synchronous function. After each attempt whose process
started, the worker runs it on the same worker, after the task process ends. It runs after
success as well as failure; if the process never started, there is no cleanup.

The function may name any of the task's inputs, with matching types. It receives their values,
including the task's defaults for inputs left out. It can read `dag_ctx` and `task_ctx`, and
use the task's declared secrets and connections. It receives neither the task's output nor its
status.

For things known only during the attempt, call `cleanup_handle` with a dataclass instance as
soon as the thing exists. The cleanup declares `handles: list[ExportJob | TempDir]` and gets
the registered instances in order. A single type is `list[ExportJob]`. Only declared types
are accepted; a cleanup without a `handles` parameter takes no handles. A handle cannot be
removed after registration, and a cleanup cannot register more.

Make cleanup actions idempotent: they must be safe after success and if tried again. For
example, cancel a job only if it is still running, and tolerate a directory already removed.
Do not register something that must outlive the attempt.

Handles are masked like task output, so a secret value in a handle may be replaced by its
mask. Declare a secret separately if the cleanup needs it. All handles together are limited
to about 1 MiB per attempt; exceeding the limit fails the registration and the attempt.
Handles are journaled on the worker before registration returns and sent to the master,
which stores them. Nothing secret belongs in a handle. Registration does not wait for the
master to acknowledge it, so a worker lost before delivery can lose a handle. Handles also
survive an agent restart when its journal disk survives; after restart the cleanup waits for
the master's `Continue`.

## Cleanup on another worker

Set `cleanup_anywhere=True` on `@task`, `CommandOperator`, `.override()`, or in
`TaskDefaults` to let the master move a started attempt's cleanup when its worker is lost.
Use it when the cleanup can work with the registered handles and the other worker's code and
resources. Do not use it for worker-local files or processes that another pod cannot access:
idempotence alone does not make a cleanup safe on another pod.

A worker that hears nothing from the master for the [worker lease](../03-guide/14-timings.md#fixed-timings)
(60 s by default) stops its running tasks with `cleanup_anywhere`. They end `lost` and use a
retry. A master outage longer than the lease therefore stops these tasks. When the connection
is lost, their cleanup waits for the master's answer, including when a task ends by itself
while disconnected: `Continue` runs it here; `Kill` drops it. The cleanup normally runs once,
here or on another worker. After the master marks the worker lost, it waits the cancel grace
(30 s by default) plus 10 s before moving cleanup.

A cleanup already running when the connection is lost continues and can overlap one moved to
another worker. A frozen VM cannot stop its task at all. The cleanup must therefore still be
safe during the task as well as safe to repeat. For example, deleting a shared temporary
directory is unsafe if the task may still be writing to it, even if deleting an already absent
directory is harmless.

The moved cleanup uses the task's queue and the receiving worker's current code; it is
assigned ahead of waiting tasks and is not held by limits. The master keeps the task result
reported in `TaskEnded`, or records the task as `lost` if none arrived. The attempt stays
open until cleanup finishes. Each move counts as a cleanup try: if the worker is lost after
`cleanup_retries + 1` moves, cleanup ends `worker_lost` and fails the run. A refused
assignment is tried again, preferring another worker; an assignment with no `Started` is
taken back without another move. If no worker is available, the instance remains
`cleaning_up`, shown as `unschedulable` with the reason "no worker for the cleanup".
Without `cleanup_anywhere`, a lost worker's cleanup ends `worker_lost`.

## A command

`CommandOperator` takes a cleanup command as a string or argument list in `cleanup=`. The
task can register a named handle with fields, and its cleanup can read them:

```sh
schedr task-ctx cleanup add Publication id="$id"
schedr task-ctx cleanup list Publication --field id
```

`add` accepts `KEY=VALUE` text fields, `KEY:=JSON` fields, or `--json` with an object. `list` runs in the cleanup and
prints one JSON object per handle, in registration order; `--field id` prints that field's
value on each line. `cleanup_handles=["Publication"]` limits the accepted types; without it,
a command accepts any handle type. The task's declared secrets and connections are available
to the cleanup too. Don't put secrets directly in the cleanup command.

## Time and retries

`cleanup_timeout` limits each cleanup try; when omitted, `[worker] cleanup_timeout` supplies it
(five minutes by default; see [Timings](../03-guide/14-timings.md)). `cleanup_retries` is the
number of retries after a failure (default three), and `cleanup_retry_delay` is the delay
before each retry (default 30 seconds). Set them on the task or in `TaskDefaults`. A cancel
does not stop a cleanup already running; its timeout still applies.

The attempt holds its worker slot through cleanup, retries and their delays. Its instance is
`cleaning_up` while it waits; downstream tasks and a task retry wait for cleanup to finish.
A cleanup that still fails after its retries fails the run, even if the task succeeded. See
[State and failures](../03-guide/07-failures.md) for what happens when a worker stops.

## Testing

`with task_context() as ctx:` records calls to `cleanup_handle` in `ctx.handles`. A test can
inspect the list and call the cleanup function itself. See [Testing a DAG](07-testing.md#one-task-and-its-context).
