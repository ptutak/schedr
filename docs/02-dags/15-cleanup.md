# Cleaning up after a task

A task can start work outside its own process, such as a warehouse job, or make a temporary
file. A cleanup removes what the attempt leaves behind, even when schedr stops the task for a
timeout or cancellation and its own `finally` block cannot run.

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
Handles live in the agent's memory and are not kept across an agent restart. If the worker is
lost, its cleanup does not run.

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

`cleanup_timeout` limits each cleanup try (default five minutes); `cleanup_retries` is the
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
