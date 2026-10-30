# Tasks and operators

A task is one step of a DAG. `@task` makes a Python function a task. `CommandOperator` runs
a command. `EmptyOperator` is a step with no work, used to join other tasks. This file uses
each of them, and uses `load` twice:

```{literalinclude} ../../examples/docs/tasks.py
:language: python
:caption: examples/docs/tasks.py
```

Define a `@task` function anywhere. Call it inside `with Dag(...)` to record a task, or
call it anywhere else to run the function directly, which is how a unit test calls it
([Testing a DAG](07-testing.md)). The call inside the block returns an
[`Output`](03-data-flow.md). The task's ID is the function's name, unless `task_id=` or
`.override(task_id=...)` sets another. `load` and `load_again` are two tasks with one
function.

An operator is constructed inside the block. Outside it, `CommandOperator` and
`EmptyOperator` raise `RuntimeError`.

## `@task`

| | |
|---|---|
| Return annotation | Required. `-> None` means no output. `-> Any` skips the parse-time check of the return type; the value still has to be storable when the task runs. |
| Parameters | Each one is declared. `*args` and `**kwargs` are a `TypeError`: arguments are stored by name. |
| Arguments | Checked against the signature when the DAG is imported. A missing or extra argument is a `TypeError` naming the task. |
| Calling it twice | The second call needs another ID: `load.override(task_id="load_again")(names)`. The same ID twice is `` task ID `load` is defined twice in DAG `tasks_demo` ``. |
| `async def` | A `TypeError`. Async tasks, sensors and event triggers are [Async task functions](../_generated/05-design/07-python-api-structure.md#async-task-functions) and [Sensors and triggers](../_generated/05-design/07-python-api-structure.md#sensors-and-triggers). |

`@task(timeout=..., retries=..., run_if=...)` sets that task's options. `.override(...)`
returns a new task function and leaves the original unchanged. The options are
[Defaults and retries](05-defaults.md) and [Routing by outcome](08-outcomes.md). Each
Python task runs in its own process. Leave the
start as it is for a task that can share the worker's preloaded libraries. Set
`preload=False` when the task misbehaves next to those libraries: it still forks, but only
from the bare fork server. Set `fresh_process=True` when the task misbehaves after a fork:
it is always a new process, and never forked. The same two arguments are on `.override(...)`
and `TaskDefaults(...)`, and the task's own argument wins. What the worker does with them is
[The fork server](../03-guide/05-workers.md#the-fork-server).

## Commands

`CommandOperator(task_id, command, *, env=..., outcomes=...)` runs one process.

| `command` | How it runs |
|---|---|
| A `str` or a t-string | A shell. In a t-string each interpolated value is shell-quoted. `:raw` inserts it as written: `t"./run.sh {flags:raw}"`. |
| A list | The program and its arguments, with no shell. Each element is one argument, so nothing is quoted. An empty list is a `ValueError`. |

An element, and a value in `env`, may be a string, a t-string or a reference such as
`names[0]`. A name in `env` matches `[A-Za-z_][A-Za-z0-9_]*`. In
`t"echo loaded {count} in $REGION"`, `{count}` is the task's output and `$REGION` is the
shell, from `env`.

Without `outcomes=`, exit code 0 is success (`DefaultOutcome.SUCCESS`). With
`outcomes=`, a declared exit code is success with that outcome; any other code, or a
signal, fails the attempt. [Routing by outcome](08-outcomes.md) is the enum and
`.on(...)`. The output is a `ProcessResult`: `stdout`, `stderr`, `exit_code`,
`stdout_truncated`, `stderr_truncated`. `announce.stdout` and `announce.stderr` are
references to those strings. The size limit applies to each stream.

`EmptyOperator(task_id, *, run_if=...)` has no output. The master completes it when its
upstream edges allow it to run, and no worker runs it.

## What a task may return

The return annotation is checked while the file is imported. A type outside this set is an
import error naming the part. The same types are what one task may pass to another
([Data flow](03-data-flow.md)).

| Annotation | What is stored |
|---|---|
| `bool`, `int`, `float`, `str`, `None` | The value. An `int` may be passed where a `float` is expected. A `bool` is not an `int`. |
| `datetime` | An aware datetime, as `{"$datetime": "2026-10-15T03:00:00+00:00"}`. A naive datetime fails when the task returns it. |
| `date`, `timedelta` | `{"$date": "2026-10-15"}`, `{"$timedelta": 3600000000}` (microseconds). |
| `list[T]`, other sequences of `T` | A list of `T`. |
| `list`, `dict` | No element or value type: treated as `Any`. A `dict` still has string keys. |
| `tuple[A, B]` | A fixed tuple, as `{"$tuple": [...]}`. `tuple[T, ...]` is refused; use `list[T]`. |
| `dict[str, T]` | String keys only. |
| `Literal[...]` | `str`, `int`, `bool` or `None`. |
| A dataclass or a `TypedDict` | Its fields. A dataclass matches by its class name and fields; the stored JSON is the fields. `InitVar` and `field(init=False)` are refused. A class that refers to itself is refused. |
| `ProcessResult` | `stdout`, `stderr`, `exit_code`, `stdout_truncated`, `stderr_truncated`. |
| `A \| B` | Either type. Two or more dataclasses (or `None`) is an outcome union; `int \| str` is ordinary data with outcome `SUCCESS`. |
| A subclass of `Outcome` | No output (`None`); the member is the outcome. |
| `Any` | No parse-time check. The returned value still has to be one of the rows above. |

`set` and `frozenset` are refused (use `list`). `bytes` is refused (encode it, for example
base64 in a `str`). Outcome unions, `Outcome` enums and `.on(...)` are
[Routing by outcome](08-outcomes.md).

## IDs

A task ID is up to 250 bytes: ASCII letters, digits and `_ . -`. A `.` is allowed
(`extract.load`); an empty part (`extract.` or `.load`) is an `IdError`. A task written
inside a [group](11-groups.md) gets the group's name as that prefix (`load.extract`).

## No custom operators

Subclassing `CommandOperator`, `EmptyOperator` or `Trigger` outside `schedr` is a
`TypeError`. Share a function that creates tasks, or share a `@task` function.
`ScriptOperator` is
[Native API](../_generated/05-design/07-python-api-structure.md#native-api).
`TriggerDagOperator` is
[Dependencies between DAGs](../_generated/05-design/07-python-api-structure.md#dependencies-between-dags).
