# The DAG context

`dag_ctx` is the current run. In the DAG body each field is a reference, filled in when
the task runs. Inside a running task the same expression is the value.
`examples/daily_report.py`
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
| `dag_ctx.params` | The run's parameters. A field is a reference in the DAG body and the value inside a task. [Run parameters](12-params.md). |

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

`dag_ctx.params.region` is a reference in the DAG body, as `planned_for` is, and the value
inside a task. `dag_ctx.params(ReportParams)` is the whole instance, typed by that class.
Declaring the class, giving a run its values, and what a field may be are
[Run parameters](12-params.md). Secrets, given to a task or declared and read inside it, are
[Secrets](13-secrets.md); an `env` entry on a command is an ordinary environment variable
([Tasks and operators](02-tasks.md#commands)). A
t-string fills a command before it starts. While the command runs, it reads the run with
`schedr dag-ctx`, and a Python script it starts reads the same values with `schedr.script`
([Reading the run from a command](#reading-the-run-from-a-command)).

A test gives `dag_ctx` its values with `task_context`
([Testing a DAG](07-testing.md#one-task-and-its-context)).

## Reading the run from a command

`schedr dag-ctx` and `from schedr.script import dag_ctx` read the run while a **command or
script**
task runs. Both talk to the worker over that attempt's context channel
([A command's context channel](../03-guide/05-workers.md#a-commands-context-channel)).
It talks only to the task's agent. Those options are also in the
[CLI reference](../_generated/04-reference/02-cli.md).

A shell script's children inherit the channel, so they can call `schedr dag-ctx`. The
`schedr` on the command's `PATH` can be another version than the worker's. A script can
pin `schedr` in its own environment. Either is accepted when it speaks protocol 1, the
protocol this channel uses. A different protocol is refused, and the message names the
caller's version, the agent's version and both protocol numbers:

```text
this schedr (0.9.0) speaks protocol 2 of the task's context, the task's agent (1.2.0) protocol 1: use schedr 1.2.0 (or one speaking protocol 1) in the task
```

### `schedr dag-ctx`

```text
schedr dag-ctx params get [--field NAME]
schedr dag-ctx run get [--field NAME]
```

| Command | What it prints |
|---|---|
| `params get` | The run's parameters, one JSON object in the form `--params` takes. `{}` when the DAG takes none. A date or datetime is ISO 8601 text, a duration is an ISO 8601 duration (a dataclass duration of one hour is `P0DT3600.000000S`, as [Field types](12-params.md#field-types) shows it; a Pydantic duration stays the text it was stored as), and a tuple is a list. |
| `run get` | One JSON object: `dag_id`, `id`, `kind`, `planned_for`, `since`. `kind` is `scheduled`, `manual`, `backfill`, `chained` or `event`. The times are ISO 8601 in UTC, such as `2026-11-01T08:00:00Z`. |

Without `--field`, the object is one line of compact JSON and a newline. Parameters:

```text
{"name":"q3","day":"2026-10-01","window":"P0DT5400.000000S","pair":[1,"a"]}
```

Run information:

```text
{"dag_id":"report","id":"0192c4e1-7b4a-7c3e-8f21-6a5b4c3d2e1f","kind":"manual","planned_for":"2026-11-01T08:00:00Z","since":"2026-10-31T08:00:00Z"}
```

`--field NAME` prints that top-level field. A string is its characters and a newline
(`eu`, or `2026-10-01`). A number, a boolean, `null`, a list or an object is JSON and a
newline (`[1,"a"]`, `true`). A string that itself contains a newline spans more than one
line; without `--field` it stays inside the JSON string. A name that is absent exits 1
with `` error: there is no field `zone` ``.

```python
CommandOperator(
    "show_region",
    ["sh", "-c", "schedr dag-ctx params get --field region"],
)
```

| Exit | When |
|---|---|
| 0 | The values were written to stdout. |
| 1 | The call could not be done. The message is on stderr and starts with `error: `. |
| 2 | Wrong usage: no `get`, an unknown subcommand, or `--field` without a name. The text is clap's, on stderr. |

Each of these is exit 1. `N` is the number in `SCHEDR_CTX_FD`.

**Outside a task.** `SCHEDR_CTX_FD` is unset. A program a Python task starts hits this
too: a Python task's processes have no channel, and the task's function reads `dag_ctx`.

```text
error: `schedr dag-ctx` must be run inside a schedr task: a command task's processes can use it (SCHEDR_CTX_FD isn't set here); a Python task reads `dag_ctx`
```

**The variable is not a descriptor number** (`SCHEDR_CTX_FD=x`):

```text
error: SCHEDR_CTX_FD=`x` isn't a descriptor number
```

**The number is 0, 1 or 2:**

```text
error: the task's context channel (SCHEDR_CTX_FD=1): descriptor 1 can't be a context channel
```

**A parent closed the descriptor.** Python's `subprocess` does, unless `pass_fds` names
it ([below](#a-child-process)).

```text
error: the task's context channel (SCHEDR_CTX_FD=N) isn't open here: a parent process closed it (Python's `subprocess` closes inherited descriptors unless given `pass_fds=[N]`; in a Python script, `from schedr.script import dag_ctx` reads the same values)
```

**The number was closed and then reused** for something else:

```text
error: descriptor N (SCHEDR_CTX_FD) isn't the task's context channel any more: a parent process closed it (Python's `subprocess` closes inherited descriptors unless given `pass_fds=[N]`; in a Python script, `from schedr.script import dag_ctx` reads the same values), and the number was reused
```

**The attempt has ended**, including a process the command left running after it exited:

```text
error: the task's agent no longer serves its context: the task has ended (a process it left running can't read it)
```

A connection the agent closes without an answer:

```text
error: the task's agent closed the connection without an answer (the task may have ended)
```

**Another protocol.** The message quoted above, after `error: `.

**The agent does not take the call, or does not answer, within 30 s:**

```text
error: the task's agent didn't take the call within 30s
error: the task's agent didn't answer within 30s
```

**64 connections are already open** for this attempt (each call holds one until it
finishes):

```text
error: the task has 64 context connections open, the most the agent serves at once
```

### A Python script

Import `dag_ctx` from `schedr.script` in a declared script or a script a command task runs
(`python
report.py`, or any process the command starts). That import is what connects `dag_ctx` to
the channel. `from schedr import dag_ctx` alone leaves the name the DAG file uses, and
outside a task it still tells you to call `schedr.testing.task_context`.

```python
CommandOperator("report", ["python", "report.py"])
```

```python
from dataclasses import dataclass
from datetime import date

from schedr.script import dag_ctx


@dataclass
class ReportParams:
    region: str
    day: date


params = dag_ctx.params(ReportParams)
print(params.region, params.day.isoformat())
print(dag_ctx.run.planned_for.isoformat(), dag_ctx.dag_id)
```

The first read loads the run on one connection (the parameters, then the run
information) and keeps it. A read that fails raises, and the next read tries again.
`dag_ctx.dag_id`, `dag_ctx.run.id`, `dag_ctx.run.kind`, `dag_ctx.run.planned_for` and
`dag_ctx.run.since` are the fields `run get` prints, as values. The datetimes are aware
and UTC.

A field is decoded from the form it was stored in, the same tags a task's output uses. A
date is a `date`, a datetime a `datetime`, a duration a `timedelta`, and a tuple a
tuple. An object, a nested dataclass included, is a `dict` (`dag_ctx.params.limits` is
`{"low": 1}`; a Pydantic date stays the text it was stored as). `dag_ctx.params["region"]`
is the same as `dag_ctx.params.region`. `dag_ctx.params(ReportParams)` decodes strictly
into the class you pass, dataclass or Pydantic model, the way a task decodes that class.
Any class the stored values fit is accepted. A class they don't fit raises `TypeError`:
`` the run's params don't fit `Other`: ... ``. A missing field raises `AttributeError`:
`` the run's params have no field `zone` ``.

Inside a `@task` function the task's own values are used, and the channel stays unused.
A program that function starts has no channel.

`schedr.script` raises `RuntimeError`, with the CLI's text and without the `error: `
prefix, for the task having ended, for 64 connections already open, for the agent not
taking the call within 30 s, and
for text that is not a number, `` SCHEDR_CTX_FD=`x` isn't a descriptor number ``. The
closed-descriptor and reused-number texts match too, and their sentence stops at
`pass_fds=[N]`. These differ:

| Situation | `RuntimeError` |
|---|---|
| Outside a task, or a program a Python task started | `` `schedr.script` must be run inside a schedr task: a command task's processes can read its context (SCHEDR_CTX_FD isn't set here); a Python task reads `dag_ctx` itself `` |
| The number is negative, or 0, 1 or 2 | `` SCHEDR_CTX_FD=`0` isn't a descriptor number `` |
| Another protocol | `` the task's agent refused: `` and then the CLI's message |
| The connection drops, or the agent does not answer within 30 s, after it was opened | `` talking to the task's agent: ... (the task may have ended) `` |

A pinned `schedr` in the script's environment is accepted on protocol 1, the same rule as
the `schedr` binary.

### A child process

Python's `subprocess` closes the descriptor unless `pass_fds` names it. The child also
needs `SCHEDR_CTX_FD` in its environment. `subprocess` keeps the environment; an `env`
you pass has to set the variable itself.

```python
import os
import subprocess

fd = int(os.environ["SCHEDR_CTX_FD"])
subprocess.run(["schedr", "dag-ctx", "run", "get"], pass_fds=[fd])
```

In Python, `from schedr.script import dag_ctx` reads the same values in the process that
already has the descriptor.

### `schedr task-ctx`

`schedr task-ctx` uses the same channel for [secrets](13-secrets.md),
[connections](14-connections.md), [cleanup handles](15-cleanup.md), and
[resource values](16-resources.md#a-command-setup), and [API calls and events](22-events.md#posting-from-a-task):

| Command | What it does |
|---|---|
| `events post`, `events list`, `api METHOD PATH` | Post or list events, or call an allowed API route as the task. |
| `resource set [FIELD ...] [--json JSON]` | Reports the setup's value, checked immediately against `CommandResource(..., value=T)` or `ScriptResource(..., value=T)`. Fields are `key=value` (text) or `key:=json`; alternatively, `--json` takes any JSON value. Report once per setup attempt. Only a command or script resource setup can call it. |
| `resource get [--field NAME]` | Prints the setup's value in a resource's command or script cleanup: one line of JSON, in the form `--params` takes. `--field` prints a top-level string as text and other fields as JSON, followed by a newline. A resource with no value returns `null`. |

## The running task's own context: `task_ctx`

`task_ctx` is the running task's own context, read inside its function while it runs:

| | |
|---|---|
| `task_ctx.task_id` | The task's ID. |
| `task_ctx.try_number` | The attempt's try number, from 1 (retries count). |
| `task_ctx.map_index` | Which instance of a [mapped task](10-mapping.md) this is, from 0; `None` for a task that isn't mapped. |

Unlike `dag_ctx`, it has no references: reading it in the DAG body, or anywhere outside a
running task, raises `RuntimeError`. A test sets it with `task_context(task_id=...,
try_number=..., map_index=...)` ([Testing a DAG](07-testing.md#one-task-and-its-context)).
A declared secret is `task_ctx.secret(...)` or `task_ctx.secret_value(...)`
([Secrets](13-secrets.md)). In an async task or wait step, `await task_ctx.asecret(...)` and
`await task_ctx.asecret_value(...)` do the same without blocking the event loop. Tasks can
also use [connections](14-connections.md) and register [cleanup handles](15-cleanup.md);
`await acleanup_handle(...)` registers a handle without blocking the loop. Resource
values for command and script setups and cleanups use `schedr task-ctx resource` above.
