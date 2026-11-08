# Testing a DAG

Three ways, from the narrowest: call the function, call it with a run's context, or run
the DAG. `schedr dags test` needs no server. `schedr dags check` needs one only with
`--against`. The commands are in
the [CLI reference](../_generated/04-reference/02-cli.md).

`examples/test_daily_report.py` does all three for [the report DAG](06-context.md):

```{literalinclude} ../../examples/test_daily_report.py
:language: python
:caption: examples/test_daily_report.py
```

## `schedr dags check`

`schedr dags check <path>` imports the DAG files with the runner and reports what they
define. Each file is imported in its own process, forked from the configured fork server when
usable, or spawned otherwise. A directory is scanned like a DAG root, and everything is read
from a copy filtered by `.schedrignore`, as workers do.
A named file or folder inside a configured root keeps that root and can import the other
configured roots; a named file excluded from the copy is an error. A folder check parses the
whole root to check its connections, but reports only files in the selected folder. A single
file is checked alone, without that root-wide connection check. A named special file is refused.

```
ok     helpers.py: no DAGs
ok     sync.py: sync_customers (3 tasks)
2 file(s), 1 DAG(s), 0 error(s)
```

`ok` is followed by five spaces. A file that defines several DAGs lists them on its line,
separated by commas. A file that fails to import prints `error`, two spaces, the path,
the exception type and the message, then the traceback indented under the path. The
summary counts that file, and the process exits 1. Files whose bytes don't contain `schedr`
are skipped, even when named alone. The text output prints `skipped <file>` and why;
skipped files aren't in the summary or `--json`. Exit 0 means every checked file imported.
Exit 2 is wrong usage.

A Python warning raised while a file is imported follows that file's line, as `warn`, three
spaces, the path, the warning's category and its message. A `TypingHint` always shows;
others as the interpreter's warning filters let them through. Warnings don't fail the check.

```
ok     fetch.py: fetch_rows (2 tasks)
warn   fetch.py: TypingHint: task `fetch`: outcome class `Rows` derives from neither schedr.Data nor schedr.pydantic.Model, so type checkers reject `.on(Rows)`; derive it from schedr.Data, or write `on(output, Rows)`
1 file(s), 1 DAG(s), 0 error(s)
```

`--json` prints `{"files": [{"path", "dags", "error", "warnings"}]}`. `dags` is the list of
DAG IDs. `error` is `null` or an object with `exception`, `message` and `traceback`.
`warnings`, a list of `Category: message` strings, is there only when the file raised any.

`schedr dags check --against <server URL>` reads that server's workers
(`GET /api/v1/workers`) after the file report. It fails, exit 1, when this `schedr` is
newer than any worker whose state is `active`. A worker drained for maintenance is
`active` and counts. Draining, disconnected, offline and lost workers take no new tasks,
so they are left out. Versions compare as `major.minor`; the patch is ignored.

When every counted worker is current or newer, one line follows the file report:

```
ok: this schedr is 0.3; 2 active workers at https://prod run 0.3 (w1, w2)
```

One worker says `1 active worker` and `runs`. Several versions are oldest first:
`run 0.3 (w1), 0.4 (w2)`.

It fails when any counted worker is older. The error names those workers, grouped by
version, oldest first, in the order the server returned:

```
error: this schedr is 0.3; older active workers at http://sched.example: 0.2: w2
```

Several versions: `0.1: w3; 0.2: w1, w2`.

A version that is not `major.minor` or `major.minor.patch` is unknown. The master stores
that as no version and still assigns tasks to the worker. This check follows that rule:
the worker stays eligible, and the check passes when no counted worker is older. The
passing line names the text as `unknown version`, with the worker id, and `workers` in
`--json` keeps the text the worker reported.

No active worker fails the same way. Right after a master restart every worker is
recorded disconnected until it registers again, so the message says to retry in a minute:

```
error: no active worker at http://sched.example to compare with this schedr (0.3): workers are recorded disconnected right after a master restart until they register again, so retry in a minute
```

A server that cannot be reached, or that rejects the call, exits 1 with that error after
the file report. The call is the same client as the other commands: the same credentials,
a 5 second connect timeout and a 30 second request timeout, so a server that never
answers stops with the usual connection error. A different major version is the API's
usual refusal, which names what to install. `--token` and `--user` are sent, as for
every server command (`SCHEDR_TOKEN`, `SCHEDR_USER`, `SCHEDR_PASSWORD`). `--server` is
not the URL this flag uses.

A worker on the same minor, or a newer one, does not fail the check. Pinning the local
package to the worker image (`schedr==1.4.*`) is still how you write DAGs against that
version.

`--json` adds `version_check` when the worker list was read: `local`, `ok`, `message`,
and `workers` (each `worker_id` and `schedr_version` as the worker reported it, every
active worker). `message` is the same line the text output prints (`ok: ...` on a pass,
`error: ...` on a failure). `ok` is false when this check fails. The decisions are in
[Testing DAGs](../_generated/05-design/08-testing-dags.md).

## `schedr dags test`

`schedr dags test <dag> [--for TIME] [--path PATH] [-p FIELD] [--params JSON]` runs one
real run in a temporary standalone: its own SQLite database, and every task in its own
process. `--path` is a file or a directory. A file is tested alone: only it is parsed,
though it can import the modules beside it. A folder inside a configured root parses all of
that root, whose connections its tasks can use, as a worker does. Without `--path`, the command uses the
configured DAG roots, or the current directory.

The run is manual. `--for` is its `planned_for`; omitted, it is now. The window follows
[Schedules](04-schedules.md#run-window). A `--for` in the future is rejected, and one
without an offset is read as UTC with a warning.

While the run goes, each task's state is printed as it changes, except `scheduled` and
`queued`. Then the report:

- each log line as `extract | loading 2 customers`
- a failed attempt as `extract | try 1 failed (failed):` and the traceback
- a table with the columns `TASK`, `STATE`, `OUTCOME`, `TRIES`, `DURATION`
- `outputs:`, and each task's output as stored JSON
- `run <id>: success` (or `failed`, or `cancelled`)

Exit 0 means the run succeeded. Exit 1 means it did not, or the DAG is unknown (the
error names the files that failed to import, when there are some), or a root can't be
scanned and copied as a worker would (``can't scan DAG root `etl`: reading …/rows.csv:
Permission denied``): every file of a root is copied, not only the `.py` files. The command waits up
to an hour; a task's own `timeout` stops that task sooner.

`--json` prints the run, the file that defined the DAG (`location`), and each task's
`state`, `outcome`, `try_number`, `output`, `attempts` and `log`. A successful task's
`outcome` is the outcome name (`SUCCESS` when the task has no outcomes of its own). A
skipped or failed task's is `null`; the table prints `-` for those. Datetimes, dates,
durations and tuples in `output` use the tags in
[What a task may return](02-tasks.md#what-a-task-may-return).
[Routing by outcome](08-outcomes.md) is what the `OUTCOME` column shows.

`-p` / `--param` and `--params` give the run's parameters, over the DAG's defaults, and are
checked the same way as `schedr runs trigger`
([Run parameters](12-params.md#giving-values-to-a-run)). `-p region=us` is text; `-p
full:=true` and `-p 'limits:=[1,2]'` are JSON. The two forms can't be combined. The report
does not print the parameters; `schedr runs show` does, for a run on a server.
`--secret 'path#key=value'` (repeatable) gives a secret's value, for every storage name
([Secrets](13-secrets.md#testing)). `--task` is accepted and then reports that it is not
implemented yet (milestone M4); running one task is
[Testing DAGs](../_generated/05-design/08-testing-dags.md).

## One task, and its context

Outside `with Dag(...)`, a `@task` function is the function. `extract()` in
`examples/test_sync.py` returns the list, not an `Output`.

For an async task, use `await task.fn(...)` in an async test. It is typed as the coroutine
it is, and the task's declarations remain in force. For a task declared `-> None`, `.fn`
returns `None` when awaited. `await task()` also runs outside a DAG, but type checkers flag
it: a task call is typed as its value for use in the DAG. Without an async test plugin,
use `asyncio.run(task.fn(...))` in a synchronous test.

A [wait step](18-sensors.md) is tested the same way: `await wait.fn(...)` (or
`asyncio.run(wait.fn(...))`) calls the function itself. For a poke, that is one call and
it may return `NOT_YET`. Use `run_dag` to run the wait and its routing for real.

`with task_context(...):` makes `dag_ctx` and `task_ctx` return values for the block, with
no process.

| Argument | Default |
|---|---|
| `planned_for` | now, UTC |
| `since` | `planned_for`, so the window is empty |
| `dag_id` | `"test"` |
| `run_id` | a new UUIDv7 |
| `kind` | `"manual"` (`scheduled`, `manual`, `backfill`, `chained`, `event`) |
| `task_id` | `"test"` (`task_ctx.task_id`) |
| `try_number` | `1` (`task_ctx.try_number`) |
| `map_index` | `None`, an unmapped task (`task_ctx.map_index`) |
| `params` | `None`. An instance of the DAG's params class (a dataclass or a Pydantic model). `dag_ctx.params(Cls)` then takes that class. A mapping is a `TypeError`. |

Both times must be aware datetimes, and `since` must not be after `planned_for`.
`secrets` is a mapping of `"path#key"` to values for `task_ctx.secret(...)`; a `@task`
function called in the block reads only what its `secrets=` declares
([Secrets](13-secrets.md#testing)). `params` does not ask the master: the block reads the
instance you give. The context manager yields an object whose `.handles` list contains the
dataclass instances registered with `cleanup_handle` in the block. The test can call the
[cleanup](15-cleanup.md) itself.

The block also gives `schedr.api` a fake API. Posts go into `values.posted_events` and
listing reads from that list:

```python
from schedr import api
from schedr.testing import task_context

with task_context() as values:
    api.post_event("paid", key="order-123", idempotency_key="pay-123")
    assert api.events(name="paid")[0].key == "order-123"
    assert values.posted_events[0]["name"] == "paid"
```

Reusing an idempotency key for another event raises `api.ApiError` with status 422; other
routes return 403. The fake does not need a master.

## `run_dag`

`run_dag(path, dag_id, *, for_=None, params=None)` runs `schedr dags test --json` and
returns a `RunResult` whether the run succeeded or failed. `for_` is `--for` and must be aware.

| `RunResult` | |
|---|---|
| `run_id`, `dag_id`, `kind` | The run. `kind` is `manual`. |
| `state` | `success`, `failed` or `cancelled`. |
| `planned_for`, `since` | The window. |
| `resources` | A `ResourceResult` per resource name (its setup task ID). |
| `tasks` | A `TaskResult` per task ID. A mapped task's is the task as a whole; `tasks["fetch"][1]` (or `.instances`) is one instance's. |

| `TaskResult` | |
|---|---|
| `state` | Where the task ended, such as `success`, `failed`, `skipped`, `upstream_failed`. |
| `outcome` | The outcome name when it succeeded (`SUCCESS` when the task has no outcomes of its own). |
| `try_number` | The last try. `0` if it never ran. |
| `output` | The output decoded with the task's return type. A command's is a `ProcessResult`. An empty task's is `None`. A mapped task's is the list of its instances' outputs (`None` for one skipped). |
| `output_json` | The output as stored. |
| `log` | Every attempt's log. |
| `attempts` | `attempt_id`, `try_number`, `status`, `exit_code`, `error_kind`, `error`, `started_at`, `ended_at`, `duration`. None for a mapped task as a whole (its instances have them). |
| `cleanup` | The task cleanup result across attempts: `ok`, `failed`, `worker_lost`, `not_needed`, or `None` before a cleanup ends or when there is none. |
| `map_index`, `instances` | An instance's index (`None` otherwise); a mapped task's instances (`None` for a task that isn't mapped). |

Each attempt's `cleanup` is a `Cleanup`, or `None` when it has no cleanup data. Its
`result` is `ok`, `failed`, `worker_lost`, `not_needed`, or `None` until it ends;
`tries`, `duration`, `error_kind` and `error` can also be `None`. `moves` counts moves to
another worker. `handles` is a tuple in registration order, as stored JSON objects with
`handle_type` and `value`, masked as task data; it is not decoded into dataclasses.

A `ResourceResult` has `setup`, `cleanup`, `users`, `blocking` and `state`. The state is
`setting_up`, `waiting_for_users`, `cleaning_up`, `cleaned`, `cleanup_failed`,
`cleanup_cancelled` or `not_needed`.

For example, using the DAGs shown in [Cleanup](15-cleanup.md) and
[Resources](16-resources.md):

```python
from schedr.testing import run_dag

result = run_dag("examples/docs/cleanup.py", "cleanup")
assert result.tasks["export"].cleanup == "ok"
cleanup = result.tasks["export"].attempts[-1].cleanup
assert cleanup is not None
assert cleanup.result == "ok" and cleanup.tries == 1 and cleanup.moves == 0
assert cleanup.handles[1]["handle_type"] == "ExportJob"
assert cleanup.handles[1]["value"]["job_id"] == "job-2026-11-04"

result = run_dag("examples/docs/resources.py", "cluster_job")
assert result.resources["work.cluster"].state == "cleaned"
```

`duration` is what the worker measured. It is `None` for an attempt the master ended.
`error` is the failure summary (`exception`, `message`, `traceback`).
The JSON for each attempt also has `start_method` (`spawn`, or `fork`), the same field as
`GET /api/v1/runs/<run id>/tasks/<task id>/attempts`. It is `null` until the worker's
`Started` is stored. The `Attempt` class doesn't carry it.

`RunDagError` means the run never started: no `schedr` binary, an unknown DAG, or a file
that does not import. The binary is `$SCHEDR_BIN`, or else the `schedr` installed with
this Python, or else `schedr` on `PATH`. Unless
`SCHEDR_CONFIG__INTERPRETERS__PYTHON` is already set, the tasks run with this Python.

`params` is an instance of the DAG's params class, or a mapping of field names to JSON
values (`{"region": "us"}`). It is sent as `--params`. An instance is encoded first, so a
date goes as `"2026-10-01"` and the instance's own defaults are included. A mapping is
sent as given, and the master fills the rest. The checks are the ones a trigger gets
([Run parameters](12-params.md#giving-values-to-a-run)). A refusal raises `RunDagError`
with the master's reason. `RunResult` does not carry the parameters. `secrets` gives
secret values by `"path#key"`, passed as `--secret` and masked in the logs and outputs as on
a worker. `step` and `replace` raise `NotImplementedError` (milestone M4). Those, and
`schedr dags test --task`, are [Testing DAGs](../_generated/05-design/08-testing-dags.md).
