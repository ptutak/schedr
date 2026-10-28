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
define. It starts a new process for that import. A worker's parse job can be forked; this
command is not. A directory is scanned like a DAG root, honouring `.schedrignore`. A file
is checked alone.

```
ok     helpers.py: no DAGs
ok     sync.py: sync_customers (3 tasks)
2 file(s), 1 DAG(s), 0 error(s)
```

`ok` is followed by five spaces. A file that defines several DAGs lists them on its line,
separated by commas. A file that fails to import prints `error`, two spaces, the path,
the exception type and the message, then the traceback indented under the path. The
summary counts that file, and the process exits 1. Exit 0 means every file imported.
Exit 2 is wrong usage.

`--json` prints `{"files": [{"path", "dags", "error"}]}`. `dags` is the list of DAG IDs.
`error` is `null` or an object with `exception`, `message` and `traceback`.

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

`schedr dags test <dag> [--for TIME] [--path PATH]` runs one real run in a temporary
standalone: its own SQLite database, and every task in its own process. `--path` is a
file or a directory. Without it, the command uses the configured DAG roots, or the
current directory.

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
error names the files that failed to import, when there are some). The command waits up
to an hour; a task's own `timeout` stops that task sooner.

`--json` prints the run, the file that defined the DAG (`location`), and each task's
`state`, `outcome`, `try_number`, `output`, `attempts` and `log`. A successful task's
`outcome` is the outcome name (`SUCCESS` when the task has no outcomes of its own). A
skipped or failed task's is `null`; the table prints `-` for those. Datetimes, dates,
durations and tuples in `output` use the tags in
[What a task may return](02-tasks.md#what-a-task-may-return).
[Routing by outcome](08-outcomes.md) is what the `OUTCOME` column shows.

`-p` / `--param`, `--secret` and `--task` are accepted and then report that they are not
implemented yet (milestone M4). Run parameters, secrets and running one task are
[Testing DAGs](../_generated/05-design/08-testing-dags.md).

## One task, and its context

Outside `with Dag(...)`, a `@task` function is the function. `extract()` in
`examples/test_sync.py` returns the list, not an `Output`.

`with task_context(...):` makes `dag_ctx` return values for the block, with no process.

| Argument | Default |
|---|---|
| `planned_for` | now, UTC |
| `since` | `planned_for`, so the window is empty |
| `dag_id` | `"test"` |
| `run_id` | a new UUIDv7 |
| `kind` | `"manual"` (`scheduled`, `manual`, `backfill`, `chained`, `event`) |

Both times must be aware datetimes, and `since` must not be after `planned_for`.
`params` and `secrets` raise `NotImplementedError` (milestone M4).

## `run_dag`

`run_dag(path, dag_id, *, for_=None)` runs `schedr dags test --json` and returns a
`RunResult` whether the run succeeded or failed. `for_` is `--for` and must be aware.

| `RunResult` | |
|---|---|
| `run_id`, `dag_id`, `kind` | The run. `kind` is `manual`. |
| `state` | `success`, `failed` or `cancelled`. |
| `planned_for`, `since` | The window. |
| `tasks` | A `TaskResult` per task ID. |

| `TaskResult` | |
|---|---|
| `state` | Where the task ended, such as `success`, `failed`, `skipped`, `upstream_failed`. |
| `outcome` | The outcome name when it succeeded (`SUCCESS` when the task has no outcomes of its own). |
| `try_number` | The last try. `0` if it never ran. |
| `output` | The output decoded with the task's return type. A command's is a `ProcessResult`. An empty task's is `None`. |
| `output_json` | The output as stored. |
| `log` | Every attempt's log. |
| `attempts` | `attempt_id`, `try_number`, `status`, `exit_code`, `error_kind`, `error`, `started_at`, `ended_at`, `duration`. |

`duration` is what the worker measured. It is `None` for an attempt the master ended.
`error` is the failure summary (`exception`, `message`, `traceback`).
The JSON for each attempt also has `start_method` (`spawn`, or `fork`), the same field as
`GET /api/v1/runs/<run id>/tasks/<task id>/attempts`. It is `null` until the worker's
`Started` is stored. The `Attempt` class doesn't carry it.

`RunDagError` means the run never started: no `schedr` binary, an unknown DAG, or a file
that does not import. The binary is `$SCHEDR_BIN`, or else the `schedr` installed with
this Python, or else `schedr` on `PATH`. Unless
`SCHEDR_CONFIG__INTERPRETERS__PYTHON` is already set, the tasks run with this Python.

`params`, `secrets`, `step` and `replace` raise `NotImplementedError` (milestone M4).
Those, and `schedr dags test --task`, are
[Testing DAGs](../_generated/05-design/08-testing-dags.md).
