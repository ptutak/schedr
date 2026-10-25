# Testing a DAG

Three ways, from the narrowest: call the function, call it with a run's context, or run
the DAG. `schedr dags check` and `schedr dags test` need no server. The commands are in
the [CLI reference](../_generated/04-reference/02-cli.md).

`examples/test_daily_report.py` does all three for [the report DAG](06-context.md):

```{literalinclude} ../../examples/test_daily_report.py
:language: python
:caption: examples/test_daily_report.py
```

## `schedr dags check`

`schedr dags check <path>` imports the DAG files with the runner and reports what they
define. A directory is scanned like a DAG root, honouring `.schedrignore`. A file is
checked alone.

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

`schedr dags check --against` is accepted by the argument parser and then reports that it
is not implemented yet. Comparing a file with a server is
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
`outcome` is `SUCCESS`. Datetimes, dates, durations and tuples in `output` use the tags
in [What a task may return](02-tasks.md#what-a-task-may-return).

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
| `state` | Where the task ended, such as `success`, `failed`, `upstream_failed`. |
| `outcome` | `SUCCESS` when it succeeded. |
| `try_number` | The last try. `0` if it never ran. |
| `output` | The output decoded with the task's return type. A command's is a `ProcessResult`. An empty task's is `None`. |
| `output_json` | The output as stored. |
| `log` | Every attempt's log. |
| `attempts` | `attempt_id`, `try_number`, `status`, `exit_code`, `error_kind`, `error`, `started_at`, `ended_at`, `duration`. |

`duration` is what the worker measured. It is `None` for an attempt the master ended.
`error` is the failure summary (`exception`, `message`, `traceback`).

`RunDagError` means the run never started: no `schedr` binary, an unknown DAG, or a file
that does not import. The binary is `$SCHEDR_BIN`, or else the `schedr` installed with
this Python, or else `schedr` on `PATH`. Unless
`SCHEDR_CONFIG__INTERPRETERS__PYTHON` is already set, the tasks run with this Python.

`params`, `secrets`, `step` and `replace` raise `NotImplementedError` (milestone M4).
Those, and `schedr dags test --task`, are
[Testing DAGs](../_generated/05-design/08-testing-dags.md).
