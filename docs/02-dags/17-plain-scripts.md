# Plain scripts

Plain Python and Bash scripts are declared in `schedr.toml` files under script roots,
separate from DAG roots. Configure the directories as a colon-separated path list in
`SCHEDR_SCRIPT_PATH` (also `SCHEDR_CONFIG__SCRIPTS__PATH`). Each entry is a root, named
by its last path component by default. Rename an entry in the configuration file:

```toml
[scripts.roots]
etl = "/git/scripts/etl-repo"
```

A rename must refer to an entry already in `SCHEDR_SCRIPT_PATH`; it does not add a directory.
Repeated or nested script root entries, duplicate root names, and a name shared by a script
root and a DAG root are startup errors. Rename a root to resolve a name conflict. Script roots
are scanned recursively; `.schedrignore` applies as it does to DAG roots.

## Declaring scripts

A script is recognized only when a `schedr.toml` inside a script root declares it. There can
be many such files, at most one per directory, and each can declare several jobs or declare the
same script more than once. Paths are relative to the `schedr.toml`: absolute paths and `..`
are rejected, so written paths stay in its directory or below. Symlinks are allowed, including
links outside that directory. Different configuration files can declare the same script.

```toml
[jobs.report_daily]
script = "reports/daily.py"
interpreter = "etl-venv"
args = ["--region", "eu"]
env = { REPORT_MODE = "full", DB_PASSWORD = { secret = "etl/db#password" } }
secrets = ["etl/api#token"]
working_dir = "reports"
schedule = "0 3 * * *"
timezone = "Europe/Warsaw"
retries = 2
retry_delay = "5m"
timeout = "30m"
description = "Daily report"
tags = ["reports"]
```

`script` and `interpreter` are required; the script must exist in the scanned root and the
interpreter names an `[interpreters]` entry. Job names contain only ASCII letters, digits,
`_` and `-`. The accepted keys are:

- `script`, `interpreter`, `args`, `env`, `secrets`, `working_dir`;
- `schedule`, `every`, `once`, `timezone`, `start`, `end`, `initial_since`, `catchup`,
  `max_active_runs`;
- `queue`, `retries`, `retry_delay`, `timeout`, `max_output_size`, `description`, `tags`;
- `params`, `outcomes`, `cleanup`, `cleanup_handles`.

`args` is a list of strings; `env` maps variable names to strings or secret references, and
`secrets` declares secrets available to the task. `working_dir` defaults to the directory
containing `schedr.toml`. Durations use strings such as `"1h30m"` and sizes use strings such as
`"4MiB"`. Give at most one of `schedule` (cron or a preset), `every` (a duration), and `once`
(a TOML date or datetime). The default timezone is UTC, `catchup` is false and
`max_active_runs` is 1. With `once`, `start`, `end` and `catchup` are errors.

Unknown keys are errors. A job’s `notify` is refused: notifications belong to the DAG; use `Dag(notify=...)` as in [Notifications](24-notifications.md).

### Parameters, outcomes and cleanup

```toml
[jobs.report_daily]
script = "reports/daily.py"
interpreter = "etl-venv"
cleanup_handles = ["Publication"]

[jobs.report_daily.params]
region = "eu"
limit = 10

[jobs.report_daily.outcomes]
SUCCESS = 0
NO_DATA = 3

[jobs.report_daily.cleanup]
script = "reports/undo.sh"
interpreter = "bash"
args = ["--report"]
retries = 2
retry_delay = "30s"
anywhere = false
```

Parameter types come from their defaults: strings, integers, floats, booleans, dates,
datetimes, lists of one type and nested tables are accepted. A datetime without an offset
uses the job's timezone. Empty or mixed lists, empty tables and times without a date are
refused; parameter names use letters, digits and `_`. A trigger replaces a nested table as a
whole. Parameters belong only to a standalone job; a script used in a DAG reads that DAG's
parameters instead. A Python script reads them with `from schedr.script import dag_ctx` and
`dag_ctx.params.region`; a script can also call `schedr dag-ctx params get --field region`.

`outcomes` maps names to distinct exit codes from 0 to 255. The declared codes count as
successful outcomes. Names use letters, digits and `_` and cannot start with a digit.

The cleanup runs after each started attempt, including a successful one. It uses the task's
environment and working directory. Besides `script`, `interpreter` and `args`, it accepts
`timeout`, `retries`, `retry_delay` and `anywhere`. To limit the handle types a task can
register, set `cleanup_handles` in `[jobs.report_daily]` as above. The task and cleanup can use
the worker's context channel:

```sh
schedr task-ctx cleanup add Publication id="$id"
schedr task-ctx cleanup list Publication --field id
```

Without `cleanup_handles`, any handle type is accepted; the key requires a cleanup. See
[Cleaning up after a task](15-cleanup.md) for handles and retries.

A scheduled declaration is a standalone one-task DAG, paused when first found. Its ID is
`<root>:<path to schedr.toml>#<job name>` and its task is named after the job: for example,
`etl:team_a/schedr.toml#report_daily` has the task `report_daily`. It can also be triggered
manually. In an API URL, percent-encode the ID as a path component, for example
`etl%3Ateam_a%2Fschedr.toml%23report_daily`.

Without `schedule`, `every` or `once`, a declaration is a script for DAGs only: it creates
no DAG and is not listed among DAGs. The DAG-level keys `timezone`, `start`, `end`,
`initial_since`, `catchup` and `max_active_runs` are errors in that case.

## Checking declarations

Run `schedr dags check path/to/schedr.toml` to check one file, or
`schedr dags check path/to/script-root` to check its `schedr.toml` files recursively. A
file named explicitly is checked even if `.schedrignore` excludes it. Scripts are never
imported. The check warns if an interpreter is absent from the local configuration: a
worker without it rejects the job's tasks.

## Running scripts on workers

The worker runs the executable named by the declaration's `[interpreters]` entry, passing
it the script's absolute path in the script root's copy and then each `args` element as one
argument. It runs in `working_dir` (relative to the declaration). By default, `PYTHONPATH`
starts with the script root's copy, so a Python script can import a library in that root;
if the worker passes its own `PYTHONPATH` through, that follows the copy. An explicitly
declared `PYTHONPATH` takes precedence.

The agent sets `SCHEDR_DAG_ID`, `SCHEDR_RUN_ID`, `SCHEDR_RUN_KIND`, `SCHEDR_TASK_ID`,
`SCHEDR_TRY_NUMBER`, `SCHEDR_PLANNED_FOR` and `SCHEDR_SINCE`; mapped tasks also get
`SCHEDR_MAP_INDEX`. Times are ISO 8601. A worker lacking the interpreter, the script root
or the script file rejects the assignment, so it can be tried on another worker; a script
gone from the code the task expects fails the task, as does a script or `working_dir`
leaving its script root. A worker also rejects the task if the cleanup script or its
interpreter is missing there. A moved or recovered cleanup needs only the cleanup script
and its interpreter, not the task's interpreter.

## Using a script in a DAG

```python
from schedr import Dag, ScriptOperator

with Dag("reports"):
    report = ScriptOperator(
        "report",
        job="etl:team_a/schedr.toml#report_daily",
        args=["--region", "west"],
        env={"REPORT_MODE": "incremental"},
        secrets=["etl/other#token"],
    )
```

`job=` names a declaration as `<root>:<path to schedr.toml>#<job>`, or as an absolute
path to a `schedr.toml` inside a script root followed by `#<job>`. A reference cannot leave
its root with `..`. During a DAG parse the agent checks the reference and declaration; a
missing root, file or job causes an import error naming the problem. Outside a parse, such
as in `schedr.testing` or a task importing its DAG file, the reference is only recorded.
Set `SCHEDR_SCRIPT_PATH` when running `schedr dags check` on DAG files to check references
before deployment.

`args=` replaces the declared list, `env=` merges by variable with the operator winning,
and `secrets=` adds to the declared secrets. Arguments and environment values can use
t-strings and output references. For task settings such as `queue`, `retries`,
`retry_delay`, `timeout` and `max_output_size`, the operator wins over the declaration,
which wins over DAG defaults. Declared outcomes apply without `outcomes=`. To route with
`.on(...)` or read `.outcome`, give `outcomes=` an `Outcome` enum; it replaces the declared
outcomes. Without an enum, only `DefaultOutcome.SUCCESS` can be routed on if declared.

`cleanup_timeout`, `cleanup_retries`, `cleanup_retry_delay` and `cleanup_anywhere` override
the declared cleanup's settings, which override DAG defaults. They require a declared
cleanup; the operator cannot replace its script. The output is a `ProcessResult`; use
`report.output`, or
`report.stdout` and `report.stderr` for its streams.
