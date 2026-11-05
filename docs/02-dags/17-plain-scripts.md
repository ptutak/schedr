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
- `queue`, `retries`, `retry_delay`, `timeout`, `max_output_size`, `description`, `tags`.

`args` is a list of strings; `env` maps variable names to strings or secret references, and
`secrets` declares secrets available to the task. `working_dir` defaults to the directory
containing `schedr.toml`. Durations use strings such as `"1h30m"` and sizes use strings such as
`"4MiB"`. Give at most one of `schedule` (cron or a preset), `every` (a duration), and `once`
(a TOML date or datetime). The default timezone is UTC, `catchup` is false and
`max_active_runs` is 1. With `once`, `start`, `end` and `catchup` are errors.

Unknown keys are errors. `params`, `outcomes`, `cleanup`, `cleanup_handles` and `notify`
are not available yet and are errors too.

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

```{note}
Workers cannot run script tasks yet. `ScriptOperator` comes next in M4.9b.
```
