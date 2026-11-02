# Changelog

Each release's section is its release notes on GitHub (`scripts/release.py notes`). Until
1.0.0 schedr is a preview: the database schema and the protocol between its processes are
changed in place, so moving from one 0.x version to the next means a fresh database
(PROJECT.md, decision 156).

## 0.1.0

The first release, a preview (`Development Status :: 3 - Alpha`). **No upgrade path:** 0.2
will need a fresh database; the schema and the protocol change in place until 1.0.0.

Install with `pip install schedr` (Python 3.14; wheels for Linux x86_64 with glibc or musl,
Linux aarch64 and macOS arm64). The images are `ghcr.io/ptutak/schedr-server` (master,
webserver and UI, no Python) and `ghcr.io/ptutak/schedr-worker` (with Python, the base for
your own worker image), x86_64.

What it has (milestones M1 to M4.5 in PLAN.md):

- **DAGs in Python:** `Dag`, `@task`, `CommandOperator`, `EmptyOperator`; data flow between
  tasks, `>>` and `chain`; `Cron`, `Every`, `Once` and presets; `TaskDefaults` (retries,
  timeouts, queues); typed outputs (dataclasses, Pydantic models with `schedr[pydantic]`),
  outcomes and routing on them (`.on(...)`, `@branch`), task groups, mapping
  (`.partial(...).expand(...)`), concurrency limits.
- **Run parameters:** `Dag(params=...)` from a dataclass or a model, checked by the master
  before a run exists, given with `runs trigger -p` / `--params`, the API or the UI's
  Trigger form, stored with the run and masked where they hold secrets; `dag_ctx.params` in
  a task, `schedr dag-ctx` in a command, `schedr.script` in a plain script.
- **Testing DAGs:** `schedr dags check`, `schedr dags test`, and `schedr.testing`
  (`run_dag`, `task_context`) for pytest.
- **Running:** `schedr standalone` (master, webserver, UI and a worker in one process, SQLite
  or Postgres), or `schedr master`, `schedr worker`, `schedr webserver` and `schedr ui` as
  separate processes on Postgres with mTLS; a standby master taking over through a lease;
  workers that keep running tasks while the master restarts, and drain on SIGTERM; a fork
  server for fast task starts.
- **Web UI:** DAGs, runs with their graph, task logs and outputs live, actions (trigger,
  cancel, pause, retry, drain) with confirmations, import errors, workers, the deployments
  graph; several deployments in one UI; sign-in by proxy header or basic auth.
- **Operations:** the REST API (`/api/v1`, OpenAPI) and the CLI over it; Prometheus metrics,
  `/healthz` and `/readyz`; a memory watchdog per task; secrets masked in logs, outputs and
  configuration views; a Helm chart in the repository (`charts/schedr`, not published).
