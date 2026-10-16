# schedr: Project Decisions

This file records what has been decided about schedr. Open questions are listed at the end
and move up into the decided sections once settled.

## Goal

A job scheduler written primarily in **Rust** that runs three kinds of work:

1. **Python scripts**: a `.py` file run as a subprocess.
2. **Bash processes**: shell commands or scripts.
3. **Python DAGs**: workflows written in Python, either with schedr's own native API or with
   a compatibility API that copies another orchestrator's API (starting with Airflow) but runs
   on the schedr scheduler.

## Deployment target

- **Distributed system running on Kubernetes.** The production target is **OpenShift**, but
  schedr integrates with **plain Kubernetes** only: no OpenShift-specific APIs or resources.
- A **single master** process (one pod) and **long-running worker** pods. Master high
  availability is not needed for now.
- **No pod-per-task execution.** It isn't planned and probably never will be: too much
  overhead, and schedr can't count on permission to create pods. Tasks run as processes
  inside the long-running worker pods.
- Remaining architecture details are open (see Open questions).

### Constraints that follow

- **Minimal Kubernetes permissions.** schedr must not need to create pods or hold
  cluster-wide permissions; ideally it doesn't call the Kubernetes API at all.
- **Images must run under an arbitrary UID** (OpenShift's default restricted security policy;
  also good practice on plain Kubernetes): no fixed user, writable paths group-owned by root
  (group 0).

## State storage

- **Production: PostgreSQL.** On OpenShift it is provisioned and managed by an operator.
  schedr only needs a connection string; it doesn't manage the database itself.
- **Local development: SQLite.**
- All persistent state lives in the database, so the master can restart at any time without
  losing anything.

## Python API structure

schedr's Python package has two kinds of API:

- **Native API**: schedr's own DAG authoring API, at the top level of the `schedr`
  package. Its design is still open (see Open questions).
- **Compatibility APIs**: look-alikes of other orchestrators' APIs, each in its own
  subpackage named after the orchestrator. `schedr.airflow` is the first. Others may be added
  later (e.g. `schedr.<other>`).

```
schedr/                 # native API
schedr/airflow/         # Airflow 3 look-alike API
schedr/<other>/         # future compatibility APIs
```

All APIs describe DAGs in terms of the same underlying model, so the Rust scheduler handles
them identically. A compatibility API is a translation layer onto that model, not a separate
engine.

## Airflow compatibility

### Approach: look-alike API under the `schedr.airflow` namespace

- schedr is **not** Airflow and does not depend on `apache-airflow`.
- The Python API **copies Airflow's names, signatures and module layout** so existing DAG
  code needs as few changes as possible. **Behaviour underneath is entirely ours.**
- Migrating a DAG = **changing its imports only**. Everything below the imports stays the same.

### Reference version: Airflow 3

Module paths mirror **Airflow 3** (the `airflow.sdk` Task SDK and the `standard` provider).
Migration is a mechanical rewrite of the import prefix, `airflow.` → `schedr.airflow.`:

| Airflow 3                                     | schedr                                               |
|-----------------------------------------------|------------------------------------------------------|
| `airflow.sdk`                                 | `schedr.airflow.sdk`                                 |
| `airflow.providers.standard.operators.bash`   | `schedr.airflow.providers.standard.operators.bash`   |
| `airflow.providers.standard.operators.python` | `schedr.airflow.providers.standard.operators.python` |
| `airflow.providers.standard.operators.empty`  | `schedr.airflow.providers.standard.operators.empty`  |

Airflow 2 import paths (`airflow.operators.*`, `airflow.decorators`) are **not** mirrored.

Example:

```python
from datetime import datetime, timedelta

from schedr.airflow.sdk import DAG, task
from schedr.airflow.providers.standard.operators.bash import BashOperator
from schedr.airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="example",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["demo"],
):
    start = EmptyOperator(task_id="start")
    extract = BashOperator(task_id="extract", bash_command="./extract.sh")

    @task
    def transform() -> None: ...

    start >> extract >> transform()
```

### Scope: Tier 1 (DAG authoring)

| Area               | Included                                                                                   |
|--------------------|--------------------------------------------------------------------------------------------|
| `schedr.airflow.sdk` | `DAG` (context manager), `@dag`, `@task`, `@task.bash`, `chain`, `TriggerRule`             |
| `DAG` arguments    | `dag_id`, `schedule` (cron, `@daily`-style presets, `timedelta`, `None`), `start_date`, `end_date`, `catchup`, `default_args`, `tags`, `description`, `max_active_runs` |
| Operators          | `BashOperator`, `PythonOperator` (`python_callable`, `op_args`, `op_kwargs`), `EmptyOperator` |
| Per-task arguments | `task_id`, `retries`, `retry_delay`, `execution_timeout`, `trigger_rule`                   |
| Dependencies       | `>>`, `<<`, `set_upstream` / `set_downstream`, `chain()`, lists on either side             |

Out of scope for now (Tier 2/3): Jinja templating, task context (`ds`, `ti`, …), Variables,
Params, Connections/hooks, TaskGroups, dynamic task mapping, branching, sensors, Assets,
callbacks, pools.

## Repository and tooling

### Layout

```
Cargo.toml              # Cargo workspace; shared lints
rust-toolchain.toml     # stable + rustfmt + clippy
crates/
  schedr-core/          # scheduling engine (library); unit tests inline, integration in tests/
  schedr/               # scheduler binary
pyproject.toml          # Python package
python/
  schedr/               # Python API package (typed: py.typed)
  tests/{unit,integration}/
Makefile                # setup / fmt / fmt-check / lint / test / check
.pre-commit-config.yaml
.github/workflows/ci.yml
```

### Rust

- Edition 2024, stable toolchain pinned via `rust-toolchain.toml`.
- `rustfmt` (max width 100).
- `clippy` with `all` and `pedantic` lint groups at warn; CI fails on any warning (`-D warnings`).
- `unsafe_code = "forbid"`, `missing_docs = "warn"`.

### Python

- Supported versions: Python 3.11+. CI tests 3.11–3.14. Local development uses 3.14.
- Dependency management: **uv** (lockfile `uv.lock`, dev deps in `[dependency-groups]`).
- Build backend: **hatchling** for now (to be revisited when the Rust/Python boundary is
  decided; maturin if Rust is compiled into the Python package).
- `ruff` handles formatting and linting with a broad rule set; `mypy` runs in strict mode;
  `pytest` runs the tests with coverage. Tests that spawn processes are marked `integration`.
- Local environment: pyenv-virtualenv selected by `.python-version` (git-ignored). The Makefile
  points uv at that env via `pyenv prefix`. Pre-commit hooks call `ruff`/`mypy` directly.

### Workflow

- `make check` runs everything CI runs: format check, clippy, ruff, mypy, cargo test, pytest.
- Pre-commit hooks run the same checks on commit.
- CI: GitHub Actions, one Rust job and a Python version matrix.

## Open questions

1. **Native Python API design**: shape of the top-level `schedr` API (how close to Airflow
   it is, and what it does differently).
2. **Passing task return values (XCom).** Should be a feature of the shared model, with the
   Airflow layer mapping onto it. Proposed mechanism: a task writes its return value as JSON to
   a result file (not stdout); it is stored per (DAG run, task, key); Rust passes stored values
   to downstream tasks as inputs. Sub-questions, each with the proposal:
   - a. Format: JSON plus `datetime`, `date`, `timedelta`, `tuple`; no pickle.
   - b. `multiple_outputs=True` and `out["key"]`: support.
   - c. `PythonOperator` return value and `.output`: support.
   - d. `BashOperator` pushes the last line of stdout by default, like Airflow: support.
   - e. `ti.xcom_pull(...)` and custom keys: leave out (Tier 2).
   - f. Size limit: about 1 MB per value; the task fails clearly if exceeded.
3. **Unsupported Airflow arguments** (`pool`, `sla`, callbacks, …): proposed: accept and log a
   warning for real Airflow parameters; raise an error for unknown ones.
4. **Templating strings** (`{{ ds }}`): proposed: raise an error when a templated field contains
   `{{`, since Tier 1 has no Jinja.
5. **Rust/Python integration.** Proposed:
   - Parse DAGs: a Python subprocess on a worker imports the DAG file and outputs JSON; the
     worker agent sends it to the master.
   - Run tasks: one Python subprocess per task inside the worker pod.
   - Runtime communication: Python talks only to its local Rust agent (files or Unix socket);
     the agent talks to the master over gRPC.
   - Shared model: Rust types are the reference; Protobuf for gRPC and a generated JSON Schema
     for the DAG format, which Python tests validate against.
   - Packaging: container images (master, worker base) and a Helm chart; the Python package is
     on pip for writing DAGs and local development.
   - Rejected: embedding Python in the scheduler (PyO3), because user-code crashes would take
     the scheduler down and one Python environment would be forced on every DAG.
   - Single binary with roles `schedr master`, `schedr worker`, `schedr standalone` (local
     development, with SQLite).
6. **Interface:** CLI only, REST API, web UI.
7. **Kubernetes architecture details**:
   - Master–worker protocol. Proposed: workers connect out to the master over gRPC and claim
     tasks under time-limited leases; the master never pushes to workers.
   - Where DAGs are parsed. Proposed: on worker pods (they have the user's dependencies); the
     master never runs user code.
   - How DAG code reaches the pods: baked into the image, git-sync sidecar, shared volume, or
     versioned bundles.
   - Workload shape: mostly long tasks (seconds to hours) or many tiny ones (milliseconds)?
     Tiny tasks would justify warm Python worker processes sooner.
   - Do different DAGs need conflicting Python dependencies? Proposed: separate worker pools
     (different images), with tasks routed to them via `queue=`.
