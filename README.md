# schedr

An open-source job scheduler written in Rust for running Python scripts, Bash processes, and Python DAGs.
DAGs are defined using schedr's own Python API or an Airflow compatibility layer.

```python
from datetime import timedelta

from schedr import CommandOperator, Cron, Dag, TaskDefaults, dag_ctx, task

with Dag(
    "sync_customers",
    trigger=Cron("0 3 * * *", tz="Europe/Warsaw", start="2026-01-01"),
    defaults=TaskDefaults(retries=2, retry_delay=timedelta(minutes=5)),
):

    @task
    def extract() -> list[dict[str, str]]:
        return [{"id": "1", "name": "Ada"}, {"id": "2", "name": "Grace"}]

    @task(timeout=timedelta(minutes=10))
    def load(rows: list[dict[str, str]]) -> int:
        print(f"loading {len(rows)} customers")
        return len(rows)

    rows = extract()  # Output[list[dict[str, str]]]
    count = load(rows)  # load depends on extract (data flow)

    CommandOperator(  # depends on load: its command uses `count`
        "notify",
        command=["echo", t"synced {count} customers for {dag_ctx.run.planned_for}"],
    )
```

(`examples/sync.py`; Python 3.14, for the t-string.)

## Status

Under development; nothing is released yet. **M1** (standalone runs a native DAG) and most of
**M2** (distributed) are done; next are the fork server and quiet DAGs (M2.5), the
web UI (M3) and the rest of the native API (M4: secrets and connections, sensors, branching,
`ScriptOperator`, ...). The Airflow layer is M5. See `PLAN.md` and the roadmap in
`PROJECT.md`.

What works today:

- **The native API:** `Dag`, `@task` (Python), `CommandOperator`, `EmptyOperator`, data flow
  between tasks, `>>` and `chain`, `Cron` / `Every` / `Once` and presets, `TaskDefaults`
  (retries, timeouts, queues), `dag_ctx`.
- **Testing DAGs:** `schedr dags check`, `schedr dags test`, and `schedr.testing`
  (`run_dag`, `task_context`) for pytest.
- **One process or separate roles:** `schedr standalone` (master, webserver and one worker,
  SQLite or Postgres; for production in one pod too), or `schedr master`, `schedr worker` and
  `schedr webserver` as separate processes on Postgres, with mTLS between them.
- **Surviving failures:** workers keep running tasks while the master restarts; a worker's
  journal survives its restarts; drains on SIGTERM; a standby master takes over through a
  lease in the database; retries by rule (see the guide, "State and failures").
- **Operations:** the REST API (`/api/v1`, Swagger UI at `/api/v1/docs/`) used by the CLI
  (`dags`, `runs`, `tasks`, `workers`); task logs through the webserver; a memory watchdog
  per task; `/healthz`, `/readyz` and Prometheus `/metrics` on an ops port.
- **Kubernetes:** images (`schedr` with Python for workers and standalone, `schedr-server`
  without Python for the master and webserver) and a Helm chart (`charts/schedr`: standalone
  or distributed, git-sync or DAGs from values, Istio, Vault), tested on kind.

There is no web UI yet (M3); the API and the CLI cover what it will show.

## Documentation

`make docs` builds the site into `site/` (`make docs-serve` serves it at
http://localhost:8765). Its sections:

1. **Overview** (this file).
2. **Writing DAGs** (`docs/02-dags/`): the native API, with tested examples.
3. **How schedr works** (`docs/03-guide/`): the running system, failures, deployment.
4. **Reference**, generated from the code: configuration, CLI, REST API, metrics.
5. **Design**: `PROJECT.md`, the decision log, one page per chapter.
6. **Plan**: `PLAN.md`.
7. **API**: Python (from docstrings) and Rust (rustdoc).

## Trying it

```sh
make standalone   # schedr standalone on the DAGs of examples/, SQLite in .schedr/, API on :8080
```

Then, from another shell: `schedr dags list`, `schedr dags unpause sync_customers`,
`schedr runs trigger sync_customers`, `schedr runs list sync_customers` (the CLI talks to
`http://localhost:8080` unless `--server` or `SCHEDR_SERVER` says otherwise). `DAGS=<directory>` runs other DAGs;
`LISTEN=127.0.0.1` keeps the API off the network (it has no authentication yet).

## Layout

```
crates/
  schedr-model/     # shared model: DAG JSON, states, JSON Schema
  schedr-proto/     # Protobuf + gRPC, master <-> worker agent
  schedr-config/    # configuration loading and validation
  schedr-db/        # schema, migrations, queries (SQLite, Postgres)
  schedr-master/    # scheduling loop, DAG validation, commands, the lease
  schedr-agent/     # worker agent: processes, journal, memory watchdog, log endpoint
  schedr-api/       # webserver: REST API, Swagger UI, log proxy
  schedr-tls/       # mTLS on the internal links: certificates, reloading, handshakes
  schedr/           # the binary: roles, user CLI, in-task CLI
    src/<module>.rs         # code
    src/<module>/tests.rs   # its unit tests
    tests/                  # integration tests; tests/stress/ is the stress suite
  xtask/            # development tasks: schema, the docs' generated pages
python/
  schedr/           # the Python package: the native API, the runner, schedr.testing
  tests/unit/schedr/    # unit tests mirroring the package: test_<module>.py
  tests/integration/
examples/           # example DAGs with their tests; examples/docs/ for the docs' examples
schema/             # generated from schedr-model, committed (make schema)
docs/               # the hand-written pages (02-dags, 03-guide, 05-design); the rest generated
charts/schedr/      # the Helm chart
images/             # Dockerfiles: schedr (with Python), schedr-server (without)
scripts/            # smoke tests; kind-*.sh cluster checks (standalone, mesh, distributed, kill)
```

Design decisions: `PROJECT.md`. Implementation plan: `PLAN.md`. Open
bugs: `BUGS.md`.

## Development

Requirements: Rust (stable, via rustup), [uv](https://docs.astral.sh/uv/), make; Podman or
Docker for the integration tests (Postgres) and the images; kind, kubectl and Helm for the
cluster scripts. `make setup` also installs `cargo-nextest`, `cargo-llvm-cov` and the
`llvm-tools-preview` component.

```sh
make setup            # dev tools, uv sync, pre-commit hooks
make fmt              # format Rust + Python
make lint             # clippy, ruff, mypy
make test             # unit tests: Rust (nextest) + pytest, both with an 85 % coverage floor
make test-integration # real processes, the binary, Postgres in a container
make schema           # regenerate schema/ from the Rust model (CI fails if it's stale)
make docs             # build the documentation site into site/ (fails on any warning)
make check            # everything CI runs
make stress           # the stress suite (STRESS_PROFILE=quick|full)
make smoke            # build the release wheel and smoke-test it
make image            # the schedr image (make server-image: schedr-server), smoke-tested
make chart-check      # lint and render the Helm chart
make help             # every target
```
