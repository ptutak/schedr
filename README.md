# schedr

A job scheduler written in Rust that runs Python DAGs, Python scripts and shell commands: a
native Python API for DAGs, one binary for the scheduler, its web UI and its workers, SQLite on
a laptop and Postgres in production.

```python
from datetime import timedelta

from schedr import CommandOperator, Cron, Dag, StepDefaults, dag_ctx, task

with Dag(
    "sync_customers",
    description="Syncs customers daily at 03:00 Warsaw time: extract, load, notify.",
    trigger=Cron("0 3 * * *", tz="Europe/Warsaw", start="2026-01-01"),
    defaults=StepDefaults(retries=2, retry_delay=timedelta(minutes=5)),
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

**1.2.1 is the current release** (`pip install schedr`; `CHANGELOG.md`). 1.0.0, the first
stable one, brought M1 to M4B of `PLAN.md`: standalone and distributed, the fork server, the
web UI and the native API. 1.1.0 added M5.1: a faster master (34-67 % more tasks a second in
the stress suite than 1.0.0) and metrics in the web UI. 1.2.0 adds M5.2: a faster master again
(107-112 tasks a second in the stress suite's throughput scenario on CI's runner) and
extensions, task kinds run by a plugin in any language, with a Kubernetes plugin; 1.2.1 gives every connection between
its processes a time limit, and the chart hot standby masters. Within 1.x the database schema changes
only through new migrations (`schedr db migrate`), and the protocol between schedr's processes
(2.x) only additively, so the master is upgraded first; DAG JSON, the REST API and the
configuration keep accepting what they accept now. Upgrading from the 0.1.0 preview needs a
fresh database. Next come hot standby masters in the chart and extension tasks placed by the
plugins workers offer (M5.3), then an Airflow DAG translator (M6); see `PLAN.md`.

What works:

- **The native API:** `Dag`, `@task` (Python, sync or `async`), `CommandOperator`,
  `EmptyOperator`, data flow between tasks, `>>` and `chain`, `Cron` / `Every` / `Once` and
  presets, `StepDefaults` (retries, timeouts, queues, memory), `dag_ctx`; outcomes and
  `.on(...)`, `@branch`, `first_of`, task groups, mapping (`.partial` / `.expand`),
  concurrency limits, run parameters (`Dag(params=)`, a form in the UI); `schedr dag-ctx` and
  `schedr task-ctx` (secrets, connections, cleanup handles, resource values) for commands and
  plain scripts, over the agent's socket.
- **Waiting and starting:** wait steps (`@sensor`, `@sensor.poke` with an `async def` or a plain
  `def`, the `delay` and `until` sensors), triggers (`@trigger`, `@trigger.poke` with `batch=`) and named events through the
  API, `RunOf` to wait for or start on another DAG's run, `TriggerDagOperator`, backfills.
- **Secrets and connections:** `secrets[...]` in tasks and commands, from Vault (KV v2) or the
  database (`schedr secrets`, encrypted), masked in logs; `@connection` with typed clients
  (HTTP, SSH, SQL, Mongo, ...), given to commands as variables and files; `schedr connections
  check` / `test`.
- **Cleanups and resources:** a task's `cleanup=` run after each attempt, on its worker or,
  with `cleanup_anywhere`, on another after its worker was lost; `@resource`,
  `CommandResource` and `ScriptResource`, set up for their users and cleaned once after the
  last one.
- **Plain scripts:** script roots (`SCHEDR_SCRIPT_PATH`) with `schedr.toml` declarations,
  standalone jobs, the `script` task kind and `ScriptOperator`.
- **Extensions:** `ExtensionTask` runs a task kind's plugin, in any language (`schedr_extension`
  for Python, `schedr extension shell` for a script), found by configuration or on the worker's
  `PATH`; it reports its status, links, metrics and events, and may keep a service on each
  worker. A Kubernetes plugin runs each task as a pod.
- **Notifications and retention:** webhooks and email on eight events (`Dag(notify=...)`,
  `channel.send` from tasks); `[retention]` for runs, outputs, events and commands.
- **Testing DAGs:** `schedr dags check`, `schedr dags test`, and `schedr.testing`
  (`run_dag`, `task_context`) for pytest.
- **One process or separate roles:** `schedr standalone` (master, webserver and one worker,
  SQLite or Postgres; for production in one pod too; its folders as `--dag-path` and
  `--script-path`), or `schedr master`, `schedr worker` and
  `schedr webserver` as separate processes on Postgres, with mTLS between them.
- **Surviving failures:** workers keep running tasks while the master restarts; a worker's
  journal survives its restarts; drains on SIGTERM; a standby master takes over through a
  lease in the database; liveness decided on the master's monotonic clock; retries by rule
  (see the guide, "State and failures").
- **Operations:** the REST API (`/api/v1`, Swagger UI at `/api/v1/docs/`) used by the CLI
  (`dags`, `runs`, `tasks`, `workers`, `secrets`, `connections`) and by `schedr.api` from
  Python; task logs through the webserver; a memory budget and watchdog per task;
  `/healthz`, `/readyz` and Prometheus `/metrics` on an ops port.
- **Tested under faults:** a stress suite and chaos scenarios (links cut one way or both,
  slowed, a frozen worker, a seeded random schedule), and a simulator of the master and its
  workers on one virtual clock, all nightly.
- **Kubernetes:** images (`schedr` with Python for workers and standalone, `schedr-slim`
  without the task libraries, `schedr-server` without Python for the master and webserver),
  on glibc and on Alpine, and a Helm chart (`charts/schedr`: standalone or distributed,
  git-sync or DAGs from values, Istio, Vault), tested on kind.
- **Web UI:** `schedr ui` (in standalone too): DAGs, runs and their graphs (groups, mapped
  tasks), logs and outputs live, import errors, resources, cleanups and connections, actions
  with confirmations, several deployments in one UI; the Metrics page (memory, CPU, throughput
  and start latency per deployment), a Status block on the Deployments page, and each task's
  memory over its last attempts; the Extensions page (a tab per extension, one version at a
  time, its deployments and workers, its panels and events); runs and DAGs in numbered pages.

## Documentation

Online at https://ptutak.github.io/schedr/ (the latest release).

`make docs` builds the site into `site/` (`make docs-serve` serves it at
http://localhost:8765). Its sections:

1. **Overview** (this file).
2. **Getting started** (`docs/01-start/`): installing, and a first standalone with one DAG.
3. **Writing DAGs** (`docs/02-dags/`): the native API, with tested examples.
4. **How schedr works** (`docs/03-guide/`): the running system, failures, deployment.
5. **Reference**, generated from the code: configuration, CLI, REST API, metrics.
6. **Design** (`design/`): how each part works now, one page per topic, and the decisions'
   index (`DECISIONS.md`, a file per decision in `decisions/`).
7. **Plan**: `PLAN.md`.
8. **API**: Python (from docstrings) and Rust (rustdoc).
9. **History**: the journal (`JOURNAL.md`, each decision's reasons since 2026-11-08) and the
   design log before it (`archive/PROJECT.md`, up to decision 366).

PyPI shows `PYPI.md` instead of this file: installing and a first run, for someone using the
package rather than working on this repository.

## Installing

For a first run with the published package, see
[Getting started](https://ptutak.github.io/schedr/01-start/index.html).

```sh
pip install schedr            # Python 3.14 or newer; add [pydantic] for Pydantic models
```

Wheels for Linux x86_64 (glibc and musl), Linux aarch64 and macOS arm64; each carries the
`schedr` binary, so there is nothing to compile. The images, x86_64: `ghcr.io/ptutak/schedr` (Python and
the package: standalone and workers, the base for your own image), `ghcr.io/ptutak/schedr-slim`
(the same without the task libraries and connection clients) and
`ghcr.io/ptutak/schedr-server` (the binary alone: master, webserver and UI), on Debian
(glibc), tagged with the version (`1.2.1`) and its series (`1.2`); the same on Alpine add
`-alpine` (`1.2.1-alpine`). There is no `latest`: a deployment names the version it runs.

## Trying it

```sh
make standalone   # schedr standalone on the DAGs of examples/, SQLite in .schedr/, API on :8080
make standalone-separate-ui   # main as separate roles (two masters, a webserver, a worker) and second as a standalone, one `schedr ui` over both, on :8082
```

Then, from another shell: `schedr dags list`, `schedr dags unpause sync_customers`,
`schedr runs trigger sync_customers`, `schedr runs list sync_customers` (the CLI talks to
`http://localhost:8080` unless `--server` or `SCHEDR_SERVER` says otherwise). After
`make standalone-separate-ui` the pages are on port 8082; the CLI still uses `main`'s API
on 8080. `DAGS=<directory>` runs other DAGs;
`schedr dags unpause metrics_load` there starts a load every minute (`examples/metrics_load.py`:
short tasks, CPU, memory) to watch on the Metrics page; `WORKER_SLOTS` sets `main`'s worker's
slots (8). `LISTEN=127.0.0.1` keeps the API off the network (`make standalone` signs no one in; see
[Sign-in](https://ptutak.github.io/schedr/03-guide/08-network.html#sign-in)).

## Layout

```
crates/
  schedr-model/     # shared model: DAG JSON, states, JSON Schema
  schedr-proto/     # Protobuf + gRPC, master <-> worker agent
  schedr-config/    # configuration loading and validation
  schedr-db/        # schema, migrations, queries (SQLite, Postgres)
  schedr-master/    # scheduling loop, DAG validation, commands, the lease
  schedr-agent/     # worker agent: processes, journal, memory watchdog, log endpoint
  schedr-api/       # webserver: REST API, Swagger UI, log proxy, the Metrics endpoints
  schedr-client/    # a client of the REST API, for the CLI and the web UI
  schedr-ui/        # the web UI: server-rendered pages over the webservers' REST APIs
  schedr-fd/        # the one place schedr touches a raw descriptor: a task's context channel
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
docs/               # the hand-written pages (02-dags, 03-guide, 05-design, 08-history indexes); the rest generated
charts/schedr/      # the Helm chart
images/             # Dockerfiles: schedr (with Python), schedr-slim (without the task libraries), schedr-server (without Python)
scripts/            # smoke tests; kind-*.sh cluster checks; the records' and release's tools
design/ decisions/ questions/ plan/ bugs/   # how it works now, decisions, open questions, the plan, open bugs
```

How each part works now: `design/`. Decisions: `DECISIONS.md` (their reasons in `JOURNAL.md`,
and in `archive/PROJECT.md` before decision 367). Plan: `PLAN.md`. Open bugs: `BUGS.md`.
Where things stand: `STATUS.md`.

## Development

Requirements: Rust (the version in `rust-toolchain.toml`, via rustup),
[uv](https://docs.astral.sh/uv/), make; Podman or Docker for the integration tests
(Postgres) and the images; kind, kubectl and Helm for the cluster scripts. `make setup` also
installs `cargo-nextest`, `cargo-llvm-cov` and the `llvm-tools-preview` component.

```sh
make setup            # dev tools, uv sync, pre-commit hooks
make fmt              # format Rust + Python
make lint             # clippy, ruff, mypy; mypy --strict and basedpyright over DAG code
make test             # unit tests: Rust (nextest) + pytest, both with an 85 % coverage floor
make test-integration # real processes, the binary; Postgres, Vault, MySQL, MariaDB, Mongo, SSH in containers
make test-ui          # the web UI's browser tests (Playwright, Chromium)
make upgrade-check    # the previous release's database, every table filled, migrated and read back
make schema           # regenerate schema/ from the Rust model (CI fails if it's stale)
make records          # regenerate PLAN.md, DECISIONS.md, STATUS.md and design/ from the records
make docs             # build the documentation site into site/ (fails on any warning)
make check            # local checks and unit/integration tests; run make docs separately
make stress           # the stress suite (STRESS_PROFILE=quick|full)
make smoke            # build the release wheel and smoke-test it
make image            # the schedr image (make server-image: schedr-server), smoke-tested
make chart-check      # lint and render the Helm chart
make help             # every target
```
