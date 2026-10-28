# schedr: implementation plan (M0, M1, M2)

Draft. The design is in PROJECT.md; this file only says **what to build first and in which
order**. Milestones: see PROJECT.md, Roadmap.

- **M0. Foundations:** the workspace, the shared model, configuration, the database schema,
  the CLI skeleton. Nothing runs yet; everything is tested.
- **M1. Standalone runs a native DAG:** `schedr standalone` on a laptop parses a native DAG,
  schedules it, runs its tasks as processes and reports the result; `schedr dags test` and
  `schedr.testing.run_dag` work. **Done.**
- **M2. Distributed:** separate roles on Postgres, workers that come and go, mTLS, the memory
  watchdog, the ops port, images and a Helm chart; survives killing the master or a worker.

## Scope of M1

**In:**

- Native API core: `Dag`, `@task` (Python tasks), `CommandOperator`, `EmptyOperator`, data flow
  and `>>` / `chain()`, `TaskDefaults` (`retries`, `retry_delay`, `timeout`, `queue`,
  `max_output_size`), `trigger=` with `Cron` / `Every` / `Once` / `None` / presets, t-strings
  in commands and task arguments, `dag_ctx.run` (`id`, `kind`, `planned_for`, `since`) and
  `dag_ctx.dag_id`.
- Outputs: the type model, the three serialization checks, tagged extra types, dataclasses,
  output references with paths, `ProcessResult`, size limits and truncation.
- Trigger rule `NoneFailed` only; every task has the single outcome `SUCCESS`.
- Runner `parse` and `task` modes; the agent ↔ runner channel (Unix socket, length-prefixed
  JSON).
- **M2.5 also gets quiet DAGs** (busy DAGs visited every round, quiet DAGs only when woken; PROJECT.md, Components and roles, "Found by the stress suite") with stress scenario 14 (many quiet DAGs).
- **Ready for the fork server (M2.5):** the runner and its protocol don't depend on how the
  process was started: the runner gets everything over the socket (nothing from inherited
  state), the agent supervises by process group, and every attempt records its start method
  (always `spawn` in M1).
- Agent: spawn per attempt, process groups, log files, clean environment and `SCHEDR_*`
  variables, command rendering with shell quoting, timeouts, cancellation (SIGTERM, then
  SIGKILL), attempt statuses `success` / `failed` / `timed_out` / `cancelled`.
- Master: DAG validation and storage per (root, hash, version), the scheduling loop (fire
  times, `catchup`, `max_active_runs`, `planned_for` / `since`, missed fire times), manual runs
  (`--for`, `queued`), dependency resolution, retries, run results, commands.
- gRPC between master and agent over localhost (the protocol as decided in PROJECT.md:
  Master–worker protocol; no TLS in standalone).
- Webserver: the **REST API subset** the M1 CLI needs (the UI comes in M3). Commands are
  written as rows and picked up by polling (SQLite).
- CLI: `dags list` / `pause` / `unpause`, `runs trigger` / `list` / `cancel`, `dags check`,
  `dags test`; `schedr db migrate`; `schedr standalone`.
- `schedr.testing`: plain-function calls of `@task`, `task_context`, `run_dag` with per-task
  inspection.
- SQLite only at run time; the schema and migrations exist for Postgres too and are tested in
  CI.

**Out (later milestones):** async task functions, sensors and triggers (M4), outcomes and
`.on(...)`, `AllSucceeded`, `@branch`, groups, mapping, `schedr.toml` and scripts, cleanup and
resources, secrets (Vault; the `db` storage for standalone, `schedr secrets`) and connections, params, notifications, `TriggerDagOperator`, concurrency
limits other than `max_active_runs`, backfill, retention, the in-task CLI (`schedr task-ctx`,
`schedr dag-ctx`, and `schedr.script` giving plain Python scripts `dag_ctx` and `task_ctx`; command tasks read their run information from `SCHEDR_*` variables) (M4);
the journal on disk, the memory watchdog, mTLS, separate roles and Postgres at run time (M2);
the web UI (M3); `run_dag(step=True)` and `replace=` (M4); anything Airflow (M5). (`Once` was
built in M1.)

## Workspace layout

Replace the placeholder `schedr-core` crate with crates along the role boundaries, so each
role's dependencies stay small and the master provably contains no Python-related code:

```
crates/
  schedr-model/    # shared model: DAG JSON, states, run/task types; serde + schemars (JSON Schema)
  schedr-proto/    # Protobuf definitions and tonic code for master <-> agent
  schedr-config/   # configuration: TOML file + SCHEDR_CONFIG__* env + flags, aliases, validation
  schedr-db/       # schema, migrations, queries (SQLite and Postgres)
  schedr-master/   # scheduling loop, DAG validation, command handling, gRPC server
  schedr-agent/    # worker agent: gRPC client, process supervision, rendering, runner channel
  schedr-api/      # webserver: REST API (M1), UI (M3); OpenAPI generation
  schedr/          # the binary: role subcommands, user CLI (REST client), in-task CLI
schema/            # generated, committed: dag.schema.json, runner.schema.json, openapi.json
python/schedr/     # the Python package
```

- `schedr-model` is the reference for every shape (PROJECT.md: Shared model and schemas).
  `schema/*.json` are generated from it by a small `xtask`-style binary; CI fails if they
  differ from the committed files.
- The user CLI lives in the binary crate (a thin REST client); the in-task CLI
  (`task-ctx`, `dag-ctx`) too, talking to the agent socket.

## Library choices

Confirmed: see PROJECT.md, Repository and tooling: Libraries.

## M0: Foundations

| # | Work | Done when |
|---|---|---|
| 0.1 | Workspace: new crates, shared lints, `make check` and CI updated; `schedr-core` removed | `make check` green |
| 0.2 | `schedr-model` v0: DAG JSON for the M1 subset (tasks `python` / `command` / `empty`, plain edges, templates as text + typed references, output references with paths, schedules, task defaults, type descriptors for outputs); run, task instance and attempt states | round-trip tests; JSON Schema generated |
| 0.3 | Schema generation + CI check (`schema/dag.schema.json`, `schema/runner.schema.json`) | CI fails on a stale schema |
| 0.4 | `schedr-config`: file discovery (`--config`, `SCHEDR_CONFIG_FILE`, default), env mirror, precedence, `SCHEDR_DAG_PATH` / `SCHEDR_SCRIPT_PATH` aliases, roots and their startup errors (nested, duplicate names), `[interpreters]`, durations and sizes | unit tests per rule |
| 0.5 | `schedr-db`: schema v1 (DAGs, DAG versions, parse results, runs, task instances, attempts, outputs, commands, workers), migrations for SQLite and Postgres, `schedr db migrate`, "refuse to start on an outdated schema" | migrations tested on both (Postgres in CI via a service container) |
| 0.6 | `schedr-proto`: Protobuf for Register / Registered / Heartbeat / AssignTask / Started / Rejected / Finished / FinishedAck / AssignParse / CodeVersionChanged, versioned `major.minor` | generated code compiles; version check unit-tested |
| 0.7 | Binary skeleton: `clap` tree for roles, user CLI, `db migrate`, in-task CLI; `tracing` setup (`[log] format`, `level`) | `schedr --help` shows the tree |
| 0.8 | Python package skeleton: `schedr/_model/` (the private builder API: hand-written DAG JSON types for the M1 subset, mirroring `schedr-model` module for module, with the same ID / version / UTC timestamp rules); tests validate produced JSON against `schema/dag.schema.json` (`jsonschema`, dev only). The other modules of the layout below are created in 1.1, with their code | `make check` green |
| 0.9 | **Documentation site** (PROJECT.md, Repository and tooling: Documentation): `cargo xtask docs-sources` (PROJECT.md → one Markdown file per chapter, plus README, PLAN and the crate list, into `docs/_generated/`, tested), Sphinx + MyST + Furo + autodoc, rustdoc copied under `rust/`, `make docs` / `make docs-serve`, ruff `D` rules (Google convention) and `rustdoc::broken_intra_doc_links`, CI docs job failing on warnings; docstrings added where missing | `make docs` builds a site with every section; CI green |

Python package layout:

```
python/schedr/
  __init__.py        # public native API re-exports
  _model/            # private builder API and DAG JSON writer (used by the Airflow layer later)
  dag.py             # Dag, TaskDefaults
  tasks.py           # @task, Output, output references and paths
  operators.py       # CommandOperator, EmptyOperator
  triggers.py        # Trigger, Cron, Every, Once, presets
  context.py         # dag_ctx
  types.py           # the Json type alias, ProcessResult, KiB / MiB
  _serialization/    # type model, parse-time checks, encode / decode (tagged types, dataclasses)
  _templates.py      # t-strings -> template parts, reference checks
  runner/            # python -m schedr.runner {parse,task}; the socket protocol
  testing.py         # task_context, run_dag
```

## M1: Standalone runs a native DAG

In dependency order; each step ends with **unit tests**, and with **integration tests**
where its "done when" needs real processes, files or the whole system. The integration tests
of 1.4–1.14 were written after 1.14 (first planned as one step at the end, 1.17); from 1.15
on, each step brings its own.

| # | Work | Done when |
|---|---|---|
| 1.1 | **Native API, parse side:** `Dag` context manager, `@task` with required return annotation, `CommandOperator`, `EmptyOperator`, data flow and `>>`, IDs and duplicates, strict arguments, no subclassing, `Cron` / `Every` / presets, `TaskDefaults`, `dag_ctx` references | `with Dag(...)` produces DAG JSON that validates against the schema |
| 1.2 | **Serialization:** the type model and parse-time checks (return types, inputs, paths, `None` from skippable tasks), run-time encode / decode with tagged types, dataclasses, strict checks, size limits | unit tests per supported / rejected type, with the exact error messages |
| 1.3 | **Templates:** t-strings to template parts, nested traversal rules, format specs (`raw`), parse errors (dict keys, untraversed containers, unknown specs) | unit tests |
| 1.4 | **Runner `parse`** and the socket protocol (length-prefixed JSON, version handshake), per-file results and import errors | integration test: a directory of DAG files → JSON + errors |
| 1.5 | **Agent, processes:** spawn with `process_group(0)`, stdout / stderr to log files, clean environment + allow-list + `SCHEDR_*`, working directory, timeouts, SIGTERM → SIGKILL on the group, attempt statuses, `ProcessResult` with truncation | integration tests with real processes (sleep, output floods, ignoring SIGTERM, child processes) |
| 1.6 | **Agent, rendering:** command templates rendered in Rust, shell quoting with `shlex.quote` parity, argument-list commands, references resolved from inputs and run information | parity tests against Python's `shlex.quote` over generated strings |
| 1.7 | **Runner `task`:** import the DAG file, find the task, decode inputs, call, encode the output or an error summary | integration tests through the agent |
| 1.8 | **Master, DAGs:** parse jobs per (root, hash, version), change detection (poll, settle, hash), validation, storage, new DAGs paused, removed DAGs, conflicts (single worker) | integration tests over a temp root with files added, changed, removed |
| 1.9 | **Master, scheduling:** fire times (cron in a zone, DST rules, `Every` in UTC), `planned_for` / `since`, `catchup`, `max_active_runs` (missed vs `queued`), manual runs with `--for` (UTC without offset, with a warning), uniqueness for scheduled runs | unit tests with a fake clock, incl. DST changes in Europe/Warsaw |
| 1.10 | **Master, execution:** task instance states, `NoneFailed`, assignment (one local worker, slots), `AssignTask` / `Started` / `Finished` / `FinishedAck`, retries and `retry_delay`, the no-`Started` timeout, run results, cancellation | integration tests: chains, joins, failures, retries, cancel |
| 1.11 | **Commands:** command rows (trigger, cancel, pause, unpause), validation and `applied` / `rejected` with reasons, polling | unit + integration tests |
| 1.12 | **Webserver REST subset** (`/api/v1`): DAGs, runs, tasks, attempts, commands; version headers; OpenAPI generated and checked in CI | API tests; CI check |
| 1.13 | **CLI:** `dags list` / `pause` / `unpause`, `runs trigger` / `list` / `cancel`, human output with UTC + local time, `--json` | CLI tests against a running standalone |
| 1.14 | **`schedr standalone`:** all roles in one process, SQLite, gRPC on localhost | the end-to-end example from PROJECT.md runs |
| 1.15 | **`dags check`, `dags test`, `schedr.testing`:** plain-function `@task` calls, `task_context`, `run_dag` with per-task inspection (`state`, `output`, `attempts`, `log`, …) | the example DAGs in `examples/` have tests that pass in CI |
| 1.16 | **Packaging:** maturin wheel with the binary for Linux x86_64 (others in M2), `pip install` + `schedr standalone` smoke test in CI | a wheel built in CI runs the example |
| 1.17 | **Integration tests:** written after 1.14 for 1.4–1.14 (a directory of DAG files through the runner, real processes through the agent, every query on Postgres, the runner as a real process, `schedr standalone` end to end through the CLI and the API, also on Postgres), and with each step since | all pass in CI |

**M1 is done** when a new user can `pip install` the wheel, write the example DAG from
PROJECT.md (Native API), run `schedr dags test` on it, start `schedr standalone`, unpause the
DAG, trigger a run with `schedr runs trigger --for ...`, and see it succeed with
`schedr runs list`.

**Status (2026-10-22): M1 is done.** Steps 1.1–1.17 are committed (the last: 1.16, `df309b8`).
The paragraph above is checked automatically by `scripts/smoke-wheel.sh` (CI's `wheel` job,
`make smoke`): the release wheel installed into a fresh venv, `dags check`, `dags test` on the
example DAG, then `schedr standalone` with `dags unpause`, `runs trigger --for ...` and `runs
list` until the run succeeds. Open bugs are in `BUGS.md`.

## M2: Distributed

**Goal (PROJECT.md, Roadmap):** schedr runs on OpenShift with 2 workers and survives killing
the master or a worker. M1 built the pieces in single-worker form (the gRPC protocol and
`Register`, the worker registry with spread, change detection and hashing, the journal in
memory); M2 splits the roles and makes every failure path real. Each step brings unit and
integration tests, as since 1.15.

| # | Work | Done when |
|---|---|---|
| 2.1 | **Separate roles:** `schedr master`, `schedr worker`, `schedr webserver` as their own processes (from `standalone`'s parts); Postgres at run time; commands picked up by `LISTEN/NOTIFY`, polling as the fallback; refuse an outdated schema | the three processes and Postgres on a laptop run the example DAG |
| 2.1b | **`standalone` in one pod** (for testing on a cluster): `--listen` for the API, SIGTERM, one standalone per data directory (a lock), the musllinux wheel (Alpine), `examples/kubernetes/` (Alpine `Dockerfile`, a one-replica `StatefulSet` on an RWO volume with git-sync) | the image runs the example DAG under an arbitrary UID; CI smoke-tests the musllinux wheel on Alpine |
| 2.2 | **Worker lifecycle:** incarnation IDs, heartbeats, the 60 s lease, `DISCONNECTED` / `LOST`, reconnect with backoff and jitter, the lease clock restarting after a master restart, the journal decisions (continue / kill / ack) in `Registered`, refusals | integration tests: a worker dropped and reconnected; a lost worker's attempts retried |
| 2.3 | **Journal on disk:** running attempts and unacknowledged results survive an agent restart; the processes a dead agent left are stopped, and their attempts end `lost` (`WorkerRestarted`, using a retry) | `kill -9` of the agent, restart: its running attempts lost and retried, finished results not lost |
| 2.4 | **Assignment across workers:** queues (the `default` rule), the rejection table (worker-specific → `unschedulable` with each worker's reason; "code not there yet" retried after 35 s, `unschedulable` after `[master] code_wait`, 10 min), wait reasons on queued tasks, `schedr tasks retry`, `schedr tasks cancel` (any unfinished task) | tests with two workers serving different queues; a running task cancelled with its downstream |
| 2.5 | **Parsing across workers:** one parse per (root, hash, schedr version) by any worker that reported the hash, reassigned after 10 min; **only the newest code runs** (each root's current hash; tasks go only to workers at it, which check it), so workers on older code cause no conflicts; mixed-hash warnings; the rolling-upgrade rule (worker version ≥ the parse's); failures from code that changed under a run say how | tests with two workers on different hashes |
| 2.6 | **Drain:** SIGTERM and `schedr workers drain` / `resume`; the grace period; interrupted attempts use no retry; a drained worker stays drained across restarts | tests: a SIGTERM-drained worker's interrupted task runs again without a retry; a drained worker gets no tasks across a master restart |
| 2.7 | **mTLS** (`tls.mode = "app"` / `"mesh"`, certificates reloaded when the files change; crate `schedr-tls`); the **worker's log endpoint** (mTLS only: no tokens) and the webserver's log proxy, `schedr tasks logs [--follow]`; log retention on the worker (age and size) | tests with self-signed certificates |
| 2.8 | **Memory watchdog:** the budget from `memory.max`, `/proc` sampling (PSS per process group), the `oom_killed` status, peak memory in the API; `setrlimit` moved to M4 with per-task budgets | an integration test with a task that allocates too much |
| 2.9 | **Ops port** on every role: `/healthz`, `/readyz`, `/metrics` (Prometheus, `prometheus-client`), loop-stall detection, `[database] query_timeout` | tests |
| 2.9b | **Documentation of the system as built:** the guide (`docs/03-guide/`: overview, glossary, life of a run, the master's rounds, workers, parsing, failures, network, observability, deployment; Mermaid diagrams), reference pages generated from the code (configuration from its JSON Schema, CLI from clap, REST API from OpenAPI, metrics from a list per role checked against a scrape); every later step updates the guide | `make docs` builds it without warnings; unit tests for the generators |
| 2.10 | **Standalone for production** (see PROJECT.md, Components and roles: production path): working through any connection pooler (PgBouncer in any mode): a `master_lease` row for one master per database (a second waits as a standby; standalone and `schedr master`; SQLite too), the query limit enforced by schedr instead of `SET statement_timeout`, sqlx's statement cache off, polling as the command guarantee; the master's writes fenced by the lease in the database (a write's start one message on Postgres); migrations by the lease holder (`schedr db migrate [--wait]`, the chart's init container; standalone only with `--migrate`); `[worker] memory_reserve` (10 % worker, 25 % standalone, measured); an image with the DAGs' libraries on top of schedr's; a Helm chart with a standalone mode (one pod, SQLite on a volume or Postgres from a Secret, probes, `terminationGracePeriodSeconds` matched) | tests: a second master waits and takes over; the chart's standalone mode installs on kind / k3d, on SQLite and on Postgres |
| 2.10b | **Stress suite** (see PROJECT.md, Testing: the stress suite): `crates/schedr/tests/stress/` on the role tests' harness (moved to `tests/support/`), 13 scenarios (throughput, command bursts, worker churn, master failover, the database stalled, through PgBouncer, rolling restart, code churn, standalone on SQLite, soak, heavy output, the memory watchdog, Python tasks), invariants from the database and the tasks' marks, reports in `target/stress/`; `make stress` (`STRESS_PROFILE`, `STRESS`); nightly in CI (`stress.yml`) | the suite passes locally (`quick`); bugs it finds go to BUGS.md |
| 2.11a | **Standalone inside an Istio mesh** (see PROJECT.md, Components and roles: "Standalone inside an Istio mesh"): `serviceAccount` (an existing one reused), `migration: initContainer \| onStart`, the `istio` values (sidecar settings for drains, VirtualService, RequestAuthentication, AuthorizationPolicy listing every port, PeerAuthentication, ServiceEntries); Swagger UI at `/api/v1/docs/` (always on; open without a token in the mesh; working behind a path prefix with `X-Forwarded-Prefix`) and an optional bearer token in the OpenAPI description; the database from a Secret's user and password (`database.postgres`, the switch from SQLite one upgrade); `vault` (the Vault Secrets Operator); DAGs from a folder (`charts/schedr/dags-values.sh` → `dags.files`, a ConfigMap root) and git-sync's credentials; the base image's libraries for tasks (API calls, Polars, networking, pymongo built from source); fixes B19 | `make chart-check`; `scripts/kind-standalone.sh` and `scripts/kind-mesh.sh` pass on kind (the latter with Istio, `REGISTRY_ONLY`) |
| 2.11 | **Images and Helm chart, distributed:** master / webserver without Python (`schedr-server`), a worker base image with Python 3.14 and `schedr`, arbitrary UID; the chart's `mode: distributed` with the same values as standalone (the API's Service keeps its name): the master a one-replica Deployment (`Recreate`; `schedr master --migrate` for `migration: onStart`), the webserver waiting for its schema, 2 workers (a StatefulSet with a headless Service and a volume each), `terminationGracePeriodSeconds` 600 and `[worker] termination_grace_period` set to match, probes, TLS Secrets per role (your own, cert-manager, or generated by the chart), ServiceAccounts per role, the Istio policies for the gRPC and log ports, a PodDisruptionBudget, `passEnv` | the chart installs on kind / k3d (`scripts/kind-distributed.sh`, also `MESH=1`) |
| 2.12 | **Kill tests** (M2's "done when"): `scripts/kind-kill.sh` on kind cluster `schedr-test` only (any other context refused; not in `make check` or CI). Hard kill (`kubectl delete pod --force --grace-period=0`): the master during a run (`retries=0`, the same attempt finishes), then a worker (`retries=1`, `lost` / `WorkerRestarted`, then success), then standalone on SQLite (an earlier run kept). On OpenShift the same checks are a procedure in the guide, run by hand | the kind script passes; runs finish; retries as the rules say |
| 2.12b | **`dags check --against`:** read the server's active workers' `schedr_version` and exit 1 when this schedr is newer by minor version than any of them (patch ignored; a drained worker counts; an unreadable version stays eligible, as in dispatch). A pass prints one `ok:` line; `--json` includes `version_check.message`. Fixes B20. The `standalone` help and the overview match production (SQLite or Postgres, one pod) | the flag no longer says it isn't implemented; `make check` and `make docs` |
| 2.13 | **More wheels:** Linux aarch64 and macOS, with the full smoke test (deferred from 1.16) | built and smoke-tested in CI |
| 2.13b | **Supply-chain pins:** every `uses:` is a full commit SHA (local `./` actions and `docker://…@sha256` accepted); tool versions pinned (`toolchain:`, `tool:`, `version:`, setup-uv's tarball checksum); Rust `1.99.1` in `rust-toolchain.toml`; wheels and the sdist use hashed maturin (no `maturin-action`); digest-pinned images, including the kind scripts; `make pin-check` fails a bad pin | `make check` and `make docs` |

- **Order:** 2.1–2.3 first: separate processes and what survives their deaths are the
  architectural risk. 2.7 includes the log endpoint because a distributed run's logs are on
  the worker (the UI showing them is M3). Auth modes stay in M3. The fork server (M2.5)
  builds on 2.8 and 2.9.
- Open bugs (`BUGS.md`) are fixed when scheduled with the user.

**Status (2026-10-26, end of day): M2's steps 2.1–2.13b are done** (the commits are listed
in `NEXT_SESSION.md`). 2.13's wheel jobs passed in CI at `b3c9c47` (`wheel-linux-aarch64`
4 min 10 s, `wheel-macos-arm64` 5 min 42 s). 2.13b (`f3a8631`) pins CI's supply chain and
builds every wheel through `scripts/build-wheel.sh`; its CI run passed with every job green
(`wheel` 17 min 32 s, `wheel-macos-arm64` 5 min 49 s, `wheel-linux-aarch64` 4 min 23 s).
Today also: B15 fixed (tokio 1.54.1), B21 fixed (the lease tests' query limit),
`scripts/kind-mesh.sh` onto `scripts/kind-lib.sh`. Open bugs: B22 (a draining worker once
missed its 8 s grace under load; not reproduced), B4 (likely B6, unproven).

**Next: M2.5, the fork server and quiet DAGs**, steps planned below (2026-10-27).

## M2.5: Fork server and quiet DAGs

Done when (PROJECT.md, Roadmap): Python tasks start from the fork server on Linux workers,
with fallback to spawn; stress scenario 14's rounds no longer grow with the quiet DAGs.

Steps planned with the user (2026-10-27; decisions in PROJECT.md, "How Python tasks are
started" and "Quiet DAGs"). Claude implements each; Grok reviews it independently
and updates the guide.

| Step | Contents | Done when |
|---|---|---|
| M2.5.1 | **Measure first:** a task process start-time histogram by start method (`runner`: starting the process to the runner's hello; `import`: sending the task to the DAG file imported), `start_method` in the attempts API; **stress scenario 14** (about 1,000 daily DAGs and a few busy ones) run on today's round as the baseline | scenario 14's baseline recorded; `make check`, `make docs` |
| M2.5.2 | **Quiet DAGs:** busy and quiet DAGs in the master's memory, wake marks (a queued run, run end, command, new version, timer), the sweep (every quiet DAG within `[master] quiet_dag_full_pass_interval`, a slice a round) and the ID check, `quiet_dags`, `quiet_dag_after` and `quiet_dag_wake_ahead`; a round reads only the DAGs it visits (active DAGs and the run overview limited to them); the comparison test (one timeline through both paths, the same runs) | scenario 14's rounds no longer grow with the quiet DAGs |
| M2.5.3 | **Fork server, runner side:** `python -m schedr.runner fork_server`: preload, the single-thread check, `gc.freeze()`, forking on request through a shim per task (decision 13A: `process_started` / `process_exited` on a status socket, the process reaped after the agent closes it; 14A: its usage and `core_dumped` reported); the task gets its channel and output pipes by `SCM_RIGHTS`, `setpgid(0, 0)`, its environment (the time zone, locale and temporary directory taken again from it) and directory, the fork server's sockets closed (a preloaded library's descriptors kept), `random` reseeded, and runs the normal `task` / `parse` code; the messages in `schedr-model` | Python unit tests, including fork safety |
| M2.5.4a | **Fork server, agent side** (decision 17A: M2.5.4 split in two): two fork servers per interpreter started with the worker (preloaded and bare), fork requests, the fallback chain (preloaded, bare, spawn), restart with backoff (1 s doubling to 5 min, reset after 10 min up), the version check, `start_method: fork`, the shim's status socket (the PID after `process_started`; a shim closing it without `process_exited`: the group killed and the attempt failed with an unknown status, or, before `process_started`, a fallback), the watchdog without the RSS fallback for forked tasks, metrics (`schedr_worker_fork_servers{interpreter, kind, state}`, `schedr_worker_fork_fallbacks_total{interpreter, reason}`), the macOS warning | tasks start forked on Linux; every fallback tested |
| M2.5.4b | **What the master stores:** `process_exited`'s `usage` and `core_dumped` sent in `Finished`, stored with the attempt and shown in the attempts API (14A); spawned processes' usage from the zombie's `/proc` before reaping (18A: CPU and faults; peak RSS and context switches null); worker `warnings` (`[{kind, interpreter, message}]`) in the protocol, the REST API and `schedr workers list` | usage and warnings visible in the API and CLI |
| M2.5.5a | **How tasks choose:** `@task(preload=False)` (only the bare fork server) and `@task(fresh_process=True)` (always spawned), also in `TaskDefaults` and `.override()`, as `preload` / `fresh_process` in a task's settings (DAG JSON); parse jobs through the fork servers (a fresh child per job, the preloaded server first; `schedr dags check` still spawns) | each choice and a forked parse job tested |
| M2.5.5b | **Finish:** the chart's `worker.forkServer` (on, preload: the worker image's libraries except `polars`; the image's build fails if the list starts a thread), scenario 13 with spawn and with fork | M2.5's "done when"; the guide updated |

## M3: Web UI

Done when (PROJECT.md, Roadmap): the web UI shows DAGs, runs (their graph, tasks, attempts,
outputs and logs, live), workers and commands, and takes the actions the API takes, with the
auth modes, times in UTC and local, light and dark themes, over a list of deployments.

Steps planned with the user (2026-10-27; decisions 21B-26A in PROJECT.md, Web UI): `schedr ui`,
a role of its own talking only to webservers' JSON APIs (21B); askama and htmx (26A); a layered
graph layout of our own (22A); the log tail polled and relayed over SSE (23A); no sessions of our
own, actions checked by `Origin` (24A); M4's views as shells (25B); one stylesheet of our own,
light and dark themes with a toggle (the user's). Claude implements each step; Grok reviews it
and updates the guide.

| Step | Contents | Done when |
|---|---|---|
| M3.1 | **Foundations:** the `schedr ui` role (a deployments list, one the common case; its own API client to the webservers; standalone serves it too), askama templates, the assets embedded in the binary (htmx vendored when first used, in M3.2), the stylesheet with light and dark themes and the toggle, times in UTC and local (the zone cookie), URLs following `X-Forwarded-Prefix`; the documentation at `/docs` from the image (28A); the DAG list and the DAG page, read-only | the UI lists DAGs and shows one, in both themes; the docs served |
| M3.2a | **Runs, the API:** the endpoints the run pages need: `GET /api/v1/runs` (every DAG's, filtered by DAG and state, paged by `before`), a run's DAG (`/runs/{run}/dag`, for the graph), a task's output (`/runs/{run}/tasks/{task}/output`), a run's commands (`/runs/{run}/commands`); the client's methods for them | the endpoints answer on SQLite and Postgres, in the OpenAPI description |
| M3.2b | **Runs, the pages:** the runs lists (all DAGs and one DAG's, filters), the run page with its SVG graph (the layered layout), the task panel (attempts, output, logs; shells for cleanup and resources), the run summary and its command history; htmx vendored; the Playwright suite started (27A) | a run is shown whole, from the API alone |
| M3.3 | **Live:** the master's `NOTIFY` on state changes (polling in standalone), the webserver's SSE change events (keep-alive, polling fallback), relayed by the UI; the live log tail (23A) | a run and its logs update without a reload |
| M3.4 | **Actions:** trigger (the moment shown in both zones), cancel, pause / unpause with their confirmations, retry, drain / resume; the workers page with its warnings; the banners (database unavailable, log unavailable, cancel too late) | every API action from the UI |
| M3.5 | **Auth modes:** `none` / `proxy_header` / `basic` with their safety rules, for the webserver and the UI; the UI's actions checked by `Origin` (24A) | each mode tested, CSRF refused |
| M3.6 | **Federation, in production:** several deployments in one UI, any mix of standalones on SQLite, standalones on Postgres and distributed ones (the user, 2026-10-28: the way from a federation of standalones on SQLite to Postgres to distributed, a deployment at a time, every stage possible); merged lists, a deployment down shown unavailable, each action to its deployment, one identity across them (the token for standalones); standalone without its own UI (`schedr standalone --no-ui`); the chart deploys `schedr ui` with distributed (37A) and **alone**, over a list of deployments in other releases or namespaces; the chart's values for M3.5's sign-in (the auth sections, the users file and tokens from Secrets, the webserver's UI port and the UI's certificate) | one UI over a standalone on SQLite, a standalone on Postgres and a distributed deployment, one of them down; the chart renders each shape |

## After M3: Grok over the whole repository

Asked by the user (2026-10-27): once M3 is done, Grok reviews the **whole repository**, not
one step's diff (it has only reviewed diffs so far), with a proof test or script per finding,
and brings the documentation up to date with the code. Claude verifies the findings and fixes
the confirmed ones (out-of-scope ones go to BUGS.md); the user schedules the fixes. To plan
then: how to split the repository into review tasks (by crate or concern) to keep each Grok
session small.

**When** (the user, 2026-10-28): after Grok's weekly allowance resets, **Friday 2026-10-30 at
9:00** (at 86% after M3.4; the rest of the week goes to M3.5's and M3.6's reviews and docs).

**Not yet reviewed, so first in it** (the user, 2026-10-28: committed before a review, to be
reviewed there): what was built after Grok's M3.6 review — the fixes of its findings (the
followed users and token files, the lease given back on a failed migration, the chart's Istio
objects for `mode: ui` and the UI's pods, the UI port kept for other UIs, a `default` listed
twice), decision 40A (`[webserver] ui_port_in_mesh`, the chart's mesh proof and its DENY
policy, `istio.authorizationPolicy.ui.principals`), 41A (the sign-in pairs, `ui.auth` taking
`webserver.auth`'s), `scripts/users-file.py`, and the guide's Sign-in section (written by
Claude, no docs round).

## M4: Native API complete

Done when (PROJECT.md, Roadmap): the native API's whole design is built, so a DAG written
against it runs as designed, distributed and in standalone. **Planned with the user
(2026-10-28; decisions 49-53 in PROJECT.md):** async points settled (49A), `setrlimit` through
`schedr task-exec` (50A), this order (51A), connections' sync and async clients and `check()`
(53); nearly all of the rest of the design is already in PROJECT.md's sections, which each step
follows. **Reviews (52A, refined by the user 2026-10-28):** steps are split into small sub-steps
(M4.1a, M4.1b, …), each committed after Claude's own review and mutation-checked tests; Grok
reviews a batch of about five sub-steps at once, then writes the guide for them. Sub-steps
built before the 2026-10-30 reset go into the whole-repository review. The order puts the result model first (everything graph-like
builds on it), the agent channel before what talks over it, and the watcher host after async
functions. Claude implements each step; Grok reviews it and updates the guide (from the
2026-10-30 reset on).

| Step | Contents | Done when |
|---|---|---|
| M4.1 | **Results and outcomes:** every task reports an (output, outcome) pair, stored per attempt, in DAG JSON (each task's outcome names), `Finished` and the database; Python outcomes (declared `Outcome` enums, unions of dataclasses / models / `None`), command exit codes as outcomes (`Outcome` `IntEnum`, `DefaultOutcome`); `.on(X)` (edge and narrowed data), `.outcome`; the edge kinds (on-outcome, selection, requirement); the trigger rules `NoneFailed` (default) and `RunIf.ALL_SUCCEEDED`, decided as soon as certain; skips and `None` data from skipped upstreams; the run result rules (PROJECT.md, Outcomes, failures and joins); the UI showing outcomes | a DAG routing on a Python outcome union, an enum and a command's exit code runs as the tables say, on SQLite and Postgres |
| M4.2 | **`@branch` and groups:** a branch task's chosen IDs and selection edges, skips propagated; `with group(...)` (prefixed IDs, dependencies on a group, its `TaskDefaults`, `run_if`), group metadata shown in the UI's graph | a branch choosing a subset and an empty list; a group as one dependency |
| M4.3 | **Mapping:** `.partial(...)` / `.expand(...)`, instances `map_index` 0..N-1 created once the input exists, element-wise `.on` and expansion over a mapped output, the whole output as `Output[list[T]]` (input order; `[]` for an empty expansion; `upstream_failed` on a failed instance), `[mapping] max_instances`; instances in the API and UI | a mapped task over a literal and over an upstream output, empty included, with per-instance results |
| M4.4 | **Concurrency limits:** `Dag(max_active_tasks=)`, `@task(max_active=, max_active_per_run=)` (also operators, `TaskDefaults`), applied in the assignment's transaction, the waiting reason and its metric | limits hold under a stress scenario (no more running than allowed), reasons shown |
| M4.5 | **The agent channel, params and `dag_ctx`:** per-call connections to the agent (`SCM_RIGHTS`), `schedr task-ctx` / `schedr dag-ctx`, `schedr.script`; `Dag(params=...)` (dataclass or model, JSON Schema in the DAG JSON), values validated by the master before a run exists (`runs trigger -p`, `--params`, the UI's form), stored with the run; `dag_ctx` references in the DAG body, values at run time, `dag_ctx.params(...)` | a Python task, a command and a plain script read the same params and run information |
| M4.6 | **Secrets:** the secret storage abstraction, Vault (KV v2, Kubernetes auth, token renewal, cache), the `db` storage for standalone (encrypted, `SCHEDR_SECRET_KEY`) and `schedr secrets set/list/get/delete/export`; placeholders rendered by the agent, `secrets=` and `SecretPrefix` on demand, masking (`min_mask_length`, `unmasked_prefixes`), the error kinds; storage names checked at DAG validation; the chart's key and Vault role | a task reads secrets from Vault (a dev-mode container in the integration tests) and from `db`, masked in its log |
| M4.7 | **Connections:** `@connection` generator functions, the built-in typed connections (`HTTP` and `Mongo` sync and async through httpx and pymongo, `SSH` and `SQL` sync, `Generic`) with `check()`, `CommandOperator(connections=)` variables and files, `schedr task-ctx connection`, `schedr connections check` / `test`, optional `@task(connections=[...])`, test doubles (decision 53) | the same DAG reads a connection's values from two environments' storages |
| M4.8 | **Cleanup and resources:** per-task cleanups (`cleanup=`, the runner's `cleanup` mode through the fork server, handles registered and journaled, `cleaning_up`, `cleanup_timeout`, cleanup retries, `cleanup_anywhere`, a failed cleanup failing the run, drain timing); resources (`@resource`, `@cleanup`, `uses=`, requirement edges, static and code-less resources, `CommandResource` / `ScriptResource`, `task-ctx cleanup/resource`); the UI's shells filled (25B) | a task killed mid-way is cleaned up after, on its worker and, with `cleanup_anywhere`, after its worker was lost; a resource cleaned once after its last user |
| M4.9 | **`schedr.toml` and `ScriptOperator`:** `SCHEDR_SCRIPT_PATH` and script roots, `[jobs.<name>]` (every key, outcomes, params, cleanup), standalone jobs (one-task DAGs, paused when first found), `ScriptOperator`, `schedr dags check` for TOML | a scheduled job and a script used from a DAG run, with params, outcomes and a cleanup |
| M4.10 | **Async task functions:** `async def` tasks (`asyncio.run` per attempt, SIGTERM cancelling the main coroutine), async in-task calls, async cleanups and resources, optional `uvloop`, a plain call returning the coroutine, leftover tasks cancelled (49A); `async with` on connections | an async task with concurrent waits, cancelled cleanly, in tests and a run |
| M4.11a | **Wait steps:** the watcher host (`schedr.runner watchers`, one per worker, the 30-second rule and its watchdog, its memory budget, logs per watcher), `@sensor` and `@sensor.poke`, `TimedOut`, watcher slots and placement, drain and loss | a wait step ends with a value and with `TimedOut`; a blocked loop is caught |
| M4.11b | **Triggers:** `@trigger` and `@trigger.poke`, events (`events` table, bookmarks), event runs (`dag_ctx.event`, queueing, back-pressure), the master's trigger supervisor, restarts, manual runs with an event, the CLI, API and UI | an event-triggered DAG runs once per event, resumes from its bookmark after a restart |
| M4.12 | **Runs between and over time:** `TriggerDagOperator` (chained runs, `wait`, cancel, cycles) and backfill (`runs backfill`, `--all`, `max_runs`, `runs cancel --backfill`, priorities) | a parent waits on a chained run; a backfill fills missing periods oldest first |
| M4.13 | **Notifications:** channels in the server's configuration (email and webhook), `Dag(notify=...)`, the events (`RUN_FAILED` by default), fixed messages, metrics | a failed run sends one email and one webhook, a pure cancel none |
| M4.14 | **Retention:** `[retention]` (runs, outputs, events, commands), the master's batched job, never the latest run nor an unfinished one, "output deleted by retention" | old runs and outputs go, the protected ones stay, on SQLite and Postgres |
| M4.15 | **Per-task memory and upgrade rules:** a task's own memory budget in the API, `setrlimit` opt-in through `schedr task-exec` (50A); the upgrade rules checked against what M2 built (version gating, warnings within a major) and completed | a task over its own budget is killed; a rolling upgrade keeps old-parse tasks on old workers |

## M4.5: Batched worker messages

Done when: a worker's turn of messages (up to `MESSAGES_PER_TURN`) is stored in **one
transaction**: one lease check and one commit fsync per turn instead of one per message, a
failed message undone alone, and the measured lock and commit time per turn lower than M4's.

Planned with the user (2026-10-28; decision 54 in PROJECT.md, "T1b"), between M4 and M5. It
builds on T1 (one transaction per message), `Dispatcher::memory` / `restore` (a message
rolled back puts memory back) and D1 (a commit cut off is read back). **A savepoint per
message:** `SAVEPOINT` before each message, `ROLLBACK TO` and its own memory snapshot restored
when it fails, `RELEASE` when it succeeds, one `COMMIT` for the turn. **When the commit itself
fails** (the lease lost, the connection gone, a time limit): the memory as the turn found it
is restored and the turn's messages are redone one by one, one transaction each, as today
(the user's fallback). Not chosen: rolling back the whole turn on any message's failure and
redoing it one by one (a bad message costs N+1 transactions).

What each message does in a turn:

| Message | Today | In a turn's transaction | When it fails | When the commit fails |
|---|---|---|---|---|
| **Started** | own transaction, memory snapshot, read back (D1) | joins it, behind a savepoint; `arrivals.start_handled` after the commit | `ROLLBACK TO`, its memory restored | the turn's memory restored, redone alone |
| **Finished** | same; its `FinishedAck` also when the read-back finds it stored | joins it, behind a savepoint; ack sent, metrics counted and wakes fired after the commit | `ROLLBACK TO`, its memory restored | the same; nothing counted twice |
| **Rejected** | same | joins it, behind a savepoint | `ROLLBACK TO`, its memory restored | the same |
| **CodeVersionChanged** | own transaction, no memory snapshot | **ends the batch** (what came before is committed), then alone as today, unless its memory (worker version, parse jobs) joins the snapshot | as today | as today |
| **Heartbeat** | `worker_seen` outside any transaction, a failure ignored | **outside**, after the commit (for people) | nothing to undo | nothing to undo |
| **Warnings** | written outside any transaction, a failure ignored | **outside**, after the commit | nothing to undo | nothing to undo |
| **Draining** | memory (dispatcher, discovery) and a write, no transaction | **ends the batch**, then alone as today | as today | as today |
| **Deregister** | `forget`: memory, connections, writes | **ends the batch**, then alone as today | as today | as today |

A commit cut off is read back once for the turn: it lands whole or not at all, so one
message's attempt answers for all.

| Step | Contents | Done when |
|---|---|---|
| M4.5.1 | **Measure first:** per-turn timings (waiting for the core lock, handling, the commit) and the messages per turn as metrics; the assign-to-started gap split into its parts (Grok's 2026-10-28 investigation: rounds holding the lock); a stress baseline (scenario chosen with the user, run only with their go) | the baseline recorded |
| M4.5.2 | **Savepoints in `schedr-db`:** `Database::savepoint` inside an open scope (Postgres and SQLite), `ROLLBACK TO` clearing the scope's `broken` mark, `RELEASE`; a failed statement behind a savepoint no longer voids the transaction | unit tests on both databases, including a failed statement undone alone |
| M4.5.3 | **Turns in one transaction:** the turn loop (`service.rs`) wrapped in one transaction, a savepoint and memory snapshot per message, the batch ended at `CodeVersionChanged`, `Draining` and `Deregister`, heartbeats and warnings after the commit; metrics, wakes and `arrivals` deferred to after the commit; the fallback when the commit fails; the read-back once per turn | a failed message undone alone while the others are stored; a commit failure redone one by one; no metric counted twice; the baseline's measurements lower |

## M6: sign-in against LDAP

Postponed from after M3 to M6 (the user, 2026-10-28; the roadmap in PROJECT.md): a sign-in mode
of its own, `[*.auth] mode = "ldap"`, checking HTTP Basic credentials against the
organisation's directory (a bind), for deployments without Istio and Keycloak (decision 39: no
users of schedr's own). Decisions laid out on 2026-10-28, to take when M6 starts:

- **45, finding the person's entry:** A search, then bind (a service account searches
  `(uid={user})` under a base DN, then the password is checked by binding as the DN found;
  works with Active Directory and people in several OUs; the service account's password from
  a Secret); B a DN template (`uid={user},ou=people,…`; no service account; fails across OUs);
  C both.
- **46, who may sign in:** A an optional `group_dn` (members only when set, by `memberOf` or a
  group search); B a group required; C none (any valid account). Membership isn't a role.
- **47, the connection:** A TLS required (`ldaps://` or StartTLS, an optional CA file; plain
  `ldap://` only on loopback); B plain allowed with a flag and a warning.
- **Settled by precedent:** `ldap3` 0.12.1 (checked 2026-10-28, re-check its age then); a check
  that passed remembered 5 minutes as `basic`'s; `behind_tls_proxy` beyond loopback; the bind
  password from a mounted Secret, never the configuration; a wrong password, an unknown user
  and a non-member answered alike, in comparable time. Tested against an OpenLDAP container.

## Testing approach

- **Rules: PROJECT.md, Repository and tooling: Testing** (unit tests for every public function
  and class, private ones not tested directly, 85 % coverage, structure mirroring the code, no
  skipped tests without asking), and integration tests with each step from 1.15 on (those of
  1.4–1.14 were written after 1.14).
- **Cross-language parity:** produced DAG JSON validated against the committed schema; shell
  quoting against `shlex.quote`; encode in Python / decode in Python after a round trip through
  Rust storage.
- **End to end:** `examples/` DAGs with `run_dag` tests, run in CI against the built binary.
- **Time:** the master's clock is injectable; scheduling tests never sleep.
