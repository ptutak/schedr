# schedr: M0 / M1 implementation plan

Draft. The design is in PROJECT.md; this file only says **what to build first and in which
order**. Milestones: see PROJECT.md, Roadmap.

- **M0. Foundations:** the workspace, the shared model, configuration, the database schema,
  the CLI skeleton. Nothing runs yet; everything is tested.
- **M1. Standalone runs a native DAG:** `schedr standalone` on a laptop parses a native DAG,
  schedules it, runs its tasks as processes and reports the result; `schedr dags test` and
  `schedr.testing.run_dag` work.

## Scope of M1

**In:**

- Native API core: `Dag`, `@task` (Python tasks), `CommandOperator`, `EmptyOperator`, data flow
  and `>>` / `chain()`, `TaskDefaults` (`retries`, `retry_delay`, `timeout`, `queue`,
  `max_output_size`), `Cron` / `Every` / `None` / presets, t-strings in commands and task
  arguments, `dag_ctx.run` (`id`, `kind`, `planned_for`, `since`, `dag_id`).
- Outputs: the type model, the three serialization checks, tagged extra types, dataclasses,
  output references with paths, `ProcessResult`, size limits and truncation.
- Trigger rule `NoneFailed` only; every task has the single outcome `SUCCESS`.
- Runner `parse` and `task` modes; the agent ↔ runner channel (Unix socket, length-prefixed
  JSON).
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

**Out (later milestones):** async task functions, sensors and triggers (M4), outcomes and `.on(...)`, `AllSucceeded`, `@branch`, groups, mapping,
`schedr.toml` and scripts, cleanup and resources, secrets and connections, params, notifications,
`TriggerDagOperator`, `Once`, concurrency limits other than `max_active_runs`, backfill,
retention, the journal, the memory watchdog, mTLS, separate roles and Postgres at run time (M2),
the web UI (M3), `run_dag(step=True)` and `replace=` (M4), anything Airflow (M5).

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
  schedules.py       # Cron, Every, presets
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
