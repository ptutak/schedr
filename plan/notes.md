# schedr: the plan's lasting notes (from PLAN.md, 2026-11-08)

Draft. The design is in archive/PROJECT.md; this file only says **what to build first and in which
order**. Milestones: see archive/PROJECT.md, Roadmap.

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
  and `>>` / `chain()`, `StepDefaults` (`retries`, `retry_delay`, `timeout`, `queue`,
  `max_output_size`), `trigger=` with `Cron` / `Every` / `Once` / `None` / presets, t-strings
  in commands and task arguments, `dag_ctx.run` (`id`, `kind`, `planned_for`, `since`) and
  `dag_ctx.dag_id`.
- Outputs: the type model, the three serialization checks, tagged extra types, dataclasses,
  output references with paths, `ProcessResult`, size limits and truncation.
- Trigger rule `NoneFailed` only; every task has the single outcome `SUCCESS`.
- Runner `parse` and `task` modes; the agent ↔ runner channel (Unix socket, length-prefixed
  JSON).
- **M2.5 also gets quiet DAGs** (busy DAGs visited every round, quiet DAGs only when woken; archive/PROJECT.md, Components and roles, "Found by the stress suite") with stress scenario 14 (many quiet DAGs).
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
- gRPC between master and agent over localhost (the protocol as decided in archive/PROJECT.md:
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

- `schedr-model` is the reference for every shape (archive/PROJECT.md: Shared model and schemas).
  `schema/*.json` are generated from it by a small `xtask`-style binary; CI fails if they
  differ from the committed files.
- The user CLI lives in the binary crate (a thin REST client); the in-task CLI
  (`task-ctx`, `dag-ctx`) too, talking to the agent socket.

## Library choices

Confirmed: see archive/PROJECT.md, Repository and tooling: Libraries.

## Testing approach

- **Rules: archive/PROJECT.md, Repository and tooling: Testing** (unit tests for every public function
  and class, private ones not tested directly, 85 % coverage, structure mirroring the code, no
  skipped tests without asking), and integration tests with each step from 1.15 on (those of
  1.4–1.14 were written after 1.14).
- **Cross-language parity:** produced DAG JSON validated against the committed schema; shell
  quoting against `shlex.quote`; encode in Python / decode in Python after a round trip through
  Rust storage.
- **End to end:** `examples/` DAGs with `run_dag` tests, run in CI against the built binary.
- **Time:** the master's clock is injectable; scheduling tests never sleep.
