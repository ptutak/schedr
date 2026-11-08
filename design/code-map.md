# Code map

Where each part of schedr lives, briefly; each topic's "Where in code" has the detail. From NEXT_SESSION.md
(moved here 2026-11-08); check against the tree before relying on a line.

- `crates/schedr-model`: shared types (`logs.rs`: the log endpoint's headers and part sizes; `trigger.rs`, `commands.rs` with every command,
  `runner.rs` with `ErrorSummary` and the error kinds `TASK_NOT_FOUND` / `DAG_FILE_MISSING`,
  `version.rs` with `SchedrVersion`).
- `crates/schedr-db`: sqlx, SQLite and Postgres, with dialect placeholders `{uuid}`, `{ts}`,
  `{json}` and `{text}`; INTEGER columns are read as i32. `workers.rs` (registry, `drained`),
  `dags.rs` (parses, `root_hashes`), `attempts.rs`, `instances.rs` (`cancel_requested_at`),
  `reads.rs` (API reads, `run_code_hashes`).
- `crates/schedr-agent`: the worker agent.
  - `agent.rs`: the loop (sessions, offline waits, assignments, `Finished`, drain wiring).
  - `journal.rs`: the on-disk journal and orphan stopping; `drain.rs`: the drain's timing.
  - `logs.rs`: the log endpoint; `retention.rs`: the log sweep.
  - `launch.rs` / `process.rs` / `supervise.rs`: processes; `attempt.rs`: planning;
    `watch.rs` / `scan.rs`: root hashes.
- `crates/schedr-master`:
  - `core.rs`: the master (registration, messages, tick, `sync_code`).
  - `discovery.rs`: reported hashes, the current code per root, parse jobs (`parse_jobs.rs`).
  - `dispatch.rs`: assignment, rejections, cancels, code wait, wait reasons.
  - `workers.rs`: the live registry, `Needs`, the spread choice.
  - `execution.rs` / `progress.rs`: run state; `commands.rs`: applying commands;
    `service.rs`: the gRPC service (`Hub`); scheduling in `scheduler.rs` and friends.
- `crates/schedr-api`: the REST `/api/v1` (axum, utoipa → `schema/openapi.json` via
  `cargo xtask`); `logs.rs`: the log proxy (`LogSource`, `HttpLogSource`).
- `crates/schedr-tls`: `material.rs` (PEM → rustls configs, the fixed worker name),
  `reload.rs` (`Tls`, polling the files), `incoming.rs` (the listener doing handshakes, for
  tonic and axum), `connect.rs` (the worker's channel), `testing.rs` (feature `testing`).
- `crates/schedr`: the binary.
  - `roles.rs`: master / webserver / worker roles (`role_tls`, `WorkerLinks`, `log_endpoint`), `stop_on_signal` (a count of stop requests:
    the first drains a worker, a second interrupts), `agent_settings`, `lock_journal`.
  - `standalone.rs` (drains its worker before stopping master and API), `cli.rs`, `app.rs`,
    `user.rs` (server commands), `local.rs` (`dags check` / `dags test`), `client.rs`,
    `interpreter.rs`, `times.rs`.
- `python/schedr`: the native API, `_serialization`, `runner/` (started with an inherited
  socket in `SCHEDR_AGENT_FD`; modes `parse` and `task`), `testing.py`.
- `examples/`: example DAGs and their tests, `examples/kubernetes/` (Dockerfile, standalone
  StatefulSet); `scripts/smoke-wheel.sh`: the wheel's smoke test.
