# Next session: where things stand

Read this file, then `PLAN.md` (M4 and M4.5), `BUGS.md`, and the parts of `PROJECT.md` a step
names. `PROJECT.md` is the decision log: every decision goes there, in its section, with
"Not chosen" and why.

## State (2026-10-30)

- **M1–M3 done; M4 in progress.** M4.1 (outcomes), M4.2 (`@branch`, groups, import errors) and
  M4.3 (mapping, a–c) are done, reviewed and fixed. **Now: a pause for bugs, M4.3e** (the
  user: "finish bugfixing before we move on with M4.4"; M4.4 not today). Then M4.3d (the CLI's
  `runs show`, `tasks attempts`, `tasks output`: B37), then M4.4 (concurrency limits:
  decisions 87–92, 95–96 taken), in PLAN.md's order.
- **Commits of 2026-10-30:** `5b451e1` the M4.3 review fix-up (decisions 93, 94, 97; the docs
  round; B37–B77 logged; Codex and the sandbox in AI_SETUP / AI_INSTRUCTIONS); `a8bf777` B39
  partly fixed (a group drawn as a rigid column; the rest in M6). CI: `gh run list`.
- **Uncommitted, under Grok's review (`5e2b8d14`, worktree `schedr-grok-harness`):** M4.3e
  part 1: the fault-injection harness (decision 98) and B49's fix. On acceptance: commit.
- **Next in M4.3e:** B42's core half (`Core::forget` removes the connection before its
  writes, which are two transactions), then B79 (a run's preparation in three writes), each
  as a harness scenario first; then the high-severity bugs section by section: F1's
  B41, B43, B44; F2's B50 (B51, B52 medium); F6's B53 (**decision 99**: refuse a placeholder
  inside quotes, or quote by context), B54, B55; F7's B58, B59; F8's B64; F3's B78.
  Medium and low wait in BUGS.md.

## Open when this session ended

- **Codex's whole-repository review** (the Codex skill's `reviews/schedr-full-2026-10-30.md`;
  ~50% of Codex's week): done F1, F2, F3, F6, F7, F8 (B41–B83, all logged). Next F4 (the
  database layer), F5 (scheduling and discovery), then F9–F14. One section at a time; its
  worktree is `schedr-codex-f1` (detached at `a8bf777`; move it to `main` before a section,
  copy the current BUGS.md in). Its 5-hour window reached 63%: wait for it before more.
- **Usage:** Grok 31% of its week (resets Fridays 9:00; ask the user for the %); Codex 21%
  weekly (read from its turns; resets Thursdays 01:57).
- **Bugs:** B4, B22, B24, B31, B32 (older, suspected); B37 (M4.3d); B38, B39 (M6); B40–B83
  (B49 fixed in the uncommitted work). The next number is **B84**.

## How we work now (2026-10-30)

- **Claude implements; two colleagues, each through a skill:** **Grok** (`/grok`, grok-4.7,
  high) reviews each step, hunts and fixes bugs, writes docs; **Codex** (`/codex`,
  gpt-6.1-sol, high) gives second opinions on architecture, reviews beside Grok, finds bugs,
  writes docs, runs the whole-repository review; **never codes**. AI_INSTRUCTIONS "Working
  with second agents" and AI_SETUP "Agent tooling" have the details.
- **Both run in `sandbox.sh`** (bubblewrap): only the worktree, the toolchains (read-only) and
  their own state; no other project, no `~/.ssh`, no `/mnt/c`; nothing the user runs later is
  writable; an empty environment but an allowlist. Prompts also say to read only inside the
  worktree. Prepare a worktree (its pyenv, `uv sync`, a build) before a colleague's turn.
- **Every finding is verified before acting;** rejected ones reported with why. Code Grok
  wrote is reviewed by Codex (or a fresh Grok session).
- **Decisions:** numbered (the next is **99**), lettered options, the recommended first, a
  "Who (does the work)" column. Ask all coming decisions at once before unattended work.
- **Commit and push only after the user accepts** each one. Check CI with `gh run list`.
- **Stress tests:** never without the user asking. **Processes:** kill only your own.
- **Checks:** `SCHEDR_TEST_PYTHON=$(pyenv prefix)/bin/python
  SCHEDR_TEST_POSTGRES_URL=postgres://postgres:schedr@127.0.0.1:45243/postgres make check`
  (the podman container `schedr-pg-check` on 127.0.0.1:45243), then `make docs`. Running
  `cargo nextest` by hand needs the same two variables (forgetting the URL fails the Postgres
  tests).
- **The fault-injection harness** (decision 98): any master operation that writes gets a
  `Scenario` (crates/schedr-master/src/faults.rs, dispatch/tests.rs `mod faults`) run by
  `fail_each_write`; a multi-write bug is proven and fixed against it.
- Commit messages read like `M4.x: <what>, <what>; <what>` and end with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Rules (also in PROJECT.md, "Repository and tooling", and in memory)

- **Unit tests:**
  - Every public item (Rust `pub`, `pub(crate)`, `pub(super)`; Python public names) gets one,
    with at least 85 % coverage. Private items aren't tested directly.
  - Python unit tests go in `python/tests/unit/schedr/.../test_<module>.py`; Rust unit tests
    in `src/<module>/tests.rs`.
- **Integration tests come with every step.** Anything that spawns processes or uses a real
  server, the binary or Postgres counts as integration. They live in `crates/*/tests/`
  (`crates/schedr/tests/roles.rs` has the `Cluster` helper: roles as processes, `start_as`,
  `terminate`, `kill`, `cli`, `attempts`, `worker_state`; `Cluster::with_tls` and
  `start_tls` for mTLS, certificates from `schedr_tls::testing::TestCa`; the cluster's
  config is `[tls] mode = "mesh"` otherwise), `python/tests/integration/` and
  `examples/`. Multi-worker tests use Postgres (`postgres_database()`), not SQLite.
- **Never skip, ignore or xfail a test without asking first**, and say why.
- **Bugs found on the way go to `BUGS.md`** (B-numbers never reused, next is B84); fix them
  when the user schedules it, removing the entry in the fixing commit. A bug that a step's
  own design has to fix anyway (like B5 in 2.4) can be fixed in that step: say so in the
  summary and in PROJECT.md.
- **No Airflow terms in core** (XCom etc. only inside `schedr.airflow`).
- Secrets only come from env vars, never from the config file. A DB URL with a password in
  the file is a startup error; tests pass it via `SCHEDR_CONFIG__DATABASE__URL`.
- Rust MSRV is 1.99, `unsafe_code = forbid` (use rustix for fcntl, flock, signals and the
  like). Clippy limits functions to 100 lines: split by meaning when it complains.
- Library ages (PROJECT.md, Libraries): a new major or minor (its `x.y.0`) at least 7 UTC
  calendar days old, a patch at least 3 days; a yanked version moves forward, else back. Check with
  `curl https://crates.io/api/v1/crates/<name>/versions` (or PyPI), and add them to the
  "Libraries" table in PROJECT.md.
- `0001_initial.sql` (both `postgres/` and `sqlite/`) and the protocol (`PROTOCOL_VERSION`
  0.1) are edited in place until the first release: **1.0.0, after M4.5** (the user,
  2026-10-29; PLAN.md, Release 1.0.0). From then on, migrations and additive protocol changes.
- Match the surrounding code: comment density, naming, and the error message style (backticks
  around names, lower case, what to do next).
- Regenerate `schema/` with `make schema` after API or DAG JSON changes (`make check` runs
  `schema-check`).

## Environment notes

- **ufw is on** (2026-10-30). This WSL runs in mirrored networking mode, where `127.0.0.1`
  goes over `loopback0`: ufw allows in and out on it (without that rule every local
  connection fails: the tests, `make standalone`, the test Postgres).

- Logs and scratch files go in the session scratchpad, not `/tmp`.
- **The kubectl current context is a real OpenShift cluster (`…bradford-lab`). Never apply
  anything to it.** Use a local kind cluster only, with an explicit `--context`, and ask
  first. The OpenShift ("bradford") manifest comes later, when the user asks.
- Python outside `make`:
  ```
  cd /home/ptutak/Git/schedr
  export UV_PROJECT_ENVIRONMENT=$(pyenv prefix) UV_PYTHON=$(pyenv prefix)/bin/python; unset VIRTUAL_ENV
  ```
  The Python with schedr installed is `$(pyenv prefix)/bin/python`.
- **Rust integration tests by hand:** `export SCHEDR_TEST_PYTHON=$(pyenv prefix)/bin/python`,
  then e.g. `cargo nextest run -p schedr --test local`. The Postgres tests also need
  `SCHEDR_TEST_POSTGRES_URL`; `make test-integration` starts Postgres
  (`postgres:17.6-alpine`, digest-pinned) with podman and sets both. The docker snap under WSL has broken
  port forwarding.
- **`run_dag` by hand** with a freshly built binary: `SCHEDR_BIN=$PWD/target/debug/schedr`.
- **Wheels:** `make smoke` and `make smoke-musl` both call `scripts/build-wheel.sh`
  (release builds, a few minutes each: manylinux x86_64 and musllinux x86_64 in
  digest-pinned images, podman locally, then `scripts/smoke-wheel.sh`). The script
  installs rustup-init 1.29.1 (minimal channel only, `maturin build --locked`). Linux
  aarch64 and macOS arm64 are CI only (one matrix, `wheel-native`, shown as
  `wheel-linux-aarch64` and `wheel-macos-arm64`; macOS installs the same rustup-init, not
  the runner's). CI does not use `maturin-action` (the sdist installs maturin from
  `scripts/maturin-requirements.txt`).
- **Every role opens the ops port** (`[ops] port`, 8090): tests set it to 0, like the other
  ports, and read `ops on http://…` from the log.
- **Roles need `[tls]`:** `app` mode (the default) needs `ca`, `cert`, `key`; set
  `SCHEDR_CONFIG__TLS__MODE=mesh` to run roles by hand without certificates. Standalone never
  uses TLS. Tests and `dags test` set `[worker] log_port = 0`.
- **Ports:** 8000 is taken on this machine. `make docs-serve` uses `DOCS_PORT` (default 8765).
  Tests bind port 0 and read the bound address from the logs.

## Code map, briefly

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
