# Next session: where things stand

Read this file, then `PLAN.md` (M4 and M4.5), `BUGS.md`, and the parts of `PROJECT.md` a step
names. `PROJECT.md` is the decision log: every decision goes there, in its section, with
"Not chosen" and why.

## State (2026-10-28, end of session)

- On `main`, clean and pushed. **M1, M2, M2.5 and M3 are done. M4 is in progress:** M4.1
  (results and outcomes, sub-steps a-d) and M4.2a (`@branch`) are done; next is **M4.2b
  (groups)**, then M4.3 (mapping) and on in PLAN.md's order (51A).
- CI: `gh run list`. The latest run passed; the only failures this week were the
  `wheel-macos-arm64` job being cancelled (not a test failure).
- The last `make check`: 1488 Rust unit, 124 integration, 782 Python and 27 browser tests,
  all passing.

Today's commits, oldest first:

| Commit | What |
|---|---|
| `669ae0f` | M3.5 + M3.6 |
| `335fb90` | B25 (hidden tabs close their streams), B26 (open requests cut 3 s after a stop), times as "UTC (local)", `make standalone` with the fork server, chart NOTES warning |
| `04d083f` | a dialog race (live reads skipped while a dialog is open) |
| `c172efd` | the round woken on `Finished` (42B/43A), Keycloak claim header, task panel times, a test port |
| `1e75e64` | LDAP moved to M6 (decisions 45-47 laid out) |
| `131afa8` | the M4 plan (M4.1-M4.15, decisions 49-53) |
| `81d40b0` | stress results in the guide, scenario 15c |
| `c4b78cc` | M4.1a: outcomes in the model and the master's rules |
| `4e9ee76` | M4.1b: the Python API (`Outcome`, `.on()`, `.outcome`, `run_if`, `CommandOperator(outcomes=)`) |
| `d9aecbf` | M4.1c: the (output, outcome) pair end to end |
| `f021fa6` | a CI race in the federation test (`data-command`) |
| `8c2f979` | M4.1d: the run graph labels edges with outcomes |
| `820e6c2` | `.outcome` refused where the output may have a field `outcome` |
| `2855681` | M4.2a: `@branch` and selection edges (55B: counted like any edge) |
| `f247cf5` | Grok's batch review of M4.1a-M4.2a: 5 fixes (None check at any depth and for outcome references, `may_skip` per rule, paths into `.on(A, B)`, a branch's output along selection edges, the AllSucceeded doc); 1 rejected; B27, B28 logged |
| `835ad4d` | **M4.5 planned:** batched worker messages, a savepoint per message (decision 54) |

## Open when this session ended

- **Grok's docs round for M4.1a-M4.2a is done and checked** (session `bdf356db`): a new page
  `docs/02-dags/08-outcomes.md` with `examples/docs/outcomes.py`, and the guide's life of a
  run, master, UI and glossary. One wrong claim corrected (run creation doesn't decide
  waiting tasks). If it isn't committed yet, it waits for the user's acceptance.
- **Grok's budget:** 98% of the week used (plus $5 of credits). The reset is **Friday
  2026-10-30 9:00**. Ask the user for the weekly % after each round and log it in
  `~/.claude/skills/grok/USAGE.md` (commit and push that repository).
- **Friday after the reset: Grok's whole-repository review** (PLAN.md, "After M3: Grok over
  the whole repository"), the unreviewed delta first. M4.1a-M4.2a are already reviewed;
  whatever is built after `835ad4d` isn't.
- **Pending user decisions:** the assign-to-started improvements (Grok's investigation: rounds
  holding the master lock; split the lock, send `AssignTask` once committed, shorter rounds,
  stamp `started_at` at arrival, grouped journal fsyncs). Not scheduled; M4.5.1 measures
  first. Small offers still open: a clearer message for a stale dev database at migration 1,
  and a preload list in `make standalone`.
- **Bugs:** B22 (a draining worker missed its grace under load) seen twice; B27 (references
  inside containers not type-checked, reproduced) and B28 (selection edges from plain tasks
  not refused, suspected) and B29 (an unchosen branch edge isn't faint on the graph,
  suspected) are new.

## How we work now (2026-10-28)

- **Steps are split into small sub-steps** (M4.2b, M4.2c, ...). Each is committed after
  Claude's own review, with mutation-checked tests (every regression test shown failing with
  its fix undone). **Grok reviews** (the user, 2026-10-29): one during the 2026-10-29
  session (on the $5 of credits), then Friday's whole-repository review; later M4 reviews to
  be planned then. No batch reviews of about five sub-steps. Read-only with throwaway proof
  tests. Claude writes the guide for each step. Every Grok finding is verified before
  acting; the rejected ones are reported with why.
- **Decisions:** numbered (the next is **74**), lettered options, the recommended one first,
  with a "Who (does the work)" column (Claude / Grok (mode); "you" only for commands the user
  must run). Ask all coming decisions at once before unattended work.
- **Commit and push only after the user accepts** each one (2026-10-28). Unattended (night)
  work commits and pushes only when the user accepted that in advance and no relevant
  decision is waiting for them; otherwise stop before the commit. Check CI with
  `gh run list`.
- **Stress tests:** never run `make stress` (or a scenario) without the user asking.
- **Processes:** kill only your own (no broad `pkill`).
- **Checks:** `SCHEDR_TEST_PYTHON=$(pyenv prefix)/bin/python
  SCHEDR_TEST_POSTGRES_URL=postgres://postgres:schedr@127.0.0.1:45243/postgres make check`,
  with the podman container `schedr-pg-check` running Postgres on 127.0.0.1:45243; then
  `make docs`.
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
- **Bugs found on the way go to `BUGS.md`** (B-numbers never reused, next is B37); fix them
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

## In progress at the end of 2026-10-29 (uncommitted)

- **M4.2b (groups)** committed (`8f7a6d7`); **M4.2c (import errors page)** built (decisions
  66-68). Next: M4.3 (mapping).
- **Docs of M4.2b and M4.2c go to a Grok session** (the user, 2026-10-29), not Claude:
  Writing DAGs' groups page, the guide's UI page (group boxes, the import errors page).
- **One `/code-review` over the session's work is still owed** (the user: later).
- Until Friday 9:00, code reviews are Claude's own (`/code-review`), not Grok's.
