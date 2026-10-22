# Next session: where things stand

Read this file, `PROJECT.md`, `PLAN.md` and `BUGS.md` before starting.
- `BUGS.md` lists the open bugs. Log new ones there instead of fixing them on the spot.
- `PROJECT.md` is the decision log. Every decision goes there, in the section it belongs to.
- `PLAN.md` holds the milestones, the M1 steps (done) and the M2 steps (2.1–2.6 done).

## State (2026-10-22)

- Branch `main`; the user pushes it to GitHub. **M1 is done; M2 steps 2.1 to 2.6 are done.**
  The M2 commits so far:

  | Commit | Step |
  |---|---|
  | `5d6328a` | 2.1: `schedr master`, `worker`, `webserver` as separate processes; Postgres `LISTEN/NOTIFY` for commands |
  | `54e661d` | 2.1b: standalone in one pod (`--listen`, one standalone per data directory, musllinux wheel, `examples/kubernetes`) |
  | `61bfd7a` | 2.2: worker lifecycle (registry, leases, incarnations, reconnect with jitter, `schedr workers list`) |
  | `d6e43b5` | fixes B3 (tests bind port 0) |
  | `87bbcd2` | 2.3: the journal on disk (`crates/schedr-agent/src/journal.rs`); orphans stopped on restart; `WorkerRestarted` / `WorkerLost` error summaries |
  | `d207fb9` | 2.4: `schedr tasks cancel` / `retry`, `[master] code_wait`, wait reasons; fixes B5 |
  | `3aea937` | 2.5: only the newest code runs (current hash per root, `root_hashes` table, `CODE_MISMATCH`), rolling upgrades, `dag_file_missing` and "changed after this run started" errors |
  | `6c75475` | 2.6: drain on SIGTERM (`crates/schedr-agent/src/drain.rs`, `[worker] termination_grace_period`, default 30 s) and `schedr workers drain` / `resume` (stored, survives restarts) |

- **Not pushed yet** at the time of writing: ask the user whether `main` is pushed and how CI
  went (`gh` isn't installed, so the user looks). The `wheel` and musllinux jobs haven't been
  reported on since 2.1b.
- The last `make check` passed: 1021 Rust unit tests, 632 Python unit tests at 99.96 %
  coverage, 58 Rust and 15 Python integration tests; Rust line coverage 88.6 % (floor 85 %).
  `make docs` passed.
- **Open bugs: B4 and B6** (`BUGS.md`). Both are role integration tests failing under disk
  load; B6 happened three times, each time on the first `make check` of a step (right after
  the coverage run), never when the suite runs alone. If `make check` fails that way, run
  `make test-integration` alone; if it passes, rerun `make check`, note the occurrence in B6,
  and go on. Its fix waits for the user to schedule it.

## Next steps

1. **Ask about CI** for the pushed commits.
2. **M2, step 2.7** (PLAN.md): **mTLS** (`tls.mode = "app"` / `"mesh"`, certificates
   reloaded when the files change), the **worker's log endpoint** (signed tokens) and the
   webserver's log proxy, log retention on the worker. Start by laying out the decisions for
   the user (see "How to work"), reading PROJECT.md's mTLS and log sections first
   (`grep -n "mTLS\|log endpoint" PROJECT.md`). The protocol already carries
   `Register.log_endpoint` (empty today); `master_host` in `crates/schedr/src/roles.rs`
   refuses `https://` until mTLS.
3. Then 2.8 (memory watchdog), 2.9 (ops port), 2.10 (images and Helm), 2.11 (kill tests on
   kind), 2.12 (more wheels).

## How to work with the user

- **One step at a time, decisions first.** Before coding a step, read what PROJECT.md and
  PLAN.md say, check what the code already has, and present the decisions: per question a
  small table of options with the recommended one first (marked), and why. Wait for the
  user's choice ("accepted, go with recommendations" or a change). When the user asks a
  question mid-step, answer it plainly (pros and cons in a table) before going on.
- At the end of each step:
  - run `make check` **and** `make docs` and confirm both pass;
  - run the code review once over everything the step changed (the `code-review` skill).
    Judge each finding, fix the real ones, say which you skipped and why, re-run the checks,
    and report the findings in the summary. If fixes or additions after the review are
    substantial new code, run the review again;
  - record the decisions in PROJECT.md (with "Not chosen" and why), and update PLAN.md's row
    if the step's scope changed;
  - present a short summary and wait.

  **Commit only when the user says so** ("commit"). **Never push**: the user pushes.
- Commit messages read like `M2.x: <what>, <what>; <what>` and end with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Pre-commit hooks (fmt, clippy,
  ruff, mypy) run on commit. Stage renames and their users together.
- The user likes options side by side in small tables, a recommendation first, and precise
  explanations when asked to "explain" or "elaborate". When an idea of theirs has a catch,
  say so with the reasons and offer a sound variant (e.g. 2.5's newest-only routing instead
  of pinning workers to old code).

## Rules (also in PROJECT.md, "Repository and tooling", and in memory)

- **Unit tests:**
  - Every public item (Rust `pub`, `pub(crate)`, `pub(super)`; Python public names) gets one,
    with at least 85 % coverage. Private items aren't tested directly.
  - Python unit tests go in `python/tests/unit/schedr/.../test_<module>.py`; Rust unit tests
    in `src/<module>/tests.rs`.
- **Integration tests come with every step.** Anything that spawns processes or uses a real
  server, the binary or Postgres counts as integration. They live in `crates/*/tests/`
  (`crates/schedr/tests/roles.rs` has the `Cluster` helper: roles as processes, `start_as`,
  `terminate`, `kill`, `cli`, `attempts`, `worker_state`), `python/tests/integration/` and
  `examples/`. Multi-worker tests use Postgres (`postgres_database()`), not SQLite.
- **Never skip, ignore or xfail a test without asking first**, and say why.
- **Bugs found on the way go to `BUGS.md`** (B-numbers never reused, next is B7); fix them
  when the user schedules it, removing the entry in the fixing commit. A bug that a step's
  own design has to fix anyway (like B5 in 2.4) can be fixed in that step: say so in the
  summary and in PROJECT.md.
- **No Airflow terms in core** (XCom etc. only inside `schedr.airflow`).
- Secrets only come from env vars, never from the config file. A DB URL with a password in
  the file is a startup error; tests pass it via `SCHEDR_CONFIG__DATABASE__URL`.
- Rust MSRV is 1.99, `unsafe_code = forbid` (use rustix for fcntl, flock, signals and the
  like). Clippy limits functions to 100 lines: split by meaning when it complains.
- New external library versions must be at least 2 weeks old. Check with
  `curl https://crates.io/api/v1/crates/<name>/versions` (or PyPI), and add them to the
  "Libraries" table in PROJECT.md.
- `0001_initial.sql` (both `postgres/` and `sqlite/`) and the protocol (`PROTOCOL_VERSION`
  0.1) are edited in place until the first release.
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
  (`postgres:17.6-alpine`) with podman and sets both. The docker snap under WSL has broken
  port forwarding.
- **`run_dag` by hand** with a freshly built binary: `SCHEDR_BIN=$PWD/target/debug/schedr`.
- **Wheels:** `make smoke` (release build, a few minutes), `make smoke-musl` (musllinux via
  `uvx maturin` + ziglang, smoke-tested on Alpine), or `uv build --wheel` and
  `scripts/smoke-wheel.sh <wheel> <python 3.14>`.
- **Ports:** 8000 is taken on this machine. `make docs-serve` uses `DOCS_PORT` (default 8765).
  Tests bind port 0 and read the bound address from the logs.

## Code map, briefly

- `crates/schedr-model`: shared types (`trigger.rs`, `commands.rs` with every command,
  `runner.rs` with `ErrorSummary` and the error kinds `TASK_NOT_FOUND` / `DAG_FILE_MISSING`,
  `version.rs` with `SchedrVersion`).
- `crates/schedr-db`: sqlx, SQLite and Postgres, with dialect placeholders `{uuid}`, `{ts}`,
  `{json}` and `{text}`; INTEGER columns are read as i32. `workers.rs` (registry, `drained`),
  `dags.rs` (parses, `root_hashes`), `attempts.rs`, `instances.rs` (`cancel_requested_at`),
  `reads.rs` (API reads, `run_code_hashes`).
- `crates/schedr-agent`: the worker agent.
  - `agent.rs`: the loop (sessions, offline waits, assignments, `Finished`, drain wiring).
  - `journal.rs`: the on-disk journal and orphan stopping; `drain.rs`: the drain's timing.
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
  `cargo xtask`).
- `crates/schedr`: the binary.
  - `roles.rs`: master / webserver / worker roles, `stop_on_signal` (a count of stop requests:
    the first drains a worker, a second interrupts), `agent_settings`, `lock_journal`.
  - `standalone.rs` (drains its worker before stopping master and API), `cli.rs`, `app.rs`,
    `user.rs` (server commands), `local.rs` (`dags check` / `dags test`), `client.rs`,
    `interpreter.rs`, `times.rs`.
- `python/schedr`: the native API, `_serialization`, `runner/` (started with an inherited
  socket in `SCHEDR_AGENT_FD`; modes `parse` and `task`), `testing.py`.
- `examples/`: example DAGs and their tests, `examples/kubernetes/` (Dockerfile, standalone
  StatefulSet); `scripts/smoke-wheel.sh`: the wheel's smoke test.
