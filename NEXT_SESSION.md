# Next session: where things stand

Read this file, `PROJECT.md`, `PLAN.md` and `BUGS.md` before starting.
- `BUGS.md` lists the open bugs. Log new ones there instead of fixing them on the spot.
- `PROJECT.md` is the decision log. Every decision goes there, in the section it belongs to.
- `PLAN.md` holds the milestones, the M1 steps (done) and the M2 steps (next).

## State (2026-10-22)

- Branch `main`, pushed to GitHub by the user. **M1 is done** (PLAN.md: the "Status"
  paragraph). The last M1 commits: `fb0d5ec` (1.15b, `schedr.testing`), `6078d4e`
  (`start` / `end` on the trigger), `df309b8` (1.16, packaging), then the M1 wrap-up.
- Since the previous notes, decided and built (all recorded in PROJECT.md):
  - `trigger=` replaces `schedule=`: `Cron` / `Every` / `Once` / `None` share a `Trigger`
    base; `catchup`, `start` and `end` are on the time triggers; `.override(...)` on every
    trigger (`DAILY.override(start=...)`); `initial_since` stays on `Dag`. Event triggers
    become `@trigger` calls (`trigger=new_files(prefix=...)`, M4).
  - `dags list`'s `TRIGGER` column: `0 3 * * * (Europe/Warsaw), catchup,
    2025-12-31 23:00Z (local:+1h)..2026-06-30 22:00Z (local:+2h)`.
  - `schedr.testing`: `task_context(...)`, `run_dag(path, dag_id, for_=...)` with per-task
    results; `dag_ctx` fails outside a task and outside a DAG body. `examples/` (the
    PROJECT.md example, kept identical by a test, and `daily_report.py`).
  - The built-in `python` interpreter is looked for: `$VIRTUAL_ENV`, a project `.venv` /
    `venv` (current directory and parents), next to the binary, `python3` on `PATH` (pyenv
    shims resolved once). A runner that can't start names its interpreter and output.
  - CI's `wheel` job: Linux x86_64 only, with `scripts/smoke-wheel.sh` (PLAN.md's "M1 is
    done", automated). aarch64 and macOS wheels come back in M2 (2.12).
  - The in-task CLI (`task-ctx`, `dag-ctx`, `dag-ctx run get` included) is M4.
- The last `make check` passed: about 900 Rust unit tests, 630 Python unit tests at 99.96 %
  coverage, 36 Rust and 15 Python integration tests; `make docs` passed; `make smoke`
  passed on a release wheel. CI was clean before 1.16; the new `wheel` job hasn't run on
  GitHub yet: ask the user how it went.
- Rust coverage: lines 88 % (the floor, 85 %, is on lines), regions just under 85 %.

## Next steps

1. **Check CI** (the user looks: `gh` isn't installed): the `wheel` job's first run.
2. **M2, step 2.1** (PLAN.md, M2): `schedr master`, `schedr worker`, `schedr webserver` as
   separate processes on Postgres, commands by `LISTEN/NOTIFY` with polling as the
   fallback. Start by reading how `standalone.rs` wires the roles together today.
3. Open bugs: B1 (a negative duration in `dags test`; didn't reproduce last time).

## How to work with the user

- One step at a time. At the end of each step:
  - run `make check` **and** `make docs` and confirm both pass;
  - record the decisions in PROJECT.md;
  - present a short summary and wait.

  The user answers with "accepted, record it, commit and go on" or similar. Don't commit before
  that.
- Commit messages read like `M2.1: <what>, <what>; <what>` and end with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Pre-commit hooks (fmt, clippy,
  ruff, mypy) run on commit. Stage renames and their users together (a commit of half a
  rename fails the hooks).
- When a real choice comes up, ask and recommend an option. Record the answer in PROJECT.md,
  including what wasn't chosen when it matters. The user likes to see options side by side
  (a small table with examples) before picking.

## Rules (also in PROJECT.md, "Repository and tooling", and in memory)

- **Unit tests:**
  - Every public function and class gets one, with at least 85 % coverage.
  - Private items (`_x` in Python, no `pub` in Rust) aren't tested directly.
  - Python unit tests go in `python/tests/unit/schedr/.../test_<module>.py`; Rust unit tests
    in `src/<module>/tests.rs`.
- **Integration tests come with every step.** Anything that spawns processes or uses a real
  server, the binary or Postgres counts as integration. They live in `crates/*/tests/`,
  `python/tests/integration/` and `examples/`.
- **Never skip, ignore or xfail a test without asking first**, and say why.
- **Bugs found on the way go to `BUGS.md`** (B-numbers never reused); fix them when the user
  schedules it, removing the entry in the fixing commit.
- **No Airflow terms in core** (XCom etc. only inside `schedr.airflow`).
- Secrets only come from env vars, never from the config file. A DB URL with a password in the
  file is a startup error; tests pass it via `SCHEDR_CONFIG__DATABASE__URL`.
- Rust MSRV is 1.99, `unsafe_code = forbid` (use rustix for fcntl and the like).
- New external library versions must be at least 2 weeks old. Check with
  `curl https://crates.io/api/v1/crates/<name>/versions` (or PyPI), and add them to the
  "Libraries" table in PROJECT.md.
- `0001_initial.sql` is edited in place until the first release.
- Match the surrounding code: comment density, naming, and the error message style (backticks
  around names, lower case, what to do next).

## Environment notes

- Logs and scratch files go in the session scratchpad, not `/tmp`.
- Python outside `make`:
  ```
  cd /home/ptutak/Git/schedr
  export UV_PROJECT_ENVIRONMENT=$(pyenv prefix) UV_PYTHON=$(pyenv prefix)/bin/python; unset VIRTUAL_ENV
  ```
  The Python with schedr installed is `$(pyenv prefix)/bin/python`.
- **Rust integration tests by hand:** `export SCHEDR_TEST_PYTHON=$(pyenv prefix)/bin/python`,
  then `cargo nextest run -p schedr --test local`. The Postgres tests also need
  `SCHEDR_TEST_POSTGRES_URL`. `make test-integration` starts Postgres
  (`postgres:17.6-alpine`) with podman if present, else docker. The docker snap under WSL has
  broken port forwarding.
- **`run_dag` by hand** with a freshly built binary: `SCHEDR_BIN=$PWD/target/debug/schedr`
  (the pyenv's installed `schedr` is rebuilt only by `uv sync` / `make`).
- **The wheel:** `make smoke` (release build, a few minutes), or `uv build --wheel` and
  `scripts/smoke-wheel.sh <wheel> <python 3.14>`. maturin isn't installed globally.
- **Ports:** 8000 is taken on this machine. `make docs-serve` uses `DOCS_PORT` (default
  8765).

## Code map, briefly

- `crates/schedr-model`: shared types (`trigger.rs`: `Trigger` with `catchup`, `start`,
  `end`).
- `crates/schedr-db`: sqlx, SQLite and Postgres, with dialect placeholders `{uuid}`, `{ts}`,
  `{json}` and `{text}`. INTEGER columns are read as i32.
- `crates/schedr-agent`: processes, the runner, attempts, the agent loop.
- `crates/schedr-master`: discovery, scheduling (`fire_times.rs`, `planning.rs`,
  `scheduler.rs`), execution, commands, the gRPC service, `workers.rs` (registry, spread).
- `crates/schedr-api`: the REST `/api/v1` (axum, utoipa → `schema/openapi.json` via
  `cargo xtask`).
- `crates/schedr`: the binary.
  - `cli.rs`: clap (`milestone()` says which commands come later).
  - `app.rs`: dispatch and `AppError`; `local_config` applies the interpreter lookup.
  - `interpreter.rs`: the built-in `python` lookup.
  - `user.rs`: server commands; `local.rs`: `dags check` and `dags test`.
  - `standalone.rs`, `client.rs` and `times.rs` (`show`, `show_short`).
- `python/schedr`: the native API (`triggers.py`, `dag.py`, `tasks.py`, ...),
  `_serialization`, `runner/`, `testing.py`.
  - The runner is started with an inherited socket in `SCHEDR_AGENT_FD`; modes `parse`
    and `task`.
- `examples/`: example DAGs and their tests; `scripts/smoke-wheel.sh`: the wheel's smoke test.
