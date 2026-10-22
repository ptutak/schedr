# Next session: where things stand

Read this file, `PROJECT.md`, `PLAN.md` and `BUGS.md` before starting.
- `BUGS.md` lists the open bugs. Log new ones there instead of fixing them on the spot.
- `PROJECT.md` is the decision log. Every decision goes there, in the section it belongs to.
- `PLAN.md` holds the milestones and the M1 steps.

## State (2026-10-22)

- Branch `main`, pushed to GitHub by the user. Last commits: `f9988c0` (M1.15a) and `ef4911d` (the CI split).
- M1 steps 1.1–1.14 are done, along with 1.15a (`dags check`, `dags test`) and the
  integration tests for everything so far (1.17 in PLAN.md).
- The last `make check` passed: about 880 Rust unit tests, 581 Python unit tests at 99.96 %
  coverage, 33 Rust integration tests and 6 Python integration tests. `make docs` passed too.

## Next steps, in order

1. **1.15b, Python `schedr.testing`** (`python/schedr/testing.py` doesn't exist yet). See
   PROJECT.md, "Testing DAGs".
   - Already works: a `@task` called outside `with Dag(...)` runs the plain function
     (`tasks.py`, `__call__`, when `_build.active()` is None).
   - **`task_context(planned_for=, since=, dag_id=, run_id=, kind=, ...)`** sets `dag_ctx`
     values. It can probably reuse `context.running(RunValues(...))`. Params, secrets and the
     handle collector come in M4. Either raise a clear "milestone M4" error for those arguments
     or leave them out; decide this and record it.
   - **`dag_ctx` outside a task and outside a DAG body must fail** with "must be run inside a
     schedr task". Today `context.py` returns a `DagCtxReference` whenever no task is running,
     including outside a DAG body. Only inside `with Dag(...)` (when `_build.active()` is set)
     should it return a reference.
   - **`run_dag(path, dag_id, for_=...)`** runs `schedr dags test <dag> --path <path> --json`
     as a subprocess and wraps the JSON in result objects:
     - `result.state`;
     - `result.tasks["x"]` with `.state`, `.outcome`, `.output` (decoded by the task's
       annotation, as a downstream task would decode it), `.output_json`, `.attempts`
       (status, exit code, duration, error summary) and `.log`.

     Finding the binary: the wheel ships `schedr` as a script. Check how `python/schedr`
     locates it, and use `shutil.which("schedr")` with an env override as the fallback.
     `step=`, `replace=`, `params=` and `secrets=` come later (M4), so give them clear errors
     or leave them out.
   - The JSON shape `run_dag` reads is in `crates/schedr/src/local.rs` (`report()`):
     `{"run": RunView, "tasks": [{task_id, state, outcome, try_number, output, attempts,
     log}]}`.
   - **`examples/`** (doesn't exist yet): the example DAG from PROJECT.md ("Native API" and
     "End-to-end flow"), plus pytest tests using plain calls, `task_context` and `run_dag`.
     Hook the tests into `make test-integration` (they need the binary).
   - Write unit tests for everything public. `run_dag` also needs integration tests under
     `python/tests/integration/`.
2. **1.16, packaging:**
   - A maturin wheel with the binary for Linux x86_64.
   - A smoke test: `pip install` the wheel into a fresh venv, then run `schedr dags test` on the
     example and a short `schedr standalone` run.
   - CI (`.github/workflows/ci.yml`) was split before 1.16: the `rust` and `python` jobs run
     unit tests only, and the `integration` job runs them with a Postgres service. It hasn't run
     on GitHub with this split yet: check the latest run (`gh run list`) and fix what fails.
3. **M1 wrap-up:** walk through the "M1 is done" paragraph in PLAN.md by hand. Fix anything
   that doesn't work, update PLAN.md and PROJECT.md, and propose what comes next (M2).

## How to work with the user

- One step at a time. At the end of each step:
  - run `make check` **and** `make docs` and confirm both pass;
  - record the decisions in PROJECT.md;
  - present a short summary and wait.

  The user answers with "accepted, record it, commit and go on" or similar. Don't commit before
  that.
- Commit messages read like `M1.15a: <what>, <what>; <what>` and end with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Pre-commit hooks (fmt, clippy,
  ruff, mypy) run on commit.
- When a real choice comes up, ask and recommend an option. Record the answer in PROJECT.md,
  including what wasn't chosen when it matters.

## Rules (also in PROJECT.md, "Repository and tooling", and in memory)

- **Unit tests:**
  - Every public function and class gets one, with at least 85 % coverage.
  - Private items (`_x` in Python, no `pub` in Rust) aren't tested directly.
  - Python unit tests go in `python/tests/unit/schedr/.../test_<module>.py`; Rust unit tests
    in `src/<module>/tests.rs`.
- **Integration tests come with every step.** Anything that spawns processes or uses a real
  server, the binary or Postgres counts as integration. They live in `crates/*/tests/` and
  `python/tests/integration/`.
- **Never skip, ignore or xfail a test without asking first**, and say why.
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
- **Ports:** 8000 is taken on this machine. `make docs-serve` uses `DOCS_PORT` (default
  8765).
- The Python package is built by maturin, so after Rust changes the installed `schedr` binary
  in the pyenv may be stale. The Rust integration tests use `CARGO_BIN_EXE_schedr` instead.

## Code map, briefly

- `crates/schedr-model`: shared types.
- `crates/schedr-db`: sqlx, SQLite and Postgres, with dialect placeholders `{uuid}`, `{ts}`,
  `{json}` and `{text}`. INTEGER columns are read as i32.
- `crates/schedr-agent`: processes, the runner, attempts, the agent loop.
- `crates/schedr-master`: discovery, scheduling, execution, commands, the gRPC service.
- `crates/schedr-api`: the REST `/api/v1` (axum, utoipa → `schema/openapi.json` via
  `cargo xtask`).
- `crates/schedr`: the binary.
  - `cli.rs`: clap.
  - `app.rs`: dispatch and `AppError`. Every failure exits 1; 2 is for usage errors.
  - `user.rs`: server commands.
  - `local.rs`: `dags check` and `dags test`.
  - `standalone.rs`, `client.rs` and `times.rs`.
- `python/schedr`: the native API, `_serialization` and `runner/`.
  - The runner is started with an inherited socket in `SCHEDR_AGENT_FD`.
  - Its modes are `parse` and `task`.
