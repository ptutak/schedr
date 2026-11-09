# Development: repository, tooling and working rules

## Current

How the project is built, checked and worked on. From NEXT_SESSION.md's lasting sections (moved here 2026-11-08,
unchanged); phase 3 of the restructuring adds archive/PROJECT.md's "Repository and tooling" here.

### Rules (also in archive/PROJECT.md, "Repository and tooling", and in memory)

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
- **Bugs found on the way go to `bugs/`** (`scripts/bugs.py new`; B-numbers never reused), each
  rated (severity, probability, kind: `bugs/README.md`, Ratings), `make bugs` after; fix them
  when the user schedules it, removing the entry in the fixing commit. A bug that a step's
  own design has to fix anyway (like B5 in 2.4) can be fixed in that step: say so in the
  summary and in archive/PROJECT.md.
- **No Airflow terms in core** (XCom etc. only inside `schedr.airflow`).
- Secrets only come from env vars, never from the config file. A DB URL with a password in
  the file is a startup error; tests pass it via `SCHEDR_CONFIG__DATABASE__URL`.
- Rust MSRV is 1.99, `unsafe_code = forbid` (use rustix for fcntl, flock, signals and the
  like). Clippy limits functions to 100 lines: split by meaning when it complains.
- Library ages (archive/PROJECT.md, Libraries): a new major or minor (its `x.y.0`) at least 7 UTC
  calendar days old, a patch at least 3 days; a yanked version moves forward, else back. Check with
  `curl https://crates.io/api/v1/crates/<name>/versions` (or PyPI), and add them to the
  Libraries table below.
- `0001_initial.sql` (both `postgres/` and `sqlite/`) and the protocol (`PROTOCOL_VERSION`
  0.1) are edited in place until the first release: **1.0.0, after M4B** (the user,
  2026-10-29; PLAN.md, Release 1.0.0). From then on, migrations and additive protocol changes.
- Match the surrounding code: comment density, naming, and the error message style (backticks
  around names, lower case, what to do next).
- Regenerate `schema/` with `make schema` after API or DAG JSON changes (`make check` runs
  `schema-check`).

### How we work now (2026-10-30)

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
- **Decisions:** numbered (the next is **103**, open: B90), lettered options, the recommended first, a
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

### Environment notes

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

### Libraries

A new dependency gets a row here: the need, the choice (with its version and date), why, and what
wasn't chosen; one the user decides is also a decision. The ones chosen before 2026-11-09 are in
`archive/PROJECT.md`, Libraries.

| Need | Choice | Why | Not chosen |
|---|---|---|---|

## Decisions

<!-- generated by scripts/records.py: decisions -->
- D372 (rule): PROJECT.md lives in archive/
- D371 (rule): Work reaches main through git; undone-check guards recent commits
- D370 (rule): JOURNAL.md holds decisions from 2026-11-08 on; archive/PROJECT.md is left as it is
- D369 (rule): Decisions point to anchors in archive/PROJECT.md and JOURNAL.md
- D368 (rule): One source for each record; every other view generated
- D367 (rule): The project's documents: STATUS, design topics, a file per decision, archive/PROJECT.md as history
- D166 (built R0.3): The documentation site shows the latest release
- D165 (built R0.3): Release documentation is deployed to GitHub Pages
- D164 (built R0.3): The Python image is schedr and the server image is schedr-server
- D163 (built R0.1): The committed tree holds the version named by the tag
- D162 (built R0.1): Release images use version and minor tags
- D161 (built R0.1): A release rehearsal performs every step as a candidate
- D160 (built R0.1): A release runs CI on its tag and publishes CI wheels
- D158 (built R0.1): Release candidates publish to TestPyPI
- D157 (built R0.1): Releases push server and worker images while the chart stays in the repository
- D156 (rule): The 0.x schema and protocol are edited in place
- D155 (built R0.1): Releases carry notes, wheels, checksums and attestations
- D154 (built R0.1): A pushed version tag starts a release
- D153 (built R0.1): Releases use Trusted Publishing to PyPI
- D147 (built M4.5b): The inherited descriptor uses unsafe code in schedr-fd (from `task-context`)
- D124 (rule): Build the Deployments page in M4.4b
- D98 (built M4.3e): Inject a failure at each database write in master operation tests
- D92 (built M4.4): Concurrency limits have integration and stress coverage
- D52 (rule): M4 uses small substeps and the revised review schedule
- D51 (rule): M4 implementation follows dependency order
- D48 (untraced): No record found
- D44 (untraced): No recorded decision found
- D29 (built M3.1): Build the documentation as part of releases
- D28 (built M3.1): Serve the versioned documentation inside the UI (from `ui-cli`)
- D27 (built M3.2b): Run a small Playwright suite for the web UI
- D12 (untraced): No recorded decision found
- D11 (untraced): No recorded decision found
- D10 (untraced): No recorded decision found
- D9 (untraced): No recorded decision found
- D8 (untraced): No recorded decision found
- D7 (untraced): No recorded decision found
- D6 (untraced): No recorded decision found
- D5 (untraced): No recorded decision found
- D4 (untraced): No recorded decision found
- D3 (untraced): No recorded decision found
- D2 (untraced): No recorded decision found
- D1 (untraced): No recorded decision found
<!-- end of decisions -->

## Where in code

`Makefile`, `scripts/`, `.github/workflows/`, `pyproject.toml`, `Cargo.toml`; the overview of the crates is
`design/code-map.md`.

<!-- generated by scripts/records.py: code -->
- `JOURNAL.md` (D367, D370)
- `STATUS.md` (D367)
- `archive/PROJECT.md` (D372)
- `crates/schedr-db/src/database.rs` (D98)
- `crates/schedr-db/src/faults.rs` (D98)
- `crates/schedr-fd/src/lib.rs` (D147)
- `crates/schedr-model/src/ctx.rs` (D147)
- `crates/schedr-model/src/runner.rs` (D163)
- `crates/schedr-model/src/runner/tests.rs` (D163)
- `crates/schedr-ui/src/deployments.rs` (D124)
- `crates/schedr-ui/src/docs.rs` (D28)
- `design/README.md` (D367)
- `plan/README.md` (D368)
- `scripts/docs-dist.sh` (D28)
- `scripts/records.py` (D367, D368, D369)
- `scripts/release.py` (D153, D154, D155, D156, D157, D158, D160, D161, D162, D163)
- `scripts/undone.py` (D371)
<!-- end of code -->

## Open

<!-- generated by scripts/records.py: questions -->
None.
<!-- end of questions -->

None recorded yet.
