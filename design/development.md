# Development: repository, tooling and working rules

How the project is built, checked and worked on. From NEXT_SESSION.md's lasting sections (moved here 2026-11-08,
unchanged); phase 3 of the restructuring adds archive/PROJECT.md's "Repository and tooling" here.

## Rules (also in archive/PROJECT.md, "Repository and tooling", and in memory)

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
- **Decisions:** numbered (the next is in `decisions/next-id`), lettered options, the recommended first, a
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

## Libraries

A new dependency gets a row here: the need, the choice (with its version and date), why, and what
wasn't chosen; one the user decides is also a decision. The ones chosen before 2026-11-09 are in
`archive/PROJECT.md`, Libraries.

| Need | Choice | Why | Not chosen |
|---|---|---|---|

## Current

<!-- generated by scripts/records.py: current -->
### Testing

- **Run a small Playwright suite for the web UI** (D27): A Python Playwright suite checks the theme,
  zone cookie, htmx swap and live updates. `make test-ui` and a CI job run it outside `make check`.
- **Concurrency limits have integration and stress coverage** (D92): An integration test logs each
  task's start and end and checks the most running at once against tight limits (`most_at_once`).
  M4.4 also writes a stress scenario to run when the user next runs the stress suite.
- **Inject a failure at each database write in master operation tests** (D98): A test hook fails the
  Nth database write; `fail_each_write` repeats an operation from fresh state for each write. A
  shared consistency check compares the database with itself and master memory. Every master
  operation that writes must have this test.

### Documentation

- **Build the documentation as part of releases** (D29): The wheel and image CI job waits for the
  strict documentation build. `make image` and `make server-image` build the docs first.

### Working in steps

- **M4 implementation follows dependency order** (D51): M4 follows PLAN.md’s dependency order: the
  result model first, the agent channel before its consumers, and async functions before the watcher
  host.
- **M4 uses small substeps and the revised review schedule** (D52): M4 is split into small substeps,
  each committed after Claude’s own review and mutation-checked tests. Reviews follow the revised
  session/whole-repository schedule, with later M4 reviews to be planned after the whole-repository
  review.
- **Build the Deployments page in M4.4b** (D124): The Deployments page is scheduled for M4.4b, after
  M4.4 and before 1.0.0. The schema and protocol can still be edited in place.

### Releases

- **Releases use Trusted Publishing to PyPI** (D153): `.github/workflows/release.yml` uses Trusted
  Publishing through SHA-pinned `pypa/gh-action-pypi-publish`; the `pypi` environment requires the
  user’s approval for each release.
- **A pushed version tag starts a release** (D154): A pushed `vX.Y.Z` tag is checked against package
  versions and starts the release. The workflow builds and smoke-tests four wheels, publishes them
  and creates a GitHub Release.
- **Releases carry notes, wheels, checksums and attestations** (D155): Release notes come from the
  version section of `CHANGELOG.md`; wheels and `SHA256SUMS` are attached to the GitHub Release.
  Each wheel has provenance attestation; there is no sdist.
- **The 0.x schema and protocol are edited in place** (D156): Until 1.0.0, the initial schema and
  protocol are edited in place. Upgrading from 0.1 to 0.2 requires a fresh database; release notes
  and README say so.
- **Releases push server and worker images while the chart stays in the repository** (D157):
  Releases push x86_64 images from the musllinux wheel to `ghcr.io/ptutak/schedr-server` and
  `ghcr.io/ptutak/schedr`. The Helm chart stays in the repository; its default images use
  `appVersion`, checked against the release version.
- **Release candidates publish to TestPyPI** (D158): A `vX.Y.ZrcN` tag publishes to TestPyPI using
  its own trusted publisher. The published wheel is installed and smoke-tested; final tags publish
  to PyPI.
- **A release runs CI on its tag and publishes CI wheels** (D160): The release workflow calls all of
  CI on the release tag. It publishes the wheels produced by that CI run.
- **A release rehearsal performs every step as a candidate** (D161): A release candidate publishes
  to TestPyPI and creates a GitHub pre-release. Its images are tagged only `X.Y.ZrcN`.
- **Release images use version and minor tags** (D162): Final release images receive `X.Y.Z` and
  `X.Y` tags. They do not receive a `latest` tag.
- **The committed tree holds the version named by the tag** (D163): Commit the version before
  tagging: Cargo uses `X.Y.Z-rcN`, the tag `vX.Y.ZrcN`, and the wheel and runner comparison
  `X.Y.ZrcN`, without a dot after `rc`. The workflow checks the committed version rather than
  rewriting it.
- **The Python image is schedr and the server image is schedr-server** (D164):
  `ghcr.io/ptutak/schedr` includes Python and runs standalone or workers. `schedr-server` runs the
  master, webserver and `schedr ui`.
- **Release documentation is deployed to GitHub Pages** (D165): The release workflow deploys CI’s
  `docs-site` build to GitHub Pages. The deploy job uses the `github-pages` environment.
- **The documentation site shows the latest release** (D166): A final release tag deploys its
  documentation site. Release rehearsals and main do not deploy it.

### Documents and records

- **The project's documents: STATUS, design topics, a file per decision, archive/PROJECT.md as
  history** (D367): `STATUS.md` (rewritten, at most 60 lines) says where things stand;
  `design/<topic>.md` how each part works now; `decisions/D<n>.toml` one file per decision, indexed
  by the generated `DECISIONS.md`. archive/PROJECT.md is the history up to D366; later decisions are
  in JOURNAL.md (D370). The layout lives on in the `project-docs` skill.
- **One source for each record; every other view generated** (D368): The plan
  (plan/<milestone>.toml), decisions (step, not a built flag) and open questions (questions/) are
  records; PLAN.md, DECISIONS.md, the design pages' lists and STATUS.md's block are generated by
  scripts/records.py as indexes: one line per item saying where the details are.
- **Decisions point to anchors in archive/PROJECT.md and JOURNAL.md** (D369): Each decision's main
  passage starts with <a id="D<n>"></a>; its history is "archive/PROJECT.md#D<n>" or
  "JOURNAL.md#D<n>"; records-check fails on a missing anchor.
- **JOURNAL.md holds decisions from 2026-11-08 on; archive/PROJECT.md is left as it is** (D370): New
  decisions are dated entries in JOURNAL.md with their anchors; archive/PROJECT.md keeps its own
  text up to decision 366 and gains nothing but anchors.
- **Work reaches main through git; undone-check guards recent commits** (D371): No worktree's files
  are copied over main: merge, rebase or cherry-pick; colleagues' record edits go in field by field.
  make undone-check and CI fail on a change removing over 20 lines the last 10 commits added, unless
  its message says Undoes: ...
- **PROJECT.md lives in archive/** (D372): The design log up to D366 is `archive/PROJECT.md`,
  unchanged but for its anchors; pointers name that path; JOURNAL.md and the design pages are the
  current documents.
- **Design pages are generated whole from the decisions** (D373): A decision has an optional
  `section`; `make records` writes each design page whole: Current (the active decisions' summaries
  by section, with their state unless built), Replaced, code (a path or `path::symbol`, checked),
  Open and Details (the anchors' sections). The pages are for the agent; the reader's documentation
  is `docs/`. Only `development.md` keeps hand-written rules.
- **A decision lists the claims nobody found in the code (unverified)** (D374): A decision's
  optional `unverified` lists claims of its summary nobody found in the code yet; its design page
  shows them after the summary and DECISIONS.md lists those decisions. Whoever next works on that
  code checks them: the claim is confirmed (removed from the list) or the summary corrected. A
  defect found in the code is a bug in `bugs/`, not a decision's state.
- **Summaries follow the code: names checked, affected and stale decisions listed** (D375): `make
  records-check` fails when a built or rule decision's summary names in code font a word no code
  file has (unless the claim is in `unverified`). `records.py affected [<since>]` lists the
  decisions whose `code` a change touches, re-read before each step's commit; `records.py stale`
  those whose code changed after `checked`. `code` entries are `path::symbol`; a summary names keys,
  not their values.
- **STATUS.md is generated from the records and status.toml** (D376): `make records` writes
  `STATUS.md` whole: the next step, open questions and bugs, the latest journal entries and steps
  done, from the records; in progress, waiting for the user, the rules in force, colleagues' usage
  and the resume line from `status.toml`, rewritten at each step's or session's end.

### From other topics

- Serve the versioned documentation inside the UI (D28, `ui-cli`; built M3.1)
- The inherited descriptor uses unsafe code in schedr-fd (D147, `task-context`; built M4.5b)
<!-- end of current -->

## Replaced

<!-- generated by scripts/records.py: decisions -->
- D48 (untraced): No record found
- D44 (untraced): No recorded decision found
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
- `.github/workflows/release.yml::gh-action-pypi-publish` (D153)
- `.github/workflows/release.yml::tags` (D154)
- `JOURNAL.md` (D367, D370)
- `Makefile::docs-dist` (D29)
- `Makefile::server-image` (D29)
- `Makefile::test-ui` (D27)
- `STATUS.md` (D367)
- `archive/PROJECT.md` (D372)
- `crates/schedr-db/src/database.rs` (D98)
- `crates/schedr-db/src/faults.rs` (D98)
- `crates/schedr-fd/Cargo.toml::unsafe_code` (D147)
- `crates/schedr-fd/src/lib.rs::inherited_datagram` (D147)
- `crates/schedr-master/src/faults.rs::fail_each_write` (D98)
- `crates/schedr-model/src/ctx.rs` (D147)
- `crates/schedr-model/src/runner.rs` (D163)
- `crates/schedr-model/src/runner/tests.rs` (D163)
- `crates/schedr-ui/src/deployments.rs` (D124)
- `crates/schedr-ui/src/docs.rs::resolve` (D28)
- `design/README.md` (D367)
- `plan/README.md` (D368)
- `python/tests/integration/test_testing.py::most_at_once` (D92)
- `python/tests/ui/test_ui.py::test_the_theme_toggle_switches_and_is_remembered` (D27)
- `scripts/docs-dist.sh` (D28)
- `scripts/records.py` (D367, D368, D369)
- `scripts/records.py::affected` (D375)
- `scripts/records.py::design_blocks` (D373)
- `scripts/records.py::generated_page` (D373)
- `scripts/records.py::read_status` (D376)
- `scripts/records.py::render_decisions` (D374)
- `scripts/records.py::render_status` (D376)
- `scripts/records.py::stale` (D375)
- `scripts/records.py::unknown_names` (D375)
- `scripts/release.py` (D153, D156, D157, D158, D160, D162)
- `scripts/release.py::Release` (D161)
- `scripts/release.py::check` (D154, D163)
- `scripts/release.py::notes` (D155, D161)
- `scripts/release.py::parse_tag` (D163)
- `scripts/release.py::wheels` (D155)
- `scripts/undone.py` (D371)
- `status.toml` (D376)
<!-- end of code -->

## Open

<!-- generated by scripts/records.py: questions -->
None.
<!-- end of questions -->

<!-- generated by scripts/records.py: details -->
Details and edge cases, with the reasons; read them before changing this:

- JOURNAL.md: D367, D368, D369, D370, D371, D372, D373, D374, D375, D376
- archive/PROJECT.md: Native API, Packaging and releases, Testing, Web UI and CLI
<!-- end of details -->
