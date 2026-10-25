# AI_SETUP: how to set up a project

For coding agents. Copy this file into a new repository: it says how the user likes a project
laid out and tooled, mainly for **Python** and **Rust** (either alone or both together). Adapt
names and versions to the project; keep the principles. How to work with the user is in
`AI_INSTRUCTIONS.md`.

## Principles

- **One command checks everything:** `make check` runs exactly what CI runs (formatting,
  linters, type checks, tests with coverage, integration tests, generated-file checks). If it
  passes locally, CI passes.
- **Strict from the first commit:** strict type checking, a broad linter rule set, warnings
  as errors in CI, a coverage floor. Loosen a rule only with a comment saying why.
- **Generated files are committed and checked:** schemas, API descriptions and other files
  generated from code are committed; CI fails when they are stale.
- **Decisions are written down** (see Project documents) and the code points to them.
- **Few dependencies,** each one justified, versions pinned in one place.

## Repository layout

```
README.md               # what it is, how to install and run
PROJECT.md              # decision log (see Project documents)
PLAN.md                 # milestones and steps
BUGS.md                 # open bugs
NEXT_SESSION.md         # hand-off for the next working session
AI_SETUP.md, AI_INSTRUCTIONS.md   # the Grok worker's skill lives in ~/.claude/skills/grok, not here
Makefile                # every task has a target; `make help` lists them
.pre-commit-config.yaml
.github/workflows/ci.yml
pyproject.toml          # Python project (also the build config for a Rust+Python wheel)
uv.lock
Cargo.toml              # Rust workspace (shared versions, lints)
Cargo.lock
rust-toolchain.toml
rustfmt.toml, clippy.toml
crates/<name>/          # Rust crates, one per responsibility
  src/<module>.rs       # code
  src/<module>/tests.rs # its unit tests
  tests/                # integration tests (public API only)
  xtask/                # development tasks in Rust (code generation, doc sources)
python/<package>/       # the Python package (src-like layout under python/)
python/tests/unit/<package>/...   # unit tests, mirroring the package
python/tests/integration/         # integration tests
examples/               # runnable examples, with their tests
scripts/                # shell scripts used by CI and make (e.g. a package smoke test)
schema/                 # generated, committed (JSON Schema, OpenAPI, ...)
testdata/               # fixtures shared between languages
docs/                   # documentation sources (conf, index pages, the guide); output goes to site/
```

A Python-only project drops the Rust parts (and vice versa); everything else stays.

## Python

- **Version:** the newest stable Python the project can use; set `requires-python` and the
  tools' target version to it.
- **Environment and dependencies: uv.** `uv.lock` committed; development tools in
  `[dependency-groups]` (`dev`, `docs`), never in the runtime dependencies. A local virtualenv
  (e.g. pyenv-virtualenv selected by a git-ignored `.python-version`) is fine; the Makefile
  points uv at it.
- **Runtime dependencies:** as few as possible; a library that runs inside users'
  environments should be dependency-free, with optional extras for integrations.
- **Formatting and linting: ruff,** line length 100, a broad rule set:

  ```toml
  [tool.ruff.lint]
  select = ["E", "W", "F", "I", "B", "UP", "SIM", "N", "RUF", "PT", "C4", "PL", "D"]

  [tool.ruff.lint.pydocstyle]
  convention = "google"

  [tool.ruff.lint.per-file-ignores]
  "python/tests/**" = ["PLR2004", "D"]
  ```

- **Type checking: mypy `strict = true`** on the package; the package ships `py.typed`.
- **Docstrings:** Google style (`Args:`, `Returns:`, `Raises:`, `Yields:`) on every public
  module, class, function and method; private (`_name`) ones are exempt.
- **Tests: pytest** with `--import-mode=importlib` (no `__init__.py` in test folders, same
  file names in different folders don't clash), `--strict-markers`, `--strict-config`;
  coverage with `pytest-cov`, branch coverage on, `fail_under = 85`.
- **Test layout mirrors the package:** `pkg/sub/module.py` →
  `tests/unit/pkg/sub/test_module.py`; a package's `__init__.py` → `test_init.py` in its test
  folder.

## Rust

- **Toolchain:** stable, pinned with `rust-toolchain.toml` (components `rustfmt`, `clippy`,
  `llvm-tools-preview`); edition 2024; an explicit MSRV (`rust-version`) in the workspace.
- **Workspace:** crates under `crates/`, split along responsibilities so each one's
  dependencies stay small; every third-party version in `[workspace.dependencies]` only.
- **Lints, shared by the workspace:**

  ```toml
  [workspace.lints.rust]
  unsafe_code = "forbid"
  missing_docs = "warn"

  [workspace.lints.clippy]
  all = { level = "warn", priority = -1 }
  pedantic = { level = "warn", priority = -1 }
  ```

  CI runs `cargo clippy --workspace --all-targets -- -D warnings`; `rustfmt` with
  `max_width = 100`. Docs: `RUSTDOCFLAGS="-D warnings"`, broken intra-doc links are errors.
- **Debug builds keep line tables only** (`[profile.dev] debug = "line-tables-only"`):
  backtraces keep functions and `file:line`, builds write about half as much (binaries a
  third of the size); `CARGO_PROFILE_DEV_DEBUG=full` when a debugger is needed.
- **Unit tests next to the code, in their own file:** `src/foo.rs` ends with
  `#[cfg(test)] mod tests;` and the tests live in `src/foo/tests.rs` (the crate root's in
  `src/tests.rs`). They can reach `pub(crate)` items; `tests/` can't.
- **Test tools:** `cargo-nextest` (runner), `cargo-llvm-cov` (coverage, `--fail-under-lines
  85`, ignoring `tests.rs` files and a binary's thin `main.rs`), `rstest` (fixtures, cases),
  `proptest` (properties), `insta` (snapshots), `pretty_assertions`, `tempfile`.
- **Common crates:** `thiserror` for error types, `tracing` for logs, `serde` for data,
  `clap` (derive) for CLIs, `tokio` for async. A binary keeps its logic in its library part
  (`lib.rs`), so `main.rs` only calls it and everything is testable.
- **Code generation:** an `xtask` crate (`cargo xtask <task>`), e.g. JSON Schemas from Rust
  types with a `--check` mode for CI.

## Rust and Python together

- Rust is the reference for shared data shapes; Python types are written by hand and tested
  against the schemas generated from Rust.
- A Rust binary shipped with a Python package: **maturin** with `bindings = "bin"` (the wheel
  holds the binary and the pure-Python package); the Python version comes from the Cargo
  workspace. uv rebuilds the project when Rust sources change (`[tool.uv] cache-keys`).
- Processes talk over explicit channels (sockets, files), not by embedding one language in
  the other, unless the project needs otherwise.

## Tests

- **Every public function, method and class has unit tests;** private ones are covered
  through the public ones, never tested directly. **85 % coverage floor** per language, on
  the whole code base, enforced by `make check` and CI.
- **Unit vs integration:** unit tests use fakes and in-memory stand-ins. Anything that starts
  a real process, binds a real server, uses a real database or the built binary is an
  **integration test** (`crates/*/tests/`, `python/tests/integration/`, `examples/`). They run
  with `make test-integration` and in CI, and don't count towards coverage.
- **Integration tests come with every step,** not at the end.
- **What a test needs is provided, never skipped:** the Makefile starts throwaway services
  (e.g. a database in a container, Podman when installed, else Docker, on a random port) and
  passes them through environment variables; a test run without them fails saying how to run
  it. Never skip, ignore or xfail a test without asking the user first.
- **Each test is isolated:** its own temporary directories, ports, databases; tests run in
  parallel.
- **Examples are tested,** and an example quoted in the docs is checked to be identical to
  the file.
- **Packages are smoke-tested as users get them:** build the release artifact, install it
  into a fresh environment, run the main flows (a script, run by CI and `make smoke`).

## Documentation

- One command builds a site from the repository: `make docs` (and `make docs-serve`, on a
  configurable port). Nothing generated is committed.
- **Sphinx** with **MyST** (Markdown pages), **Furo** (theme), **autodoc** + **autosummary**
  + **napoleon** (the Python API from docstrings); **rustdoc** copied under `rust/`.
- Long Markdown documents (e.g. `PROJECT.md`) stay one file in the repository; a small
  generator splits them into pages at build time.
- **Three kinds of pages:**
  - **The guide** (e.g. `docs/02-guide/`, written by hand, committed): how the running system works
    (components, the flow of a request, loops, failure handling, deployment), as built, not
    as planned. It describes, and links to the design log for the reasons. Updated in the same
    step as the code it describes.
  - **The reference**, generated from the code at build time so it can't go stale: the
    configuration (from its JSON Schema, doc comments included), the CLI (from the argument
    parser), the API (from its OpenAPI description), the metrics (from a list per component
    that the code registers from, checked by a test against a scrape).
  - **The design log and plan** (`PROJECT.md`, `PLAN.md`), split into pages at build time.
- **Files numbered in the site's order:** each section gets its number (a folder when it has
  several pages, `docs/02-guide/`; a file when one, `05-plan.md`), and the pages in a folder
  too (`02-guide/04-master.md`, two digits); generated pages use the same numbers. A page
  with pages of its own becomes a folder (`05-workers/01-journal.md`, read as 5.1).
- **Diagrams in Mermaid**, written as plain ```` ```mermaid ```` fences so GitHub draws them too
  (`sphinxcontrib-mermaid`, with `myst_fence_as_directive`; mermaid.js pinned).
- CI builds the docs and **fails on warnings** (a broken reference, a missing docstring, a
  page not in the navigation).

## Makefile

Targets with `## help` comments: `help`, `setup` (dev tools, dependencies, pre-commit hooks),
`fmt`, `fmt-check`, `lint`, `schema` / `schema-check` (generated files), `test` (unit tests
with coverage, per language: `test-rust`, `test-python`), `test-integration`, `docs`,
`docs-serve`, `smoke` (package smoke test), `check` (everything CI runs), `clean`, and
`target-size` (`check` runs it first: cleans the build directory once it holds more than a
set size, 50 GiB here, since Cargo never deletes old artifacts).

- **Integration tests start on a quiet disk:** build the test binaries, `sync`, then start
  the database and the tests. A rebuild leaves gigabytes unflushed, and a database's fsyncs
  wait behind them for seconds (seen as flaky timing tests).

## Pre-commit

`pre-commit` runs the same checks on commit: whitespace and end-of-file fixers, TOML / YAML
checks, merge-conflict markers, `cargo fmt --check`, `cargo clippy -D warnings`, `ruff format
--check`, `ruff check`, `mypy`. Hooks call the tools directly (`language: system`), so
they use the project's versions.

## CI (GitHub Actions)

Separate jobs, each one reproducible with a make target:

- `rust`: fmt, clippy, generated-file check, unit tests with coverage, doc tests;
- `python`: ruff format and check, mypy, unit tests with coverage;
- `integration`: integration tests, with services as service containers (same images as the
  Makefile uses);
- `docs`: the site, failing on warnings;
- `package` / `wheel`: the release artifact built and smoke-tested; uploaded.

## Dependencies

- **New versions must be at least two weeks old** (check the registry: crates.io API, PyPI)
  when added.
- Each dependency is listed in `PROJECT.md` (a "Libraries" table: need, choice, why, and what
  wasn't chosen).

## Configuration and secrets

- Configuration from one file and/or environment variables mirroring its structure, with a
  clear precedence (flags > environment > file); unknown keys are errors naming the key and
  where it came from.
- **Secrets only from environment variables,** never from configuration files (a secret in
  the file is a startup error).

## Agent tooling

- **Claude Code is the main agent;** Grok Build (xAI's terminal agent, on a SuperGrok or X
  Premium+ subscription, so no per-token billing) can be its worker. How the two work
  together is in `AI_INSTRUCTIONS.md`, Working with a worker agent.
- **Set up once per machine,** not per repository: the `/grok` skill lives in
  `~/.claude/skills/grok/`. It has `SKILL.md`, `grok-turn.sh` (one headless turn, resumed by
  session ID, logs in `logs/`) and `install.sh`. Copy the folder to a new machine and run
  `~/.claude/skills/grok/install.sh`. It installs the Grok CLI (`npm install -g
  @xai-official/grok`), runs `grok login`, and allows the turn script in
  `~/.claude/settings.json`. Running it again is safe.
- **Per repository, nothing to add:** the skill finds the working rules (this file and
  `AI_INSTRUCTIONS.md`, else `AGENTS.md` / `CLAUDE.md`) and the checks (`make check`,
  `make docs`). Grok's worktree is a sibling folder (`../<repo>-grok-<step>`), outside the
  repository, so nothing needs ignoring.
- **The worktree has its own build directory,** so Grok's first build is a full one. Kind
  clusters, ports and containers are shared with the main checkout: don't run the same
  cluster script in both at once.

## Project documents

- **`PROJECT.md`, the decision log:** what is decided, by topic; each decision with its
  reasons and **"Not chosen: …"** alternatives; open questions at the end, moved up when
  decided.
- **`PLAN.md`:** milestones, each with a "done when"; steps in dependency order, each with
  its own "done when"; a status line when a milestone is done.
- **`BUGS.md`:** open bugs with stable IDs (B1, B2, …, never reused): found, symptom, repro,
  cause or suspect, plan. A fix removes its entry in the same commit, with a test that would
  have caught it.
- **`NEXT_SESSION.md`:** where things stand, next steps in order, working rules, environment
  notes, a short code map; rewritten at the end of a milestone or session.

## Style

- Errors and messages: lower case, names in backticks, what to do next
  (``unknown time zone `Mars/Base` ``; ``set `X` (or [section] key)``).
- Comments and docstrings say *why* and *what*, not *how*; match the surrounding code's
  density and naming.
- Plain, precise English in documents; the same name for the same thing everywhere.
