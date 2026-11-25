# AI_SETUP: how a Python + Rust project is set up

For coding agents, meant to be read whole. It says how the user likes a project laid out and
tooled, for **Python** and **Rust**, alone or together (drop the part that doesn't apply). It is
the same in every project: what is true of one project only (its crates, release, machine) is in
its `AI_PROJECT.md`; how to work with the user in `AI_INSTRUCTIONS.md`; the records (status, plan,
decisions, bugs) in the `project-docs` skill, whose `init.sh <repo>` seeds a new project.

## Principles

- **`make check` is the gate:** everything CI runs that a developer's machine runs reasonably fast.
  What only CI or a separate target runs (browser tests, wheels, docs) is listed in `AI_PROJECT.md`.
- **Strict from the first commit:** strict types, broad lints, warnings as errors, a coverage
  floor. A rule is loosened only with a comment saying why.
- **Generated files are committed and checked** (schemas, API descriptions, record views): CI fails
  when one is stale. Built docs aren't committed.
- **Few dependencies,** each justified, versions pinned in one place.
- **Each fact has one home;** other files point there.

## Layout

```
README.md  CHANGELOG.md
CLAUDE.md                     # one line: @AI_INSTRUCTIONS.md
AI_INSTRUCTIONS.md  AI_SETUP.md  AI_PROJECT.md
STATUS.md PLAN.md DECISIONS.md NOTES.md BUGS.md   # generated from the records
status.toml plan/ decisions/ questions/ notes/ bugs/ JOURNAL.md   # the records, edited
design/<topic>.md             # generated from the decisions; README.md lists the topics
Makefile  .pre-commit-config.yaml  .github/workflows/
Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml clippy.toml
crates/<name>/src/<module>.rs, src/<module>/tests.rs, tests/   # crates/xtask: code generation
pyproject.toml uv.lock
python/<package>/             # python/tests/unit/ mirrors it; python/tests/integration/
examples/                     # runnable, tested; the docs include them
scripts/  schema/  docs/      # docs: numbered in the site's order; output in site/
```

## Python

- The newest stable Python the project can use, in `requires-python` and the tools' targets.
- **uv**, `uv.lock` committed; tools in `[dependency-groups]`. A library that runs in users'
  environments has no runtime dependencies; integrations are extras.
- **ruff**, line length 100, `select = ["E","W","F","I","B","UP","SIM","N","RUF","PT","C4","PL","D"]`,
  Google docstrings on every public name (tests exempt from `D` and `PLR2004`).
- **mypy `strict`**, `py.typed` shipped.
- **pytest** with `--import-mode=importlib --strict-markers --strict-config`; no `__init__.py` in
  test folders; `pkg/sub/mod.py` is tested in `tests/unit/pkg/sub/test_mod.py`.

## Rust

- An exact toolchain in `rust-toolchain.toml` (rustfmt, clippy, llvm-tools-preview), edition 2024,
  an explicit `rust-version`.
- Crates split by responsibility; third-party versions in `[workspace.dependencies]` only.
- Workspace lints: `unsafe_code = "forbid"`, `missing_docs = "warn"`, clippy `all` and `pedantic`
  at warn, broken doc links denied; CI uses `-D warnings`. `max_width = 100`.
- `[profile.dev] debug = "line-tables-only"` (builds half the size).
- Unit tests in `src/<module>/tests.rs`; integration tests in `tests/`, through the public API.
- Tools: `cargo-nextest`, `cargo-llvm-cov`, `rstest`, `proptest`, `insta`, `pretty_assertions`.
  Common crates: `thiserror`, `tracing`, `serde`, `clap`, `tokio`.
- A binary's logic is in its library, so `main.rs` only calls it.

## Rust and Python together

- Rust is the reference for shared data; Python types are by hand, tested against its schemas.
- **maturin**, `bindings = "bin"`: the wheel holds the binary and the Python package, the version
  comes from the Cargo workspace, and `[tool.uv] cache-keys` rebuilds on Rust changes.
- They talk over sockets and files; neither is embedded in the other.

## Tests

- 85 % coverage floor per language, over the whole code base, without counting integration tests.
  Public items have unit tests; private ones are covered through them.
- Anything with a real process, server, database or the built binary is an **integration test**.
- **What a test needs is provided, never skipped:** make starts throwaway containers (podman first)
  on random ports and passes them by environment variable; without them a test fails saying how.
- Tests are isolated (own directories, ports, databases) and run in parallel. Build the test
  binaries, then `sync`, before starting a database: its fsyncs wait behind the build's writes.
- Examples are tested. Packages are smoke-tested as users get them: built, installed fresh, run.

## Documentation

- `make docs` builds one site, failing on warnings: Sphinx, MyST, Furo, autodoc with napoleon,
  rustdoc beside it. Mermaid diagrams as plain fences.
- **By hand, as built, not as planned:** how to use it, and the guide to how it works, updated in
  the step that changes them. **Generated:** the reference (configuration, CLI, API, metrics) from
  the code's own descriptions. **Design and history** from the records.

## Makefile, pre-commit, CI

- Every task is a target with a `## help` comment: `help setup fmt fmt-check lint schema
  schema-check test test-rust test-python test-integration docs docs-serve smoke check clean
  target-size records records-check bugs bugs-check undone-check pin-check`.
- `check` first runs `target-size` (cleans an oversized build directory) and `undone-check`
  (uncommitted changes removing what recent commits added; CI checks each pushed commit).
- **Pre-commit** calls the project's own tools: whitespace, TOML and YAML, fmt, clippy, ruff, mypy.
- **CI** has separate jobs: rust, python, integration (the Makefile's service images), docs, wheels
  with smoke tests, and the generated-file checks.
- **Supply chain:** `uses:` by commit SHA, images by digest, downloaded tools by exact version and
  checksum; `pin-check` enforces it.
- **Releases** run from a pushed tag: all of CI, then attested wheels to PyPI by Trusted Publishing,
  a smoke test of the published one, images, the GitHub release. An uploaded version is spent: fix
  and release the next.

## Dependencies

A new major or minor (its `x.y.0`) is at least 7 UTC calendar days old, a patch on a qualifying
minor at least 3; a yanked `x.y.0` still dates its minor. A yanked version in the lock moves forward
to one that qualifies, else back. Actions and downloaded tools follow the same rule. A new
dependency gets a row in `AI_PROJECT.md`, Libraries: need, choice, why, not chosen.

## Configuration, secrets, messages

- One file and environment variables mirroring it; flags over environment over file; an unknown key
  is an error naming it and its source.
- Secrets only from environment variables; one in the file is a startup error.
- Messages in lower case, names in backticks, what to do next:
  ``unknown time zone `Mars/Base` ``. Plain, precise English; one name per thing.

## Agent tooling

- Claude Code writes the code. The colleagues `agy`, `codex` and `grok` and the `project-docs`
  skill live in `~/.claude/skills/`, one private git repository each, set up once per machine (clone,
  `install.sh`). Nothing is added per repository but `CLAUDE.md`.
- A colleague works in a sibling worktree (`../<repo>-<agent>-<step>`), in a sandbox, with its own
  Python environment and build directory, prepared before its first turn. Never `make setup`
  there: git hooks are shared by all checkouts, as are clusters, ports and containers.
