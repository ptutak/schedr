# AI_PROJECT: what is schedr's own

For coding agents, meant to be read whole: schedr's layout, checks, rules, release and this
machine. What holds for every Python + Rust project is in `AI_SETUP.md`, how to work with the user
in `AI_INSTRUCTIONS.md`, where things stand and the rules that hold only for now in `STATUS.md`.

Where something has one home, this file points there: pins and versions in `Cargo.toml`,
`pyproject.toml`, `rust-toolchain.toml` and the workflows, targets in `make help`, records in the
`project-docs` skill, old reasons in `archive/PROJECT.md` (read only).

## Layout

| Path | What |
|---|---|
| `crates/<name>/` | Rust, one crate per responsibility; `xtask` generates code; `design/code-map.md` is the overview |
| `python/schedr`, `python/schedr_extension` | the Python package (the wheel holds it and the binary) |
| `python/tests/` | `unit/` mirrors the package, `integration/`, `ui/` (Playwright) |
| `crates/schedr/tests/stress/` | the stress suite |
| `examples/` | tested examples; the docs include them |
| `docs/` | `01-start`, `02-dags`, `03-guide` by hand; the reference generated at build time |
| `schema/` | generated, committed (`make schema`) |
| `charts/`, `images/`, `scripts/` | Helm chart, image builds, scripts CI and make use |

## Checks

- **`make check`:** undone, pin, format, lint, schema, bugs, records, third-party and chart
  checks, unit tests with 85 % coverage per language, integration tests. Once, on the final state.
- **CI runs more:** docs (`make docs`, warnings fail), browser tests (`make test-ui`), the upgrade
  check (`make upgrade-check`), wheels and their smoke tests (`make smoke`, `make smoke-musl`;
  aarch64 and macOS on CI only), the sdist. Run the ones a step touches.
- **While working:** `cargo clippy --keep-going --workspace --all-targets -- -D warnings`,
  `cargo nextest run -p <crate> --no-fail-fast`, the touched Python tests. The stress suite runs
  only with `--ignore-default-filter` (`.config/nextest.toml`); integration tests without their
  variables stop the run once, saying what to run.
- **By hand, the integration tests need** `SCHEDR_TEST_PYTHON=$(pyenv prefix)/bin/python` and
  `SCHEDR_TEST_POSTGRES_URL`; `make test-integration` starts the services (podman) and sets both.
- **Generated, after a change to:** the API or DAG JSON, `make schema`; `Cargo.lock`,
  `make third-party`; a record, `make records`; a bug, `make bugs`.

## Stress

On CI, not locally: `gh workflow run stress.yml` with a filter after a push, the full profile for
a release; scenarios 17-21 for a change to the master's recovery (lost workers, streams,
take-backs, moves), and grep the stress checks for assumptions it breaks. Up to 10 scenarios run at
once, the longest first, and one run at a time: a second waits, a third replaces it, so several
scenarios go in one filter. Locally only one quick scenario aimed at a feature. Compare CI runs
with CI runs, several of them: they vary. A claim of speed (faster, no slower) comes from the paired
job: `-f base=<ref>` with a filter, both builds in turn on one runner (docs/03-guide/11-stress.md).

## Rules of the code

- **Rust:** rustix for fcntl, flock and signals (no `unsafe`); functions up to 100 lines (clippy).
- **Python:** basedpyright on DAG code, besides ruff and mypy.
- **Tests:** multi-worker tests on Postgres (`postgres_database()`), roles as processes with
  `Cluster` (`crates/schedr/tests/support/`), TLS by `with_tls` and `schedr_tls::testing::TestCa`,
  port 0 for every port (`[ops] port` too). Browser tests don't count rows other tests also make.
- **Fault injection (D98):** a master operation that writes gets a `Scenario` run by
  `fail_each_write` (`crates/schedr-master/src/faults.rs`).
- **Migrations and the protocol change once per release:** one open migration per backend
  (`crates/schedr-db/migrations/`), edited until it ships, never after; `PROTOCOL_VERSION`
  (`crates/schedr-proto/src/version.rs`) bumped at the first step that needs it. An rc is not a
  release, but a migration changed after one needs another rc.
- **Extensions are controllers:** no backend-specific recovery in schedr, only protocol
  capabilities.
- **The master's round:** a change to it updates `docs/03-guide/19-masters-round.md`.
- **How to bump a pin** (age check, changelog, SHA): `archive/PROJECT.md`, CI supply-chain pins.

## Release `X.Y.Z`

1. `CHANGELOG.md`'s `## X.Y.Z` section; `make upgrade-check`.
2. **Rehearsal,** required when the release adds a migration (D498): `version = "X.Y.Z-rc1"` in
   `Cargo.toml` (no dot after `rc`), `appVersion: "X.Y.Zrc1"` in `charts/schedr/Chart.yaml`,
   `cargo update -w`, `make schema`, `make third-party`; commit, tag `vX.Y.Zrc1`, push (TestPyPI, a
   pre-release, images).
3. **The release:** the same with `X.Y.Z`, tag `vX.Y.Z`; the user approves the `pypi` environment.
4. A version commit is pushed at once and `make check` run beside CI; tag when both pass.

After an upload the version is spent (PyPI refuses a file twice): fix and release the next, or
another rc. A job failing after it: `gh run rerun <id> --failed`. An image's first push to ghcr.io
is denied until its package is linked to the repository; a new package is private until made public.

## This machine

- WSL in mirrored networking: `127.0.0.1` goes over `loopback0`, which ufw must allow both ways.
  Containers with podman, not Docker. Port 8000 is taken; `make docs-serve` uses 8765.
- **Never apply anything to a cluster that isn't a local kind one,** always with an explicit
  `--context`, and ask first (an OpenShift "bradford-lab" context is real). The target cluster's
  Istio is REGISTRY_ONLY: internal calls go to Services (headless for workers).
- The pyenv environment `schedr-3.14.8` is shared with the user: no `uv sync` while they may run
  make. Python outside make:
  `export UV_PROJECT_ENVIRONMENT=$(pyenv prefix) UV_PYTHON=$(pyenv prefix)/bin/python; unset VIRTUAL_ENV`.
- A fresh binary by hand: `SCHEDR_BIN=$PWD/target/debug/schedr`. Roles need certificates, or
  `SCHEDR_CONFIG__TLS__MODE=mesh`; standalone never uses TLS.
- `target/` grows: `make target-size` cleans it; check free space on Windows' C: before big builds.

## Libraries

Dependencies added since 2026-11-09 (earlier ones: `archive/PROJECT.md`, Libraries). One the user
decides is also a decision.

| Need | Choice | Why | Not chosen |
|---|---|---|---|
