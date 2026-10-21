# schedr

A scheduler written in Rust that runs Python scripts, Bash processes, and Python DAGs
defined with an Airflow-like API.

## Layout

```
crates/
  schedr-model/     # shared model: DAG JSON, states, JSON Schema
  schedr-proto/     # Protobuf + gRPC, master <-> worker agent
  schedr-config/    # configuration loading and validation
  schedr-db/        # schema, migrations, queries (SQLite, Postgres)
  schedr-master/    # scheduling loop, DAG validation, commands
  schedr-agent/     # worker agent
  schedr-api/       # webserver: REST API, web UI
  schedr/           # the binary: roles, user CLI, in-task CLI
    src/<module>.rs         # code
    src/<module>/tests.rs   # its unit tests
  xtask/            # development tasks: `cargo xtask schema [--check]`
schema/             # generated from schedr-model, committed (make schema)
docs/               # Sphinx config and index pages; the rest is generated (make docs)
python/
  schedr/           # Python DAG API package
  tests/
    unit/schedr/    # unit tests mirroring the package: test_<module>.py
```

Design decisions: PROJECT.md. Implementation plan: PLAN.md.

## Development

Requirements: Rust (stable, via rustup), [uv](https://docs.astral.sh/uv/), make. `make setup`
also installs `cargo-nextest`, `cargo-llvm-cov` and the `llvm-tools-preview` component.

```sh
make setup      # uv sync + pre-commit hooks
make fmt        # format Rust + Python
make lint       # clippy, ruff, mypy
make test       # Rust (nextest) + pytest, both with an 85 % coverage floor
make schema     # regenerate schema/ from the Rust model (CI fails if it's stale)
make docs       # build the documentation site into site/
make docs-serve # ...and serve it at localhost:8765 (DOCS_PORT=9000 for another port)
make check      # everything CI runs
```
