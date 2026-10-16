# schedr

A scheduler written in Rust that runs Python scripts, Bash processes, and Python DAGs
defined with an Airflow-like API.

## Layout

```
crates/
  schedr-core/      # scheduling engine (library)
    src/            #   unit tests live next to code (#[cfg(test)])
    tests/          #   integration tests
  schedr/           # scheduler binary
python/
  schedr/           # Python DAG API package
  tests/
    unit/
    integration/
```

## Development

Requirements: Rust (stable, via rustup), [uv](https://docs.astral.sh/uv/), make.

```sh
make setup      # uv sync + pre-commit hooks
make fmt        # format Rust + Python
make lint       # clippy, ruff, mypy
make test       # cargo test + pytest
make check      # everything CI runs
```
