# If .python-version selects a pyenv(-virtualenv) environment, make uv install into
# and run from it instead of creating .venv. Without pyenv, uv's defaults apply.
PYENV_PREFIX := $(shell pyenv prefix 2>/dev/null)
ifneq ($(PYENV_PREFIX),)
export UV_PROJECT_ENVIRONMENT := $(PYENV_PREFIX)
export UV_PYTHON := $(PYENV_PREFIX)/bin/python
unexport VIRTUAL_ENV
endif

.PHONY: help setup fmt fmt-check lint test test-rust test-python check clean

help:            ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n",$$1,$$2}'

# Rust test coverage: unit-test files (`src/**/tests.rs`) and `main.rs` (only calls into the
# library) aren't counted; see PROJECT.md, Repository and tooling: Testing.
COVERAGE_MIN := 85
LLVM_COV_IGNORE := '(^|/)(tests|main)\.rs$$'

setup:           ## Install dev tools, Python dev deps and pre-commit hooks
	rustup component add llvm-tools-preview
	command -v cargo-nextest >/dev/null || cargo install --locked cargo-nextest
	command -v cargo-llvm-cov >/dev/null || cargo install --locked cargo-llvm-cov
	uv sync
	uv run pre-commit install

fmt:             ## Format Rust and Python code
	cargo fmt --all
	uv run ruff format python
	uv run ruff check --fix python

fmt-check:       ## Verify formatting without changing files
	cargo fmt --all -- --check
	uv run ruff format --check python

lint:            ## Run clippy, ruff and mypy
	cargo clippy --workspace --all-targets -- -D warnings
	uv run ruff check python
	uv run mypy

test-rust:       ## Run Rust tests with coverage (fails below COVERAGE_MIN)
	cargo llvm-cov nextest --workspace --ignore-filename-regex $(LLVM_COV_IGNORE) \
		--fail-under-lines $(COVERAGE_MIN)
	cargo test --workspace --doc

test-python:     ## Run Python tests with coverage (fails below COVERAGE_MIN)
	uv run pytest --cov --cov-report=term-missing --cov-fail-under=$(COVERAGE_MIN)

test: test-rust test-python  ## Run all tests

check: fmt-check lint test   ## Everything CI runs

clean:           ## Remove build artifacts
	cargo clean
	cargo llvm-cov clean --workspace 2>/dev/null || true
	rm -rf .venv .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
