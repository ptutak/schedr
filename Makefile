# If .python-version selects a pyenv(-virtualenv) environment, make uv install into
# and run from it instead of creating .venv. Without pyenv, uv's defaults apply.
PYENV_PREFIX := $(shell pyenv prefix 2>/dev/null)
ifneq ($(PYENV_PREFIX),)
export UV_PROJECT_ENVIRONMENT := $(PYENV_PREFIX)
export UV_PYTHON := $(PYENV_PREFIX)/bin/python
unexport VIRTUAL_ENV
endif

.PHONY: help setup fmt fmt-check lint schema schema-check docs docs-serve test test-rust test-python test-integration check clean

help:            ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n",$$1,$$2}'

# Rust test coverage: unit-test files (`src/**/tests.rs`) and `main.rs` (only calls into the
# library) aren't counted; see PROJECT.md, Repository and tooling: Testing.
COVERAGE_MIN := 85

# Integration tests: the Python with `schedr` installed (for the real runner, found through uv
# inside the recipe, where the uv settings above apply) and a throwaway Postgres container; see
# PROJECT.md, Repository and tooling: Testing.
POSTGRES_IMAGE := docker.io/library/postgres:17.6-alpine
# Podman when installed (Docker's port forwarding doesn't work everywhere, e.g. the snap
# under WSL), else Docker; override with `make test-integration CONTAINER=docker`.
CONTAINER ?= $(shell command -v podman >/dev/null 2>&1 && echo podman || echo docker)

# Port for make docs-serve; override with e.g. `make docs-serve DOCS_PORT=9000`.
DOCS_PORT ?= 8765
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

schema:          ## Regenerate the committed files under schema/
	cargo xtask schema

schema-check:    ## Fail if the files under schema/ are out of date
	cargo xtask schema --check

docs:            ## Build the documentation site into site/ (fails on any warning)
	cargo xtask docs-sources
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --exclude xtask
	uv run --group docs sphinx-build -W --keep-going -q -b html docs site
	rm -rf site/rust
	cp -r target/doc site/rust

docs-serve: docs ## Build the docs and serve them at http://localhost:$(DOCS_PORT)
	uv run python -m http.server --directory site $(DOCS_PORT)

test-rust:       ## Run Rust unit tests with coverage (fails below COVERAGE_MIN)
	cargo llvm-cov nextest --workspace --ignore-filename-regex $(LLVM_COV_IGNORE) \
		--fail-under-lines $(COVERAGE_MIN) -E 'not kind(test)'
	cargo test --workspace --doc

test-python:     ## Run Python unit tests with coverage (fails below COVERAGE_MIN)
	uv run pytest python/tests/unit --cov --cov-report=term-missing --cov-fail-under=$(COVERAGE_MIN)

test: test-rust test-python  ## Run all unit tests

test-integration: ## Run integration tests (real processes, the binary, Postgres in Docker)
	@set -e; \
	name=schedr-test-pg-$$$$; \
	$(CONTAINER) run -d --rm --name $$name -e POSTGRES_PASSWORD=schedr -p 127.0.0.1::5432 \
		$(POSTGRES_IMAGE) >/dev/null; \
	trap '$(CONTAINER) rm -f '$$name' >/dev/null' EXIT; \
	port=$$($(CONTAINER) port $$name 5432/tcp | head -1 | sed 's/.*://'); \
	until $(CONTAINER) exec $$name psql -h 127.0.0.1 -U postgres -c 'select 1' >/dev/null 2>&1; do \
		sleep 0.5; done; \
	export SCHEDR_TEST_POSTGRES_URL=postgres://postgres:schedr@127.0.0.1:$$port/postgres; \
	export SCHEDR_TEST_PYTHON=$$(uv run python -c 'import sys; print(sys.executable)'); \
	test -x "$$SCHEDR_TEST_PYTHON"; \
	cargo nextest run --workspace -E 'kind(test)'; \
	uv run pytest python/tests/integration

check: fmt-check lint schema-check test test-integration   ## Everything CI runs

clean:           ## Remove build artifacts
	cargo clean
	cargo llvm-cov clean --workspace 2>/dev/null || true
	rm -rf .venv .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov site docs/_generated
