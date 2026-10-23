# If .python-version selects a pyenv(-virtualenv) environment, make uv install into
# and run from it instead of creating .venv. Without pyenv, uv's defaults apply.
PYENV_PREFIX := $(shell pyenv prefix 2>/dev/null)
ifneq ($(PYENV_PREFIX),)
export UV_PROJECT_ENVIRONMENT := $(PYENV_PREFIX)
export UV_PYTHON := $(PYENV_PREFIX)/bin/python
unexport VIRTUAL_ENV
endif

.PHONY: help setup fmt fmt-check lint schema schema-check docs docs-serve test test-rust test-python test-integration smoke smoke-musl image chart-check check clean target-size

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
# `make check` cleans target/ first (before anything builds, even with -j) when it holds more
# than this (Cargo never deletes old
# artifacts: every dependency or feature change leaves a copy of everything above it).
TARGET_MAX_GIB ?= 50
LLVM_COV_IGNORE := '(^|/)(tests|main)\.rs$$'

setup:           ## Install dev tools, Python dev deps and pre-commit hooks
	rustup component add llvm-tools-preview
	command -v cargo-nextest >/dev/null || cargo install --locked cargo-nextest
	command -v cargo-llvm-cov >/dev/null || cargo install --locked cargo-llvm-cov
	uv sync
	uv run pre-commit install

fmt:             ## Format Rust and Python code
	cargo fmt --all
	uv run ruff format python examples
	uv run ruff check --fix python examples

fmt-check:       ## Verify formatting without changing files
	cargo fmt --all -- --check
	uv run ruff format --check python examples

lint:            ## Run clippy, ruff and mypy
	cargo clippy --workspace --all-targets -- -D warnings
	uv run ruff check python examples
	uv run mypy

schema:          ## Regenerate the committed files under schema/
	cargo xtask schema

schema-check:    ## Fail if the files under schema/ are out of date
	cargo xtask schema --check

docs:            ## Build the documentation site into site/ (from scratch; fails on any warning)
	cargo xtask docs-sources
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --exclude xtask
	rm -rf site
	uv run --group docs sphinx-build -W --keep-going -q -b html docs site
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
	cargo nextest run --workspace -E 'kind(test)' --no-run; \
	: "the build's writes reach the disk before the tests start, or Postgres's fsyncs wait behind them (B6)"; \
	sync; \
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
	uv run pytest python/tests/integration examples

smoke:           ## Build the release wheel and smoke-test it (install, dags test, standalone run)
	rm -f dist/*manylinux*.whl dist/*linux_x86_64.whl
	uv build --wheel --out-dir dist
	scripts/smoke-wheel.sh dist/*-linux_x86_64.whl $$(uv run python -c 'import sys; print(sys.executable)')

# The musllinux wheel (Alpine) is cross-built with zig, and smoke-tested on the image the
# standalone example uses (examples/kubernetes/Dockerfile).
MATURIN := uvx --python 3.14 --from 'maturin==1.15.0' --with 'ziglang==0.17.0' maturin
ALPINE_IMAGE := docker.io/library/python:3.14.8-alpine3.24

smoke-musl:      ## Build the musllinux wheel into dist/ and smoke-test it on Alpine
	rustup target add x86_64-unknown-linux-musl
	rm -f dist/*musllinux*.whl
	$(MATURIN) build --release --target x86_64-unknown-linux-musl --zig \
		--compatibility musllinux_1_2 --out dist
	$(CONTAINER) run --rm -v "$(CURDIR):/repo:ro,Z" $(ALPINE_IMAGE) sh -c \
		'apk add --no-cache bash >/dev/null && cd /repo && bash scripts/smoke-wheel.sh dist/*musllinux*.whl python3'

IMAGE ?= schedr:$(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)

image: smoke-musl ## Build the base image from the musllinux wheel and smoke-test it
	$(CONTAINER) build -f images/schedr/Dockerfile -t $(IMAGE) .
	scripts/smoke-image.sh $(CONTAINER) $(IMAGE)

chart-check:     ## Lint the Helm chart and render it with its main combinations of values
	helm lint charts/schedr
	helm template t charts/schedr >/dev/null
	helm template t charts/schedr --set database.existingSecret=pg >/dev/null
	helm template t charts/schedr --set gitSync.enabled=true \
		--set gitSync.repo=https://example.com/dags.git >/dev/null
	helm template t charts/schedr --set serviceMonitor.enabled=true >/dev/null
	! helm template t charts/schedr --set mode=distributed >/dev/null 2>&1
	: "the data volume writable: fsGroup 0, except on OpenShift, which sets its own"
	helm template t charts/schedr | grep -q 'fsGroup: 0'
	! helm template t charts/schedr --api-versions security.openshift.io/v1 | grep -q fsGroup
	helm template t charts/schedr --set podSecurityContext.fsGroup=2000 | grep -q 'fsGroup: 2000'
	: "the schema migrated by an init container, holding the lease; schedr itself doesn't"
	helm template t charts/schedr | grep -q 'args: \["db", "migrate", "--wait"\]'
	! helm template t charts/schedr | grep -q -- '--migrate'

check:           ## Everything CI runs
	@$(MAKE) --no-print-directory target-size
	@$(MAKE) --no-print-directory fmt-check lint schema-check chart-check test test-integration

target-size:     ## Clean target/ when it holds more than TARGET_MAX_GIB (default 50)
	@size=$$(du -s --block-size=1G target 2>/dev/null | cut -f1); \
	if [ "$${size:-0}" -gt $(TARGET_MAX_GIB) ]; then \
		echo "target/ holds $$size GiB, over TARGET_MAX_GIB = $(TARGET_MAX_GIB): cargo clean"; \
		cargo clean; \
	fi

clean:           ## Remove build artifacts
	cargo clean
	cargo llvm-cov clean --workspace 2>/dev/null || true
	rm -rf .venv .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov site docs/_generated
