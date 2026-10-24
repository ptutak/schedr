# If .python-version selects a pyenv(-virtualenv) environment, make uv install into
# and run from it instead of creating .venv. Without pyenv, uv's defaults apply.
PYENV_PREFIX := $(shell pyenv prefix 2>/dev/null)
ifneq ($(PYENV_PREFIX),)
export UV_PROJECT_ENVIRONMENT := $(PYENV_PREFIX)
export UV_PYTHON := $(PYENV_PREFIX)/bin/python
unexport VIRTUAL_ENV
endif

.PHONY: help setup fmt fmt-check lint schema schema-check docs docs-serve test test-rust test-python test-integration stress standalone smoke smoke-musl image image-requirements chart-check check clean target-size

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
	cargo nextest run --workspace -E 'kind(test) & not binary(stress)' --no-run; \
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
	cargo nextest run --workspace -E 'kind(test) & not binary(stress)'; \
	uv run pytest python/tests/integration examples

# A local standalone for trying things by hand (development only): the debug binary just
# built, the Python of `uv sync` (with schedr installed) for DAG files and tasks, SQLite and the
# worker's data in .schedr/ in this repository (git-ignored; open .schedr/schedr.db in an IDE),
# the DAGs of DAGS (default examples/). The API on LISTEN:8080, all addresses by default so it
# can be reached from outside WSL (it has no authentication: LISTEN=127.0.0.1 on a network you
# don't trust; `schedr dags list` works without --server with either), ops on 8090. Ctrl-C stops
# it; `rm -rf .schedr` starts afresh.
DAGS ?= examples
STANDALONE_DIR ?= .schedr
LISTEN ?= 0.0.0.0

standalone:      ## Run schedr standalone here: SQLite in .schedr/, the DAGs of DAGS (examples/)
	cargo build -p schedr
	mkdir -p $(STANDALONE_DIR)
	SCHEDR_DAG_PATH="$(abspath $(DAGS))" \
	SCHEDR_CONFIG__DATABASE__URL="sqlite://$(abspath $(STANDALONE_DIR))/schedr.db" \
	SCHEDR_CONFIG__WORKER__DATA_DIR="$(abspath $(STANDALONE_DIR))" \
	uv run --no-sync target/debug/schedr standalone --migrate --listen $(LISTEN)

# The stress suite (crates/schedr/tests/stress, PLAN.md 2.10b): Postgres and a PgBouncer in
# transaction pooling in front of it (any database name passed through), on a network of their
# own; one scenario at a time. `STRESS_PROFILE=full` for the large one, `STRESS=<name>` for some
# scenarios (a nextest name filter, e.g. `STRESS=failover`); reports in target/stress/.
PGBOUNCER_IMAGE := docker.io/edoburu/pgbouncer:v1.26.0-p0
STRESS_PROFILE ?= quick
STRESS ?=

stress:          ## Run the stress suite (STRESS_PROFILE=quick|full, STRESS=<filter>)
	@set -e; \
	cargo nextest run -p schedr --test stress --no-run; \
	sync; \
	net=schedr-stress-$$$$; pg=schedr-stress-pg-$$$$; bouncer=schedr-stress-bouncer-$$$$; \
	$(CONTAINER) network create $$net >/dev/null; \
	trap '$(CONTAINER) rm -f '$$pg' '$$bouncer' >/dev/null; $(CONTAINER) network rm '$$net' >/dev/null' EXIT; \
	$(CONTAINER) run -d --rm --name $$pg --network $$net -e POSTGRES_PASSWORD=schedr \
		-p 127.0.0.1::5432 $(POSTGRES_IMAGE) -c max_connections=500 >/dev/null; \
	$(CONTAINER) run -d --rm --name $$bouncer --network $$net -e DB_HOST=$$pg \
		-e DB_USER=postgres -e DB_PASSWORD=schedr -e AUTH_TYPE=scram-sha-256 \
		-e POOL_MODE=transaction -e MAX_CLIENT_CONN=1000 -e DEFAULT_POOL_SIZE=40 \
		-p 127.0.0.1::5432 $(PGBOUNCER_IMAGE) >/dev/null; \
	port=$$($(CONTAINER) port $$pg 5432/tcp | head -1 | sed 's/.*://'); \
	bport=$$($(CONTAINER) port $$bouncer 5432/tcp | head -1 | sed 's/.*://'); \
	until $(CONTAINER) exec $$pg psql -h 127.0.0.1 -U postgres -c 'select 1' >/dev/null 2>&1; do \
		sleep 0.5; done; \
	export SCHEDR_TEST_POSTGRES_URL=postgres://postgres:schedr@127.0.0.1:$$port/postgres; \
	export SCHEDR_TEST_PGBOUNCER=127.0.0.1:$$bport; \
	export SCHEDR_TEST_PYTHON=$$(uv run python -c 'import sys; print(sys.executable)'); \
	export SCHEDR_STRESS_PROFILE=$(STRESS_PROFILE); \
	test -x "$$SCHEDR_TEST_PYTHON"; \
	cargo nextest run -p schedr --test stress --test-threads 1 --no-fail-fast \
		--no-capture $(if $(STRESS),-E 'test(/$(STRESS)/)')

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

# The base image's libraries, pinned with their dependencies for Alpine (musl) and Python 3.14,
# none newer than two weeks (PROJECT.md, Repository and tooling: Libraries).
IMAGE_PIN = uv pip compile --python-platform x86_64-unknown-linux-musl --python-version 3.14 \
	--exclude-newer $$(date -u -d '14 days ago' +%Y-%m-%d) --no-annotate --quiet \
	--custom-compile-command 'make image-requirements'

image-requirements: ## Pin the base image's libraries (images/schedr/requirements.in) again
	$(IMAGE_PIN) images/schedr/requirements.in -o images/schedr/requirements.txt
	$(IMAGE_PIN) --only-binary :all: images/schedr/build-requirements.in \
		-o images/schedr/build-requirements.txt

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
	helm template t charts/schedr --set serviceAccount.create=false \
		--set serviceAccount.name=existing --set migration=onStart >/dev/null
	helm template t charts/schedr -f charts/schedr/ci/istio-values.yaml >/dev/null
	helm template t charts/schedr -f charts/schedr/ci/istio-values.yaml \
		--api-versions security.istio.io/v1 --api-versions networking.istio.io/v1 >/dev/null
	! helm template t charts/schedr --set mode=distributed >/dev/null 2>&1
	: "the data volume writable: fsGroup 0, except on OpenShift, which sets its own"
	helm template t charts/schedr | grep -q 'fsGroup: 0'
	! helm template t charts/schedr --api-versions security.openshift.io/v1 | grep -q fsGroup
	helm template t charts/schedr --set podSecurityContext.fsGroup=2000 | grep -q 'fsGroup: 2000'
	: "notifications: on by default; a listening URL from the database Secret when given"
	helm template t charts/schedr | grep -q -A1 'SCHEDR_CONFIG__DATABASE__NOTIFICATIONS$$'
	helm template t charts/schedr --set database.existingSecret=pg \
		--set database.notificationsUrlKey=direct | grep -q -A3 'NOTIFICATIONS_URL'
	: "the schema migrated by an init container, holding the lease; schedr itself doesn't"
	helm template t charts/schedr | grep -q 'args: \["db", "migrate", "--wait"\]'
	! helm template t charts/schedr | grep -q -- '--migrate'
	helm template t charts/schedr --set migration=onStart | grep -q -- '"--migrate"\]'
	! helm template t charts/schedr --set migration=onStart | grep -q initContainers
	: "an existing ServiceAccount: none created, the pods run as it, no token mounted"
	! helm template t charts/schedr --set serviceAccount.create=false \
		--set serviceAccount.name=existing | grep -q 'kind: ServiceAccount'
	helm template t charts/schedr --set serviceAccount.create=false \
		--set serviceAccount.name=existing | grep -q 'serviceAccountName: existing'
	helm template t charts/schedr | grep -q 'automountServiceAccountToken: false'
	! helm template t charts/schedr --set serviceAccount.create=false >/dev/null 2>&1
	: "Istio: the docs open to everyone, the API with a token, the ops port listed too"
	helm template t charts/schedr -f charts/schedr/ci/istio-values.yaml | grep -q -- '- /api/v1/openapi.json'
	helm template t charts/schedr -f charts/schedr/ci/istio-values.yaml | grep -q 'requestPrincipals: \["\*"\]'
	helm template t charts/schedr -f charts/schedr/ci/istio-values.yaml | grep -q 'ports: \["8090"\]'
	helm template t charts/schedr -f charts/schedr/ci/istio-values.yaml | grep -q 'holdApplicationUntilProxyStarts'
	helm template t charts/schedr -f charts/schedr/ci/istio-values.yaml --api-versions security.istio.io/v1 \
		| grep -q 'apiVersion: security.istio.io/v1$$'
	helm template t charts/schedr -f charts/schedr/ci/istio-values.yaml | grep -q 'X-Forwarded-Prefix: "/service/schedr"'
	helm template t charts/schedr -f charts/schedr/ci/istio-values.yaml | grep -q 'namespaces: \["default"\]'
	helm template t charts/schedr -f charts/schedr/ci/istio-values.yaml | grep -q 'forwardOriginalToken: true'
	helm template t charts/schedr --set istio.virtualService.enabled=true --set 'istio.virtualService.hosts={*}' \
		--set 'istio.virtualService.gateways={g}' | grep -q 'prefix: "/"'
	! helm template t charts/schedr --set istio.virtualService.enabled=true --set 'istio.virtualService.hosts={*}' \
		--set 'istio.virtualService.gateways={g}' | grep -q rewrite
	! helm template t charts/schedr --set istio.virtualService.enabled=true >/dev/null 2>&1
	! helm template t charts/schedr --set istio.requestAuthentication.enabled=true >/dev/null 2>&1
	! helm template t charts/schedr --set migration=later >/dev/null 2>&1
	: "Postgres from a Secret's user and password: the URL built by Kubernetes"
	helm template t charts/schedr --set database.postgres.host=pg --set database.postgres.existingSecret=pg \
		--set database.postgres.params=sslmode=require --set database.postgres.notificationsHost=direct \
		| grep -q '"postgres://$$(SCHEDR_DB_USER):$$(SCHEDR_DB_PASSWORD)@direct:5432/schedr?sslmode=require"'
	! helm template t charts/schedr --set database.postgres.host=pg >/dev/null 2>&1
	! helm template t charts/schedr --set database.postgres.host=pg --set database.postgres.existingSecret=pg \
		--set database.existingSecret=url >/dev/null 2>&1
	: "DAG files from a folder (dags-values.sh) through the ConfigMap and back, and their root"
	charts/schedr/ci/test-dags-values.sh
	helm template t charts/schedr --set dags.files.a\\.py=eA== | grep -q 'value: "/dags/current:/dag-files/..data"'
	helm template t charts/schedr --set dagsPath= --set dags.files.a\\.py=eA== \
		| grep -q 'value: "/dag-files/..data"'
	! helm template t charts/schedr --set dagsPath= >/dev/null 2>&1
	! helm template t charts/schedr --set 'dags.files.a b\.py=eA==' >/dev/null 2>&1
	! helm template t charts/schedr --set dags.files.a\\.py=eA== --set dagsRoot=files >/dev/null 2>&1
	! helm template t charts/schedr --set dags.files.a\\.py=eA== --set dagsRoot= \
		--set dagsPath=/x/files >/dev/null 2>&1
	! helm template t charts/schedr --set 'dags.files.\.\.data=eA==' >/dev/null 2>&1
	helm template t charts/schedr --set 'dags.files.\.schedrignore=eA==' --set dags.files.a\\.py=eA== >/dev/null
	! helm template t charts/schedr --set database.postgres.host=h --set database.postgres.existingSecret=s \
		--set database.notificationsUrlKey=n >/dev/null 2>&1
	: "the proxy's settings merged into ones of podAnnotations"
	helm template t charts/schedr --set istio.enabled=true \
		--set-string 'podAnnotations.proxy\.istio\.io/config=terminationDrainDuration: 30s' \
		| grep -q '"holdApplicationUntilProxyStarts":true,.*"terminationDrainDuration":"30s"'
	: "git-sync: a token or an SSH key from a Secret"
	helm template t charts/schedr --set gitSync.enabled=true --set gitSync.repo=https://h/r.git \
		--set gitSync.credentials.type=token --set gitSync.credentials.secret=g | grep -q GITSYNC_PASSWORD
	helm template t charts/schedr --set gitSync.enabled=true --set gitSync.repo=git@h:r.git \
		--set gitSync.credentials.type=ssh --set gitSync.credentials.secret=g | grep -q GITSYNC_SSH_KEY_FILE
	! helm template t charts/schedr --set gitSync.credentials.type=ssh >/dev/null 2>&1
	! helm template t charts/schedr --set gitSync.credentials.type=password \
		--set gitSync.credentials.secret=g >/dev/null 2>&1
	: "Vault: a VaultAuth for the pods' account, a VaultStaticSecret restarting schedr on a change"
	helm template t charts/schedr --set vault.enabled=true --set vault.auth.role=r \
		--set 'vault.staticSecrets[0].name=db' --set 'vault.staticSecrets[0].path=p' \
		| grep -q -A1 'rolloutRestartTargets'
	! helm template t charts/schedr --set vault.enabled=true >/dev/null 2>&1
	! helm template t charts/schedr --set vault.enabled=true --set vault.auth.role=r \
		--set 'vault.staticSecrets[0].name=db' >/dev/null 2>&1

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
