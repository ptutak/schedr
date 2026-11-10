# If .python-version selects a pyenv(-virtualenv) environment, make uv install into
# and run from it instead of creating .venv. Without pyenv, uv's defaults apply.
PYENV_PREFIX := $(shell pyenv prefix 2>/dev/null)
ifneq ($(PYENV_PREFIX),)
export UV_PROJECT_ENVIRONMENT := $(PYENV_PREFIX)
export UV_PYTHON := $(PYENV_PREFIX)/bin/python
unexport VIRTUAL_ENV
endif
# make syncs as CI does, also under a shell's UV_NO_SYNC (set for quick `uv run`s by hand):
# the integration tests run the binary installed in the environment, rebuilt by the sync.
unexport UV_NO_SYNC

.PHONY: demo demo-image demo-check demo-down demo-clean help setup fmt fmt-check lint schema schema-check bugs bugs-check undone-check records records-check third-party third-party-check docs docs-dist docs-serve test test-rust test-python test-integration test-ui stress standalone standalone-separate-ui showcase smoke smoke-musl image server-image image-requirements chart-check pin-check check clean target-size

help:            ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n",$$1,$$2}'

# Rust test coverage: unit-test files (`src/**/tests.rs`) and `main.rs` (only calls into the
# library) aren't counted; see archive/PROJECT.md, Repository and tooling: Testing.
COVERAGE_MIN := 85

# Integration tests: the Python with `schedr` installed (for the real runner, found through uv
# inside the recipe, where the uv settings above apply) and a throwaway Postgres container; see
# archive/PROJECT.md, Repository and tooling: Testing.
# postgres:17.6-alpine, created 2025-10-15 (archive/PROJECT.md, CI supply-chain pins).
POSTGRES_IMAGE := docker.io/library/postgres:17.6-alpine@sha256:ef257d85f76e48da1c64832459b59fcaba1a4dac97bf5d7450c77753542eee94
# vault:2.1.2, created 2026-10-07: the Vault storage's tests (dev mode, on the host's network so
# it reaches the tests' fake Kubernetes API on 127.0.0.1).
VAULT_IMAGE := docker.io/hashicorp/vault:2.1.2@sha256:c2f666266f383d2cf424d86b8bb8ce7d065562173ffec2b476d762943608bb55
# Podman when installed (Docker's port forwarding doesn't work everywhere, e.g. the snap
# under WSL), else Docker; override with `make test-integration CONTAINER=docker`.
# The typed connections' servers (decision 185): mysql:8.4.11, created 2026-10-19; mongo:7.0.45,
# created 2026-10-26 (8.x won't start on Linux 6.19+, as CI's runners have: SERVER-121912);
# linuxserver/openssh-server:10.3_p1-r2-ls242, created 2026-11-05 (Alpine's fix of sshd's
# password bypass, its advisory of the same day);
# mariadb:11.8.9 (decision 196), created 2026-10-26.
MYSQL_IMAGE := docker.io/library/mysql:8.4.11@sha256:0f51e63e537a1f176423fa03ef559aabe6fa4372a53160a44bc7e75da7d15364
MARIADB_IMAGE := docker.io/library/mariadb:11.8.9@sha256:c4124d1afa875e47f2892edf26b482fa54f796e2f342d3717a04ca091a22a9bd
MONGO_IMAGE := docker.io/library/mongo:7.0.45@sha256:8bb4442761a63a8dda452744082cdac9644b429807eb977b42324a63cef59ca4
SSH_IMAGE := docker.io/linuxserver/openssh-server:10.3_p1-r2-ls242@sha256:aa4e1b3916e8555f24e574e6108b0b89a1b20362b0a11800a71280b45999649e
CONTAINER ?= $(shell command -v podman >/dev/null 2>&1 && echo podman || echo docker)

# Port for make docs-serve; override with e.g. `make docs-serve DOCS_PORT=9000`.
DOCS_PORT ?= 8765
# `make check` cleans target/ first (before anything builds, even with -j) when it holds more
# than this (Cargo never deletes old
# artifacts: every dependency or feature change leaves a copy of everything above it).
TARGET_MAX_GIB ?= 50
# The same for each other worktree of this repository (each has a target/ of its own).
WORKTREE_TARGET_MAX_GIB ?= 20
LLVM_COV_IGNORE := '(^|/)(tests|main)\.rs$$'

setup:           ## Install dev tools, Python dev deps and pre-commit hooks
	rustup component add llvm-tools-preview
	command -v cargo-nextest >/dev/null || cargo install --locked cargo-nextest
	command -v cargo-llvm-cov >/dev/null || cargo install --locked cargo-llvm-cov
	uv sync
	uv run pre-commit install

fmt:             ## Format Rust and Python code
	cargo fmt --all
	uv run ruff format python examples scripts
	uv run ruff check --fix python examples scripts

fmt-check:       ## Verify formatting without changing files
	cargo fmt --all -- --check
	uv run ruff format --check python examples scripts

# DAG code as authors write it (decision 57): the example DAGs and the typing checks.
DAG_TYPED = $(filter-out %/conftest.py,$(filter-out examples/test_% examples/docs/test_%,$(wildcard examples/*.py examples/docs/*.py))) python/tests/typing/dag_types.py
# The demo's DAG folder apart: its `connections` module is another than examples/docs/'s.
DEMO_DAGS = $(wildcard examples/demo/dags/*.py)
# The greenhouse showcase's DAG folder apart too, for the same reason.
SHOWCASE_DAGS = $(wildcard examples/showcase/dags/*.py)

lint:            ## Run clippy, ruff, mypy, and mypy and basedpyright over DAG code
	cargo clippy --workspace --all-targets -- -D warnings
	uv run ruff check python examples scripts
	uv run mypy
	uv run mypy --strict $(DAG_TYPED)
	uv run mypy --strict $(DEMO_DAGS)
	uv run mypy --strict $(SHOWCASE_DAGS)
	uv run basedpyright --project pyproject.toml

schema:          ## Regenerate the committed files under schema/
	cargo xtask schema

schema-check:    ## Fail if the files under schema/ are out of date
	cargo xtask schema --check

bugs:            ## Regenerate BUGS.md from bugs/
	python3 scripts/bugs.py render

bugs-check:      ## Fail if a bug's file is invalid or BUGS.md is out of date
	python3 scripts/bugs.py check

undone-check:    ## Fail if the changes not committed remove the last commit's lines (B190; SCHEDR_UNDO_OK=1 if meant)
	python3 scripts/undone.py

records:         ## Regenerate PLAN.md, DECISIONS.md and the generated blocks from plan/, decisions/, questions/
	python3 scripts/records.py render

records-check:   ## Fail if a record is invalid or missing, an anchor is gone, or a generated view is stale
	python3 scripts/records.py check

third-party:     ## Write LICENSE-THIRD-PARTY (the binary's crates' licences) from Cargo.lock
	cargo fetch --locked
	python3 scripts/third_party.py

third-party-check: ## Fail if LICENSE-THIRD-PARTY is out of date
	cargo fetch --locked
	python3 scripts/third_party.py --check

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

test-integration: ## Run integration tests (real processes, the binary; Postgres, Vault, MySQL, MariaDB, Mongo and an SSH server in containers)
	@set -e; \
	cargo nextest run --workspace -E 'kind(test) & not binary(stress)' --no-run; \
	: "the build's writes reach the disk before the tests start, or Postgres's fsyncs wait behind them (B6);"; \
	: "this filesystem only (the build's and the containers' disk): a plain sync waits on every mount, /mnt/c too"; \
	sync -f .; \
	name=schedr-test-pg-$$$$; vault=schedr-test-vault-$$$$; \
	mysql=schedr-test-mysql-$$$$; mariadb=schedr-test-mariadb-$$$$; \
	mongo=schedr-test-mongo-$$$$; sshd=schedr-test-ssh-$$$$; \
	keys=$$(mktemp -d); \
	: "set before any container starts: a start failing leaves none behind"; \
	trap '$(CONTAINER) rm -f '$$name' '$$vault' '$$mysql' '$$mariadb' '$$mongo' '$$sshd' >/dev/null 2>&1; rm -rf '$$keys EXIT; \
	$(CONTAINER) run -d --rm --name $$name -e POSTGRES_PASSWORD=schedr -p 127.0.0.1::5432 \
		$(POSTGRES_IMAGE) >/dev/null; \
	vault_port=$$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'); \
	$(CONTAINER) run -d --rm --name $$vault --network host -e VAULT_DEV_ROOT_TOKEN_ID=root \
		-e VAULT_DEV_LISTEN_ADDRESS=127.0.0.1:$$vault_port -e SKIP_SETCAP=1 $(VAULT_IMAGE) >/dev/null; \
	ssh-keygen -q -t ed25519 -N '' -C schedr-test -f $$keys/id_ed25519; \
	$(CONTAINER) run -d --rm --name $$mysql -e MYSQL_ROOT_PASSWORD=schedr -p 127.0.0.1::3306 \
		$(MYSQL_IMAGE) >/dev/null; \
	$(CONTAINER) run -d --rm --name $$mariadb -e MARIADB_ROOT_PASSWORD=schedr -p 127.0.0.1::3306 \
		$(MARIADB_IMAGE) >/dev/null; \
	$(CONTAINER) run -d --rm --name $$mongo -p 127.0.0.1::27017 $(MONGO_IMAGE) >/dev/null; \
	$(CONTAINER) run -d --rm --name $$sshd -e USER_NAME=schedr \
		-e PUBLIC_KEY="$$(cat $$keys/id_ed25519.pub)" -p 127.0.0.1::2222 $(SSH_IMAGE) >/dev/null; \
	port=$$($(CONTAINER) port $$name 5432/tcp | head -1 | sed 's/.*://'); \
	until $(CONTAINER) exec $$name psql -h 127.0.0.1 -U postgres -c 'select 1' >/dev/null 2>&1; do \
		sleep 0.5; done; \
	export SCHEDR_TEST_POSTGRES_URL=postgres://postgres:schedr@127.0.0.1:$$port/postgres; \
	until curl -sf http://127.0.0.1:$$vault_port/v1/sys/health >/dev/null; do sleep 0.5; done; \
	export SCHEDR_TEST_VAULT_URL=http://127.0.0.1:$$vault_port SCHEDR_TEST_VAULT_TOKEN=root; \
	mysql_port=$$($(CONTAINER) port $$mysql 3306/tcp | head -1 | sed 's/.*://'); \
	mariadb_port=$$($(CONTAINER) port $$mariadb 3306/tcp | head -1 | sed 's/.*://'); \
	mongo_port=$$($(CONTAINER) port $$mongo 27017/tcp | head -1 | sed 's/.*://'); \
	ssh_port=$$($(CONTAINER) port $$sshd 2222/tcp | head -1 | sed 's/.*://'); \
	: "MySQL's first start runs a server without networking, then the real one: wait on TCP"; \
	until $(CONTAINER) exec $$mysql mysqladmin ping --protocol=TCP -h 127.0.0.1 -uroot -pschedr \
		--silent >/dev/null 2>&1; do sleep 1; done; \
	until $(CONTAINER) exec $$mariadb mariadb-admin ping --protocol=TCP -h 127.0.0.1 -uroot \
		-pschedr --silent >/dev/null 2>&1; do sleep 1; done; \
	: "the CA of the certificate MySQL made itself, for the TLS tests"; \
	$(CONTAINER) cp $$mysql:/var/lib/mysql/ca.pem $$keys/mysql-ca.pem; \
	until $(CONTAINER) exec $$mongo mongosh --quiet --eval 'db.runCommand({ping: 1}).ok' \
		>/dev/null 2>&1; do sleep 0.5; done; \
	until ssh-keyscan -p $$ssh_port 127.0.0.1 > $$keys/known_hosts 2>/dev/null \
		&& [ -s $$keys/known_hosts ]; do sleep 0.5; done; \
	export SCHEDR_TEST_MYSQL_URL=mysql://root:schedr@127.0.0.1:$$mysql_port/mysql; \
	export SCHEDR_TEST_MYSQL_CA=$$keys/mysql-ca.pem; \
	export SCHEDR_TEST_MARIADB_URL=mysql://root:schedr@127.0.0.1:$$mariadb_port/mysql; \
	: "MariaDB's own client, run in its container, reading an option file from stdin"; \
	export SCHEDR_TEST_MARIADB_EXEC="$(CONTAINER) exec -i $$mariadb"; \
	export SCHEDR_TEST_MONGO_URI=mongodb://127.0.0.1:$$mongo_port/; \
	export SCHEDR_TEST_SSH="127.0.0.1:$$ssh_port" SCHEDR_TEST_SSH_USER=schedr; \
	export SCHEDR_TEST_SSH_KEY_FILE=$$keys/id_ed25519 SCHEDR_TEST_SSH_KNOWN_HOSTS=$$keys/known_hosts; \
	export SCHEDR_TEST_PYTHON=$$(uv run python -c 'import sys; print(sys.executable)'); \
	test -x "$$SCHEDR_TEST_PYTHON"; \
	cargo nextest run --workspace -E 'kind(test) & not binary(stress)'; \
	uv run pytest python/tests/integration examples

# The web UI's browser tests (decision 27A): a standalone with the example DAG and a finished
# run, driven by Playwright's Chromium (downloaded once, into ~/.cache/ms-playwright). Kept out
# of `check` for that download; CI runs it as a job of its own.
test-ui:         ## Run the web UI's browser tests (Playwright, Chromium)
	cargo build -p schedr
	uv sync --group ui-test
	uv run --no-sync playwright install chromium
	SCHEDR_BIN=$(abspath target/debug/schedr) uv run --no-sync pytest python/tests/ui

# A local standalone for trying things by hand (development only): the debug binary just
# built, the Python of `uv sync` (with schedr installed) for DAG files and tasks, SQLite and the
# worker's data in .schedr/ in this repository (git-ignored; open .schedr/schedr.db in an IDE),
# the DAGs of DAGS (default examples/). The API on LISTEN:8080, all addresses by default so it
# can be reached from outside WSL (it signs no one in, auth mode none: LISTEN=127.0.0.1 on a
# network you don't trust; `schedr dags list` works without --server with either), ops on 8090.
# Ctrl-C stops it; `rm -rf .schedr` starts afresh. Python tasks fork from a fork server as the chart's do
# (FORK_SERVER=false: spawned, as the configuration's default; Linux only), the preloaded one
# importing PRELOAD (the libraries the examples' DAG files use; a JSON list, `[]` for the bare
# server alone), so a forked task doesn't import them itself. Both servers load all of schedr,
# with pydantic, on their own.
DAGS ?= examples
STANDALONE_DIR ?= .schedr
LISTEN ?= 0.0.0.0
FORK_SERVER ?= true
PRELOAD ?= ["sqlite3", "asyncio", "json", "shutil", "tempfile", "random"]

# The docs examples' configuration (the notification channels and URLs) and secrets, as
# examples/test_docs.py gives them.
STANDALONE_CONFIG := $(abspath examples/standalone.toml)
STANDALONE_SECRETS := "etl/db password=s3cret-db-password" "etl/api token=s3cret-api-token" \
	"tenants/acme api_key=s3cret-acme-key" "greenhouse/devices token=s3cret-device-token"
# `prepare DIR`, a shell function for a standalone whose SQLite database is DIR/schedr.db: a
# secret key made once in DIR/secret.key (`SCHEDR_SECRET_KEY`), and STANDALONE_SECRETS set
# where missing (listed, never decrypted: one written under another key is kept). A new database is migrated first to hold them; one there already is migrated
# by `standalone --migrate`.
STANDALONE_PREPARE = prepare() { \
	local key="$$1/secret.key" url="sqlite://$$1/schedr.db" entry path pair; \
	mkdir -p "$$1"; \
	if [ ! -s "$$key" ]; then (umask 077; target/debug/schedr secrets new-key > "$$key"); fi; \
	[ -e "$$1/schedr.db" ] || SCHEDR_CONFIG__DATABASE__URL="$$url" target/debug/schedr db migrate; \
	stored="$$(SCHEDR_CONFIG__DATABASE__URL="$$url" target/debug/schedr secrets list | cut -f1,2)"; \
	for entry in $(STANDALONE_SECRETS); do \
		path=$${entry%% *}; pair=$${entry\#* }; \
		grep -qxF "$$path	$${pair%%=*}" <<< "$$stored" \
		|| SCHEDR_CONFIG__DATABASE__URL="$$url" SCHEDR_SECRET_KEY="$$(cat "$$key")" \
			target/debug/schedr secrets set "$$path" "$$pair" >/dev/null; \
	done; \
}

standalone: SHELL := /bin/bash
standalone:      ## Run schedr standalone here: SQLite in .schedr/, the DAGs of DAGS (examples/); a secret key in .schedr/secret.key, the docs examples' secrets; the UI's Docs from site/ once `make docs` built it
	cargo build -p schedr
	set -euo pipefail; $(STANDALONE_PREPARE); \
	dir="$(abspath $(STANDALONE_DIR))"; prepare "$$dir"; \
	SCHEDR_SECRET_KEY="$$(cat "$$dir/secret.key")" \
	SCHEDR_CONFIG__DATABASE__URL="sqlite://$$dir/schedr.db" \
	SCHEDR_CONFIG_FILE="$(STANDALONE_CONFIG)" \
	SCHEDR_DAG_PATH="$(abspath $(DAGS))" \
	SCHEDR_CONFIG__WORKER__DATA_DIR="$(abspath $(STANDALONE_DIR))" \
	SCHEDR_CONFIG__INTERPRETERS__PYTHON__PATH="$$(uv run --no-sync python -c 'import sys; print(sys.executable)')" \
	SCHEDR_CONFIG__INTERPRETERS__PYTHON__FORK_SERVER=$(FORK_SERVER) \
	SCHEDR_CONFIG__INTERPRETERS__PYTHON__PRELOAD='$(PRELOAD)' \
	$(if $(wildcard site/index.html),SCHEDR_CONFIG__UI__DOCS_DIR="$(abspath site)") \
	uv run --no-sync target/debug/schedr standalone --migrate --listen $(LISTEN)

# A federation for trying the UI over many deployments by hand: two standalones without a UI
# (`--no-ui`), `main` with the DAGs of DAGS and `second` with those of DAGS2, and a `schedr ui`
# over both in a process of its own, as one UI over many deployments is run. Each has its
# data in .schedr/federation/<name>/ and the UI's configuration is .schedr/federation/ui.toml.
# The UI on LISTEN:8082; `main` with its API on 8080, gRPC 8070, log endpoint 8081, ops 8090;
# `second` the same plus 100; the UI's ops on 8092. `main` also has a standby master in a
# process of its own (gRPC 8071, ops 8091), started once `main` has migrated its database: it
# takes over when `main` stops leading. Its gRPC is plain (`[tls] mode = "mesh"`), as a
# standalone's own links are: everything is on this machine. `second` also has a root `broken`
# (written to .schedr/federation/broken/) of two DAG files failing to import on purpose, for
# the import errors and the warnings they give. PORT_OFFSET is added to all of them, to run
# it next to a `make standalone` (PORT_OFFSET=10000). Ctrl-C, or any of them stopping,
# stops them all.
DAGS2 ?= examples/docs
PORT_OFFSET ?= 0

standalone-separate-ui: SHELL := /bin/bash
standalone-separate-ui: ## Run two standalones with no UI (DAGS and DAGS2; the first with a standby master) and one `schedr ui` over both, on 8082
	cargo build -p schedr
	$(STANDALONE_PREPARE); \
	dir="$(abspath $(STANDALONE_DIR))/federation"; \
	python="$$(uv run --no-sync python -c 'import sys; print(sys.executable)')"; \
	ui=$$((8082 + $(PORT_OFFSET))); ui_ops=$$((8092 + $(PORT_OFFSET))); \
	run() { \
		prepare "$$dir/$$1" || exit 1; \
		SCHEDR_SECRET_KEY="$$(cat "$$dir/$$1/secret.key")" \
		SCHEDR_DAG_PATH="$$2" \
		SCHEDR_CONFIG__DATABASE__URL="sqlite://$$dir/$$1/schedr.db" \
		SCHEDR_CONFIG_FILE="$(STANDALONE_CONFIG)" \
		SCHEDR_CONFIG__WORKER__DATA_DIR="$$dir/$$1" \
		SCHEDR_CONFIG__INTERPRETERS__PYTHON__PATH="$$python" \
		SCHEDR_CONFIG__INTERPRETERS__PYTHON__FORK_SERVER=$(FORK_SERVER) \
		SCHEDR_CONFIG__INTERPRETERS__PYTHON__PRELOAD='$(PRELOAD)' \
	SCHEDR_CONFIG__INTERPRETERS__PYTHON__PRELOAD='$(PRELOAD)' \
		SCHEDR_CONFIG__WEBSERVER__PORT=$$(($$3 + 80)) \
		SCHEDR_CONFIG__MASTER__GRPC_PORT=$$(($$3 + 70)) \
		SCHEDR_CONFIG__WORKER__LOG_PORT=$$(($$3 + 81)) \
		SCHEDR_CONFIG__OPS__PORT=$$(($$3 + 90)) \
		uv run --no-sync target/debug/schedr standalone --migrate --no-ui --listen $(LISTEN) & \
		printf '[[ui.deployments]]\nname = "%s"\nurl = "http://127.0.0.1:%s"\n\n' "$$1" $$(($$3 + 80)) >> "$$dir/ui.toml"; \
	}; \
	mkdir -p "$$dir"; \
	printf '[ui]\nbind = "%s"\nport = %s\ndocs_dir = "%s"\n\n[ops]\nbind = "127.0.0.1"\nport = %s\n\n' \
		"$(LISTEN)" $$ui "$(if $(wildcard site/index.html),$(abspath site))" $$ui_ops > "$$dir/ui.toml"; \
	trap 'kill $$(jobs -p) 2>/dev/null; wait' EXIT INT TERM; \
	run main "$(abspath $(DAGS))" $$((8000 + $(PORT_OFFSET))); \
	main=$$((8000 + $(PORT_OFFSET))); \
	( until curl -sf -o /dev/null "http://127.0.0.1:$$((main + 80))/api/v1/dags"; do sleep 0.5; done; \
	  SCHEDR_SECRET_KEY="$$(cat "$$dir/main/secret.key")" \
	  SCHEDR_CONFIG__DATABASE__URL="sqlite://$$dir/main/schedr.db" \
	  SCHEDR_CONFIG_FILE="$(STANDALONE_CONFIG)" \
	  SCHEDR_CONFIG__MASTER__GRPC_PORT=$$((main + 71)) \
	  SCHEDR_CONFIG__OPS__PORT=$$((main + 91)) \
	  SCHEDR_CONFIG__TLS__MODE=mesh \
	  exec target/debug/schedr master ) & \
	broken="$$dir/broken"; mkdir -p "$$broken"; \
	printf '%s\n' '"""Fails to import on purpose: a module not installed."""' '' \
		'import not_installed_module  # noqa: F401' > "$$broken/missing_module.py"; \
	printf '%s\n' '"""Fails to import on purpose, its password masked where it is shown."""' '' \
		'raise RuntimeError("cannot reach postgres://etl:hunter2@warehouse:5432/dw")' \
		> "$$broken/bad_connection.py"; \
	run second "$(abspath $(DAGS2)):$$broken" $$((8100 + $(PORT_OFFSET))); \
	target/debug/schedr --config "$$dir/ui.toml" ui & \
	wait -n

# The greenhouse showcase (examples/showcase): its simulator against a standalone running its
# DAGs, started first in another terminal with `make standalone DAGS=examples/showcase/dags`.
# Unpauses the four DAGs, then plays the devices, an operator and the pager's receiver
# (examples/showcase/simulate.py). Ctrl-C stops it.
SHOWCASE_DAG_IDS := readings_ingest anomaly_response device_maintenance greenhouse_rollup

showcase:        ## Play the greenhouse showcase against `make standalone DAGS=examples/showcase/dags`
	cargo build -p schedr
	@curl -sf -o /dev/null http://localhost:8080/api/v1/dags/readings_ingest || { \
		echo "no standalone with the showcase's DAGs on localhost:8080: start one first, in another" \
			"terminal, with \`make standalone DAGS=examples/showcase/dags\`" >&2; exit 1; }
	for dag in $(SHOWCASE_DAG_IDS); do target/debug/schedr dags unpause $$dag >/dev/null || exit 1; done
	uv run --no-sync python examples/showcase/simulate.py --schedr target/debug/schedr demo

# The connections demo (examples/demo, decisions 202-205): the base image (built once, `make
# image`), the demo image FROM it, every password and key generated into the demo's volumes,
# then compose: the servers, `dev` and `prod`, and `schedr ui` on http://127.0.0.1:8082.
# `make demo-down` stops it; `make demo-clean` also drops the generated passwords and keys.
DEMO_IMAGE ?= localhost/schedr-demo:latest
export DEMO_IMAGE  # compose.yaml reads it
# Compose of the same engine as CONTAINER: images and volumes made with one are seen by it.
COMPOSE ?= $(if $(filter podman,$(notdir $(CONTAINER))),podman-compose,docker compose)
DEMO_COMPOSE = $(COMPOSE) -f examples/demo/compose.yaml
# One volume per reader (examples/demo/keys.sh): each container mounts only its own.
DEMO_VOLUMES := schedr-demo-dev schedr-demo-prod schedr-demo-servers schedr-demo-ssh-users \
	schedr-demo-ssh-host

# The base image: there already, else pulled when IMAGE names a registry
# (`IMAGE=ghcr.io/ptutak/schedr:<version>`), else built from this checkout (`make image`,
# which needs Rust and uv).
demo-image:      ## Build the connections demo's image (on the base image: there, pulled, or built)
	$(CONTAINER) image inspect $(IMAGE) >/dev/null 2>&1 || case "$(IMAGE)" in \
		*.*/*) $(CONTAINER) pull $(IMAGE) ;; \
		*) $(MAKE) image ;; \
	esac
	$(CONTAINER) build --build-arg SCHEDR=$(IMAGE) -t $(DEMO_IMAGE) examples/demo
	for volume in $(DEMO_VOLUMES); do \
		$(CONTAINER) volume inspect $$volume >/dev/null 2>&1 || $(CONTAINER) volume create $$volume >/dev/null; \
	done
	$(CONTAINER) run --rm --user 0 -v schedr-demo-dev:/demo/dev -v schedr-demo-prod:/demo/prod \
		-v schedr-demo-servers:/demo/servers -v schedr-demo-ssh-users:/demo/ssh-users \
		-v schedr-demo-ssh-host:/demo/ssh-host --entrypoint schedr-demo-keys $(DEMO_IMAGE)

demo: demo-image ## Run the connections demo: every connection type, dev and prod, one UI on 127.0.0.1:8082
	$(DEMO_COMPOSE) up

demo-check: demo-image ## Run the demo in the background, check every connection from both deployments, stop it
	COMPOSE="$(DEMO_COMPOSE)" scripts/demo-check.sh

demo-down:       ## Stop the connections demo
	$(DEMO_COMPOSE) down

demo-clean: demo-down ## Stop the demo and drop its generated passwords and keys
	$(CONTAINER) volume rm -f $(DEMO_VOLUMES)

# The stress suite (crates/schedr/tests/stress, PLAN.md 2.10b): Postgres and a PgBouncer in
# transaction pooling in front of it (any database name passed through), on a network of their
# own; one scenario at a time. `STRESS_PROFILE=full` for the large one, `STRESS=<name>` for some
# scenarios (a nextest name filter, e.g. `STRESS=failover`); reports in target/stress/.
# pgbouncer:v1.26.0-p0, created 2026-09-30 (archive/PROJECT.md, CI supply-chain pins).
PGBOUNCER_IMAGE := docker.io/edoburu/pgbouncer:v1.26.0-p0@sha256:b17551c776ef7e5769ef80b956d20f85e2fd25dd8912d31d58f782aad495b711
STRESS_PROFILE ?= quick
STRESS ?=

stress:          ## Run the stress suite (STRESS_PROFILE=quick|full, STRESS=<filter>)
	@set -e; \
	cargo nextest run -p schedr --test stress --no-run; \
	sync -f .; \
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
	rm -f dist/*manylinux*x86_64.whl dist/*-linux_x86_64.whl
	CONTAINER=$(CONTAINER) scripts/build-wheel.sh manylinux-x86_64
	scripts/smoke-wheel.sh dist/*manylinux*x86_64.whl $$(uv run python -c 'import sys; print(sys.executable)')

# The musllinux wheel is built in the pinned musllinux image (scripts/build-wheel.sh) and
# smoke-tested on the Alpine image the standalone example uses.
# python:3.14.8-alpine3.24, created 2026-10-01 (archive/PROJECT.md, CI supply-chain pins).
ALPINE_IMAGE := docker.io/library/python:3.14.8-alpine3.24@sha256:f6a589d43c42b9e7f7dc67a12d37132491f362859a5d750607710cc56da3bc72

smoke-musl:      ## Build the musllinux wheel into dist/ and smoke-test it on Alpine
	rm -f dist/*musllinux*.whl
	CONTAINER=$(CONTAINER) scripts/build-wheel.sh musllinux-x86_64
	$(CONTAINER) run --rm -v "$(CURDIR):/repo:ro,Z" $(ALPINE_IMAGE) sh -c \
		'apk add --no-cache bash >/dev/null && cd /repo && bash scripts/smoke-wheel.sh dist/*musllinux*.whl python3'

IMAGE ?= schedr:$(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)

# The base image's libraries, pinned with their dependencies for Alpine (musl) and Python 3.14,
# none newer than two weeks (archive/PROJECT.md, Repository and tooling: Libraries).
IMAGE_PIN = uv pip compile --python-platform x86_64-unknown-linux-musl --python-version 3.14 \
	--exclude-newer $$(date -u -d '14 days ago' +%Y-%m-%d) --no-annotate --quiet \
	--custom-compile-command 'make image-requirements'

image-requirements: ## Pin the base image's libraries (images/schedr/requirements.in) again
	$(IMAGE_PIN) images/schedr/requirements.in -o images/schedr/requirements.txt
	$(IMAGE_PIN) --only-binary :all: images/schedr/build-requirements.in \
		-o images/schedr/build-requirements.txt

SERVER_IMAGE ?= schedr-server:$(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)

docs-dist: docs    ## The documentation the images serve at /docs, into dist/docs
	scripts/docs-dist.sh

server-image: smoke-musl docs-dist ## Build the server image (master, webserver, ui: no Python) and smoke-test it
	$(CONTAINER) build -f images/schedr-server/Dockerfile -t $(SERVER_IMAGE) .
	scripts/smoke-server-image.sh $(CONTAINER) $(SERVER_IMAGE)

image: smoke-musl docs-dist ## Build the base image from the musllinux wheel and smoke-test it
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
	: "worker.forkServer.preload: the list the image's build checks (images/schedr/preload.txt)"
	helm template t charts/schedr | python3 scripts/check-preload-list.py images/schedr/preload.txt
	: "mode distributed: the roles, TLS (generated certificates checked with openssl), accounts, Istio"
	charts/schedr/ci/test-distributed.sh
	: "the data volume writable: fsGroup 0, except on OpenShift, which sets its own"
	helm template t charts/schedr | grep -q 'fsGroup: 0'
	! helm template t charts/schedr --api-versions security.openshift.io/v1 | grep -q fsGroup
	helm template t charts/schedr --set podSecurityContext.fsGroup=2000 | grep -q 'fsGroup: 2000'
	: "secretStorage in standalone: the db storage's key from its Secret, Vault with a projected token"
	helm template t charts/schedr --set secretStorage.db.existingSecret=sk \
		--set secretStorage.vault.enabled=true --set secretStorage.vault.address=https://v:8200 \
		--set secretStorage.vault.auth.role=r | grep -q -A3 'SCHEDR_SECRET_KEY$$'
	! helm template t charts/schedr | grep -q 'SCHEDR_SECRET_KEY\|SECRET_STORAGE'
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

pin-check:       ## Fail if a workflow, tool, image or requirements pin is missing
	uv run --no-sync python scripts/check-action-pins.py

check:           ## Everything CI runs
	@$(MAKE) --no-print-directory target-size
	@$(MAKE) --no-print-directory undone-check pin-check fmt-check lint schema-check bugs-check records-check third-party-check chart-check test test-integration

target-size:     ## Clean target/ over TARGET_MAX_GIB (50), other worktrees' over WORKTREE_TARGET_MAX_GIB (20)
	@size=$$(du -s --block-size=1G target 2>/dev/null | cut -f1); \
	if [ "$${size:-0}" -gt $(TARGET_MAX_GIB) ]; then \
		echo "target/ holds $$size GiB, over TARGET_MAX_GIB = $(TARGET_MAX_GIB): cargo clean"; \
		cargo clean; \
	fi
	@: "a worktree building now (its build lock held) is left alone"; \
	git worktree list --porcelain | sed -n 's/^worktree //p' | while read -r tree; do \
		[ -n "$$tree" ] && [ "$$tree" != "$$(pwd)" ] && [ -f "$$tree/Cargo.toml" ] || continue; \
		size=$$(du -s --block-size=1G "$$tree/target" 2>/dev/null | cut -f1); \
		[ "$${size:-0}" -gt $(WORKTREE_TARGET_MAX_GIB) ] || continue; \
		lock="$$tree/target/debug/.cargo-lock"; \
		if [ -e "$$lock" ] && ! flock -n "$$lock" true; then \
			echo "$$tree/target holds $$size GiB but is building: left alone"; continue; \
		fi; \
		echo "$$tree/target holds $$size GiB, over $(WORKTREE_TARGET_MAX_GIB) GiB: cargo clean there"; \
		cargo clean --manifest-path "$$tree/Cargo.toml"; \
	done

clean:           ## Remove build artifacts
	cargo clean
	cargo llvm-cov clean --workspace 2>/dev/null || true
	rm -rf .venv .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov site docs/_generated
