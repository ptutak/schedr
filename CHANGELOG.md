# Changelog

Each release's section is its release notes on GitHub (`scripts/release.py notes`). Until
1.0.0 schedr is a preview: the database schema and the protocol between its processes are
changed in place, so moving from one 0.x version to the next means a fresh database
(PROJECT.md, decision 156).

## Unreleased

- **Connections in the web UI** (M4.7e, decisions 207-211): a **Connections** page (every
  deployment's, by DAG root) and a section on each DAG's page (those its tasks declare), with
  **Check all** per root and **Test** per connection (after a confirmation), the results
  filled in as the workers answer. The API: `GET /api/v1/connections`, each root's
  connections with their type, file and the DAGs and tasks declaring them.
- **Every connection type works in the base image** (decision 212): psycopg and PyMySQL join
  its libraries, and the command-line clients commands use their connections with: psql,
  mysql and mariadb (with their dump tools), Mongo's database tools, ssh and scp, curl (the
  image grows to about 670 MB). `pip install schedr[connections]` installs every type's
  client elsewhere.
- **A connections demo** (M4.7d, decisions 202-205): `make demo` runs every connection type's
  server, two deployments (`dev`, `prod`) over one DAG folder with secrets of their own, and
  one UI on http://127.0.0.1:8082, in containers; `make demo-check` runs it unattended. See
  `examples/demo/README.md`.
- **Mongo's login database documented** (decision 206): a user logs in on the URI's
  `authSource`, else its path's database, else `admin`; `database=` is only the database
  the task gets.
- **`schedr connections check` and `test`** (M4.7c, decisions 174, 193, 198-200): `check
  [ROOT]` renders every connection on a worker having the root's current code, saying of
  each secret whether it is there (never its value); `test NAME` opens one there and runs its
  check, beside the tasks (no slot, one at a time per worker), stopped after 60 s
  (`--timeout`). `--worker` picks the worker. Exit 1 when one fails. The API:
  `POST /api/v1/connections/check` and `POST /api/v1/connections/{name}/test`.
- **Commands get connections as variables and files** (M4.7b2, decisions 184, 190-197):
  `CommandOperator(connections={"DB": warehouse})` gives `DB_*` variables and writes the
  files each type's tools read in a folder of the attempt's own (0700, in memory under
  `/dev/shm` where the machine has it, else the data directory, unless `[worker]
  connection_dir` says otherwise; removed when it ends): curl's `-K` config, an SSH config (`ssh -F "$DB_SSH_CONFIG" target`) with its key
  and `known_hosts`, Postgres's `.pgpass` named in `DB_URL` (`psql "$DB_URL"`), one option
  file for MySQL's and MariaDB's clients (`mysql --defaults-file="$DB_MYSQL_CNF"`),
  Mongo's database tools' file (`mongodump --config "$DB_TOOLS_CONFIG"`), and `DB_JSON` with
  every value. A URL variable never holds a password; the TLS defaults are Python's.
  `schedr task-ctx connection NAME --env PREFIX` now prints these same variables, the files
  written. **MariaDB** is supported (decision 196): `SQL` takes `mariadb://`.
- **Built-in connection types** (M4.7b1, decisions 174, 183): `HTTP`, `SSH`, `Mongo`, `SQL`
  (Postgres, MySQL) and `Generic` in `schedr.connections`, named by their first argument,
  giving `httpx.Client`, a paramiko `SSHClient`, a pymongo `Database`, a DB-API connection or
  a dict; each library an extra (`schedr[http]`, `[ssh]`, `[mongo]`, `[postgres]`,
  `[mysql]`), imported on use. Each has a check (`run_check()`); `@name.check` adds one to a
  `@connection`. SSH trusts only the host keys in `known_hosts` unless told
  `insecure_skip_host_check=True`; SQL verifies TLS (`sslmode=verify-full`, libpq's
  `sslmode` for MySQL too, decision 187) unless the URL says `?sslmode=...`, and Mongo
  unless the URI says `tls=false` (decision 189). `SQL` and `Mongo` take `user` and
  `password` apart from the URL; a value a t-string fills into their URL is percent-encoded
  (decision 188).
- **Connections** (M4.7a, decisions 174, 179, 180): `@connection(...)` on a generator function
  defines one by name for the DAG folder, its values secrets and plain values; a task
  declares what it opens (`@task(connections=[...])`, `CommandOperator(connections=)`), and
  the worker renders those before it starts, masking their secrets. `with name() as client:`
  in a Python task; `schedr task-ctx connection NAME --env PREFIX` (export lines for `eval`)
  or `--json` in a command. The error kinds `connection_not_found` (retried) and
  `connection_denied`; a DAG's page warns of a declared connection its folder lacks, and
  `schedr dags check DIR` fails on it. A connection's values take at most 1 MiB. Doubles: `task_context(connections=)`,
  `run_dag(connections=)`, `schedr dags test --connection name=JSON`.
- **Live pages share one stream per browser** (decision 175, B31 and B32): a SharedWorker
  holds the UI's `/events` stream for all of a browser's schedr pages, so six live tabs no
  longer leave clicks and page loads waiting for one of the browser's six connections; a
  running try's log is polled instead of streamed. `/events` now carries every change as JSON
  (each page decides which concern it). htmx's SSE extension is no longer shipped.
- **Secrets** (M4.6): `secrets['path']['key']` (or `secrets.value('path#key')`),
  `secrets.<storage>[...]` and a whole secret `secrets['path']`, typed as the values they
  become (`str`, `dict[str, str]`), as task arguments, in `env=` and in t-strings
  (`text(t"...")` types one as `str`); `.get(key, fallback)` and `value(..., default=)` for a
  missing key, a fallback written in the DAG and never masked; `secrets=` declarations with
  `SecretPrefix` for reads on demand (`task_ctx.secret(path, key)`, `task_ctx.secret(path)`,
  `task_ctx.secret_value('path#key')` in a Python task, `schedr task-ctx secret get` in a command); every fetched value masked in the task's
  log and stored output; the error kinds `secret_unavailable` (retried), `secret_denied` and
  `secret_not_found`; workers report their storages and the master refuses a DAG naming
  another. Test values with `schedr dags test --secret`, `run_dag(secrets=)` and
  `task_context(secrets=)`. **The `db` storage** (standalone): values encrypted in the
  database under `SCHEDR_SECRET_KEY` (XChaCha20-Poly1305, each bound to its `path#key`),
  always there and the default; `schedr secrets set/list/get/delete`, `schedr secrets
  export --to DIR` (one file per path, for `vault kv put`) and `schedr secrets new-key`;
  `[secret_storage]` in the configuration (`default`, `min_mask_length`, a storage's
  `cache_ttl` and `unmasked_prefixes`; several storages need `default`). **Vault** (KV v2) with Kubernetes auth (the chart's
  `secretStorage.vault`, a projected service-account token) or a token, renewed by the agent;
  the chart's `secretStorage.db` for standalone's key. A task can log in to Vault as its
  worker (decision 173): teams that must be kept apart run separate deployments, still in one
  federated UI.

## 0.1.0

The first release, a preview (`Development Status :: 3 - Alpha`). **No upgrade path:** 0.2
will need a fresh database; the schema and the protocol change in place until 1.0.0.

Install with `pip install schedr` (Python 3.14; wheels for Linux x86_64 with glibc or musl,
Linux aarch64 and macOS arm64). The images, x86_64: `ghcr.io/ptutak/schedr` (with Python:
standalone and workers, the base for your own image) and `ghcr.io/ptutak/schedr-server`
(master, webserver and UI, no Python).

What it has (milestones M1 to M4.5 in PLAN.md):

- **DAGs in Python:** `Dag`, `@task`, `CommandOperator`, `EmptyOperator`; data flow between
  tasks, `>>` and `chain`; `Cron`, `Every`, `Once` and presets; `StepDefaults` (retries,
  timeouts, queues); typed outputs (dataclasses, Pydantic models with `schedr[pydantic]`),
  outcomes and routing on them (`.on(...)`, `@branch`), task groups, mapping
  (`.partial(...).expand(...)`), concurrency limits.
- **Run parameters:** `Dag(params=...)` from a dataclass or a model, checked by the master
  before a run exists, given with `runs trigger -p` / `--params`, the API or the UI's
  Trigger form, stored with the run and masked where they hold secrets; `dag_ctx.params` in
  a task, `schedr dag-ctx` in a command, `schedr.script` in a plain script.
- **Testing DAGs:** `schedr dags check`, `schedr dags test`, and `schedr.testing`
  (`run_dag`, `task_context`) for pytest.
- **Running:** `schedr standalone` (master, webserver, UI and a worker in one process, SQLite
  or Postgres), or `schedr master`, `schedr worker`, `schedr webserver` and `schedr ui` as
  separate processes on Postgres with mTLS; a standby master taking over through a lease;
  workers that keep running tasks while the master restarts, and drain on SIGTERM; a fork
  server for fast task starts.
- **Web UI:** DAGs, runs with their graph, task logs and outputs live, actions (trigger,
  cancel, pause, retry, drain) with confirmations, import errors, workers, the deployments
  graph; several deployments in one UI; sign-in by proxy header or basic auth.
- **Operations:** the REST API (`/api/v1`, OpenAPI) and the CLI over it; Prometheus metrics,
  `/healthz` and `/readyz`; a memory watchdog per task; secrets masked in logs, outputs and
  configuration views; a Helm chart in the repository (`charts/schedr`, not published).
