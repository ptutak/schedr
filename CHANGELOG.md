# Changelog

Each release's section is its release notes on GitHub (`scripts/release.py notes`). From 1.0.0
on, the database schema changes only through new migrations and the protocol between schedr's
processes only additively within a major version (decision 456); 0.1.0 was a preview, its
schema and protocol changed in place (decision 156).

## 1.1.0

Not released yet: the notes grow with M5.1 (performance, metrics in the UI).

### Fixed

- **A worker could start a task after its cutoff** (B246, critical, found by Grok's review of
  M5.1b). When a worker loses contact with the master, its cutoff is the moment it must stop
  starting tasks, since the master may hand them to another worker from then on. An
  assignment already in hand could still be started after the cutoff (in 1.0.0 one such
  assignment, after the alive tick had noticed the cutoff), so the same task could run on two
  workers. The worker now starts nothing once it is cut off.

## 1.0.0

The first stable release (`Development Status :: 5 - Production/Stable`). From 1.0.0 on, the
database schema changes only through new migrations, which `schedr db migrate` applies; the
protocol between schedr's processes (now 2.0) changes only additively within 2.x; DAG JSON, the
REST API and the configuration keep accepting what they accept now.

### Upgrading from 0.1.0

- **A fresh database, every node at once.** 0.1.0 was a preview: its schema and protocol were
  changed in place since. A 1.0.0 master refuses a 0.1 worker, and a worker of the 1.0.0rc1
  rehearsal (protocol 1.0; another protocol major, decision 460), and the
  schema starts afresh: `schedr db status` reports a 0.1 database as created by an earlier
  schedr, and the master, the webserver and `schedr db migrate` refuse it.
- **The images' plain tag is now glibc** (decision 454): `ghcr.io/ptutak/schedr`,
  `schedr-slim` and `schedr-server` are built on Debian (`python:3.14.8-slim`) from the
  manylinux wheel; the Alpine ones, the plain tag in 0.1.0, are tagged `-alpine`
  (`schedr:1.0.0-alpine`). An image of your own built `FROM` the base takes `apt` (or `apk` on
  `-alpine`); `examples/kubernetes/Dockerfile` shows both. The glibc base has PostgreSQL 18's
  client from PostgreSQL's own apt source.
- **A slim image** (decision 383): `ghcr.io/ptutak/schedr-slim`, Python and schedr without the
  task libraries and connection clients; the full image stays the default.
- **Timings a deployment rarely tunes left the configuration** (M4.8t, decisions 231-235): 18 of
  them are set only by `SCHEDR_HIDDEN__<SECTION>__<NAME>` (environment only, warned about,
  unsupported); the others are keys with today's values as defaults.
- **A shortened worker lease may be refused at start** (decision 444). A master
  keeps acting for its lead after its last lease renewal: `SCHEDR_HIDDEN__MASTER__LEASE_PERIOD`
  less `SCHEDR_HIDDEN__DATABASE__QUERY_TIMEOUT` (30 s less 10 s, 20 s by default). The worker
  lease (`SCHEDR_HIDDEN__MASTER__LEASE_TIMEOUT`, 60 s) must now be longer than that lead, or
  schedr refuses to start. The lease period must already be at least three query timeouts, so
  the worker lease must be longer than twice the query timeout (20 s by default). Anyone who
  lowered the worker lease to 20 s or less, keeping the default lease period and query timeout,
  must also lower those before upgrading: for example a 15 s worker lease with a 3 s heartbeat
  interval (the lease must stay at least four heartbeats), a 6 s lease period and a 2 s query
  timeout (a 4 s lead).

### New since 0.1.0

- **Cleanups and resources** (M4.8, decisions 214-230, 252-279): per-task `cleanup=` with
  registered handles (masked, journaled), `cleanup_timeout` and retries, a failed cleanup
  failing the run; `cleanup_anywhere`, a lost worker's cleanup moving to another, a worker cut
  off stopping those tasks at its lease; `@resource` and `uses=`, `CommandResource` and
  `ScriptResource`, `task-ctx cleanup` and `resource`; `schedr tasks cancel` stopping a hung
  resource cleanup.
- **Script roots and `schedr.toml`** (M4.9, decisions 292-305): `SCHEDR_SCRIPT_PATH`, standalone
  jobs declared in TOML, the `script` task kind and `ScriptOperator`, libraries shipped in roots;
  only `.py` files mentioning `schedr` are parsed.
- **Async tasks** (M4.10, decision 316): `async def` tasks, cleanups and resources, on `uvloop`
  when installed (`schedr[uvloop]`).
- **Wait steps, triggers and events** (M4.11, decisions 320-356): `@sensor` and `@sensor.poke`
  on a watcher host, `TimedOut`; `@trigger` and `@trigger.poke` (with `batch=`) starting event
  runs; `first_of` and `at_least`; `RunOf` to wait for or start on another DAG's run; named
  events posted through the API; the `delay` and `until` sensors; `schedr.api`, the REST API
  from Python, inside a task through its agent.
- **Runs between and over time** (M4.12, decisions 357-407): `TriggerDagOperator` (a target
  needs `trigger=Chained()`), with `.partial()`/`.expand()`; backfills (`schedr runs backfill`,
  the API and the DAG page's form).
- **Notifications** (M4.13, decisions 408-429): eight events, webhooks and email through an
  outbox, `Dag(notify=...)`, `channel.send` from tasks.
- **Retention** (M4.14, decisions 430-435): `[retention]` for runs, outputs, events and
  commands, never the latest run nor an unfinished one.
- **Per-task memory** (M4.15a, decisions 50, 436-438): `memory=` budgets and
  `hard_memory_limit=`.
- **Master time** (M4B.0, decisions 443-453): liveness decided on the master's monotonic clock,
  carried on every message; a worker's cutoff on master time; a replaced
  incarnation's attempts held for its lease; a new leader waiting out a deposed one (lease
  epochs); metrics for the clock and the fencing.
- **A DAG's trigger has the switch** (decision 459): pausing switches a trigger off, so a DAG
  without one has nothing to pause (`dags pause` and `unpause` are refused; manual runs start
  either way). A trigger is stored paused when it appears or changes kind; another of the same
  kind keeps the switch. The API's `paused` is now `trigger_state` (`paused`, `active` or
  null); `dags list` has STATE (`paused`, `active`, `manual`); the UI shows the switch in the
  Trigger column. Tables: `dag_triggers`, and `triggers` and `trigger_passed` renamed
  `trigger_progress` and `trigger_progress_passed`.
- **A DAG page's actions in one row** (decision 461): an event trigger's Restart and Skip run sit
  beside Trigger, Backfill and Pause; Restart only for a trigger stopped, failing or stuck (a
  poke trigger with no call ended for over three times its `every` plus a heartbeat: the API's
  `stuck`, `(stuck)` in `schedr triggers list`).
- **Numbered messages with an echo** (decision 460, B240): every message between master and
  worker is numbered and names the latest the other side had, so the master takes an
  assignment back only once the worker's messages show it never started it. No start
  deadline: `AssignTask.start_by_ms`, the `TOO_LATE` rejection and the metrics
  `worker_start_slack_seconds` and `worker_too_late_rejections` are gone. Protocol 2.0.
- **Throughput** (M4B, decisions 396, 397, 452): a worker's turn handled in groups, its results
  written together, the agent's journal in groups: about half again as many tasks a second on
  Postgres (the guide, Stress tests).
- **A simulator** (decisions 446, 455): the real master and workers on one virtual clock,
  through cuts, lags, restarts and takeovers, checking that no task instance runs twice;
  600 fresh scenarios nightly.
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

### Fixed

Every bug known at the release, but the DAG page layout's (B38, B39: a redesign, in M6).
Among them: a worker lost just as an assignment reached it no longer has it
queued again at once when its task has `cleanup_anywhere` (B234, decision 455).
A worker connecting again gets back the parse jobs it held, instead of the DAG waiting
`parse_job_timeout` (B235); a database of 0.1 is refused rather than read as current (B236).
A draining worker whose master acknowledges its last results late still deregisters and
exits within its grace period: the wait ends half the report period before it (B237).
A worker whose messages were cut off while an assignment still reached it no longer has that
assignment run again beside it (B240).

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
