# schedr: Project Decisions

This file records what has been decided about schedr. Open questions are listed at the end
and move up into the decided sections once settled.

## Goal

A job scheduler written primarily in **Rust** that runs three kinds of work:

1. **Python scripts**: a `.py` file run as a subprocess.
2. **Bash processes**: shell commands or scripts.
3. **Python DAGs**: workflows written in Python, either with schedr's own native API or with
   a compatibility API that copies another orchestrator's API (starting with Airflow) but runs
   on the schedr scheduler.

**Plain scripts are first-class.** Plain Python scripts (no schedr API) and Bash scripts are
supported on their own, not only as part of DAGs. The Python API also has **dedicated
operators that run a plain Python script or a Bash script file** as a task.

## Deployment target

- **Distributed system running on Kubernetes.** The production target is **OpenShift**, but
  schedr integrates with **plain Kubernetes** only: no OpenShift-specific APIs or resources.
- A **single master** process (one pod) and **long-running worker** pods. Master high
  availability is not needed for now.
- **No pod-per-task execution.** It isn't planned and probably never will be: too much
  overhead, and schedr can't count on permission to create pods. Tasks run as processes
  inside the long-running worker pods.
- Architecture: see Architecture below.

### Constraints that follow

- **Minimal Kubernetes permissions.** schedr must not need to create pods or hold
  cluster-wide permissions; ideally it doesn't call the Kubernetes API at all.
- **Images must run under an arbitrary UID** (OpenShift's default restricted security policy;
  also good practice on plain Kubernetes): no fixed user, writable paths group-owned by root
  (group 0).

## Architecture

### Components and roles

One Rust binary, four roles:

| Role                | Runs                                                                  | User code?  |
|---------------------|-----------------------------------------------------------------------|-------------|
| `schedr master`     | Scheduling loop, state in Postgres, internal API for workers          | **Never.** Its image contains no user packages. |
| `schedr worker`     | Rust agent; starts Python and Bash subprocesses for tasks and parsing | Yes, in subprocesses |
| `schedr webserver`  | Web UI and the API used by the UI and CLI                             | **Never.**  |
| `schedr standalone` | All of the above in one process, with SQLite                          | Yes (local development only) |

### Web UI and CLI

- A **web dashboard** is required: live view of what is running, and access to task logs.
- A **CLI** is required alongside the web UI.
- The web UI is served by a **separate process from the master scheduler** (`schedr webserver`),
  so the UI and the scheduler can restart, fail and scale independently.
- **How the webserver gets state: through Postgres, for reads and actions.**
  - **Reads:** the webserver queries Postgres directly, with a **read-only database role**. The
    UI keeps working (history, runs, logs) while the master restarts.
  - **Actions** (trigger, cancel, pause, drain, …): the webserver writes a **command** row; the
    master picks it up, validates and applies it in its own transaction, and marks it
    `applied` or `rejected` (with a reason). The UI shows a command as pending until then.
    Actions are durable: one sent while the master restarts is applied when it returns. The
    master stays the **only writer of scheduling state** (a command is a request).
  - The master picks commands up via Postgres **`LISTEN/NOTIFY`** (at once), with polling every
    few seconds as a fallback; standalone (SQLite) uses polling only.
  - No API between the webserver and the master: both are the same binary at the same version
    and `schedr db migrate` runs first, so sharing the schema is safe.
  - Not chosen: an API on the master for reads and actions (the UI would go dark whenever the
    single master is down); reads from Postgres and actions by gRPC to the master (actions
    would fail while the master restarts).
- **Live updates: Server-Sent Events (SSE).** Updates only flow from the server to the
  browser; actions are ordinary HTTP requests (WebSockets would add complexity for nothing).
  - The master issues a **`NOTIFY`** when state changes (task states, runs, workers); each
    webserver replica listens and fans events out to its browsers. Events are small ("run X
    changed"); the browser then fetches the details through the normal API.
  - A comment line every ~15 s keeps proxies (e.g. the OpenShift router) from closing idle
    streams; reconnection is built into SSE.
  - **Live log tail** is SSE too: the webserver relays the worker's tail stream (see Task logs).
  - Standalone: the webserver polls the database instead of listening.
- **The user CLI uses the webserver's API**, the same one as the UI: one API, one place for
  authentication and permissions. Commands (see Runs: manual runs, backfill and pausing):

  | Command | Does |
  |---|---|
  | `schedr dags list`, `schedr dags pause <dag>` / `unpause <dag>` | list DAGs; stop / resume the scheduler creating runs |
  | `schedr runs trigger <dag> [--for <time>] [-p …] [--params '{…}']` | a manual run |
  | `schedr runs backfill <dag> --from … [--to …] [--all] [-p …]` | a backfill |
  | `schedr runs list <dag> [--for <time>]` | runs, with their IDs |
  | `schedr runs cancel <run id>`, `schedr runs cancel --backfill <id>` | cancel a run; what's left of a backfill |
  | `schedr tasks cancel <run id> <task id> [--map-index N]` | cancel one running task (see Master–worker protocol) |
  | `schedr tasks retry <run id> <task id> [--map-index N]` | retry an `unschedulable` task of an unfinished run |
  | `schedr workers drain <worker>` / `resume <worker>` | maintenance drain |
 The master's gRPC API stays internal to the workers.
  - **REST with JSON**; an **OpenAPI** description is generated from the Rust types, committed
    to the repository, and **CI fails if it is out of date** (like the JSON Schema).
  - **Compatibility between the CLI and the server** (the CLI comes from `pip install schedr`
    on a laptop or in CI and drifts from the deployed version; the UI is served by the
    webserver, so it always matches):
    - **Paths carry the major version** (`/api/v1/...`); within a major, API changes are
      **additive only** (new endpoints, new optional fields, new enum values clients treat as
      open), as for DAG JSON and the protocol. Clients ignore unknown fields. **CI fails on a
      breaking change** to the committed OpenAPI description within a major.
    - **Versions on every request:** the CLI sends `Schedr-Client-Version`, the server answers
      with `Schedr-Server-Version`. Same major, any minor: works. A command or flag the server
      doesn't have yet gives a clear error ("`schedr runs backfill --all` needs schedr server ≥
      1.6 (server: 1.4)"; the CLI knows which version introduced each command and flag). A CLI
      older than the server works (optionally a hint at most once a day that newer commands
      exist). A different major: the CLI refuses ("server is schedr 2.x, this CLI is 1.x:
      `pip install 'schedr>=2,<3'`").
    - Other API clients (scripts, OpenAPI-generated clients) follow the same rules.
  - Not part of the user CLI: `schedr task-ctx` and `schedr dag-ctx` (talk to the agent),
    server commands such as `schedr db migrate` (talk to the database), the server roles,
    and the local commands `schedr dags check <path>` and `schedr dags test <dag>` (no server;
    see Testing DAGs).
  - The CLI only **sends** credentials, it never validates them: a bearer token (`--token` /
    `SCHEDR_TOKEN`, e.g. a Keycloak token required by an Istio ingress) or basic credentials
    (`--user`, `SCHEDR_USER` / `SCHEDR_PASSWORD`). From inside the namespace no credentials
    are needed (with `auth.mode = "none"`). `schedr login` (fetching a token through OIDC's
    device flow): possibly later.
- **Frontend: server-rendered HTML with htmx.** Pages are HTML rendered by the webserver from
  Rust templates (`askama`, checked at compile time); **htmx** adds the interactive parts
  (buttons, filters, partial refreshes) and, with its SSE extension, live updates and the log
  tail.
  - **No JavaScript build:** no Node.js / npm, nothing new in CI. htmx and a small pan/zoom
    helper for the graph are vendored single files; all assets are **embedded in the `schedr`
    binary**, so `schedr webserver` (and standalone) serves everything itself.
  - The **DAG graph** is drawn as **SVG on the server** (a Rust graph-layout crate); logs are
    converted from ANSI colours to HTML on the server.
  - The webserver serves HTML fragments besides the JSON API used by the CLI; both use the same
    queries, so the HTML layer is thin.
  - Not chosen: a single-page app without a build step (Preact + `htm`: a JavaScript
    application to maintain without types or tests, and a JS graph layout); a full
    single-page app (React / Svelte + Vite + TypeScript: Node.js in the build and CI, a second
    ecosystem to maintain).
- **What the UI shows** (beyond what is decided elsewhere: DAG conflicts, import errors, parse
  warnings, unschedulable tasks with each worker's reason, mixed code hashes in a run):
  - **A task's status at a glance: state · outcome · cleanup**, each part only when it says
    something: `success` (the only outcome `SUCCESS` isn't shown), `success · Unavailable`,
    `success · cleanup failed`, `cleaning_up`, `skipped · not chosen` / `skipped · on Rows`
    (why it was skipped), `failed · timed out`.
  - **The run's graph (SVG):** nodes coloured by state, the outcome under the task name.
    Edges styled by kind, with a legend: taken (solid); not taken (dashed, grey, labelled with
    their condition, e.g. `on Unavailable`); branch selection (marked as from a branch);
    requirement / `uses=` (dotted, own colour). **Generated tasks** (from the Airflow
    translation: last-line extractor, `Empty` / `NotEmpty` check, start anchor) are **folded
    into their task** by default, with a "show generated tasks" toggle. A **mapped task** is one
    node with a summary (e.g. `120 success · 3 Unavailable · 2 skipped`); clicking it lists the
    instances.
  - **Task details panel** (on clicking a node or row), with tabs: **Attempts** (try number,
    status, duration, worker, peak memory, error summary; rejections with each worker's
    reason), **Output** (the outcome; a preview of the output as pretty JSON, cut off after a
    size limit, with a download link), **Cleanup** (only if the task has one: its attempts,
    result, duration, the registered handles, masked like everything else), **Logs** (the
    task's log with live tail; the cleanup's log on its own sub-tab).
  - **Resources:** a **Resources panel** in the run view (name, setup task, users, state:
    `waiting for users` / `cleaning_up` / `cleaned` / `cleanup_failed` / `not needed`, the
    cleanup's attempts and logs); **selecting a resource highlights its setup and users in the
    graph** and dims the rest. Not chosen: drawing bands around users (hard to lay out well in
    SVG).
  - **Run summary:** the result with a **one-line reason** from the run result rule (e.g.
    `failed: task load failed (timed out); cleanup of export failed`), and the **command
    history** (the audit trail: who triggered, cancelled or paused, when, applied or rejected).
  - **History and filters:** the runs list filters by result and date; a **per-task history
    page** shows one task across runs (state, outcome, cleanup, duration) with **filters by
    outcome** (e.g. runs where `fetch_primary` returned `Unavailable`).
- **Authentication: as little as possible in schedr.** Access is normally decided in front of
  schedr (e.g. Istio with Keycloak at the ingress, namespace policies inside); schedr doesn't
  repeat it. Modes (`[webserver.auth] mode`):

  | Mode | How it works | Fits |
  |---|---|---|
  | **`none`** | schedr trusts whoever reaches it | behind a mesh that authenticates (the main deployment: Istio + Keycloak, the whole namespace trusted); standalone (localhost) |
  | **`proxy_header`** | an SSO proxy in front (`oauth2-proxy`, OpenShift `oauth-proxy`, …) logs users in and passes `X-Forwarded-User`; schedr trusts it | Kubernetes without a mesh, with any identity provider |
  | **`basic`** | HTTP Basic against users in the configuration; bcrypt hashes from an environment variable or a mounted Secret, never the config file | small setups without an identity provider (a VM, Docker Compose, a lab server) |

  - **No roles:** an authenticated user can do everything (trigger, cancel, drain, …);
    restricting that is the job of whatever is in front.
  - **The user name is recorded for audit** on every command row and shown in the UI: the
    header in `proxy_header`, the login in `basic`, and in `none` an optional
    `identity_header` (e.g. `x-forwarded-user`, set by Istio from the Keycloak token). It is
    **never used to allow or deny** anything.
  - **Safety rules at startup:** `proxy_header` listens only on a loopback address unless
    overridden explicitly (anything reaching it around the proxy could forge the header);
    `basic` refuses plain HTTP on a non-loopback address unless `behind_tls_proxy = true`
    (passwords would cross the network in the clear).
  - **Not built** (possibly later, for deployments that need them): `jwt` (schedr validating
    OIDC tokens itself), roles mapped from groups, per-DAG permissions (Airflow's
    `access_control` is already kept as metadata), schedr-issued API tokens, user management
    in the UI.
- **Times and time zones: the server works only in UTC; people see UTC and local.**
  - **Server side:** the database, the scheduler, the API, JSON and schedr's own logs use
    UTC. Times are **always stored with their zone** even though it is always UTC
    (`timestamptz` in Postgres; ISO 8601 with `Z` in SQLite), never as naive timestamps.
    Cron expressions are evaluated in their schedule's zone and converted to UTC at once.
  - **Every time shown to a person appears twice, UTC and local**, e.g.
    `01:00 UTC · 21:00 EDT`. "Local" is: in the **UI** the browser's zone (read by a few lines
    of inline JavaScript into a cookie; until then only UTC, labelled); in the **CLI** the
    machine's zone (JSON output: UTC only); in **email notifications**, which have no viewer,
    the **DAG's schedule zone**; in the **task log viewer**, the browser's zone next to the UTC
    written in the log. **Webhooks:** UTC only (ISO 8601), plus the DAG's zone name as a field.
  - The schedule's zone is shown with the cron expression on the DAG's page (`0 3 * * *`
    Europe/Warsaw), not as a third display zone.
  - **Input** (`--for`, `--from` / `--to`, the UI's date fields): without an offset it is
    **UTC, with a warning** that it was interpreted as UTC; any explicit offset is accepted.
    The UI's date field shows the entered time in both zones before confirming.
- **Live updates through VPNs and HTTP proxies:** SSE responses carry `Cache-Control: no-cache`
  and `X-Accel-Buffering: no`; since some proxies buffer `text/event-stream` anyway, the
  browser **falls back to polling** (every few seconds) when no event, not even a keep-alive,
  has arrived for ~30 s.

### Task logs

- Task logs are **stored on the worker's local disk** and **served by the worker itself** over
  HTTP (a small endpoint inside the worker agent, including live tail of running tasks).
- What backs the log directory (volume type, StatefulSet, …) is a deployment concern, outside
  schedr: the worker just serves local files.
- Each task attempt records which worker ran it, so its logs can be found.
- The **webserver proxies** log requests to the worker; the browser never talks to workers
  directly.
- The worker's log endpoint accepts only **short-lived tokens signed with a shared secret**
  (with `tls.mode = "app"`; in `mesh` mode none, see Master–worker protocol).
- The worker deletes logs **after N days or beyond a size limit** (configurable).
- When a log can't be fetched (worker offline, file deleted, …), the UI states clearly that
  **the log file is unavailable at this location** (e.g. it may have been deleted).

### DAG code delivery

- schedr reads DAG code from **directories on the worker's filesystem**. How the code gets
  there is a deployment concern, outside schedr.
- Currently: **git-sync** sidecar. **Shared volumes** must also work, and so must other ways of
  filling the directories (e.g. code baked into the image).
- The directories are listed in **`SCHEDR_DAG_PATH`**, using the usual `PATH` syntax
  (colon-separated).
- Each directory is scanned **recursively**, so git submodules inside a DAG repository work.
- **Every `.py` file is parsed**, unless excluded by an ignore file.
- Ignore files are named **`.schedrignore`**, use **gitignore syntax**, and may appear in any
  subdirectory (like `.gitignore`). Airflow's `.airflowignore` files are **not** honoured.
- Only DAGs defined with **schedr's own APIs** (the native API or a compatibility layer such
  as `schedr.airflow`) are recognized.
- A `.py` file that defines no DAG is **ignored silently** (helper modules are common).
  Parsing imports the file, so its top-level code runs on every parse; helper modules with
  heavy import-time work should be ignored or refactored. This will be documented.
- **New DAGs are paused** when first found; a user unpauses them from the UI or CLI.
- **Removed DAGs disappear.** When a DAG's file is deleted (or no longer defines that DAG), the
  DAG is removed from the web UI and from all listings, and its schedule stops.
  - **When a DAG counts as removed:** only when a parse of a **newer hash** of its root no
    longer contains it. A worker that simply hasn't synced yet (rejection 6, DAG file missing)
    never removes a DAG.
  - **Runs in progress:** attempts already running finish; tasks not started yet are
    cancelled.
  - **History** stays in the database, hidden. If a DAG with the same ID appears again, its
    history shows again, until retention deletes it (see State storage: Retention).

### Plain script delivery

- Plain Python and Bash scripts live in directories listed in **`SCHEDR_SCRIPT_PATH`**, separate
  from `SCHEDR_DAG_PATH`. Files there are **never imported or parsed** as Python.
- `SCHEDR_SCRIPT_PATH` is a normal configuration key (`SCHEDR_CONFIG__SCRIPTS__PATH`) with
  `SCHEDR_SCRIPT_PATH` as a **short alias**. Same `PATH` syntax as `SCHEDR_DAG_PATH`.
- Scanned **recursively**; `.schedrignore` files apply, as in `SCHEDR_DAG_PATH`.
- A script is recognized **only if a script configuration file declares it**. Script
  configuration files are **TOML** files named **`schedr.toml`**, with predefined keys, and
  must themselves be inside `SCHEDR_SCRIPT_PATH`.
- There can be **many `schedr.toml` files**, **at most one per directory**.
- One `schedr.toml` can declare **several scripts**, and can declare **the same script more
  than once** (e.g. with different schedules).
- Paths in a `schedr.toml` are **relative to that file**. **`..` is not supported**, so a
  configuration file can only refer to scripts in its own directory or below. **Absolute paths
  are rejected.**
- Several `schedr.toml` files (e.g. a parent and a subdirectory's) **may declare the same script
  file**. Each declaration is independent and creates its own job/task.
- **Symlinks are allowed**, including ones pointing outside the directory. This is a deliberate
  escape hatch.
- Change detection and content hash work **the same as for DAG code** (polling, settle time,
  content hash).
- Internally every job is a DAG: a declared script becomes a **one-task DAG**.
- Every declaration in a `schedr.toml` has a **name**, unique within that file (a TOML table
  key, e.g. `[jobs.report_daily]`).
- **DAG operators refer to a declared script** (via a dedicated operator,
  **`ScriptOperator`**, an ordinary class in schedr's Python API) by the location of the
  `schedr.toml` that declares it and the declaration's **name**. The location is root-relative
  (see Script roots) or absolute.
- **Script roots** make references portable between environments (cluster, local
  development, pods with different mount paths):
  - **Every `SCHEDR_SCRIPT_PATH` entry is a root**, named by default after its last path
    component (`/git/scripts/etl-repo` → `etl-repo`).
  - The configuration file can **rename** an entry; the path must be one of the
    `SCHEDR_SCRIPT_PATH` entries (renaming never adds directories to scan):

    ```toml
    [scripts.roots]
    etl = "/git/scripts/etl-repo"
    ```

  - **Nested entries** in `SCHEDR_SCRIPT_PATH` (one inside another) are a **startup error**.
  - **Duplicate root names** are a **startup error**, unless resolved by renaming in
    `[scripts.roots]`.
  - Every `schedr.toml` is therefore under exactly one root. Script references and standalone
    jobs are stored as **(root name, path of `schedr.toml` relative to the root, job name)**,
    e.g. `etl-repo:team_a/schedr.toml#report_weekly`, and **resolved by the worker that runs
    the task**. This is also the job's stable ID in the UI, CLI and run history.
  - Absolute paths to a `schedr.toml` remain accepted in `ScriptOperator` references.

#### `schedr.toml` keys

One table per job under `[jobs.<name>]`:

```toml
[jobs.report_daily]
script = "reports/daily.py"        # required; relative to this file, no "..", no absolute
interpreter = "etl-venv"           # required; an [interpreters] entry
args = ["--region", "eu"]          # argument list, never a shell string
env = { REPORT_MODE = "full", DB_PASSWORD = { secret = "etl/db#password" } }   # env var <- secret
secrets = ["etl/api#token"]        # readable only with `schedr task-ctx secret get` (not in env)
working_dir = "reports"            # relative to this file; default: this file's directory
schedule = "0 3 * * *"             # optional; cron or preset ("@daily")
every = "1h"                       # alternative to schedule
once = 2026-12-24T23:00:00         # alternative to schedule: one fire time (native Once)
timezone = "Europe/Warsaw"         # default UTC
start = 2026-01-01                 # TOML date/datetime
end = 2026-12-31                   # optional; last scheduled run at or before it
initial_since = 2020-01-01         # optional; `since` of the first run (native initial_since)
catchup = false                    # default false
max_active_runs = 1                # default 1 (no overlapping runs)
notify = ["ops-slack"]             # server channels; or { "ops-slack" = ["RUN_FAILED", "RUN_RECOVERED"] }
queue = "default"
retries = 2
retry_delay = "5m"
timeout = "30m"
max_output_size = "4MiB"
description = "Daily report"
tags = ["reports"]
cleanup_handles = ["ExportJob"]    # optional; handle types `schedr task-ctx cleanup add` accepts

[jobs.report_daily.outcomes]       # optional; same meaning as an Outcome enum
SUCCESS = 0
NO_DATA = 3

[jobs.report_daily.params]         # optional; run parameters with defaults (see Run parameters)
region = "eu"

[jobs.report_daily.cleanup]        # optional; per-task cleanup (see Cleanup and resources)
script = "reports/undo.sh"         # required; same rules as the job's script
interpreter = "bash"               # required
args = ["--region", "eu"]
timeout = "5m"                     # native cleanup_timeout; default: the server default
retries = 3                        # the cleanup's own retries
retry_delay = "30s"
anywhere = false                   # native cleanup_anywhere
```

- Same semantics as the native API (schedules, run time semantics, defaults, outcomes, output
  limits, `ProcessResult` output). **Unknown keys are an error.**
- Durations as `"30s"`, `"5m"`, `"1h30m"`; sizes as `"4MiB"` (same format as the server
  configuration).
- **At most one of `schedule`, `every`, `once`.**
- **`notify`** names **server channels** only, as a list (default events) or a table choosing
  events per channel (event names as in `Event`). DAG-defined `EmailChannel`s are Python
  objects, so they exist only in DAG files.
- **No concurrency keys** (`max_active_tasks`, `max_active`, `max_active_per_run`): a job is a
  one-task DAG, so `max_active_runs` covers it.
- **`schedule` is optional.** With a schedule (or `every` / `once`), the declaration is a
  **standalone job**: a one-task DAG, paused when first found like any new DAG, whose ID is its
  reference (e.g. `etl-repo:team_a/schedr.toml#report_daily`); it can also be used from DAGs.
  Without one, it is only used from DAGs or triggered manually.
- **To use a script in a DAG, it must be declared** in a `schedr.toml` inside
  `SCHEDR_SCRIPT_PATH`, typically **without a schedule**. There is no way to run an undeclared
  script file.
- **Run information** (no t-strings or other templating in TOML) is passed as **environment
  variables**, set by the agent for every `script` and `command` task: `SCHEDR_DAG_ID`,
  `SCHEDR_RUN_ID`, `SCHEDR_RUN_KIND`, `SCHEDR_TASK_ID`, `SCHEDR_MAP_INDEX` (mapped tasks
  only), `SCHEDR_TRY_NUMBER`, `SCHEDR_PLANNED_FOR`, `SCHEDR_SINCE` (times in ISO 8601).

#### `ScriptOperator`

```python
ScriptOperator("report", job="etl:team_a/schedr.toml#report_daily",
               args=["--day", t"{dag_ctx.run.planned_for}"])
```

- Refers to a declaration by its **reference string**, the same format as the job ID (absolute
  `"/path/schedr.toml#name"` also accepted). **Checked at parse time** (file exists, job
  declared); otherwise an import error.
- Takes **everything except the DAG-level keys** (`schedule`, `every`, `once`, `timezone`,
  `start`, `end`, `initial_since`, `catchup`, `max_active_runs`, `notify`, `params`) and can
  **override** `args`, `env`, `secrets`, `queue`, `retries`, `retry_delay`, `timeout`,
  `max_output_size`, `outcomes` (an `Outcome` enum).
- In the operator, `args` and `env` values may be t-strings / output references (rendered by
  the agent; argument lists, so no shell quoting needed).
- It is the **only** script operator: there is no operator that runs a script file directly.
- Also exposed in **`schedr.airflow`**, so migrated DAGs can call declared scripts without
  mixing APIs, at **`schedr.airflow.providers.schedr.operators.script`** (schedr as one more
  "provider", following Airflow's `providers.<provider>.operators.<module>` convention; it can
  never collide with a real Airflow module).

#### `SchedrScriptOperator` (Airflow layer)

The native `ScriptOperator` stays as it is. A **separate** Airflow-like class,
`SchedrScriptOperator` (provider-prefixed, like `SnowflakeOperator`), parametrizes a
`ScriptOperator` and adapts its result so it behaves like a standard Airflow operator:

```python
from schedr.airflow.providers.schedr.operators.script import SchedrScriptOperator

SchedrScriptOperator(
    task_id="report",
    job="etl:team_a/schedr.toml#report_daily",   # forwarded to ScriptOperator
    args=["--day", t"{ds}"],                     # forwarded (overrides)
    env={"MODE": "full"},                        # forwarded (overrides)
    output=OutputMode.LAST_LINE,                 # how stdout becomes .output
    skip_on_exit_code=99,
    retries=2, execution_timeout=timedelta(minutes=30), default_args=..., trigger_rule=...,
)
```

- **Name:** `SchedrScriptOperator` (provider-prefixed, like `SnowflakeOperator`), so it isn't
  confused with the native `ScriptOperator`.
- **Parameters:** the `ScriptOperator` overrides (`job`, `args`, `env`, `secrets`, `queue`,
  `timeout`, `max_output_size`, `outcomes`) are forwarded; standard `BaseOperator` arguments
  (`task_id`, `retries`, `retry_delay`, `execution_timeout`, `trigger_rule`, `default_args`,
  `doc_md`, …) work as on every Airflow-layer operator.
- **Output parsing, built in (no user code):** `.output` comes from the `ProcessResult` via
  a fixed set of modes: `LAST_LINE` (default, like `BashOperator`), `STDOUT` (all of it),
  `NONE`. **`JSON` and `JSON_LAST_LINE` are dropped**: they parse stdout, which the
  native API doesn't do; no migration cost, since no Airflow DAG uses this schedr-only class;
  for structured output, a downstream Python task parses `.stdout`. `LAST_LINE` is produced
  like `BashOperator`'s `.output` (a hidden native task, see Thin translating classes). A
  user-supplied Python callable
  (like Airflow's `output_processor`) is not supported.
- **Reporting like Airflow:** `skip_on_exit_code` (default 99) skips the downstream tasks, as
  for `BashOperator` (translated onto outcomes: see Airflow compatibility, `skip_on_exit_code`);
  any other failing exit code fails the task with an Airflow-style error summary ("script
  returned non-zero exit code N" plus the tail of stderr) shown in the UI.

### Master–worker protocol

- **gRPC over HTTP/2** (`tonic`), typed with Protobuf. Internal only (workers ↔ master).
- **Workers always open the connection** to the master's Service; the master never connects
  to workers. (Log requests are separate: the webserver connects to workers.)
- Each worker keeps **one long-lived bidirectional stream** for control messages. Over it the
  master can send work, cancellations, etc. immediately, even though the worker opened it.
- **Plain request/response (unary) calls** alongside the stream for bulky one-off messages
  (e.g. parse results with serialized DAG JSON).
- **Cancellation grace time** (between SIGTERM and SIGKILL): owned by the master's
  configuration, **default 30 s**. If a task finishes just as a cancel arrives, **the finished
  result wins**; the UI notes that the cancel came too late.
- **Delivery is at least once.** Any message may arrive more than once; the attempt ID makes
  repeats harmless (e.g. a second `Finished` for the same attempt is acknowledged and ignored).
- **Protocol versioning:** `major.minor`. The master accepts workers with the **same major**
  and an **equal or lower minor** version. **Upgrade order: master first, then workers**, so
  rolling upgrades work and an old worker is never sent something it doesn't understand.
- **Standalone mode uses the same gRPC protocol** (over localhost) as the distributed setup,
  not a separate in-process transport: less code, same code paths. In `standalone` mode
  **TLS is disabled** (everything listens on localhost).
- **Mutual TLS from the start**: master and workers verify each other's certificates.
  Certificates are files mounted from Kubernetes Secrets (how they are issued is a deployment
  concern).
- **`tls.mode = "app" | "mesh"`.** `app` (the **default**): schedr's own mTLS on every internal
  link, as below, plus signed tokens on the workers' log endpoint. `mesh`: behind a service
  mesh that provides mTLS between pods (e.g. Istio), schedr sends plain traffic to its sidecar
  and does **no app-level mTLS and no log tokens**; the whole namespace is trusted (all schedr
  pods in one namespace; which other namespaces may reach it is controlled by the mesh). So
  schedr doesn't repeat what the mesh does. Long-lived streams (master ↔ worker, SSE) keep
  alive with heartbeats (10 s / 15 s), within Envoy's idle timeouts; no VirtualService
  timeouts on these routes.
- Each worker has a **fixed number of task slots**, set in its configuration. The master never
  assigns more concurrent tasks to a worker than it has slots.
- **Heartbeat interval, lease timeout and grace period are configurable**, with defaults.
- **Running tasks can be cancelled from the UI** (the master sends a cancel command to the
  worker over the stream).

- Defaults: **heartbeat every 10 s**; a worker with **no heartbeat for 60 s is considered
  lost**; **2 slots** per worker.
- **Cancellation** (UI and CLI): SIGTERM to the task's whole process group, then SIGKILL after
  a configurable timeout. The attempt ends as **cancelled** (not failed) and is **not retried**.
- **mTLS details:** mTLS replaces any shared token; certificates are **reloaded when the files
  change** (rotation without restart); the same mTLS setup is used for **every internal link**
  (master ↔ worker, webserver → worker logs). There is no webserver → master link (see Web
  UI and CLI: the webserver talks to the master through Postgres).

### Worker lifecycle

**Identity:**

- **Worker ID**: stable across restarts; from configuration, defaulting to the hostname (the
  pod name in Kubernetes). The worker's journal belongs to this ID.
- **Incarnation ID**: random UUID generated each time the agent process starts. Lets the master
  tell a **reconnection** (same incarnation, tasks probably still running) from a **restart**
  (new incarnation).
- A registration with a worker ID that already has a **live** stream is rejected. If the old
  stream is dead, the new incarnation takes over and the old incarnation's attempts are
  reconciled.

**States (master's view):** `ACTIVE` → `DRAINING` → `OFFLINE` (clean deregistration);
`ACTIVE` → `DISCONNECTED` (stream dropped) → back to `ACTIVE` on reconnect with the same
incarnation, or `LOST` after 60 s without a heartbeat (its attempts fail and are retried per
`retries`).

**Messages:**

1. **Register** (worker → master), on every connect: worker ID, incarnation ID, protocol and
   schedr versions, queues, slots, log endpoint address (pod IP from the Kubernetes downward
   API + port), content hashes per DAG/script root, the names of its configured secret
   storages (see Secret storage), and the journal summary (running attempts, unacknowledged
   results).
2. **Registered** (master → worker): timing settings (heartbeat interval, lease timeout, cancel
   timeout; **owned by the master's configuration** so all workers behave the same) and a
   decision per reported attempt: **continue**, **kill** (already abandoned/reassigned), or
   **ack** (result stored; remove from the journal).
3. **Heartbeat** (worker → master), every 10 s by default: free slots and running attempts.
   Kept small.
4. **CodeVersionChanged** (worker → master): sent when a content hash changes (after the settle
   time). A new hash makes the master create a parse job.
5. **Drain**:
   - On **SIGTERM**: the worker sends `Draining`; the master stops assigning to it; running
     tasks get until the termination grace period, then remaining ones are killed and reported
     as interrupted; the worker sends `Deregister` and exits.
   - On **admin request from the UI or CLI** (maintenance): the same, but the worker **stays
     running and idle** and can be resumed.

**Rules:**

- **When the master's connection drops, the worker keeps running its tasks**, reconnects with
  exponential backoff and jitter (capped at ~30 s), and journals events until it is
  re-registered.
- **Schedules missed while the master is down** are created when it comes back; with
  `catchup=False` only the **most recent** missed run is created.
- **Master downtime doesn't count against workers**: after the master starts, every worker's
  lease clock starts again from zero.
- A worker that was declared `LOST` and comes back is told to **kill** the attempts it still
  runs (they were already failed and retried).
- The worker's **liveness probe** (`/healthz` on the ops port, see Observability) checks only
  the agent's own health, **never** whether the master is reachable (otherwise a master outage
  would restart every worker and kill all running tasks). Workers need no readiness probe.
- `OFFLINE` and `LOST` workers **stay listed for the same period as log retention**, so old
  logs can still be located (the UI shows them as unavailable).

### Task assignment

- **Worker choice: spread.** Among `ACTIVE` workers serving the task's queue with a free slot,
  the master picks the one with **the most free slots**; ties go to the least recently
  assigned. Keeps load even and makes heavy tasks less likely to share a pod's memory.
- **No separate `Accepted` message.** Flow: `AssignTask` → `Started` (which means accepted) or
  `Rejected` → `Finished` → `FinishedAck`.
- **Queues** route tasks to groups of workers. A worker registers with the queues it serves; a
  task names one (`queue=`); the master assigns a task only to workers serving its queue.
  - There is a **default queue** named **`default`**. A worker with **no queues
    configured serves the default queue**; a task with no queue goes to the default queue.
  - A worker with queues configured serves **exactly those queues**. `default` is never added
    implicitly: `queues = ["large"]` serves only `large`; `queues = ["default", "large"]`
    serves both.
  - With identical worker images, queues are for e.g. different pod sizes, reserved capacity
    for critical DAGs, or network access restrictions; not for dependencies.
- **Order:** ready tasks run **oldest-ready first** (priorities and fairness between DAGs may
  come later). The assignment is **saved to Postgres before `AssignTask` is sent**.
- **Concurrency limits** (see Native API: `max_active_runs`, `max_active_tasks`, `max_active`,
  `max_active_per_run`): a ready task whose limit is reached stays **`queued`**, with the reason
  shown in the UI (e.g. "waiting: `max_active_per_run` (10) reached"). Limits are checked
  **when a task is assigned, in the same transaction** that records the assignment (two tasks
  can't both take the last place); they count tasks in `running` and `cleaning_up` (a cleanup
  still holds the slot) and assigned tasks not yet `Started`. When a place frees up, the order
  stays oldest-ready first among tasks whose limits allow them.
- `EmptyOperator` tasks and tasks skipped by branching are completed **by the master**, without
  a worker.
- **The master resolves data; the worker renders and executes.** The master supplies upstream
  outputs (as inputs), run information and placeholder values; the worker renders templates
  (the agent for commands, the Python runner for Python tasks) and runs the task.
  **Connections are fetched on the worker**; secrets never pass through the master.
- **No `Started` within ~10 s:** the assignment is taken back and the task requeued without
  using a retry. If the worker reports it later, it is told to kill it.
- **Retries are decided by the master.** An attempt interrupted by **draining** does **not** use
  a retry; one ended by a **lost worker or container crash does** (it may have been the task's
  own memory spike). **Failures certain to repeat are not retried** (e.g. a secret that is not
  found or not permitted, see Secret storage); the master learns this from the error kind in
  `Finished`.
- **Parse jobs** are assigned with `AssignParse` over the stream; the result (DAG JSON, import
  errors per file) is sent with a unary call. Each worker has **one dedicated parse slot**, not
  counted against task slots, with its own memory budget.
- **Task specification** (`AssignTask`) contains everything needed to run one attempt:
  - Identity: attempt ID; DAG ID, run ID, task ID, map index, try number.
  - Kind and what to run: `python` (DAG file reference + task ID), `command` (command
    template), `script` (script reference: root, `schedr.toml` path, job name).
  - Expected code hash (the worker reports a mismatch).
  - Inputs: upstream outputs used by the task, and run information.
  - Execution settings: environment variables, working directory, interpreter (scripts),
    `execution_timeout`, cancel grace time, memory budget, `setrlimit` opt-in.
- **Input size:** small inputs travel inside `AssignTask`; when the total exceeds a threshold
  (default 4 MB, gRPC's default message limit) the worker downloads them separately in chunks
  with a unary call. A configurable **cap on total inputs per task** (default 64 MB) makes the
  task fail clearly instead of overloading anything.
- **Attempt statuses** (how one try ended):

  | Status               | Meaning                                                   | Decided by |
  |----------------------|-----------------------------------------------------------|------------|
  | `success`            | Exit code 0, or the Python function returned normally     | Worker     |
  | `failed`             | Non-zero exit code or unhandled exception (incl. `ImportError`) | Worker |
  | `timed_out`          | Exceeded `execution_timeout`; killed by the worker        | Worker     |
  | `oom_killed`         | Killed by the memory watchdog for exceeding its budget    | Worker     |
  | `cancelled`          | Cancelled by a user                                       | Worker     |
  | `interrupted`        | Killed when a draining worker's grace period ended        | Worker     |
  | `lost`               | The worker disappeared                                    | Master     |
  | `assignment_expired` | No `Started` within ~10 s                                 | Master     |

- **`Finished` contains:** the attempt status, the exit code or signal, the **(output, outcome)
  pair** on success (see Task results and outcomes; for `command` / `script` tasks the output is a
  `ProcessResult` with stdout and stderr), the duration, the peak memory, and an **error kind** for
  failures schedr itself recognises (e.g. `secret_unavailable`, `secret_denied`,
  `secret_not_found`).
  - When stdout/stderr exceed the output size limit, the output keeps **the last part (up to the
    limit) and is marked `truncated`**. A downstream task that reads a truncated output **gets
    an error**, so partial data is never used silently. The full stdout/stderr are always in
    the task's log file.
  - **Inline commands** (`command` kind) produce their output **the same way as scripts** (a
    `ProcessResult`). The Airflow layer exposes the **last line of stdout** through `.output` for
    compatibility.
  - Failed Python tasks include a structured **error summary**: exception type, message and the
    last lines of the traceback, so the UI can show it without opening the log.
- **Run states:** **`queued`** (created, waiting for `max_active_runs`; only manual and backfill
  runs wait, see Runs: manual runs, backfill and pausing), **`running`**, and the results
  **`success`**, **`failed`**, **`cancelled`** (set once every task and cleanup has finished,
  by the run result rule, see Outcomes, failures and joins: Run result).
- **Task instance states** (where the task stands in its DAG run, decided by the master from
  attempt statuses, retries and the DAG structure): `scheduled`, `queued`, `unschedulable`,
  `running`, **`cleaning_up`** (the task's process has ended and its per-task cleanup is
  running; the slot is still taken and downstream tasks wait), `up_for_retry`, `success`,
  `failed`, `cancelled`, `skipped`, `upstream_failed`.
  - Two **attributes** next to the state instead of more states: the **outcome** (on success)
    and the **cleanup result** (`ok`, `failed`, `didn't run: worker lost`), shown together, e.g.
    "success · `Unavailable` · cleanup failed".
  - **Resources** have their own instance per run, with their own states: `waiting for users`,
    `cleaning_up`, `cleaned`, `cleanup_failed`, `not needed` (the setup didn't succeed).
- **Plain scripts are one kind of task, whatever the language.** A Bash script is treated
  exactly like a Python script, only run with a different interpreter. Task kinds:
  - `python`: a Python function from a DAG file (the runner imports the DAG and calls it).
  - `command`: an inline command (e.g. `BashOperator`'s `bash_command`).
  - `script`: a script file declared in a `schedr.toml`, run with an **interpreter** (Python,
    Bash, …).
- **Interpreters are named in the configuration file**, in an `[interpreters]` section, so each
  worker image can map names to its own paths and virtualenvs:

  ```toml
  [interpreters]
  python = "/usr/bin/python3"
  bash = "/bin/bash"
  etl-venv = "/opt/venvs/etl/bin/python"
  ```

- A `schedr.toml` declaration picks one with an **`interpreter` key** naming an entry from
  `[interpreters]`. The key is **required**: there is no default from the file extension.
- **Shebang lines are ignored**; only the `interpreter` key counts.
- If the named interpreter isn't defined on the worker, the worker **rejects the task with a
  clear reason** (e.g. "interpreter `etl-venv` not defined on worker-1").

### Task rejections

A worker may **reject** an assigned task (`Rejected` instead of `Started`). A rejection never
uses up a retry. Reasons and how the master reacts:

| # | Reason                     | Example                                                        | Reaction |
|---|----------------------------|----------------------------------------------------------------|----------|
| 1 | No free slot               | Master's view of slots momentarily out of date                 | Temporary: requeue immediately, any worker |
| 2 | Worker draining            | Drain arrived just as the task was assigned                    | Temporary: requeue immediately, any worker |
| 3 | Interpreter not defined    | `etl-venv` not in this worker's `[interpreters]`               | Worker-specific: don't offer this attempt to this worker again |
| 4 | Script root unknown        | Root not in this worker's `SCHEDR_SCRIPT_PATH`                 | Worker-specific |
| 5 | Script reference not found | `schedr.toml`, job name or script file missing                 | Code not there yet: retry after a short delay, any worker |
| 6 | DAG file missing           | The `python` task's DAG file isn't on this worker              | Code not there yet |
| 7 | Memory budget too large    | Task asks for more than this worker's pod limit allows         | Worker-specific |
| 8 | Disk not writable          | Journal or log directory full or read-only                     | Worker-specific; also a warning on the worker in the UI |
| 9 | Invalid specification      | Message can't be decoded, unknown task kind                    | Worker-specific (bug or version mismatch) |

- **Worker-specific** reasons: the master tries other eligible workers. When every eligible
  worker has rejected the attempt, the task instance becomes **unschedulable**, shown in the
  UI with each worker's reason. It is tried again only when the set of workers changes or a
  user retries it (`schedr tasks retry`, or the UI). Retry is allowed **only for
  `unschedulable` tasks of a run not yet finished**; it doesn't conflict with "no clear", which
  is about re-running finished runs.
- **Code not there yet** (e.g. git-sync on that pod hasn't pulled the new commit): retry after
  roughly the sync interval plus settle time, on any worker. Marked **unschedulable** only if
  still missing everywhere after a configurable wait.
- An **import error** in a DAG file is **not** a rejection: the agent can't detect it without
  starting Python, so it is an ordinary task **failure** with the `ImportError` in the error
  summary.

### Worker memory protection and crash recovery

All tasks on a worker share the pod's memory limit. On cgroup v2 (current OpenShift /
Kubernetes 1.28+), an out-of-memory event kills **every process in the container**: all tasks
and the worker agent. schedr therefore uses both:

- **Memory watchdog** in the worker agent: about once a second it measures each task's memory
  (summed over the task's process tree, from `/proc`) and **kills only the task that exceeds
  its memory budget**, reporting the exact reason (e.g. "exceeded memory budget 2 GiB").
  Needs no privileges; negligible overhead.
- **Journal on the worker's disk** of the attempts it is running and of finished results not
  yet acknowledged by the master. If the container dies anyway (memory spike faster than the
  watchdog, node failure, eviction), the restarted agent reads the journal and reports those
  attempts as **interrupted: worker restarted (possibly out of memory)**; they are retried per
  `retries`. Finished results saved on disk are not lost.

Watchdog budget: by default **(pod memory limit − reserve for the agent, about 10 %) ÷ slots**.
The limit is read from `/sys/fs/cgroup/memory.max` (no privileges needed). Configurable per
worker, later per task via the API; the check interval is configurable.

Optional **hard cap per task with `setrlimit`** (`RLIMIT_DATA` / `RLIMIT_AS`): **opt-in per
task, off by default**. It is crude (counts reserved, not used, memory; breaks programs that
reserve a lot up front), but fails the allocation inside the task immediately.

Per-task cgroups are ruled out: they need write access to the cgroup filesystem, which
OpenShift's restricted security policy doesn't grant.

### Worker environment

- **All workers use the same image.** There are no worker pools with different images, so
  any worker can parse any DAG and run any task.
- Tasks and DAG files can use **only the Python libraries installed in the worker
  environment**. Any additional code must come through the DAG or script roots (e.g. a git
  submodule, or a symlink).
- **Task processes get a clean environment by default.** They do **not** inherit the worker
  agent's environment variables. A task process gets only: a small allow-list (`PATH`, `HOME`,
  `LANG`, `LC_*`, `TZ`, `TMPDIR`), the `SCHEDR_*` run variables, and the task's own `env`
  (with secret placeholders filled in; declared `secrets=` are not in the environment, see
  Secret storage). The agent's own variables (configuration, certificate paths, Vault auth, anything
  from the pod spec) are passed only if listed in the worker configuration
  (`[worker] pass_env = ["HTTP_PROXY", ...]`). In the Airflow layer, `append_env=True`
  (`BashOperator`) means "add the allow-listed worker variables", never the whole agent
  environment.
- **Each DAG root is added to `sys.path`** when parsing DAGs and running Python tasks, so helper
  modules and code from submodules inside a root can be imported.

### Expected workload

A **mix**: mainly a **synchronization scheduler** (many short tasks), plus some heavier or
longer tasks (downloads, longer-running commands). **No big-data workloads.** Starting a
fresh Python process per task is acceptable; warm Python worker processes are not needed.
Tasks run on **long-running workers** (not pod-per-task) because schedr can't count on
permission to create pods.

### Rust/Python integration

- **Separate processes.** Rust and Python never share a process as a way of integrating
  schedr: the Rust worker agent starts Python as a subprocess and they talk over a local
  channel (details below).
- **The master never runs Python** (and neither does the webserver).
- **DAG parsing** runs in a Python subprocess on a worker (`AssignParse`, parse slot).
- **Python tasks** run in their own Python processes (see How Python tasks are started).
- **Python talks only to its local worker agent**; only the agent talks to the master.
- **Rejected as the integration mechanism:**
  - Embedding Python in the Rust binary (PyO3): user-code crashes would take the scheduler or
    agent down, and one Python environment would be forced on every DAG.
  - A Rust extension module that connects Python to the scheduler.
- **The Python runner** is a module of the `schedr` package, started by the agent as
  `<interpreter> -m schedr.runner parse`, `... task` or `... cleanup` (`-m`, so it works with
  any configured interpreter executable, whatever virtualenv it belongs to). Only the mode and
  the socket path are passed on the command line / environment; everything else comes from the
  agent over the channel, so no secrets appear in the command line.
  - `parse`: gets the root and file list from the agent, puts every DAG root on `sys.path`,
    imports the files one at a time (native API calls only record DAGs), and sends back the
    DAG JSON and per-file import errors.
  - `task`: gets the task specification from the agent, imports the DAG file (latest code on
    disk), finds the task by ID, fills in t-strings (secrets fetched by the agent), calls the
    function, and sends back the output or a structured error summary; exit code 0 or non-zero.
    Python resource setups (`@resource`) run in this mode too: a setup is a task.
  - `cleanup`: like `task`, for a per-task cleanup (gets the task's inputs and the attempt's
    handles) or a Python resource cleanup (`@cleanup`, gets the setup's output).
  - The runner does **not** decide retries, enforce timeouts, watch memory or write log files.
    The agent supervises it from outside (process group, stdout/stderr to the log file,
    watchdog, SIGTERM/SIGKILL).
- **Interpreter for parsing and Python tasks:** a configuration key naming an
  `[interpreters]` entry, **default `python`**:

  ```toml
  [dags]
  interpreter = "etl-venv"
  ```

  - The **same interpreter** is used for parsing and for tasks, so a DAG can't parse in one
    environment and fail to import in another.
  - That environment must have **`schedr` installed at the same version as the agent**, plus
    everything the DAG files import.
  - Plain script tasks don't use the runner: they run with the interpreter from their
    `schedr.toml` declaration, which doesn't need `schedr` installed.
  - Possible later: an interpreter per DAG root (in `[dags.roots]`).
- **Agent ↔ runner channel: a Unix domain socket** per runner process, created by the agent in
  a private directory; its path is passed in **`SCHEDR_AGENT_SOCKET`**. Files are not enough
  because a running task can ask for things not known in advance (`Connection.get(conn_id)`,
  secrets). Messages:
  - task specification and inputs (agent → runner, on request);
  - secret / connection requests and answers;
  - outputs, or a structured error summary (runner → agent);
  - for parsing: per-file progress and results (DAG JSON, import errors).
- **Encoding on the socket: length-prefixed JSON.** Simple to write from Python with no
  dependencies; messages are small. (Protobuf would add a dependency to the Python package.)
- **Parsing granularity:** **one parse process per parse job** (a root whose hash changed),
  importing files one at a time and reporting progress per file. A **per-file timeout**
  (configurable) is enforced by the agent: on timeout it kills the process, records an import
  error ("timed out") for that file and starts a new process for the remaining files. A fresh
  process per parse job avoids stale cached modules.
- **Trust boundary: a worker is one trust domain.** All tasks on a worker run under the same
  UID, so they are **not isolated from each other** (a task can read another task's
  environment or files). Teams that must be isolated get separate queues and workers. This is
  stated in the documentation.
- **Shared model and schemas.** **Rust types are the reference.** Protobuf for gRPC. A **JSON
  Schema generated from the Rust types** (`schemars`) describes the DAG JSON and the agent ↔
  runner messages; it is committed to the repository and **CI fails if it is out of date**.
  Python tests validate what Python produces against it. Python-side types are **written by
  hand** (no code generation) for now.
- **Compiled Rust extensions as ordinary libraries are fine.** A Python package (a user's
  dependency, or possibly schedr's own Python package) may contain a compiled Rust extension
  used as a library inside the Python process. That is not an integration channel with the
  scheduler; it just runs inside the task's or parser's own process.

### How Python tasks are started

- **Now: a fresh process per task.** The agent starts a new Python process for every task
  attempt; it imports the DAG file, runs one task and exits. A straight `1 >> 2 >> 3 >> 4 >> 5`
  DAG starts five processes, possibly on five workers. Main cost: re-importing the DAG file
  and its libraries for each task.
- **M2.5 (designed, see Roadmap): a fork server** as an optional start method. A parent process
  imports libraries once and forks one child per task attempt, so each task is still its own process
  (the memory watchdog, timeouts, signal-based cancellation and the journal work unchanged). The
  runner and the agent ↔ runner protocol must not depend on how the process was started, so this can
  be added without changing them.
  - **One fork server per worker and interpreter** (each `[interpreters]` entry may be a
    different virtualenv, and a child must come from a parent with the same environment).
  - It preloads only a **configured list of libraries**, **never DAG code**; each task child
    imports its DAG file itself. So the fork server never goes stale on a code push (libraries
    change only with the image, which restarts the pod), its memory doesn't grow with the
    number of DAGs, and no user top-level code runs before `fork()`.
  - The memory watchdog must measure a forked task's **own** memory (PSS/USS from
    `/proc/<pid>/smaps_rollup`), not RSS, so memory shared with the parent isn't counted
    against every task.
  - Not planned: a fork server per DAG (memory multiplied by the number of DAGs, restarts on
    every push to its root, user code run before `fork()`).
  - **Configuration: per interpreter, off by default.** An `[interpreters]` entry may be a
    table; a plain string means spawn:

    ```toml
    [interpreters]
    python = "/usr/bin/python3"

    [interpreters.etl-venv]
    path = "/opt/venvs/etl/bin/python"
    fork_server = true
    preload = ["schedr", "pandas", "sqlalchemy"]
    ```

  - **What uses it:** Python tasks, Python cleanups and resource setups (runner `task` /
    `cleanup`), and **parse jobs** (a fresh child per parse job imports the DAG files; the
    parent never sees DAG code). **`script` tasks always spawn**, even with a Python
    interpreter (a plain script expects a fresh interpreter: `__main__`, `sys.argv`, `atexit`;
    and needs no `schedr`); `command` tasks aren't Python. A task can opt out with
    **`@task(fresh_process=True)`** (also in `TaskDefaults`), for code that misbehaves after a
    fork.
  - **Process model:** the agent starts `<interpreter> -m schedr.runner forkserver` **when the
    worker starts** (the first task is fast; a failing preload shows at once). Per attempt the
    agent asks it to fork; the child calls `setsid()` (its own process group, as with spawn, so
    timeouts, cancellation and the watchdog are unchanged), gets the **log file descriptors
    passed by the agent** over the Unix socket as stdout / stderr, replaces its whole
    environment with the task's clean environment, changes to the working directory, closes
    inherited descriptors, and runs the normal runner `task` code with **its own** connection to
    `SCHEDR_AGENT_SOCKET`. The agent is a **child subreaper** (`PR_SET_CHILD_SUBREAPER`) and
    watches each child through a **`pidfd`**: signals reach the right process even if PIDs are
    reused, and children of a fork server that dies are re-parented to the agent and keep
    running.
  - **Safety checks:** after preloading, the parent must be **single-threaded** (checked in
    `/proc/self/task`; forking with threads, e.g. BLAS pools or database drivers, can deadlock
    the child). A second thread or a failing preload **disables the fork server for that
    interpreter** with a worker warning in the UI, and tasks **fall back to spawn**: a task never
    fails because of the fork server's setup. **Linux only:** on macOS `fork_server = true` is
    ignored with a warning (fork is unsafe there), like the memory watchdog.
  - **State after fork:** the child reseeds `random`, resets `logging` handlers, closes the
    parent's sockets and runs the libraries' `os.register_at_fork` handlers. The parent calls
    **`gc.freeze()`** after preloading, so the garbage collector doesn't copy shared pages in
    every child (keeps each task's PSS small).
  - **Crashes and versions:** a crashed fork server is restarted by the agent with backoff;
    running children are unaffected, and meanwhile tasks spawn. It reports its `schedr` version
    like any runner; a mismatch disables it. It can't go stale (libraries change only with the
    image, which restarts the pod).
  - **Visibility:** each attempt records its **start method** (`spawn` / `fork`), shown in the
    Attempts tab; metrics: fork server up / down per interpreter, process start time by start
    method, fallbacks to spawn by reason.
- **Rejected:**
  - Warm worker pools (long-lived Python processes reused across tasks): globals, leaked
    memory and old code carry over between tasks; the watchdog can't tell whose task used
    the memory.
  - One process per DAG run or per chain of tasks: breaks per-task retries, timeouts, memory
    budgets and queues.
  - Subinterpreters (`concurrent.interpreters`) and threads/asyncio in one process: one
    address space, so no per-task memory limit or signal-based cancellation; a C-extension
    crash kills every task.
  - Python embedded in the worker agent: user code could crash the agent and every task on
    the worker.
  - Checkpoint/restore (CRIU): needs privileges OpenShift's restricted policy doesn't grant.

### In-task CLI: `schedr task-ctx` and `schedr dag-ctx`

Commands that only work **inside a running task** (they talk to the agent over
`SCHEDR_AGENT_SOCKET`), for `command` / `script` tasks, their cleanups and resource setups.
Python tasks use the Python API instead.

- **Namespace `schedr task-ctx`**, separate from the user CLI (listing DAGs, triggering runs) and
  the server roles and commands (`schedr master`, `schedr worker`, `schedr db migrate`), and
  **`schedr dag-ctx`** for the run's context (see Run parameters and the DAG context). Outside a
  task they fail with "must be run inside a schedr task".
- **Noun, then verb**, the same verbs everywhere: `add` / `list` for collections, `set` / `get`
  for single values.

  | Purpose | Command | Used in |
  |---|---|---|
  | Register a per-task cleanup handle | `schedr task-ctx cleanup add ExportJob job_id="$id"` | the task |
  | Read the handles | `schedr task-ctx cleanup list [ExportJob] [--field job_id]` | its cleanup |
  | Report the resource value | `schedr task-ctx resource set cluster_id="$id" size:=8` | a `CommandResource` / `ScriptResource` setup |
  | Read the resource value | `schedr task-ctx resource get [--field cluster_id]` | a command / script resource cleanup |
  | Read a declared secret | `schedr task-ctx secret get 'ci/deploy#token'` | any command / script task |
  | Read the run's parameters | `schedr dag-ctx params get [--field region]` | any command / script of the run |
  | Read the run information | `schedr dag-ctx run get [--field planned_for]` | any command / script of the run |

- **Field syntax** (from HTTPie): `key=value` (a string), `key:=json` (a raw JSON value:
  `size:=8`, `enabled:=true`), `--json '{...}'` (a whole object). Checked against the declared
  type with no coercion (`size=8` into an `int` field is an error).
- **Output:** JSON by default (JSON Lines for lists); `--field NAME` prints raw values, one
  per line (not for values that can contain newlines: use the JSON output for those). Not
  chosen: a `-0` (NUL-separated) flag (only matters for file paths with newlines; JSON already
  handles any value).
- **Exit codes:** `0` success, `1` error, `2` wrong usage; messages on stderr.
- Per command:
  - `cleanup add`: synchronous (when it returns, the handle is journaled and forwarded to the
    master); the type must be in `cleanup_handles=` if declared; not allowed inside a cleanup.
  - `cleanup list`: only inside a cleanup; registration order; optional type filter; `--field`
    on a type without that field is an error.
  - `resource set`: only in a resource setup, once per attempt; validated against `value=` at
    once; a setup exiting 0 without it fails ("resource value not reported").
  - `resource get`: only in a resource cleanup.
  - `secret get`: only secrets declared in the task's `secrets=`; printed **without a trailing
    newline**; masked by the agent like placeholders.

```bash
# export.sh
job_id=$(warehouse start-export orders "$1")
schedr task-ctx cleanup add ExportJob job_id="$job_id"
warehouse wait "$job_id"

# undo_export.sh (its cleanup)
schedr task-ctx cleanup list ExportJob --field job_id |
  while read -r id; do warehouse cancel-if-running "$id"; done

# create_cluster.sh (a resource setup)
id=$(cloud create-cluster --size "$2")
schedr task-ctx resource set cluster_id="$id" size:=8

# deploy.sh
curl -H "Authorization: Bearer $(schedr task-ctx secret get 'ci/deploy#token')" ...
```

### Default deployment size

- **2 workers** by default, **2 slots each** (4 concurrent tasks). To be tuned per deployment.

### Ports

All configurable; none clashes with Istio's sidecar ports (15000–15090).

| Port | Role | Default |
|---|---|---|
| webserver HTTP (UI and REST API) | webserver | **8080** |
| master gRPC (workers connect) | master | **8070** |
| worker log endpoint | worker | **8081** |
| ops (`/healthz`, `/readyz`, `/metrics`) | every role | **8090** |

Standalone uses 8080 and 8090, and 8070 / 8081 on localhost (it keeps the gRPC protocol
internally). Not chosen: 9090 for ops (Prometheus's own port: a clash in standalone next to a
local Prometheus, and confusing in a `ServiceMonitor`).

### DAG roots

DAG files are referenced the same way as scripts, so references are portable between workers
and environments:

- **Every `SCHEDR_DAG_PATH` entry is a root**, named by default after its last path component.
- The configuration file can **rename** an entry in a **`[dags.roots]`** section (the path must
  be one of the `SCHEDR_DAG_PATH` entries).
- **Nested entries** are a **startup error**; **duplicate root names** are a **startup error**
  unless resolved by renaming.
- A DAG file is referenced as **(root name, path relative to the root)** and resolved by the
  worker that runs the task.

### DAG ID conflicts

A DAG ID must be defined in exactly one place. When it isn't, the DAG is in **conflict**:

- **Same ID in two places on one worker** (two files in one folder, two folders, two roots):
  **none of the definitions can run**. The UI shows the DAG as in conflict, with every place
  that defines it.
- **Different workers define the same ID in different places** (e.g. worker A has it in
  `a.py`, worker B, not yet synced, still in `b.py`): also a **conflict**, and the DAG can't
  run, **until every relevant worker** has parsed and reported its code and they all agree on
  one place. Typical cause: git-sync on some pods hasn't pulled the latest commit yet, so the
  conflict clears by itself once they catch up.
- **"Different" means a different place** (root + file). The **same file at different code
  versions** on different workers is **not** a conflict: tasks run on the latest code and the UI
  warns about mixed hashes (see Code versions and change detection). Otherwise every code push
  would block the DAG until all workers have synced.
- **Relevant workers** are the **`ACTIVE`** workers serving any queue used by the DAG's tasks
  (queues taken from all reported definitions). `DISCONNECTED`, `LOST` and `OFFLINE` workers
  don't count, so a dead pod can't block a DAG forever.
- **No duplicate parsing.** Workers report a content hash per root; each (root, hash) is
  **parsed once** per schedr version (see Upgrades and compatibility), by any `ACTIVE` worker
  that reported that hash. The master derives each
  worker's DAGs from the parse of the hash it reported, so "parsed and reported by every
  worker" doesn't mean parsing the same code on every pod.
- While a DAG is in conflict, **no new runs are created and no tasks are assigned**. In runs
  already in progress, **running attempts finish**; tasks not yet started **wait** until the
  conflict is resolved.
- When the conflict is resolved, the DAG becomes runnable again on its own (its paused or
  unpaused setting is unchanged).

### Code versions and change detection

- **Tasks run on the latest code** the worker has. schedr does not keep old code versions.
- schedr computes a **content hash** of the DAG code and records it for each task attempt.
  When tasks in the same DAG run used **different content hashes**, schedr shows a **warning**.
- Changes are detected by **polling on a schedule** (interval set in configuration), not by
  filesystem notifications (which are unreliable on network volumes and with git-sync's symlink
  switching).
- Defaults: **poll every 30 s**, **settle time 5 s**.
- After a change is seen, schedr waits for a **settle time** (configurable; no further changes)
  before computing the new content hash, so half-written files are never parsed.

### Upgrades and compatibility

Upgrading schedr doesn't change a root's content hash, and the upgrade order (master first,
then workers) means a new master works with old workers for a while.

- **DAG JSON carries the schedr version that produced it** (`major.minor`). The master accepts
  DAG JSON of the **same major and an equal or lower minor** version (the protocol's rule):
  between the master and worker upgrades, every parse comes from old workers. Within a major
  version, DAG JSON changes are **additive only** (new optional fields), so a newer master reads
  older JSON with no migration code.
- **Re-parse after an upgrade:** parse jobs are keyed by **(root, hash, schedr version)**. When
  a worker of a newer version reports a hash that has only an older parse, the master creates a
  parse job, once per root, so existing DAGs get the new checks and features. Until it is
  stored the old parse stays in use; runs in progress keep the DAG version they started with.
- **A DAG that becomes invalid after an upgrade:**
  - **Within a major version this must not happen:** a minor release accepts every DAG the
    previous minor accepted; a new check starts as a **parse warning** and becomes an error only
    in the next major.
  - **Across a major upgrade** the new parse records an import error and the **last good
    version keeps running**, with the error shown in the UI ("doesn't parse with schedr 2.0");
    not treated as a removed DAG (the code didn't change, schedr did).
  - **Checking before an upgrade:** `schedr dags check <path>` (from the new `pip install
    schedr`) parses DAG files and `schedr.toml` files locally, without a cluster, and reports
    import errors and parse warnings; also for CI.
- **Rolling upgrade:** a task is assigned only to workers whose version is **at least the
  version of the parse in use** (a DAG parsed by a new runner may use a feature only the new
  `schedr` package has). Until enough workers are upgraded such tasks wait as `queued`
  ("waiting: no worker with schedr ≥ 1.5"); old workers keep running tasks of DAGs on older
  parses, so an upgrade needs no downtime.
- **Other stored data:** the database schema is upgraded by `schedr db migrate` (see State
  storage). Outputs, cleanup handles and resource values (tagged JSON) also change only
  additively within a major version: a new runner must decode what an old one stored, since a
  run started before the upgrade continues after it.

### End-to-end flow: running a native-API DAG

What happens from the moment a DAG file written with the native Python API lands in
`SCHEDR_DAG_PATH` (already on the worker) until its run finishes. Each step is tagged:
**[D]** decided elsewhere in this file.

Running example: `/git/dags/etl-repo/team_a/sync.py` defines DAG `sync` with two Python tasks,
`extract` → `load`, scheduled daily; `load` takes `extract`'s output as input.

**Phase 1: Discovery (worker)**

1. **Poll.** Every 30 s the worker agent scans each `SCHEDR_DAG_PATH` root recursively,
   skipping whatever `.schedrignore` files exclude, and notices that root `etl-repo` changed.
   [D]
2. **Settle.** The agent waits 5 s with no further changes, so half-written files are never
   read. [D]
3. **Hash.** It computes a new content hash `H2` for root `etl-repo`. [D]
4. **Report.** It sends `CodeVersionChanged(root=etl-repo, hash=H2)` over its stream to the
   master. [D] Every worker sees the change on its own, so the master gets one report per
   worker; it creates only one parse job per (root, hash, schedr version) (see DAG ID
   conflicts; Upgrades and compatibility). [D]

**Phase 2: Parsing (master → worker → master)**

Why the DAG structure travels to the master (steps 8–10): the master schedules (when runs are
due, task order, retries, trigger rules) but **never runs user code and has no Python**, and the
DAG code is only on the workers. A Python DAG file can't be read without running it, so a worker
runs it once and sends the result as plain data. The webserver also needs it (graph view, task
list) and likewise has no Python.

5. **Parse job.** The master creates a parse job for `etl-repo@H2` and sends `AssignParse` to
   any `ACTIVE` worker that reported `H2`. That worker uses its dedicated parse slot. [D]
6. **Runner start.** The agent starts `<python> -m schedr.runner parse` [D] with a Unix socket
   [D]. Every DAG root is on `sys.path`. [D]
7. **Import.** The runner imports every `.py` file in the root, one at a time; `team_a/sync.py`
   is one of them.
   - Native API calls only **record** the DAG: tasks, edges, schedule, retries, t-strings as
     literal text plus references. Nothing runs.
   - Files that define no DAG are ignored silently. [D]
   - Each file's progress is reported to the agent; a file that hangs gets a per-file timeout.
     [D]
8. **Serialize.** The runner turns each DAG into DAG JSON in the shared model, along with the
   import errors for each file, and sends it all to the agent. [D]
9. **Upload.** The agent sends the result to the master with a unary call. [D]
10. **Store.** The master validates the JSON (template references, cycles, unknown task kinds)
    and stores the DAG version for `sync` with hash `H2`. Import errors are stored and shown in
    the UI. [D in outline] A newly found DAG is stored **paused** until a user unpauses it. [D]
    A DAG that disappeared from the root is removed from the UI and listings. [D] A DAG ID
    defined in more than one place, or defined differently by different workers, is marked as
    in conflict and can't run (see DAG ID conflicts). [D]

**Phase 3: Creating a run (master)**

11. **Trigger.** The master's scheduling loop finds that `sync` is due by its schedule; missed
    fire times follow the `catchup` rules. [D] The master creates a **DAG run** in Postgres
    (kind `scheduled`, ID a UUIDv7) with `planned_for` = the fire time and `since` = the
    previous fire time; the run covers `(since, planned_for]`. [D] A run can also be created
    by a person (`schedr runs trigger`, a backfill, the UI): the command is validated and
    applied by the master, and the run may wait as `queued` for `max_active_runs`. [D]
12. **Task instances.** The master creates `extract` and `load` in state `scheduled`. `extract`
    has no upstream tasks, so it is ready and moves to `queued`. [D]

**Phase 4: Assignment (master → worker)**

13. **Choose a worker.** Among `ACTIVE` workers serving the task's queue (here `default`) with a
    free slot, the master picks the one with the most free slots. [D]
14. **Save first.** The assignment is written to Postgres **before** anything is sent. [D]
15. **`AssignTask`** goes over the worker's stream: attempt ID; DAG, run and task IDs, try
    number; kind `python` with the DAG file reference `etl-repo:team_a/sync.py` and the task ID;
    expected hash `H2`; inputs (none for `extract`) and run information; timeout and memory
    budget. [D]
16. **Checks.** The agent checks that it has a free slot, isn't draining, has the file and has
    the interpreter.
    - If a check fails, it sends `Rejected` with a reason, and the master reacts according to
      the rejection table (see Task rejections). [D]
    - If all checks pass, it writes the attempt to its journal on disk and sends `Started`,
      which also means the task is accepted. [D]
    - If no `Started` arrives within about 10 s, the master takes the assignment back. [D]

**Phase 5: Execution (worker)**

17. **Start.** The agent starts `<python> -m schedr.runner task` [D] in a new process group.
    stdout and stderr go to the task's log file on the worker's disk. [D]
18. **Fetch the spec.** The runner asks the agent over the socket for the task specification
    and inputs. [D]
19. **Prepare.** The runner imports `team_a/sync.py` again (the latest code on the worker:
    `H2` or newer [D]) and finds task `extract`. It fills in t-strings; any `secrets[...]`
    values are fetched from secret storage **by the agent** and passed over the socket. [D]
20. **Run.** The runner calls the task's function. Meanwhile the agent's memory watchdog checks
    the process tree's memory about once a second. [D]
21. **Report.** The runner sends the return value (the output) or a structured error summary
    over the socket, then exits. [D]
22. **Finish.** The agent masks secret values in the log and outputs, records the attempt
    status and exit code (`success`, `failed`, `timed_out`, `oom_killed`, …), and saves the
    result in its journal. [D]

**Phase 6: Result and what happens next (worker → master)**

23. **`Finished`** goes to the master: status, output and outcome, duration, peak memory. Delivery
    is at least once; the attempt ID makes repeats harmless. [D]
24. **Store and acknowledge.** The master stores the attempt result and the (output, outcome) pair
    under (run, `extract`), then sends `FinishedAck`; the agent removes the attempt from its
    journal. [D]
25. **Decide.**
    - On success, `extract` → `success`. `load`'s upstream condition is now met, so it goes
      through steps 13–24; its `AssignTask` carries `extract`'s output as input.
    - On failure, the master either retries (`up_for_retry` after `retry_delay`) or marks the
      task `failed`, and downstream tasks become `upstream_failed`. [D]
26. **Run complete.** When every task instance is in a final state and every cleanup has
    finished, the DAG run gets its result by the run result rule: **`failed`** if anything
    unexpected went wrong (a task or a cleanup failed after its last retry), **`cancelled`** if a
    user cancelled it and nothing failed, otherwise **`success`** (see Outcomes, failures and
    joins: Run result). [D] The UI shows the run; logs come from the worker through the
    webserver proxy. If tasks in the run used different code hashes, the UI shows a warning.
    [D]

### Observability

- **Ops port on every role** (master, worker, webserver): a separate **plain-HTTP** port,
  `[ops] port` (default **8090**, see Ports), serving only **`/healthz`**, **`/readyz`** and
  **`/metrics`**, never data or actions, so no TLS or authentication. Needed because a
  kubelet's probe and Prometheus can't present a client certificate to the mTLS log port
  (`tls.mode = "app"`). Not chosen: `/healthz` on the worker's log port (fails under mTLS).
- **Health endpoints.** **Liveness never depends on another component** (otherwise a database
  outage would restart the master and the webserver in a loop); **readiness doesn't check the
  database either** (with one master, or webserver replicas sharing one database, taking pods
  out of the Service helps nobody: the UI shows a "database unavailable" banner, workers keep
  retrying the master).

  | Role | `/healthz` (liveness) | `/readyz` (readiness) |
  |---|---|---|
  | master | the process is up and the scheduling loop is ticking (an iteration started within `[ops] loop_stall_timeout`, default **2 min**, whether or not the database answered; every database query has a timeout below it, default 30 s, so one hung query doesn't look like a stuck loop): catches a stuck scheduler | startup finished (schema checked, listening for workers) |
  | webserver | the process is up | startup finished |
  | worker | the agent's own health (see Worker lifecycle) | not needed |

- **Metrics: Prometheus format** on `/metrics` (scraped e.g. by OpenShift's user-workload
  monitoring through a `ServiceMonitor`).

  | Role | Examples |
  |---|---|
  | master | scheduling loop duration; runs by state; tasks `queued` by reason (`max_active_*`, no free slot); `unschedulable` tasks; assignments and rejections by reason; parse jobs and durations; command apply latency; workers by state; notifications sent / failed |
  | worker | slots used / free; attempts by status; watchdog kills; peak memory per task (histogram); journal size; log disk usage; secret fetches, errors by kind, cache hits |
  | webserver | HTTP requests by route and status; open SSE streams; log proxy errors |

  - **Labels:** `dag_id` on run-level metrics (bounded, useful for alerts); **never** `task_id`,
    `run_id`, `attempt_id` (unbounded; per-task detail is in the database and the UI).
  - OpenTelemetry (OTLP push, traces of a run across master and workers): possibly later.
- **schedr's own logs** go to stdout/stderr (collected by Kubernetes), separate from task logs
  (files on the worker, see Task logs).
  - `[log] format = "auto" | "json" | "text"`: `auto` writes JSON when stdout isn't a terminal
    (pods) and readable text in a terminal (standalone, development).
  - `[log] level`, with per-module filters in the `RUST_LOG` style (e.g.
    `"info,schedr::scheduler=debug"`); the `tracing` crate.
  - **Context fields** where they apply: `dag_id`, `run_id`, `task_id`, `attempt_id`,
    `worker_id` ("everything about run X" is one search).
  - **No secrets:** configuration secrets are never logged; task values are already masked by
    the agent.

## Configuration

- schedr reads **one configuration file** and/or **environment variables**.
- Environment variables mirror the configuration file's structure, under the prefix
  **`SCHEDR_CONFIG__`** (e.g. a `key` in section `section` →
  `SCHEDR_CONFIG__SECTION__KEY`).
- File format: **TOML**.
- The file is found via the **`--config <path>`** flag, then the **`SCHEDR_CONFIG_FILE`**
  environment variable, then a default location.
- Precedence: **command-line flags > environment variables > configuration file**.
- `SCHEDR_DAG_PATH` is a normal configuration key (`SCHEDR_CONFIG__DAGS__PATH`);
  `SCHEDR_DAG_PATH` is kept as a **short alias**.
- **Secrets** (database password, signing keys, …) come only from **environment variables**
  (filled from Kubernetes Secrets), never from the configuration file.
- **Outgoing HTTP and proxies:** schedr's own outgoing HTTP calls (notification webhooks from
  the master; Vault from the agent, if outside the cluster) honour `HTTPS_PROXY`, `HTTP_PROXY`
  and `NO_PROXY`. **Internal links never use a proxy** (gRPC between master and workers, the
  webserver's calls to the workers' log endpoints), whatever the environment says. Documented
  for deployments: `NO_PROXY` with the cluster's service domains (`.svc`, `.cluster.local`)
  and internal services; with Istio's `REGISTRY_ONLY` outbound policy, `ServiceEntry` resources
  for outside hosts. Tasks get the proxy variables only via `[worker] pass_env`.

## Secret storage

- **Secret storage is built into schedr**, not an optional add-on or plugin.
- The core defines a generic **secret storage** abstraction, so several kinds of secret storage
  can be supported. **Only one implementation is built for now: HashiCorp Vault**, plus a
  development-only `file` storage (see Testing DAGs).
- Core code, the shared model and the native API use the generic name (secret storage), never
  "Vault". Vault-specific names appear only in the Vault implementation.
- **Templates have a dedicated syntax for secrets**: a **general placeholder** (not a Vault-specific
  one) that refers to a value in secret storage (see Airflow compatibility, Scope: Templating), e.g.
  `t"{secrets['etl/db#password']}"`.
- **Several secret storages** can be configured at the same time, each with a name. For now
  only one is set up, but the placeholder syntax **names the storage** a secret comes from, so
  adding storages later doesn't change the syntax:
  - `secrets['path#key']`: the **default** storage. The configuration marks one storage as
    the default.
  - `secrets.<storage>['path#key']`: the named storage.
- Secrets are **fetched by the worker agent** that runs the task, never by the master
  (consistent with Task assignment: secrets never pass through the master) and not by the
  Python task itself. The agent fills in secret placeholders when it renders templates.
- **Vault authentication:** Vault's **Kubernetes auth method** with the pod's service-account
  token; a token from an environment variable for local development.
- **Secret values are masked** in task logs and outputs by the worker agent.
- **Two separate, explicit ways for `command` / `script` tasks** (and their cleanups and resource
  setups) to get a secret; never in the command line (a parse error, see Airflow compatibility,
  Scope: Templating):
  - **An environment variable**, set up by the user with a secret placeholder:
    `env={"PGPASSWORD": t"{secrets['etl/db#password']}"}` (TOML: `env = { PGPASSWORD = {
    secret = "etl/db#password" } }`). For the many tools that read credentials only from the
    environment.
  - **On demand over the agent socket**, for secrets that should stay out of the environment
    (not inherited by child processes, not in `/proc/<pid>/environ`, not printed by a stray
    `env`): the task **declares** them with `secrets=["ci/deploy#token"]` (TOML:
    `secrets = [...]`) and the script reads one with `schedr task-ctx secret get
    'ci/deploy#token'` (see In-task CLI), the same mechanism as for cleanup handles and
    resource values. Only **declared** secrets can be
    read (a typo or another team's path fails at once; the UI can show which secrets a task
    uses). `secrets=` never sets environment variables.
  - Python tasks get secrets through t-strings in their arguments (over the socket, never a
    command line).
- **Writing a named storage** (the default storage needs no name):

  | Place | Default storage | Named storage |
  |---|---|---|
  | Python templates | `secrets['ci/deploy#token']` | `secrets.other['ci/deploy#token']` |
  | Python `secrets=[...]` | the placeholder object, or a plain string `"ci/deploy#token"` | `secrets.other['ci/deploy#token']` |
  | TOML `secrets = [...]`, `env = { X = { secret = ... } }` | `"ci/deploy#token"` | `{ storage = "other", path = "ci/deploy#token" }` |
  | CLI | `schedr task-ctx secret get 'ci/deploy#token'` | `schedr task-ctx secret get --storage other 'ci/deploy#token'` |

  The storage is never put inside the string in TOML or the CLI (`"other:path#key"` would be
  ambiguous: a Vault path may contain `:`).
- **Configuration** (only on **workers**: the agent fetches secrets; the master and the
  webserver never do):

  ```toml
  [secret_storage]
  default = "vault"                           # which storage secrets['...'] means

  [secret_storage.vault]                      # one table per storage, named by its key
  type = "vault"
  address = "https://vault.example.com:8200"
  mount = "secret"                            # the KV mount
  namespace = "team-x"                        # optional (Vault Enterprise)
  ca_cert = "/etc/schedr/vault-ca.pem"        # optional
  auth = { method = "kubernetes", role = "schedr-worker" }

  [secret_storage.other]
  type = "vault"
  address = "https://vault-2.example.com:8200"
  mount = "kv"
  auth = { method = "token" }                 # local development
  ```

  - **Credentials only from environment variables** (as for all secrets): with
    `auth = { method = "token" }` the token comes from
    `SCHEDR_CONFIG__SECRET_STORAGE__<NAME>__TOKEN`; Kubernetes auth uses the pod's
    service-account token file (no secret at all). Every key can also be set through
    `SCHEDR_CONFIG__SECRET_STORAGE__...` variables.
  - **Startup errors:** `default` not a configured storage; unknown `type` (`vault`, and
    `file` in standalone only); auth without the fields its method needs.
  - **Vault: KV v2 only** (KV v1 is not supported).
  - **Validation of storage names in DAGs:** workers **report their configured storage names**
    when they register; the master's DAG validation shows a DAG error for an unknown storage.
    Not chosen: a list of names in the master's configuration (would have to be kept in sync).
- **Fetching: everything before the process starts.** The agent fetches every secret an
  attempt needs (placeholders in `env`, in Python task arguments, in cleanup and setup
  commands, and all declared `secrets=`) **before starting its process**; `schedr task-ctx
  secret get` is served from the agent's memory. Nothing runs with a missing secret, and a
  storage outage during a task doesn't break it. (The master can't check earlier: it never
  contacts secret storage.)
- **When a secret can't be fetched** the attempt ends as `failed`, with an error summary naming
  the secret and an error kind in `Finished`. Not a rejection (every worker uses the same
  storage).

  | Cause | Error kind | Retried? |
  |---|---|---|
  | storage unreachable, timeout, server error | `secret_unavailable` | yes, by the task's `retries` (usually temporary) |
  | permission denied | `secret_denied` | no (a configuration error) |
  | path or key not found | `secret_not_found` | no (usually a typo or a missing secret) |

- **Caching:** fetched values are kept **in the agent's memory only** (never on disk) for a
  short TTL per storage, `cache_ttl` (default **60 s**; `0` disables it): the workload is many
  short tasks, and without a cache every attempt would call the storage.
- **Token renewal:** the agent renews its own Vault token before it expires; on an expired
  token it logs in again (Kubernetes auth) once and retries. Invisible to tasks.
- **Static KV values only** for now. Dynamic secrets (Vault leases, e.g. short-lived database
  credentials) possibly later: each lease would be revoked when the attempt ends, a natural fit
  for the per-task cleanup. Pinning a secret's version (e.g. `@3`): possibly later.
- **Short values are not masked:** values shorter than `[secret_storage] min_mask_length` (default
  **5**, like Airflow's `min_length_masked_secret`) are left unmasked, since masking `"1"` or
  `"true"` would mangle the logs; the agent writes a warning at the top of the task's log
  ("secret `ci/flag#enabled` is too short to be masked"), without the value.
- **Connections** are stored in secret storage (see Scope), in **Airflow's JSON format**.
  - **Layout:** one KV v2 secret per connection at `<mount>/<connections_path>/<conn_id>`;
    `connections_path` is configurable per storage, default `"connections"` (as in Airflow's
    Vault backend). Fields: `conn_type`, `host`, `login`, `password`, `schema`, `port`, `extra`
    (JSON). A `conn_uri` field is also accepted when reading (parsed by the `Connection` class
    in `schedr.airflow`: library code in the task's process, not engine behaviour).
  - **Access API** in `schedr.airflow`: `Connection.get(conn_id)` (`schedr.airflow.sdk`, as in
    Airflow 3's Task SDK) and `BaseHook.get_connection(conn_id)` (common in existing DAGs).
    Hooks themselves stay out of scope.
  - **Natively** a connection is just a secret with fields (`secrets['connections/my_db#host']`);
    a typed native `Connection` is not needed now.
- **Airflow Variables** are secrets too.
  - **Layout:** one KV v2 secret per Variable at `<mount>/<variables_path>/<key>`, field
    `value` (Airflow's format); `variables_path` is configurable per storage, default
    `"variables"`. Natively no separate Variables concept: a Variable is just a secret.
  - **Code:** `Variable.get(key, default_var=..., deserialize_json=...)` in
    `schedr.airflow.sdk` reads through an implicit `SecretPrefix("<variables_path>/")` (see
    Secret prefixes). A missing Variable with `default_var` returns the default; without one it
    raises, as in Airflow, and the attempt fails as `secret_not_found` (not retried).
    **`Variable.set(...)` is an error**: tasks never write to secret storage (it would become
    hidden state shared between runs).
  - **Templates:** `var.value.<key>` and `var.json.<key>...` translate onto native secret
    placeholders (`secrets['variables/<key>#value']`, JSON-parsed for `var.json`), as the
    placeholder rule in Templating requires.
- **Unmasked prefixes:** `[secret_storage.<name>] unmasked_prefixes = ["variables/"]` in the
  server configuration marks parts of a storage as **non-secret configuration**, whose values
  are not masked in logs (otherwise e.g. a bucket name would become `***` on every log line).
  Everything else stays masked. Documented: keep only non-secret values under such prefixes.
  Not chosen: always masking (unreadable logs wherever Variables are used); Airflow's key-name
  heuristic (masks only keys that look sensitive: guesswork, `db_pass` would slip through).
- **Secret prefixes: reading secrets chosen at run time.** A task can declare a **prefix** in
  `secrets=`, covering every secret under it (at any depth). Secrets under a prefix are
  **fetched on demand** (they can't be known in advance): an explicit exception to "fetch
  before the process starts", for prefixes only, so a storage outage can fail such a task in
  the middle of a run (same error kinds and retry rules as above).
  - **Written as an explicit object**, never a plain string:

    ```python
    secrets=[
        secrets['etl/db#password'],             # one value (placeholder object)
        "etl/api#token",                        # one value, default storage (plain string)
        "connections/db_eu",                    # one whole secret, all its keys
        SecretPrefix("connections/"),           # a prefix: every secret under it
        SecretPrefix("tenants/", storage="other"),
    ]
    ```

    ```toml
    secrets = [
        "etl/api#token",
        { prefix = "connections/" },
        { prefix = "tenants/", storage = "other" },
    ]
    ```

    Widening access is visible in review and can't happen by a typo. A plain string ending in
    `/` is a **parse error** ("use `SecretPrefix(...)` for a prefix"). Only prefixes: no globs
    (`a/*/b`, `db_*`). Not chosen: a trailing `/` on a plain string (one character decides
    between one secret and a whole tree); a `*` glob (suggests full glob support; `*` isn't
    forbidden in Vault paths).
  - Native use: a task reading secrets chosen at run time (e.g. one per tenant).
  - **Python tasks** get an on-demand reader mirroring `schedr task-ctx secret get` (e.g.
    `task_ctx.secret("connections/db_eu#password")`), restricted to declared secrets and
    prefixes; `schedr task-ctx secret get` accepts secrets under a declared prefix too.
  - **Airflow:** every Airflow-layer Python task implicitly declares the prefix
    `SecretPrefix("<connections_path>/")`, so `Connection.get` / `BaseHook.get_connection` (often
    with computed IDs) work unchanged, using only native constructs. Cost: the "only declared
    secrets" protection is coarser for Airflow tasks (any connection, not a list).
  - Not chosen: requiring Airflow tasks to declare their connections (a schedr-only argument in
    Airflow code, a manual change in every DAG, awkward with computed IDs); on-demand reads only
    in the Airflow layer (breaks the principle in Thin translating classes).

## State storage

- **Production: PostgreSQL.** On OpenShift it is provisioned and managed by an operator.
  schedr only needs a connection string; it doesn't manage the database itself.
- **Local development: SQLite.**
- All persistent state lives in the database, so the master can restart at any time without
  losing anything.
- **Schema upgrades** are applied by a separate command, **`schedr db migrate`**, run as a
  Kubernetes Job or init container before the master starts. The master **refuses to start**
  against an outdated schema.

### Retention

How long the database keeps things (logs live on the workers and have their own retention,
see Task logs). A log may disappear before its run or the other way round; the UI already
handles "log unavailable".

```toml
[retention]
runs = "90d"          # "forever" disables it
outputs = "90d"       # default: the same as runs; may be shorter
commands = "365d"
interval = "1h"       # how often the retention job runs
batch_runs = 100      # runs deleted per transaction
```

- **Runs:** finished runs older than `runs` are deleted with everything that belongs to them:
  task instances, attempts, outputs, cleanup handles, resource instances.
  - **Never deleted:** runs not finished (`queued`, `running`), and **the latest run of each
    DAG**, whatever its age: a DAG without a schedule quiet for longer than `runs` would
    otherwise lose its chain (the next manual run would fall back to `initial_since` and reload
    everything); `RUN_RECOVERED` needs the previous run's result; the UI shows the last run.
  - Documented consequence: a backfill's default ("fire times without a successful run")
    sees deleted runs as missing, so a backfill over an old range creates them again.
- **Outputs:** optionally shorter than runs (they are the bulky part, and nothing needs them
  once a run has finished, since there's no "clear"; only the UI shows them). The run, its
  outcomes and statuses stay; the UI shows "output deleted by retention".
- **Command rows** (the audit trail) are kept independently of runs: a command about a
  deleted run stays readable.
- **DAG versions** are kept while **current** or **referenced by a retained run** (a run's
  graph is drawn from the version it ran with); others are deleted. A **removed DAG** (hidden,
  history kept) disappears completely once its last run is deleted; if its ID appears again,
  it starts with no history.
- **Parse results and import errors:** only the latest per file is kept (older ones are
  deleted when a newer parse of that root is stored); no setting.
- **How:** a background job on the master (`interval`, every hour by default), deleting in
  **batches** of `batch_runs` runs per transaction (default **100**: one run can hold over a
  thousand rows, e.g. a mapped task's instances, attempts and outputs), repeated until nothing
  is left, so it never holds long locks or blocks scheduling; the same in standalone (SQLite).
  Partitioning tables by time: possibly later.

## Python API structure

schedr's Python package has two kinds of API:

- **Native API**: schedr's own DAG authoring API, at the top level of the `schedr`
  package (see Native API).
- **Compatibility APIs**: look-alikes of other orchestrators' APIs, each in its own
  subpackage named after the orchestrator. `schedr.airflow` is the first. Others may be added
  later (e.g. `schedr.<other>`).

```
schedr/                 # native API
schedr/airflow/         # Airflow 3 look-alike API
schedr/<other>/         # future compatibility APIs
```

All APIs describe DAGs in terms of the same underlying model, so the Rust scheduler handles
them identically. A compatibility API is a translation layer onto that model, not a separate
engine.

### How the Python API works with the engine

The Python API is a **description layer, not an execution engine**. Python code of the API runs
at two moments only; all scheduling behaviour lives in Rust, on the master.

**Parse time: Python describes the DAG.** The runner (`parse` mode) imports the DAG file. API
calls (creating a DAG, tasks, dependencies, groups, mapping) only **record** what was declared;
the runner turns the record into DAG JSON in the shared model:

| Feature                         | What Python records                                  | Who implements the behaviour |
|---------------------------------|------------------------------------------------------|------------------------------|
| Dependencies (`>>`, `chain()`)  | Edges between task IDs                               | Master: dependency resolution |
| Trigger rules                   | A per-task field                                     | Master: when a task is ready, skipped or `upstream_failed` |
| Branching                       | A `python` task flagged as a branch                  | Master: skips the branches not chosen and propagates the skip |
| Task groups                     | Flattened to `group_id.task_id` names and edges, plus group metadata for the UI | Master sees ordinary tasks |
| Dynamic task mapping            | The task, its fixed arguments, and what it maps over (a literal or an output reference) | Master: once the upstream output exists, creates N task instances (`map_index` 0…N−1) |
| t-strings                       | Literal text plus typed references                   | Agent / runner: filled in at run time |
| `EmptyOperator`                 | An `empty` task                                      | Master completes it, no worker |
| `TriggerDagOperator`            | A `trigger_dag` task                                 | Master creates the target run and waits for it, no worker |

**Run time: Python runs one callable.** For a `python` task the runner (`task` mode) imports
the DAG file again, finds the task by ID, fills in its arguments (inputs from upstream
outputs, t-strings, secrets via the agent), calls the function and returns the result over the
socket. A branch task is no different: its return value (the task IDs to follow) is just its
output, and the master acts on it. `command` and `script` tasks need no Python: the agent
renders and runs them.

**Difference from Airflow:** in Airflow, operator classes carry behaviour in Python (e.g.
`BranchPythonOperator.execute()` marks other tasks skipped itself). In schedr, operator
classes are thin: their type and arguments go into the model, and Rust implements what each
type means. That is why the native API and every compatibility API can share one engine.
User code can never change scheduling behaviour (skips, branching, retries) from inside a task.

### Native API

- **DAGs are defined only with `with Dag(...)`.** There is no `@dag` decorator: a decorated
  function must be called to create the DAG, and a forgotten call would make the DAG vanish
  silently (files without DAGs are ignored). The context manager also makes generating DAGs in
  a loop straightforward.
- **A `@task` must annotate its return type** (`-> None` for a task without output). A missing
  annotation is a parse error. `-> Any` is the explicit opt-out from the parse-time type check
  (the run-time check still applies).
- **Task kinds:** Python tasks are **decorated functions** (`@task`); other kinds are **operator
  classes**: `CommandOperator`, `ScriptOperator`, `EmptyOperator`, `TriggerDagOperator`.
- **No custom operators.** Users can't subclass an operator class to define their own task kind
  with its own `execute()`; subclassing a schedr operator class is a parse error. Reuse is by
  **composition**: a plain Python function that creates tasks with parameters (a task
  factory), or a shared `@task` function in a helper module. Every task is therefore one of the
  known kinds, which the master and the agent understand without user code. Consequence for
  the Airflow layer: see Airflow compatibility (Scope).
- **Data flow defines dependencies.** Calling a task inside a DAG returns an **output
  reference**; passing it to another task (as an argument, or inside a t-string) creates the
  dependency. `>>` remains for **order-only** dependencies (no data passed).
- **Typing of calls.** A `@task` returning `T` gives an **`Output[T]`** when called. Arguments
  receiving an `Output[T]` instead of `T` can't be expressed in Python's type system without a
  plugin, so they are **checked at parse time** (see Output serialization); a mypy plugin may
  come later.
- **IDs.** The DAG ID is an **explicit, required string**. A task ID defaults to the **function
  name** for `@task` and is **required** for operators. Duplicate IDs are a parse error.
- **Schedules are typed objects:** `Cron("0 3 * * *")`, `Every(timedelta(hours=1))`,
  `Once("2026-12-24T23:00")` (a single fire time), `None` (manual runs only), plus preset
  constants (`DAILY`, …). The time zone is set per schedule, **default UTC**.
  - **`Once`** is the way to run a DAG once at a future time (e.g. a migration tonight at
    23:00): an ordinary schedule with one fire time, so catchup, `end`, pausing and the UI's
    "next run" work as for any schedule (it has no previous fire time, so a DAG using `since`
    must set `initial_since`). It is set in the DAG file (reviewed, in git);
    there is no deferred manual trigger (see Runs: manual runs, backfill and pausing).
  - **Daylight-saving changes** (cron schedules in a time zone with DST; e.g. `30 2 * * *` in
    Europe/Warsaw):
    - **Gap** (spring forward, 02:30 doesn't exist): the fire time moves to the **first valid
      moment after the gap** (03:00), with `planned_for` = that moment; several fire times
      falling into the gap collapse into that one run. Not chosen: skipping (a hole in a daily
      DAG); shifting by the gap (03:30).
    - **Overlap** (fall back, 02:30 happens twice): a fire time whose **hour field is fixed**
      (`30 2 * * *`) fires **once, at the first occurrence**, so a daily schedule gives exactly
      one run per day; one with a **wildcard or step in the hour field** (`*/15 * * * *`,
      `0 * * * *`) fires on **each real occurrence** (otherwise an hour without runs).
    - Windows stay consecutive: the runs around a change cover 23.5 or 25 hours of real time,
      correct for "one run per day".
    - **`Every(...)`** is measured in real time (UTC), unaffected by DST.
- **Run time semantics.** No Airflow-style "logical date" shifted one interval back. A run is
  created **when its schedule fires** and knows **`dag_ctx.run.planned_for`** (the fire time)
  and **`dag_ctx.run.since`** (the previous fire time), so the run covers
  `(since, planned_for]` (see Run parameters and the DAG context; manual runs: see Runs:
  manual runs, backfill and pausing). The **first run** is the first fire time at or after
  `start`. **`catchup` defaults to False.** (Airflow 3 moved the same way: cron
  schedules run at the fire time.)
- **`end`** (optional, mirror of `start`): the **last scheduled run** is the last fire time **at
  or before `end`** (in the schedule's time zone). After it the DAG stays listed with its
  history, but no new scheduled runs are created; the UI shows it as "ended". Manual runs are
  still allowed: **`start` and `end` apply only to runs of kind `scheduled`**; manual and
  backfill runs may be for any time, before `start` or after `end` (see Runs: manual runs,
  backfill and pausing). Native use: temporary DAGs (a campaign, a migration window) that stop on
  their own.
- **`max_active_runs`** (default **1**; `None` = no limit): at most this many runs of the DAG
  at a time. A fire time that comes while the DAG is at its limit is **treated as missed**
  (like fire times missed while the master is down) and the **`catchup` rules** decide: with
  `catchup=False`, when a run finishes only the most recent missed fire time gets a run (a slow
  DAG never builds a backlog); with `catchup=True` the missed ones get runs in order, still at
  most `max_active_runs` at a time. Scheduled fire times never queue (they are missed);
  only manual and backfill runs wait, as `queued`. Default 1 because overlapping runs are
  rarely wanted in a sync-style workload. Manual and backfill runs count too, and wait instead
  of being dropped (see Runs: manual runs, backfill and pausing).
- **`tags`** and **`description`**: metadata (shown in the UI, filter by tag).
- **Run parameters:** `Dag(..., params=SomeDataclass)`; see Run parameters and the DAG context.
- **Concurrency limits** (besides worker slots and queues, which are capacity, not intent):
  - **`max_active_tasks`** on the `Dag` (default `None`, no limit): at most this many tasks of
    the DAG running at once, across all its runs (limit the load a DAG puts on other systems;
    keep one big DAG from filling every worker).
  - **`max_active_per_run`** on a task (default `None`): at most this many instances of the task
    running at once **within one run**; mainly to limit a mapped task's fan-out
    (`@task(max_active_per_run=10)`, 1,000 instances, 10 at a time).
  - **`max_active`** on a task (default `None`): the same **across all runs** (e.g. a task holding
    an exclusive external lock).
  - Task limits also on operators and in `TaskDefaults`; an unmapped task has one instance per
    run. `schedr.toml` jobs need no key (one-task DAGs: `max_active_runs` covers them).
  - **Across DAGs** (a named limit shared by several DAGs, Airflow's pools): not now; queues (a
    dedicated worker group with a fixed number of slots) cover it. Possibly later: named
    limits in the server configuration, referenced by tasks.
  - How the master applies them: see Task assignment.
- **Defaults are typed, not a dict:** `Dag(defaults=TaskDefaults(retries=2, ...))` instead of
  `default_args={...}`. Groups can set their own defaults; a task's own arguments win.
- **Routing: outcomes first, `@branch` for subsets** (see Task results and outcomes). To
  route on what a task found, the task returns an **outcome** and downstream tasks use
  `.on(...)`:

  ```python
  @task
  def size_of(rows: list[Row]) -> Big | Small: ...

  size = size_of(rows)
  full_load(size.on(Big))
  incremental(size.on(Small))
  ```

  **`@branch`** chooses **any subset** of its direct downstream tasks. The function returns
  **task handles** (one or a list), not task ID strings, so typos are caught at parse time. In
  the model the branch's output is the list of task IDs; the master skips the tasks not
  chosen. The **empty subset** is allowed: returning `[]` skips all direct downstream tasks
  (`None` is not allowed; write `[]`).

  ```python
  @branch
  def targets(cfg: Config) -> list[Task]:
      return [t for t, on in ((to_s3, cfg.s3), (to_gcs, cfg.gcs), (to_local, True)) if on]

  targets(cfg) >> [to_s3, to_gcs, to_local]
  ```

- **Task groups: `with group("name"):`.** Task IDs become `name.task_id`; a dependency on the
  group applies to every task in it; a group can have its own `TaskDefaults`.
- **Mapping keeps Airflow's names**, `.partial(...)` / `.expand(...)`, so both APIs share one
  mental model (e.g. `load_file.partial(bucket="raw").expand(path=files)`). Which mapping
  features beyond that: see Airflow compatibility (Dynamic task mapping), decided for both
  APIs.
- **Strict arguments.** An unknown argument is a plain Python `TypeError` when the DAG file is
  imported (so an import error). The native API never "accepts and warns"; that applies only to
  the Airflow layer (see Airflow compatibility: Thin translating classes).

Example:

```python
from datetime import timedelta

from schedr import Cron, Dag, CommandOperator, TaskDefaults, task
from schedr import dag_ctx
from schedr.templates import secrets

with Dag(
    "sync_customers",
    schedule=Cron("0 3 * * *", tz="Europe/Warsaw"),
    start="2026-01-01",
    defaults=TaskDefaults(retries=2, retry_delay=timedelta(minutes=5)),
):

    @task
    def extract() -> list[dict[str, str]]: ...

    @task(queue="large")
    def load(rows: list[dict[str, str]]) -> int: ...

    rows = extract()            # Output[list[dict[str, str]]]
    count = load(rows)          # load depends on extract (data flow)

    CommandOperator(            # depends on load: its command uses `count`
        "notify",
        command=t"./notify.sh {dag_ctx.run.planned_for} {count}",
        env={"TOKEN": t"{secrets['slack/bot#token']}"},
    )
```

### Outcomes, failures and joins

- **Only success branches.** A user cannot create a failure branch in the native API (no
  `on_failure` edges, no failure trigger rules, no callbacks). A failed task fails; its
  downstream tasks become `upstream_failed`.
  - **Rationale: a failure means an unexpected failure.** A failure branch is the same as
    catching an exception and reporting success: failure becomes control flow. Instead, the
    user handles the problems they **expect** inside the task and turns them into success
    paths: **outcomes**, routed with `.on(...)` (a caught exception returned as an outcome of
    a Python task; a declared exit code of a `command` / `script` task; see Task results and
    outcomes). So whenever a task fails, the failure
    was not expected, and **schedr handles it**: retries, notifications, failing the run.
  - **Rejected: master-side failure rules** (trigger rules such as `one_failed` or `all_done`
    evaluated by the master). They would be sound (the master sees timeouts, memory kills and
    lost workers, and retries stay intact), but they are still failure branches and defeat the
    rationale above, even if limited to the Airflow layer.
  - Work that must happen **whatever the result** (releasing a lock, deleting temporary
    resources) is not error handling; see Cleanup and resources.
- **Python tasks:** an exception caught inside the task is a normal return (route on it by
  returning an outcome); an uncaught exception is a failure. No exception-to-outcome mapping,
  so no ambiguous cases with `try`/`except`.
- **Failures are still recognized and recorded** by schedr itself (UI, logs, run history,
  notifications): the attempt status (`failed`, `timed_out`, `oom_killed`, …), the structured
  error summary of a failed Python task, and the `ProcessResult` of a failed command/script
  (stderr, exit code) are stored and shown. They are **not** available to other tasks as
  inputs.
- **Process outcomes for `command` / `script` tasks: an enum** (Python tasks have outcomes
  too: see Task results and outcomes). The user declares an `IntEnum`
  subclass of `schedr.Outcome`; values are exit codes, names are outcome names (shown in the
  UI). A declared exit code means **success** (no retry); any exit code not in the enum is a
  **failure**. Possibly later: retry control per exit code (e.g. retry on 75 /
  `EX_TEMPFAIL`, never on 2).

  ```python
  class Check(Outcome):
      CHANGED = 0
      UNCHANGED = 1

  check = CommandOperator("check", command="./has_changes.sh", outcomes=Check)
  check.on(Check.CHANGED) >> load
  check.on(Check.UNCHANGED) >> report_nothing
  check >> cleanup                      # plain >>: after any success outcome
  ```

  - Typed: a typo is caught by the type checker and at parse time. One exit code per member;
    several codes → several members, `check.on(Check.A, Check.B)`.
  - **Default:** `schedr.DefaultOutcome` (single member `SUCCESS = 0`) when `outcomes` isn't
    given: **0 is success, everything else is a failure**.
  - A **custom enum replaces the default entirely**; `0` is not added implicitly (a user who
    declares their own enum is expected to know what they're doing).
  - A plain `>>` from a task with outcomes means **any success outcome**. Edges not taken are
    skipped like branch skips.
- **Two trigger rules, no others.** The model (neutral names, no Airflow terms) has exactly two
  rules, and the native API offers both:
  - **`NoneFailed`** (the **default**): a task runs when **every upstream task that wasn't
    skipped succeeded**, and at least one upstream succeeded; it is **skipped** when all its
    upstream tasks were skipped; it becomes **`upstream_failed`** if any upstream task failed.
    Without skipped upstream tasks this is exactly Airflow's `all_success`; after a branch,
    joins work with nothing to configure.
  - **`AllSucceeded`** (opt-in: `@task(run_if=RunIf.ALL_SUCCEEDED)`, also on operators,
    groups and in `TaskDefaults`; enum `RunIf { NONE_FAILED, ALL_SUCCEEDED }`, default
    `NONE_FAILED`): runs only when **every** upstream task succeeded; any
    failure → `upstream_failed`; otherwise any skip → skipped. For "run only if every upstream
    task really ran":

    ```python
    check.on(Check.CHANGED) >> load      # load is skipped when nothing changed
    [load, refresh_meta] >> publish      # with ALL_SUCCEEDED: skipped too
    ```

  - Both decide **as soon as the result is certain**, as Airflow does: the first failure makes
    the task `upstream_failed` without waiting for the others; for `AllSucceeded`, the first
    failure or skip decides.

    | Upstream tasks…                    | `NoneFailed`                          | `AllSucceeded`    |
    |------------------------------------|---------------------------------------|-------------------|
    | all succeeded                      | runs                                  | runs              |
    | some skipped, the rest succeeded   | **runs**                              | **skipped**       |
    | all skipped                        | skipped                               | skipped           |
    | any failed                         | `upstream_failed`                     | `upstream_failed` |
    | Data from a skipped upstream task  | `None` (parameter must allow `None`)  | never happens     |

  - A `NoneFailed` task taking **data** from a task that can be skipped (downstream of a branch
    or outcome) must have a parameter type that **allows `None`**, or it's a parse error. An
    `AllSucceeded` task never receives `None` from upstream, so it needs no `Optional`.
    Order-only dependencies (`>>`) need nothing.
  - In the native API skips come only from `@branch` and `.on(...)`.
  - Airflow's trigger rules are translated onto these two or are an error (see Airflow
    compatibility: Trigger rules).
  - **Rejected: `AtLeastOneSuccess`** (run as soon as one upstream task succeeded, tolerating
    failures of the others; Airflow's `one_success`). Its main use, redundant sources ("if A is
    down, use B"), is an *expected* problem: the source task returns it as an outcome and
    `NoneFailed` joins the sources (see Task results and outcomes), with no failure tolerated. What
    remains is racing (not waiting for the slower upstream), which doesn't justify the costs: inputs
    that depend on timing (a retry could see different data), upstream tasks that can't be
    cancelled, and a downstream task that succeeded in a run that still fails.
- **Run result: a run is `failed` if anything unexpected went wrong in it**, otherwise
  `success`. Follows from the rationale above: failing the run (and notifying) is part of how
  schedr handles an unexpected failure. In both APIs.

  | Event | Fails the run? | Why |
  |---|---|---|
  | A task failed **after its last retry** (`failed`, `timed_out`, `oom_killed`, `lost`, …) | **yes** | an unexpected failure that wasn't recovered |
  | A cleanup failed **after its last retry** (see Cleanup and resources) | **yes** | the same; a resource may be leaking |
  | An attempt failed but a retry then succeeded | no | schedr recovered from it |
  | A task became `upstream_failed` | no | a consequence; the original failure already counts |
  | A task was skipped (branch, outcome, edge not taken) | no | an expected path |
  | A task returned an outcome such as `Unavailable` | no | an expected problem handled by the user: a success |

  - The result is set when **every task and every cleanup has finished**; independent parts of
    the graph keep running after a failure (no fail-fast; the UI may show "will fail" earlier).
  - **Not Airflow's leaf rule** (the run fails only if a leaf task failed), which turns runs
    green that hid a failure: a task failing behind a join already skipped (`AllSucceeded`
    decides on the first skip); a failed cleanup (not in the graph); a cleanup leaf that
    succeeded after the work failed. Migrated DAGs differ only in such cases.
  - **Runs cancelled by a user:** running attempts are cancelled (SIGTERM, then SIGKILL) and
    their per-task cleanups run; tasks not started become `cancelled`; resource cleanups run if
    their setup succeeded (`cancelled` is a final state, so all users are done). The run ends as
    **`cancelled`**, unless an unexpected failure happened in it (including a cleanup failing
    during the cancel): then **`failed`**, so a cancel never hides a failure. A pure cancel
    sends no "run failed" notification.
  - **No "clear" (re-running tasks of an existing run).** A run is never re-run partially; to
    run again, **trigger the DAG again** (a new run). So a run's results, outcomes and
    resources never change once it has finished.
- **Notifications are configured, not programmed**, and live outside the task graph.
  A DAG names notification channels (`Dag(..., notify=["team-a-slack"])`); channels are defined
  in the **server configuration** by operators, with credentials from **environment variables**
  (server secrets, like the database password), not from the user-facing secret storage, since
  the master never fetches from it. The **master** sends them (formatting a message runs no user
  code).
  - **Events** (an `Event` enum):

    | Event | When | Default |
    |---|---|---|
    | `RUN_FAILED` | a run ended `failed` (see Run result); includes cleanup failures, named in the message | **yes** |
    | `TASK_FAILED` | a task failed after its last retry (earlier than `RUN_FAILED`; Airflow's `email_on_failure`) | opt-in |
    | `TASK_RETRY` | an attempt failed and will be retried (Airflow's `email_on_retry`; noisy) | opt-in |
    | `RUN_CANCELLED` | a run was cancelled by a user | opt-in |
    | `RUN_SUCCEEDED` | a run ended `success` ("the report is ready") | opt-in |
    | `RUN_RECOVERED` | a run succeeded after the DAG's previous run failed ("it's fixed") | opt-in |

    Not now: a task running too long / SLAs (a separate, large feature; errors in the Airflow
    layer). The default event set is a server setting (default: `RUN_FAILED`).
  - **Choosing events per channel:** a plain list means the default events; a dict chooses
    them per channel. Events are chosen where the DAG names the channel (not on
    `EmailChannel`), so the same form works for server and DAG-defined channels; `Event` is an
    enum, so a typo is an attribute error at import.

    ```python
    Dag(..., notify=[team_a, "ops-slack"])                 # default events: RUN_FAILED

    Dag(
        ...,
        notify={
            team_a: [Event.RUN_FAILED, Event.TASK_RETRY],
            "ops-slack": [Event.RUN_FAILED, Event.RUN_RECOVERED],
        },
    )
    ```

  - **Channel types:** **email** (server transport; channels in the server configuration or
    DAG-defined, see below) and a generic **webhook** (an HTTP POST with a JSON body), defined
    only in the server configuration with credentials from environment variables; it covers
    Slack, Microsoft Teams, PagerDuty and Opsgenie through their incoming webhooks. Dedicated
    Slack / Teams channels with nicer formatting: possibly later.
  - **Message content is fixed by schedr** (no user templates: formatting runs on the master
    and must never run user code): the DAG, the run, the event, the failed tasks with their
    error summaries (exception type and message, or exit code and the tail of stderr),
    cleanup failures, links to the run and the logs in the UI. Webhooks get the same
    information as **structured JSON**, so receiving systems can format it. Secret values never
    appear (error summaries are already masked by the agent).
  - **DAG-defined email channels.** Besides channels defined in the server configuration
    (named by strings), a DAG can define an **`EmailChannel`** holding only recipients:

    ```python
    team_a = EmailChannel("team-a", ["a@example.com", "b@example.com"])

    with Dag("etl", schedule=DAILY, notify=[team_a, "ops-slack"]): ...
    ```

    - **Objects are DAG channels, strings name server channels.** Passing the object (not its
      name) means a typo is a `NameError` at import, and a DAG channel can't shadow a server
      channel of the same name. It can be reused through a helper module in the DAG root; each
      DAG's JSON carries the recipients it uses.
    - **The server keeps everything with credentials:** the email **transport** (SMTP server
      and its credentials) is in the server configuration; the DAG never handles a credential.
    - **Checks:** address syntax at parse time; server channel names, and recipients against an
      optional **allow-list of email domains** in the server configuration, when the master
      validates the DAG (a violation is a DAG error in the UI). The server can **switch off**
      DAG-defined channels entirely.
    - **Email only, for now:** other channel types (e.g. a Slack webhook) need a credential,
      and the master never reads the user-facing secret storage; they stay server-defined.
    - Which events each channel gets: chosen in `notify=` (see above).
    - Not chosen: naming DAG channels by strings in `notify=` (mixes them with server channels
      in one namespace); an inline `Email("a@x.com")` in `notify=` (can't be reused).

### Run parameters and the DAG context

The **DAG context**, **`dag_ctx`**, describes the current run: the **run information** and the
run's **parameters**. It is the same for every task of the run, known before any task starts,
and never comes from another task, so reading it anywhere is safe and deterministic (it isn't
task data: "data flow is the only way to pass task data" still holds).

- **One name everywhere.** In the DAG body (parse time) `dag_ctx.params.region` is a
  **reference**, filled in at run time; in a running task the same expression gives the
  **value**. Using it as a value in the DAG body (e.g. `if dag_ctx.params.full:`) is a parse
  error ("a reference can't be used as a value in the DAG file; branch with a task"), as for
  outputs. `from schedr import dag_ctx`. (Replaces the earlier `schedr.templates.run`
  placeholder.)
- **Run information:** `dag_ctx.run.id`, `dag_ctx.run.kind`, `dag_ctx.run.planned_for`,
  `dag_ctx.run.since`, `dag_ctx.dag_id`, … (scripts also get it as `SCHEDR_*` environment variables,
  see Plain script delivery).

**Declaring parameters.**

```python
@dataclass
class ReportParams:
    region: Literal["eu", "us"] = "eu"
    full: bool = False
    day: date | None = None
    limits: list[int] = field(default_factory=list)

with Dag("report", schedule=DAILY, params=ReportParams):
    ...
```

- A dataclass or a Pydantic model, with the same supported types as outputs.
- On a DAG with a schedule **every field needs a default** (scheduled runs use the defaults); a
  DAG without a schedule may have required fields, given at trigger time.
- `schedr.toml` jobs: `[jobs.<name>.params]` with default values (each type taken from its
  default):

  ```toml
  [jobs.report_daily.params]
  region = "eu"
  full = false
  limit = 100
  ```

- The runner turns the declaration into a **JSON Schema** stored in the DAG JSON.

**Setting values (per run).**

| Run | Values |
|---|---|
| scheduled | the defaults |
| manual, UI | a form generated from the schema, pre-filled with the defaults (text, number, checkbox, date picker, a select for `Literal`, JSON for lists / nested objects) |
| manual, CLI | `schedr runs trigger report -p region=us -p full:=true -p 'limits:=[1,2]'`, or `--params '{"region": "us"}'` (the field syntax of In-task CLI) |

The **master validates** the values against the schema **before** creating the run (no user
code runs; Pydantic validators run only when a task decodes the values); unknown keys or wrong
types are a rejected command with the reason. The values are stored with the run and shown in
its summary.

**Reading.**

| Where | How | Example |
|---|---|---|
| a task argument (typed) | a reference in the DAG body | `extract(region=dag_ctx.params.region, day=dag_ctx.params.day)` |
| t-strings (commands, script `args`, `env`, task arguments) | a reference, rendered by the agent | `command=["./publish.sh", t"{dag_ctx.params.region}"]` |
| Python task code (also cleanups, resource setups) | read at run time, decoded strictly into the dataclass passed | `p = dag_ctx.params(ReportParams)`; `dag_ctx.run.planned_for` |
| scripts and commands (incl. `schedr.toml` jobs) | `schedr dag-ctx` (see In-task CLI) | `schedr dag-ctx params get --field region` |

- Paths are **checked at parse time** against the declared dataclass (an unknown field, or a type
  that doesn't fit the task's parameter, is an import error). Statically `dag_ctx.params.x` is
  untyped (`Any`), as with output paths; `dag_ctx.params(ReportParams)` is typed by the class
  passed (one not matching the declaration is an error).
- A task argument gets the **value with its type** (a `date` stays a `date`).
- The context travels in each task's specification (`AssignTask`, run information): reading it
  never calls the master.
- Parameters are **not** environment variables (no clashes with other variables).
- **What can't use parameters:** the master (it only validates them; scheduling never depends
  on them); edge conditions (no routing on a parameter: a task returns an outcome, or a
  `@branch` decides); notification messages show the values but have no templating.

**Airflow** (see Airflow compatibility):

| Airflow | Becomes |
|---|---|
| `params={"region": Param("eu", enum=["eu", "us"]), "full": False}` | the JSON Schema (`Param` is JSON Schema; supported: `type`, `enum`, `default`, `minimum` / `maximum`, `minLength` / `maxLength`, `format: date`; others a parse error) |
| `{{ params.region }}` | `dag_ctx.params.region` (a t-string) |
| `context["params"]`, a `params` argument of a callable | `dag_ctx`, behind the scenes (callables taking `**context` / `params`) |
| `dag_run.conf` | the parameter values; unknown keys are an error |

### Runs: manual runs, backfill and pausing

Runs created by the scheduler are of kind `scheduled`; a person can also trigger a run
(`manual`) or a series of runs for past fire times (`backfill`). What the run's time and window
are, how runs share `max_active_runs`, how they are identified, and what pausing does:

1. **Time:** every run has **`dag_ctx.run.planned_for`**, the moment the run is
   **for** (renamed from `scheduled_at`, which promised a schedule a manual run doesn't have;
   not `planned_at`: `*_at` names say when something happened, like `triggered_at`, so it would
   read as the time the run was planned), and **`dag_ctx.run.since`**; the run covers
   **`(since, planned_for]`**. Both are always set (never `None`), computed **once when the run
   is created** and stored with it (a later schedule change or another run's result never
   changes them).

   ```python
   extract(since=dag_ctx.run.since, until=dag_ctx.run.planned_for)
   # WHERE updated_at > since AND updated_at <= until
   ```

   | Run | `planned_for` | `since` |
   |---|---|---|
   | `planned_for` is a fire time: scheduled, backfill, `trigger --for <fire time>` | the fire time | the **previous fire time** of the schedule |
   | plain `trigger` | the trigger time | the `planned_for` of the nearest earlier run **of any kind** |
   | `trigger --for <other time>` | that time | the `planned_for` of the nearest earlier run **of any kind** |
   | DAG without a schedule (only manual runs) | the trigger time or `--for` | the `planned_for` of the nearest earlier run |

   - **One rule for every run on a fire time:** its window is one schedule period, whatever
     runs exist. So a scheduled run, a backfill run and a `--for` re-run of the same fire time
     get the same window (the way to re-run a past day, since there's no "clear"); a backfill
     cuts its range into clean, consecutive periods; a manual click never cuts a scheduled
     period (a daily aggregation still covers the whole day), and the scheduler stays
     independent of manual runs.
   - **Manual runs not on a fire time** load everything since the last run of any kind
     ("previous" = the nearest earlier `planned_for`, not the run created last), so they may
     **overlap** with the next scheduled run (harmless for idempotent loads). A DAG without a
     schedule has only such runs: a plain chain of its own runs.
   - **Missed fire times leave a hole, by design.** With `catchup=False`, fire times without a
     run (master down, `max_active_runs` reached) are skipped **with their data**: with the
     master down for the 10-14 and 10-15 fire times, it creates only the 10-15 run when it
     returns (the most recent missed one), covering `(10-14 03:00, 10-15 03:00]`; the period of
     10-14 is never loaded. A user who needs the hole filled says so explicitly:
     **`catchup=True`** on the DAG (every period gets its own run) or a **manual backfill**
     (whose default picks exactly the fire times without a successful run). The UI marks
     skipped fire times on the DAG's run list. Not chosen: folding missed fire times into the
     next scheduled run's window (a hidden third behaviour besides `catchup=True/False`; windows
     would depend on which runs exist, so a backfill or `--for` run couldn't get one clean
     period).
   - **Failed runs still count**: `since` doesn't depend on how other runs ended (a failed day
     stays failed until re-run with `--for` or a backfill). Not chosen: "since the last
     successful run" (windows would depend on other runs' results and timing).
   - **First run:** on a `Cron` / `Every` schedule the rule above already gives one ordinary
     period (the cron has a previous fire time even before `start`). A DAG can set
     **`initial_since=`** (a datetime) for the run on its first fire time at or after `start`,
     e.g. to load history on the first run. A DAG **without a schedule** (its first run) or with
     a **`Once`** schedule (no previous fire time) whose references use `since` must set
     `initial_since` (a parse error otherwise). Not chosen: `-inf` (no such datetime in Python;
     `datetime.min` breaks many systems, e.g. MySQL's `DATETIME` starts at year 1000; `None`
     would bring `Optional` into every task); an empty window (the first run silently does
     nothing).
   - Not chosen: separate chains for manual and scheduled runs (a manual run's window would
     depend on when somebody last clicked, possibly months ago); manual runs moving the
     scheduled chain (a click would split a scheduled period; a failed click would lose data).
   - **`dag_ctx.run.kind`**: `scheduled` / `manual` / `backfill` / `triggered` (by another
     DAG's run, see Dependencies between DAGs).
   - **`triggered_at`** (when the run was created) is **run metadata only** (UI, CLI, audit
     trail), **not in `dag_ctx`**: a task using it would behave differently on a re-run of the
     same day. It differs from `planned_for` for missed runs, catchup, re-runs and backfills.
   - Parameters from `-p` / `--params` / the UI form.
   - Airflow's `ds` / `logical_date` / `data_interval_end` translate onto `planned_for`,
     `data_interval_start` onto `since`. Scripts get `SCHEDR_PLANNED_FOR` and `SCHEDR_SINCE`.
   - **`--for`** (option of `schedr runs trigger`, and a field in the UI's trigger form) sets
     `planned_for`; without it the run is for now.
   - **`--for` in the future is rejected** ("`--for` must not be in the future"; measured by the
     master's clock with a small tolerance, about a minute, for clock skew). Running it now
     would build a report for a day that hasn't happened or record a window covering data that
     doesn't exist yet; almost always a typo. A run once at a future time comes from the
     schedule instead (`Once(...)`, or `start` / `end`; see Native API). Not chosen: a deferred
     manual trigger (a run stored now and started later: waiting runs to show, cancel, and
     reconcile with pausing and DAG removal; the schedule already does all of that).
   - **`start` / `end` don't limit manual runs.** They are data of the **scheduled** runs: the
     check is made **by the run's kind**, only for `kind = scheduled`. A manual trigger or a
     backfill may be for any time, before `start` or after `end`, by design: a person chose that
     date (e.g. backfilling December for a DAG deployed in January, without moving `start`, which
     with `catchup=True` would make the scheduler create those runs itself). Fire times before
     `start` are still fire times of the schedule, so such runs get the usual window.
   - **Time zone of `--for`:** a time without an offset is **UTC**, with a warning ("no offset
     given: interpreted as 2026-10-15T03:00Z (UTC)"); an explicit offset (`+02:00`, `-04:00`)
     is accepted. The UTC time shown in the UI can be copied as is (see Web UI and CLI: Times
     and time zones), and `--for` is never ambiguous (UTC has no DST gaps or overlaps; the DST
     rules apply only to schedules). Not chosen: the schedule's time zone (not shown in the UI,
     and ambiguous around DST changes).
   - **The scheduler is independent of manual runs:** it keeps its own bookkeeping (the last
     fire time it created a run for), which manual and backfill runs never change; a manual run
     for a fire time never stops the scheduled run for it (e.g. a `--for` run for a fire time
     missed while the master was down: the scheduler still creates its own run). **Uniqueness:**
     (DAG, `planned_for`) is unique for `kind = scheduled` (a database constraint), so a master
     that crashes after creating a run and restarts can't create the same scheduled run twice;
     manual and backfill runs are exempt (several runs for the same day are allowed).
2. **Backfill:** `schedr runs backfill report --from … --to … [-p …]`: one run (kind
   `backfill`) per fire time in the range, each like `trigger --for <fire time>`.
   - **Window:** one schedule period per run (see point 1), whether or not a scheduled run
     ever happened at that fire time; the runs of a backfill cover consecutive periods with
     no overlap.
   - **Details:**
     - **Fire times that already have a run:** by default only fire times with **no successful
       run** for that `planned_for` (of any kind), evaluated when the backfill is created
       ("31 fire times, 27 already succeeded, creating 4 runs"); `--all` for every fire time.
     - **Creation:** all runs at once, as waiting runs sharing a **backfill ID** (progress in
       the UI; `schedr runs cancel --backfill <id>` cancels what's left); `[backfill] max_runs`
       (default **1000**: a year of a daily DAG, a month of an hourly one; close to
       `[mapping] max_instances`) rejects bigger backfills, suggesting to split the range.
     - **Range:** `--from` / `--to` inclusive, time zone as for `--for` (UTC without an
       offset, with a warning); `--to` optional, a range ending in the future ends at now; no
       fire time in the range is an error; a DAG without a schedule is an error; `start` /
       `end` don't limit it (point 1).
     - **Order:** oldest first, at most `max_active_runs` at a time (competition with scheduled
       runs: point 3).
     - **Parameters:** the `-p` / `--params` values apply to every run, validated once before
       anything is created.
3. **Overlap:** manual and backfill runs **count towards `max_active_runs`**. They are
   **never dropped**: at the limit a run is created and **waits**, unlike a scheduled fire time,
   which is treated as missed (a missed fire time recurs with the schedule and `catchup`
   decides; a person's request doesn't recur). Several runs with the same `planned_for` are
   allowed.
   - **Run state `queued`**, next to `running`, `success`, `failed`, `cancelled`, with the reason
     in the UI ("waiting: `max_active_runs` (1) reached"). Cancelling a queued run ends it as
     `cancelled` without starting any task.
   - **Who gets a freed place: by kind**, then oldest first within each kind: **scheduled** (a
     missed fire time, per the `catchup` rules) → **manual** and **triggered** → **backfill**.
     Production runs are never delayed by people or bulk work; a person who clicked doesn't wait
     behind a long backfill; a backfill uses whatever capacity is left. Not chosen: creation order
     (a click could wait days behind a backfill, and production behind both); manual first (delays
     production for little gain).
   - **No per-backfill override** of `max_active_runs` (e.g. `--max-active 4`): the limit
     usually protects something (a source system, a table being written). To go faster, change
     `max_active_runs` in the DAG.
   - With `max_active_runs=None` nothing waits: every manual and backfill run starts at once
     (a 365-day backfill starts 365 runs, limited only by `max_active_tasks`, worker slots and
     queues; documented).
4. **Identity:** an opaque, globally unique, time-ordered run ID, a **UUIDv7**
   (RFC 9562; Postgres stores it natively as `uuid`), as `dag_ctx.run.id`, never built from the
   date, so re-running a day can't collide. Not chosen: ULID (a community spec; its only
   advantage, a shorter text form, matters little since runs are shown by DAG and date).
   - **Each run records:** `id`, `kind` (`scheduled` / `manual` / `backfill` / `triggered`),
     `planned_for`, `since`, `triggered_at`, **`triggered_by`** (`scheduler`, the parent run for
     `triggered`, or the user name from the command row, the same one recorded for audit and never
     used to allow or deny anything), `backfill_id` (backfill runs), and the parameter values.
   - **UI label:** `sync_orders · 2026-10-15 03:00 · manual by anna` (or `· scheduled`,
     `· backfill 3/31 by anna`); runs with the same `planned_for` are told apart by kind and
     `triggered_at`; the ID is in the run's details, with a copy button.
   - **CLI:** runs are referred to by their **full ID** (`schedr runs cancel 0192c4e1-…`); no
     short prefixes (a UUIDv7 starts with its timestamp, so runs created around the same time
     share long prefixes). `schedr runs list sync_orders --for 2026-10-15T03:00` lists the runs
     for a day with their IDs.
   - **Airflow:** `run_id` → `dag_ctx.run.id`; a DAG parsing Airflow's run ID format
     (`manual__2026-10-15T…`) breaks (in the migration notes; the migration tool reports it
     where it can see it). `dag_run.run_type` → `dag_ctx.run.kind` (`scheduled`, `manual`,
     `backfill`; `asset_triggered` doesn't exist, Assets being out of scope).
5. **Pausing** (`schedr dags pause` / unpause, in the UI too; new DAGs start paused) **stops
   the scheduler from creating runs, and nothing else.**
   - **Runs in progress finish**, including retries, cleanups and resources. To stop work now,
     cancel the runs; the pause dialog offers "pause" and "pause and cancel runs in progress".
     Not chosen: freezing runs in progress until unpause (a frozen run holds its resources, e.g.
     a cluster left running for the whole pause, and continues later with stale data); cancelling
     them (one click away, not the default).
   - **Fire times while paused are treated as missed**, as during a master outage: on unpause
     `catchup=False` gives the most recent one a run, `catchup=True` gives every one a run, in
     order. Before confirming an unpause the UI says how many runs it will create (e.g. the
     first unpause of a new DAG with `catchup=True`: "unpausing will create 290 runs"). Not
     chosen: skipping them for good (would break `catchup=True`'s "every period runs").
   - **Manual runs and backfills of a paused DAG run normally**, with a confirmation in the UI
     ("`sync_orders` is paused; run anyway?"): pausing limits the scheduler, as `start` / `end`
     do. Testing a fix by hand before unpausing is a common need, and new DAGs start paused.
     Not chosen: queueing them until unpause; rejecting them.
   - **Queued runs** (manual or backfill, waiting for `max_active_runs`) keep their place.

### Dependencies between DAGs

"After `extract_crm` finishes, run `load_dwh`", across teams or schedules: a **`trigger_dag`**
task kind, carried out by the **master** (like `empty`: no worker, no slot, no user code).

```python
TriggerDagOperator(
    "load_dwh",
    dag="dwh_load",                    # the target DAG's ID
    params={"region": dag_ctx.params.region},
    wait=True,                         # default: succeed or fail with the target run
)
```

- **The new run:** kind **`triggered`**, linked to the parent run (`triggered_by`; the UI links
  both ways); `planned_for` = the parent run's `planned_for` (same day), `since` by the usual
  rules for the target's schedule (see Runs: manual runs, backfill and pausing). It counts
  towards the target's `max_active_runs` and waits as `queued` with the priority of manual
  runs; it runs even if the target is paused (like a manual run).
- **`params`** may use references (outputs, `dag_ctx`), resolved by the master, and are
  validated against the target's schema when the task runs; invalid values fail the task,
  not retried (certain to repeat).
- **`wait=True`** (default): the task stays `running` until the target run ends, and succeeds
  if it succeeded, **fails if it failed or was cancelled** (the failure propagates). It holds no
  worker slot but counts as a running task for `max_active_tasks` / `max_active*`.
  **`wait=False`**: succeeds as soon as the run is created.
- **Cancelling the parent run** cancels a triggered run it is still waiting for; with
  `wait=False` the triggered run is independent.
- **Output:** the triggered run's ID (`Output[str]`).
- **Checks:** an unknown target DAG ID is a DAG **warning** at validation (the target may be in
  a root not parsed yet); a missing or conflicting target fails the task at run time; a
  trigger whose target is **already in the chain of parent runs** fails the task ("cycle: `a`
  → `b` → `a`"), so runs can't spawn forever.
- Not chosen: leaving it out (merging DAGs, or triggering from task code: no link, no waiting,
  failures don't propagate); waiting on another DAG's run (a sensor: see Open questions,
  Sensors and events).
- **Airflow:** `TriggerDagRunOperator`: `trigger_dag_id` → `dag`, `conf` → `params`,
  `logical_date` → `planned_for` of the new run, `wait_for_completion` → `wait` (Airflow's
  default `False` is set explicitly); `reset_dag_run=True` is an error (no "clear"); non-default
  `allowed_states` / `failed_states` are an error; `poke_interval` is filtered out with a parse
  warning. `ExternalTaskSensor`: see Open questions, Sensors and events.

### Task results and outcomes

Every task, whatever its kind, can report **which kind of success** it had (its **outcome**),
and downstream tasks route on it with **`.on(...)`**. Before this, only `command` / `script`
tasks had outcomes, and a Python task needed an extra `@branch` task to route on its result.
Outcomes are how a user turns the problems they **expect** into success paths (see Outcomes,
failures and joins).

```python
@dataclass
class Rows:
    rows: list[dict[str, str]]

@dataclass
class Unavailable:
    reason: str

@task
def fetch_primary() -> Rows | Unavailable:
    try:
        return Rows(download(PRIMARY))
    except ConnectionError as e:      # expected problem → a success path
        return Unavailable(str(e))

primary = fetch_primary()
mirror = fetch_mirror()               # same signature

load(primary.on(Rows), mirror.on(Rows))        # load(primary: Rows | None, mirror: Rows | None)
report_outage(primary.on(Unavailable))
```

With `NoneFailed`, `load` runs if at least one source returned `Rows` and is skipped if both
returned `Unavailable`; a real failure (timeout, crash) still blocks it and fails the run.

**Result structure.**

- Every finished attempt has a **status** (`success`, `failed`, `timed_out`, …) and, when it
  succeeded, the pair **(output, outcome)**: in the DAG JSON, the `Finished` message and
  storage, whatever the task kind. The output may be `None`. The output size limit applies to
  the output only. Mapped tasks: one pair per instance.
- **Failures are never outcomes.** An outcome is always a kind of success, so "a failure is
  unexpected" still holds.
- **Every task has an outcome enum:** `DefaultOutcome` (single member `SUCCESS`) for ordinary
  return types, `empty` tasks and commands without `outcomes=`; a declared `Outcome` enum
  (`-> Check`, `outcomes=Check`); or an implicit one derived from an outcome union (one member
  per class, each carrying its own data). In the backend an outcome is always a **name** (a
  member's name or a class name) and each task declares the list of its names; the enums and
  classes exist only in the Python API.
- **Users never write the pair.** They write return types, `outcomes=` and `.on(X)`; the
  runner (Python tasks) or the agent (`command` / `script`) builds the pair. Returning tuples
  would be worse to write and to type-check, and would allow nonsense like
  `(Rows(...), "Unavailable")`.
- **The stored outcome name** is shown in the UI (see Web UI and CLI: What the UI shows) and
  **decides which member a stored output is decoded into** (a plain JSON object could fit several
  dataclasses). The name comes from the DAG's declaration; decoding follows the downstream
  annotation (a parameter annotated `Any` gets a plain `dict`), so no code is ever loaded by a
  stored name.
- **Exceptions are never mapped to outcomes**: outcomes are only returned values (Python) or
  declared exit codes (`command` / `script`). An uncaught exception is a failure.

**Python outcomes.**

- **With data: a union of classes.** A return type defines outcomes when it is a **union of
  two or more outcome members**: dataclasses, Pydantic models and `None` (outcome name
  `"None"`). They are the only types the runner tells apart reliably at run time
  (`isinstance`): a `TypedDict` is a plain `dict`, `bool` is a subclass of `int`, `list[int]`
  and `list[str]` are the same class. Any other return type (`int`, `Rows`, `int | None`,
  `int | str`, `Rows | int`) is ordinary data with the single outcome `SUCCESS`; to route on
  such a value, return an outcome instead, or use `@branch`.
- **Without data: an `Outcome` enum**, the same kind as for `command` / `script` tasks:
  `def check() -> Check`, then `.on(Check.CHANGED)`. The member values (exit codes) are ignored
  for Python tasks; the output is `None` and the member is the outcome. The same enum works for
  a Python task and a command task, so a check can be reimplemented (Bash ↔ Python) without
  changing any edge.
- A Python task's outcomes are **either a union of classes or one `Outcome` enum, never
  both**; for "data or nothing", use an empty dataclass (`-> Rows | NoData`) or `None`.
- **No marker is needed:** routing is opt-in, so an outcome union behaves like any other type
  until `.on(...)` is used.
- **Parse errors:** members that are subclasses of one another; the same class with different
  type parameters (`Page[int] | Page[str]`); two members with the same name from different
  modules. Only the top level of the return type counts (`list[Rows | Unavailable]` is data);
  type aliases are resolved.
- Not chosen: classes only (boilerplate, a different style from commands); mixing classes and
  enum members in one union with `Literal[Enum.MEMBER]` (obscure, needs two typed variants of
  `.on()`, adds nothing over an empty dataclass).

What each task produces:

| Task / return type | Outcome names | Stored (output, outcome), examples |
|---|---|---|
| `-> None` | `SUCCESS` | `(null, "SUCCESS")` |
| `-> int` | `SUCCESS` | `(42, "SUCCESS")` |
| `-> int \| None` | `SUCCESS` | `(42, "SUCCESS")`, `(null, "SUCCESS")` |
| `-> int \| str`, `-> Rows \| int` | `SUCCESS` | `(42, "SUCCESS")`, `("abc", "SUCCESS")` |
| `-> Rows` | `SUCCESS` | `({"rows": [...]}, "SUCCESS")` |
| `-> Rows \| None` | `Rows`, `None` | `({...}, "Rows")`, `(null, "None")` |
| `-> Rows \| Unavailable` | `Rows`, `Unavailable` | `({"rows": [...]}, "Rows")`, `({"reason": "..."}, "Unavailable")` |
| `-> Rows \| Unavailable \| None` | `Rows`, `Unavailable`, `None` | also `(null, "None")` |
| `-> Check` (`Outcome` enum) | `CHANGED`, `UNCHANGED` | `(null, "CHANGED")` |
| `command` / `script`, no `outcomes` | `SUCCESS` (exit code 0) | `(<ProcessResult>, "SUCCESS")` |
| `command` / `script`, `outcomes=Check` | `CHANGED`, `UNCHANGED` (from exit codes) | `(<ProcessResult>, "CHANGED")` |
| `empty` | `SUCCESS` | `(null, "SUCCESS")` |
| `@branch` | `SUCCESS` | `(["full_load"], "SUCCESS")` (the chosen task IDs) |

`SUCCESS` is `DefaultOutcome.SUCCESS`.

**Routing.**

- **One edge model:** every edge is "on these outcomes"; a plain reference or plain `>>` means
  "on any outcome". The only other edge kinds are the **selection edge** of a `@branch` task
  and the **requirement edge** added by `uses=` (see Cleanup and resources):

  | Edge kind | Comes from | Counts in the trigger rule |
  |---|---|---|
  | on-outcome (incl. plain `>>` and data) | `>>`, data, `.on(...)` | taken when the upstream's outcome matches |
  | selection | `@branch` | taken when the target was chosen |
  | requirement | `uses=` | never as a success; a failed setup → `upstream_failed`, a skipped setup → skipped |
- **`.on(X)` is both an edge and data:** it makes the downstream task depend on that outcome
  and passes the output **narrowed** to `X` (typed `def on[T](self, kind: type[T]) ->
  Output[T]`, so the narrowing is checked statically). Edges whose outcome didn't happen are
  not taken.
- **`.outcome`** passes the outcome itself as data (`Output[Check]`), for enum tasks and
  `command` / `script` tasks.

Examples, with `f = fetch()` (`-> Rows | Unavailable`), `c = check()` (`-> Check`),
`cmd = CommandOperator(..., outcomes=Check)`, `g` returning `Rows | None`. *Taken* means the
edge's outcome condition was met (a plain reference or plain `>>` is always taken). *Data rule*
when not taken: a `NoneFailed` task passes `None` (the parameter must allow `None`); an
`AllSucceeded` task is skipped instead. If the upstream task itself was skipped, every edge
from it counts as not taken.

| Expression | Edge condition | Static type | Downstream receives when taken | When not taken |
|---|---|---|---|---|
| `x(f)` | any outcome | `Output[Rows \| Unavailable]` | the value, decoded into the member the outcome names | n/a |
| `f >> x` | any outcome | — | nothing (order only) | n/a |
| `x(f.on(Rows))` | `Rows` | `Output[Rows]` | the `Rows` value | data rule |
| `f.on(Rows) >> x` | `Rows` | — | nothing (order only) | data rule (no data involved) |
| `x(f.on(Rows, Unavailable))` | `Rows` or `Unavailable` | `Output[Rows \| Unavailable]` | the value | data rule |
| `x(f.on(Rows).rows)` | `Rows` | `Output[list[...]]` | the field of the `Rows` value | data rule |
| `x(f.rows)` | — | **parse error** | `rows` isn't on every member; narrow with `.on(Rows)` first | — |
| `t"... {f.on(Rows).rows} ..."` | `Rows` | in a t-string | the rendered value | data rule (`None` in a template is an error) |
| `x(c)` | any outcome | `Output[None]` | `None` (an enum task has no output) | n/a |
| `c.on(Check.CHANGED) >> x` | `CHANGED` | — | nothing | data rule (no data involved) |
| `x(c.outcome)` | any outcome | `Output[Check]` | the member, e.g. `Check.CHANGED` | n/a |
| `x(cmd.on(Check.CHANGED))` | `CHANGED` | `Output[ProcessResult]` | the `ProcessResult` | data rule |
| `x(cmd.on(Check.CHANGED).stdout)` | `CHANGED` | `Output[str]` | stdout (an error if truncated) | data rule |
| `x(cmd.outcome)` | any outcome | `Output[Check]` | the member | n/a |
| `x(g.on(None))` | `None` | `Output[None]` | `None` | data rule (useful only for order) |

Rules:

- **Trigger rules count edges, not tasks:** an edge not taken counts as skipped, so
  `x(f.on(Rows), f.on(Unavailable))` runs under `NoneFailed` and is skipped under
  `AllSucceeded`.
- **A path on an outcome union needs narrowing first:** `f.rows` is a parse error,
  `f.on(Rows).rows` is fine (the parse-time path check: a field must exist on the referenced
  type).
- **`.outcome` on a union task is a parse error:** its type would be an invented
  `Literal["Rows", "Unavailable"]`, and the value's class already says it. On a plain task it
  is always `DefaultOutcome.SUCCESS`.
- **`.on()` accepts only members of the task's own outcome enum**, otherwise a parse error
  (`.on(Other.CHANGED)`, `f.on(Check.CHANGED)`, `f.on(Unrelated)`). Such an edge could never be
  taken: the downstream task would be skipped in every run, silently. Python itself catches
  only misspelt members, not a wrong enum or an unrelated class.
- **`.on(DefaultOutcome.SUCCESS)`** follows from the previous rule: allowed on a task whose
  enum is `DefaultOutcome` (same as a plain reference; useful for generated edges, e.g. the
  Airflow translation), a parse error on a task with another enum, even one with a member named
  `SUCCESS` (membership is checked, not names).

**`@branch` and outcomes.**

- Outcomes cover "route on what happened" (exactly one outcome per result, routing in the DAG
  structure); `@branch` covers what outcomes can't: choosing **any subset** of the direct
  downstream tasks in the function's code. The documentation recommends **outcomes first**.
- A branch task's result is `(list of chosen task IDs, "SUCCESS")`; its outgoing edges are
  **selection edges**, taken when their target is in the list. `.on(...)` on a branch task is a
  parse error.
- Airflow's `@task.branch` / `BranchPythonOperator` translate onto `@branch` (they can select
  several targets, so they can't translate onto outcomes).
- Not chosen: several outcomes per result (would break "one outcome per result": `.outcome`,
  decoding by outcome name, the tables above); removing `@branch` (Airflow's `@task.branch`
  would become an error by the principle in Thin translating classes).

**Mapped tasks.** Each mapped instance has its own (output, outcome) pair.

- **Element by element inside `.expand()`: allowed.** `check.expand(r=results.on(Rows))` gives
  `check[i]` the output of `process[i]` when its outcome was `Rows`; otherwise the edge to
  `check[i]` isn't taken and `check[i]` is skipped. Map-index alignment is kept (the number of
  instances doesn't change).
- **On the whole mapped output: a parse error for now.** `x(results.on(Rows))` (with `x` not
  mapped) has no single clear meaning (filter the list and lose the alignment with map
  indexes; replace non-matches with `None`; take the edge only if all instances matched). To
  collect them, write a task that takes the whole list and filters it. The same applies to
  `.outcome`.
- A downstream task taking the whole list of a mapped task whose instances can be skipped gets
  **`None` for skipped instances**, in map-index order, so the element type must allow `None`
  (`list[Rows | None]`), as for any skippable data.

### Cleanup and resources

Work that must run **whatever the result** of the tasks before it (releasing a lock, deleting
temporary files, stopping something that was started): `finally`, not `except`. Outcomes cover
the problems a user expects (see Task results and outcomes); cleanup is for what is left behind,
including after unexpected failures, and sees neither outcomes nor failures.

**Principles** (consistent with "no failure branches", see Outcomes, failures and joins):

1. A cleanup runs when what it cleans up after has finished, **however it ended** (including
   timeouts, memory kills and lost workers, which only the master sees).
2. It **never changes the result**: a failure stays a failure, the run still fails and
   notifications are still sent.
3. It **can't see the failure**: no failure status, no routing on it (otherwise it becomes error
   handling).
4. Its own failure is an unexpected failure: it **fails the run** (see Run result).

**Two layers**, solving different problems:

| | Per-task cleanup | Resource |
|---|---|---|
| Cleans up | what **one attempt** started | what **several tasks** share |
| Runs | after every attempt, however it ended | once, after its **last user** is done, if its setup succeeded |
| Where | the same worker (local disk) | any worker |
| In the graph | no (shown inside the task, e.g. a dropdown or submenu) | no (shown in the run's Resources panel; selecting it highlights its users) |
| Receives | the task's inputs, run information, the attempt's handles | the setup's output, run information |
| Typical use | cancel a remote query after a timeout; delete temporary files | delete a cluster; release a lock held by several tasks |
| In Airflow | `on_kill()` | setup / teardown, `all_done_setup_success` |

Neither is a node of the graph, so neither is ever a leaf. Airflow translation: see Airflow
compatibility (Cleanup and resources).

#### Per-task cleanup

`@task(cleanup=...)`, `CommandOperator(..., cleanup=[...])`: `finally` for one task. It covers
what schedr itself interrupts: when the agent kills a task (timeout, memory watchdog, cancel,
drain), work it started outside its process (a remote query, a Spark job) keeps going, and the
task's own `try`/`finally` can't run. Each attempt cleans up after itself, so a retry starts
clean.

**What it receives: a real `finally`.** The cleanup function gets the task's **inputs**, the
**run information**, and **every handle the attempt registered**, in registration order. It
never gets the output or the status.

- **Handles** carry what only existed during the attempt (e.g. a remote job ID; a killed task
  has no output). `cleanup_handle(x)` **only registers** ("clean this up when the attempt
  ends"), like Go's `defer`; it is sent to the agent at once and journaled. Handles are **never
  released**: the task doesn't clean up after itself, all cleanup logic is in the cleanup
  function.
- So the cleanup receives **the same list on success and on failure** and does the same thing;
  a shorter list only means the attempt stopped earlier, not why. Cleanup actions must be
  **idempotent** (e.g. `cancel_if_running`); after a success they usually have nothing to do.
  Anything that must outlive the attempt isn't registered.
- **Typing:** the cleanup declares `handles: list[ExportJob | TempDir]`; checked at parse time
  (the cleanup's parameters are the task's inputs plus `handles`; the element type follows the
  outcome-union rules) and at registration (a handle of another type fails the attempt at
  once). `cleanup_handle` accepts any dataclass, so mypy can't check the match. Handles are
  stored with their class name, like (output, outcome).
- Not chosen: releasing a handle when the task finished normally (the cleanup would then get
  handles only after a failure and behave like an `except` block, against principle 3); an
  injected, typed `Cleanup[H]` parameter (too awkward for `@task`'s typing).

```python
def undo_export(day: date, handles: list[ExportJob | TempDir]) -> None:
    for h in handles:
        match h:
            case ExportJob(job_id=j):
                warehouse.cancel_if_running(j)      # no-op if the job finished
            case TempDir(path=p):
                shutil.rmtree(p, ignore_errors=True)

@task(cleanup=undo_export)
def export(day: date) -> Exported:
    tmp = tempfile.mkdtemp()
    cleanup_handle(TempDir(tmp))
    job = warehouse.start_export("orders", day)
    cleanup_handle(ExportJob(job.id))
    job.wait()
    rows = compress_and_upload(tmp, f"s3://bucket/orders/{day}")
    return Exported(f"s3://bucket/orders/{day}", rows)
```

**Where and when it runs.**

- **Same worker, same slot:** the agent starts the cleanup as soon as the attempt's process has
  ended, for whatever reason (`<python> -m schedr.runner cleanup` for a Python task, the
  cleanup command for a `command` / `script` task), with the same memory budget and watchdog.
  The slot is freed only after the cleanup. Same worker, so the same local disk.
- **The attempt finishes after its cleanup,** as a function call returns only after its
  `finally` block: one `Finished` message carries the task's and the cleanup's results;
  downstream tasks and retries start only then (task instance state `cleaning_up` meanwhile).
  Not chosen: reporting the task's result at once and cleaning up in parallel (a retry or
  downstream task could meet the previous attempt's resources, e.g. a lock not yet released).
- **`cleanup_timeout`** per task (and in `TaskDefaults`), server default **5 minutes**. On
  timeout the agent kills the cleanup (SIGTERM, then SIGKILL): that cleanup attempt failed.
  Cleanup retries run on the same worker.
- **Cancel:** the agent kills the task, then runs the cleanup (a cancelled export should cancel
  its remote job too). **Drain:** the agent stops the remaining tasks early enough to leave room
  for their cleanups (at the end of the grace period minus the longest `cleanup_timeout` among
  them); if the pod is killed anyway, it is a lost worker.
  - Kubernetes' default `terminationGracePeriodSeconds` (30 s) is far too short for that: the
    **Helm chart sets 600 s** for workers (5 min for tasks, then 5 min for cleanups). The worker
    is told its grace period (`[worker] termination_grace_period`, matching the pod spec) and
    logs a warning at startup when a task's `cleanup_timeout` doesn't fit in it.
- **Agent restart on the same pod** (journal kept): the agent runs the cleanups of the
  interrupted attempts from its journal.
- **Lost worker** (pod gone, node failure): handles are also **sent to the master** when
  registered, so they survive the pod. The cleanup runs on another worker only if the task opts
  in with **`cleanup_anywhere=True`** (idempotent isn't the same as safe on another pod:
  `rmtree("/data/orders")` there could delete another task's files; only the author knows
  whether the cleanup depends on where it runs). Without the opt-in the cleanup is recorded as
  **"didn't run: worker lost"**, which counts as a failed cleanup: the run fails even if the
  task's retry succeeds.

| Situation | Where the cleanup runs |
|---|---|
| The task ends (success, failure, timeout, OOM, cancel) | the same worker, at once, in the same slot |
| Drain | the same worker; tasks stopped early enough to leave time for it |
| The agent restarts on the same pod | the same worker, from the journal |
| The pod is lost, `cleanup_anywhere=True` | another worker, with the handles from the master |
| The pod is lost, no opt-in | not run → "didn't run: worker lost" → the run fails |

**A failed cleanup is a separate failure; the task keeps its own result.**

- The task's result stands: on success its outcome and output are kept and downstream tasks
  run. The UI shows e.g. "success · cleanup failed" (the cleanup result is an attribute of the
  task instance).
- The cleanup has **its own retries** (count and delay configurable, separate from the task's);
  being idempotent, it is always safe to run again.
- After its last retry it **fails the run** (principle 4) and triggers the failure notifications
  ("cleanup of `export` failed", with its error summary).
- If the attempt failed too, both failures are recorded; the task's retry goes ahead after the
  cleanup has ended.
- Not chosen: failing the task (throws away a result that was delivered, retries the work
  instead of the cleanup); success with only a warning (a leak nobody acts on; contradicts the
  rationale of Outcomes, failures and joins).

**`command` / `script` tasks** (commands: see In-task CLI).

- **The cleanup is of the same kind as the task:** a command for `CommandOperator`
  (`cleanup=["./undo_export.sh"]`, same rules as `command`), a script for a script job
  (`[jobs.<name>.cleanup]` with `script`, `interpreter`, `args`). It gets the same environment
  as the task (`SCHEDR_*` run variables, the task's `env`, its declared `secrets`, working
  directory); its arguments may use t-strings. `ScriptOperator` can override its `args` /
  `env`. (A Python cleanup for a command task: possibly later.)
- **Registering a handle:** `schedr task-ctx cleanup add ExportJob job_id="$job_id"` (or
  `--json '{...}'`), over the agent socket, like the Python runner; **synchronous**: when it
  returns the handle is journaled (and forwarded to the master); on failure it exits non-zero.
- **Reading the handles** in the cleanup command, with the same tool:
  `schedr task-ctx cleanup list ExportJob --field job_id` (one value per line), or JSON Lines
  (the default, in registration order). No `jq` needed, no size limit, nothing in the
  environment or on disk; works unchanged with `cleanup_anywhere`.

  ```bash
  # export.sh
  job_id=$(warehouse start-export orders "$1")
  schedr task-ctx cleanup add ExportJob job_id="$job_id"
  warehouse wait "$job_id"

  # undo_export.sh
  schedr task-ctx cleanup list ExportJob --field job_id |
    while read -r id; do warehouse cancel-if-running "$id"; done
  ```

- **Typing:** optional `cleanup_handles=["ExportJob", "TempDir"]` (TOML:
  `cleanup_handles = [...]`); registering another type name then fails at once.
- Not chosen: appending JSON lines to a file named by an environment variable (not synchronous,
  a handle can be lost if the pod dies before the agent reads it, quoting JSON in shell);
  passing the handles to the cleanup in an environment variable (128 KiB limit per string,
  readable through `/proc/<pid>/environ`, needs `jq`) or in a JSON Lines file (needs `jq`, a
  second mechanism next to the CLI).

#### Resources

A resource is created by a **setup** task, used by **users**, and cleaned up once by its
**cleanup** when every user is done: freed as soon as its last user finishes, not at the end of
the run, and no scope is computed from the graph (like reference counting).

```python
with Dag("etl", schedule=DAILY):

    @resource                              # the setup: creates the resource
    def cluster(size: int) -> ClusterInfo:
        return cloud.create_cluster(size)

    @cleanup(cluster)                      # how to clean it up
    def delete_cluster(c: ClusterInfo) -> None:
        cloud.delete_cluster_if_exists(c.cluster_id)

    @task(uses=[cluster])                  # a user that doesn't need its data
    def warm_cache() -> None: ...

    @task
    def transform(c: ClusterInfo) -> Parts: ...

    @task
    def load(c: ClusterInfo, parts: Parts) -> int: ...

    c = cluster(size=8)                    # Output[ClusterInfo]; the setup runs once
    warm_cache()
    load(c, transform(c))                  # taking a resource's output makes a user
```

- **`@resource`** marks the setup: an ordinary task (options as for `@task`) whose return value
  is the resource. Its output follows the usual output rules; an outcome union as a setup's
  return type is a parse error for now.
- **`@cleanup(resource)`** marks the cleanup; it receives **the setup's output** and the run
  information, nothing else (no other outputs, no statuses). Options: timeout, retries, retry
  delay, queue. Exactly one cleanup per resource (parse error otherwise).
- **Users:** a task **taking a resource's output** as data is a user (only a resource's output
  counts; not general inference from data flow); a task needing the resource but not its data
  declares **`uses=[cluster]`** (also on operators and groups, not in `TaskDefaults`: too easy
  to make every task a user). The setup is implicitly a user. Users depend on the setup, so
  they never start before the resource exists.
- **`uses=` adds a requirement edge** (see Task results and outcomes: edge kinds), which can
  only stop a user, never make it run: setup failed → the user is `upstream_failed`; setup
  skipped → the user is skipped; setup succeeded → **no effect** on the user's rule (evaluated
  on its other edges; a user with no other edges runs). It still orders the tasks. Without
  this, a user skipped by a branch or an untaken `.on()` edge would run under `NoneFailed`
  because the setup succeeded. A **data** edge from the setup (`transform(c)`) stays an
  ordinary edge. Under `AllSucceeded` nothing changes.
- **Typing:** `@cleanup(cluster)` takes its type from the resource, so mypy checks that the
  cleanup accepts the setup's return type; parse time checks it too.

  ```python
  class Resource[**P, T]:                # called like a task
      def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Output[T]: ...

  def resource[**P, T: Json](fn: Callable[P, T]) -> Resource[P, T]: ...

  def cleanup[T](r: Resource[..., T]) -> Callable[[Callable[[T], None]], Cleanup[T]]: ...
  ```

- **No handles for resources:** the setup's return value is what the cleanup gets. A setup that
  fails halfway (cluster created, then a crash before `return`) is handled by the setup's own
  **per-task cleanup** (`cleanup=` and `cleanup_handle(...)` on the setup task). Handles are
  for attempts, setup outputs for resources.
- **Done** means every user is in a final state: `success`, `failed` after its last retry,
  `upstream_failed`, skipped, cancelled (a task that will never run counts as done; a failed
  attempt with retries left doesn't; a mapped user is done when all its instances are).
- **The cleanup runs once all users are done, if the setup succeeded** (`try: create → use →
  finally: delete`: the `try` starts only once creation succeeded). It then receives the setup's
  output, never `None`, and must be idempotent. A deterministic name (e.g. an S3 prefix from
  the run ID) is simply returned by the setup. Not chosen: running every time with `T | None`
  (puts `None` in every cleanup's type for a check that almost never matters).
- It runs on **any worker**, so it must not depend on location; local leftovers belong to the
  per-task cleanup. Its own retries, then it **fails the run** (see Run result). Resource
  instances have their own states (see Task assignment: task instance states).
- **Resources without a value** (a static resource: a fixed lock, a well-known bucket prefix):
  the setup returns `None` (`@resource def lock() -> None`). The cleanup then takes **no
  parameter** (`@cleanup(lock) def release() -> None`; an overload of `cleanup` for
  `Resource[..., None]` keeps it typed); users declare `uses=[lock]`. No ambiguity with a
  failed setup: the cleanup runs only if the setup succeeded.
- **A resource without setup code:** `tmp = Resource("tmp-prefix")`; its setup is completed by
  the master (like an `empty` task, no process), its value is `None`, the cleanup takes no
  parameter, users declare `uses=[tmp]`. Use: "once these tasks are done, delete
  `s3://tmp/<run_id>/`".

  ```python
  tmp = Resource("tmp-prefix")

  @cleanup(tmp)
  def drop_tmp() -> None:
      s3.delete_prefix(f"tmp/{dag_ctx.run.id}/")

  @task(uses=[tmp])
  def stage() -> None: ...
  ```

- **Setups and cleanups as commands or scripts.** Any setup kind works with any cleanup kind
  (exactly one cleanup per resource).

  ```python
  cluster = CommandResource(
      "cluster",
      command=["./create_cluster.sh", "--size", "8"],
      value=ClusterInfo,                    # the type of the reported value
  )
  CommandCleanup(cluster, command=["./delete_cluster.sh", t"{cluster.cluster_id}"])
  load(cluster, transform(cluster))         # users receive a ClusterInfo
  ```

  ```bash
  # create_cluster.sh
  id=$(cloud create-cluster --size "$2")
  schedr task-ctx resource set cluster_id="$id" region=eu   # synchronous, validated at once
  ```

  - **Setup: `CommandResource` / `ScriptResource`** (`job=` a declared script), operator forms
    of `@resource`, with a declared **`value=` type**. The command reports the value with
    **`schedr task-ctx resource set`** (see In-task CLI) over the agent socket; validated
    against `value` at once.
  - The setup **succeeds only if it exits with 0 and reported a value**; a missing value is a
    setup failure ("resource value not reported"), so the cleanup never gets `None`. Without
    `value=`, the resource has no value (type `None`) and nothing is reported.
  - **Users receive the value** (`Output[ClusterInfo]`), not the `ProcessResult` (which stays in
    the log): they can't tell which kind of setup made the resource.
  - **Cleanup:** `@cleanup(cluster)` (Python, value as a parameter, decoded from the reported
    value), **`CommandCleanup(cluster, command=[...])`**, **`ScriptCleanup(cluster, job=...,
    args=[...])`**. Command and script cleanups receive the value **through t-strings**: the
    resource object stands for its value (`t"{cluster.cluster_id}"`, rendered by the agent,
    paths checked at parse time against the value type); a script can also read it with
    `schedr task-ctx resource get --field cluster_id`.
  - A setup that fails halfway: the setup's own per-task cleanup (`cleanup=`,
    `schedr task-ctx cleanup add`), as for Python.
  - Not chosen: the setup prints JSON on stdout for schedr to parse (stdout mixes with whatever
    the tools print, brings stdout parsing into the native API, a malformed value is noticed
    only when the cleanup needs it); giving the cleanup the whole `ProcessResult` (every
    cleanup would dig the ID out of text).
- A per-run cleanup is a resource used by every task (shorthand such as `Dag(cleanup=...)`
  possible later).
- Not chosen: a setup–cleanup pair with a scope computed from the graph between two tasks; a
  cleanup for a group, the whole run or an explicit task list (none of them knows whether
  anything was created); a `Resource(...)` object with a handle list.

### Output serialization

Task outputs are JSON (with tagged extra types, below); there is no pickle. Three checks make
sure an output can be serialized, from earliest to last:

1. **Type checker (editing time).** `@task` only accepts functions whose return type is
   JSON-compatible, via a recursive type alias bound on its type variable:

   ```python
   type Json = (
       None | bool | int | float | str | datetime | date | timedelta
       | Sequence[Json] | Mapping[str, Json]
   )

   def task[**P, R: Json](fn: Callable[P, R]) -> Callable[P, R]: ...
   ```

   `Sequence`/`Mapping` (not `list`/`dict`, which are invariant) so that e.g. `list[int]` is
   accepted. Verified with `mypy --strict`: `set[int]`, `dict[int, str]` and custom classes are
   rejected. Only as good as the user's tooling, and `Sequence` lets a few wrong types through
   (e.g. `bytes`), so it is a help, not the guarantee.
2. **Parse time (the guarantee).** The runner reads the return annotation
   (`typing.get_type_hints()`) and walks it recursively against the supported types: `None`, `bool`,
   `int`, `float`, `str`, `datetime`, `date`, `timedelta`, `list`, `tuple`, `dict[str, …]`,
   `Literal`, unions/`Optional`, `TypedDict`, dataclasses and Pydantic models (see Dataclasses and
   Pydantic models below). An unsupported type is an **import error for the file**, naming the task
   and the offending part (e.g. "task `extract`: return type `dict[int, str]`: output keys must be
   `str`"); the DAG is not registered. The same type model checks **inputs**: passing an `Output[T]`
   to a parameter whose annotation doesn't accept `T` is a parse error.
3. **Run time (the actual value).** The runner encodes the returned value **guided by the
   annotation** and fails the attempt (`failed`) on: unsupported types (`Decimal`, `set`,
   custom objects), non-`str` dict keys, `NaN`/`inf`, values not matching the annotation, or
   values over the output size limit. The error summary names the exact location (e.g. "output
   of `extract` not serializable at `[3].created`: `Decimal` is not supported").

**Tagged extra types.** `datetime`, `date`, `timedelta` and `tuple` are stored tagged, e.g.
`{"$datetime": "2026-10-19T03:00:00+00:00"}`, not as plain strings, so Rust can render them
correctly in t-strings without Python, and a downstream task annotated `Any` still gets the
real type back.

**One output per task; references can point inside it.** There is no separate "multiple
outputs" concept in the native API. A task has **one output value**, stored once per (DAG run,
task, map index). A reference can address a part of it: `out["rows"]` (dict / `TypedDict`
key), `out.email` (dataclass / Pydantic field), `out[0]` (list / tuple index). In the DAG JSON a
reference is plain data (task + path, e.g. `{"ref": "output", "task": "extract", "path":
["rows"]}`); the master extracts that part from the stored value when it builds the downstream
task's inputs, so downstream tasks receive only what they use. The same reference works as a
task argument, inside a t-string (rendered by Rust without Python) and in `.expand()`.

- Paths are **checked at parse time** against the annotations (unknown field or `TypedDict` key
  → parse error). Only free-form `dict[str, …]` keys can't be checked; a missing key fails at
  run time with a clear error.
- Indexing loses the **static** type (`Output[Any]`): Python's type system can't express it for
  a generic wrapper. The parse-time check still uses the real annotations.
- The size limit applies to the **whole** value.
- The Airflow layer's `multiple_outputs=True` and `output["key"]` map onto this.

**Outputs of `command` and `script` tasks: `ProcessResult`.** Following the one-output rule,
their output is a `ProcessResult` (a dataclass in the `schedr` package): `stdout: str`,
`stderr: str`, `exit_code: int`, `stdout_truncated: bool`, `stderr_truncated: bool`.

- `listing.output` is an `Output[ProcessResult]`; `listing.stdout` / `listing.stderr` are
  shortcuts for the paths `.output.stdout` / `.output.stderr` (`Output[str]`).
- A downstream `@task` can take the whole result (`res: ProcessResult`) and gets the truncation
  flags and exit code explicitly.
- "Reading a truncated output is an error" applies to `.stdout` / `.stderr` references; a task
  taking the whole `ProcessResult` has opted in and sees the flags instead.
- No automatic parsing of stdout (JSON, CSV, …): a downstream `@task` parses it and gives it a
  proper type.
- In the Airflow layer, `.output` stays the last line of stdout (not a `ProcessResult`),
  produced by a hidden native task (see Thin translating classes).

**Output size limits.**

- Each task has an **output size limit, default 1 MiB** (encoded JSON for Python return values;
  per stream for stdout/stderr).
  - A Python task's return value over the limit: the attempt **fails** (partial JSON is
    useless).
  - stdout/stderr over the limit: **truncated** to the last part and marked truncated; a
    downstream `.stdout` / `.stderr` reference to it gets an error.
- A DAG author can **raise the limit per task** (or in `TaskDefaults`) with **`max_output_size`**
  (bytes; constants `KiB`, `MiB` in `schedr`, e.g. `@task(max_output_size=8 * MiB)`), but
  **never above the server-wide maximum**. A task asking for more is rejected when the master
  validates the DAG (shown as a DAG error in the UI).
- Configuration:
  - `[outputs] default_max_size`, default `"1MiB"`;
  - `[outputs] max_size_limit`, default `"16MiB"` (env `SCHEDR_CONFIG__OUTPUTS__MAX_SIZE_LIMIT`).
- `max_size_limit` above the **cap on total inputs per task** (default 64 MB) is a **startup
  error**: an output that big could never be passed to a downstream task.
- The **4 MB inline threshold** in `AssignTask` is unchanged (bigger inputs are downloaded in
  chunks).

**Outputs of mapped tasks.** Each mapped instance has its own output (stored per run, task,
map index). A reference to the mapped task's output as a whole, e.g.
`sizes = process.expand(path=files)`, is an `Output[list[T]]`:

- A task that isn't mapped receives a **plain list** of the instances' outputs, in **map-index
  order** (the order of the input list, not completion order), so the result is deterministic.
  No lazy loading: the list counts against the receiving task's **total-inputs cap**; huge
  fan-ins are not a goal.
- If any instance fails (after retries), the receiving task becomes `upstream_failed`; it never
  sees a partial list.
- **Empty expansion:** zero instances means the mapped task **succeeds** and receivers get `[]`
  (not skipped, unlike Airflow).
- Mapping over a mapped output maps **element by element**: `check.expand(size=sizes)` gives
  `check[i]` the output of `process[i]`.
- **Paths on a mapped output only index the list:** `results[0]`, `results[0].rows`.
  A field path applied to every element (`results.rows`) is **always an error**; to collect a
  field from all instances, write a small task that gathers it.

**Missing outputs.** In the native API a downstream task never runs after a failed upstream
(`upstream_failed`), so an output can only be missing because its task was **skipped**. A
parameter taking data from a task that can be skipped must allow `None` (parse-time check) and
receives `None` (see Outcomes, failures and joins).

**Retention.** Outputs are stored with their DAG run and **deleted together with it**, or
earlier if `[retention] outputs` is shorter (see State storage: Retention).

**Dataclasses and Pydantic models** are supported as output and input types.

- **Pydantic models: only when Pydantic is installed** (detected from annotations; extra
  `schedr[pydantic]`). Never a required dependency: the `schedr` package shares the user's
  environment and stays **dependency-free**, and importing Pydantic would add start-up time to
  every task. schedr hands validation to Pydantic (`model_dump(mode="json")` /
  `model_validate`), so **Pydantic's own rules** apply (lax by default, strict if the model
  says so).
- **Dataclasses** (standard library):
  - Encoded field by field (an `asdict()`-like walk guided by the annotations); decoded by
    calling the constructor with keyword arguments, recursively, guided by the **downstream
    annotation**. **No class name is stored**: the stored value is a plain JSON object, so
    renaming or moving a class doesn't break lookups and no code is loaded by a name taken
    from stored data; a downstream task annotated `Any` gets a plain `dict`.
  - Supported: plain fields, defaults, `frozen`, `slots`, `kw_only`, inheritance.
    `field(init=False)` and `InitVar` are a parse error for now.
  - **Field types are checked recursively at parse time**: an unsupported field type is an
    import error naming the field (e.g. "`Customer.tags`: `set[str]` is not supported"). `Any`
    on a field is allowed as an explicit opt-out (run-time check only).
  - **Field values are checked strictly** at run time, on output and again on input, with **no
    coercion**: `"5"` for `int` and `True` for `int` are errors; `int` for `float` is allowed.
    A mismatch on input means the class changed between runs (e.g. a retry after a code
    change) and fails loudly.
  - Type checker: a `DataclassInstance` protocol in the `Json` alias makes dataclass return
    types accepted (verified with `mypy --strict`); unsupported field types are caught at parse
    time, not by the type checker.

## Testing DAGs

For a DAG author on a laptop or in CI, with `pip install schedr` (the wheel includes the binary)
and no cluster. Four levels, from cheapest to fullest.

- **Static check: `schedr dags check <path>`** (see Upgrades and compatibility): parses DAG
  files and `schedr.toml` files, reports import errors and parse warnings; non-zero exit code
  on errors.
- **Local version vs the workers:** a DAG written with a newer local `schedr` than the worker
  image may use a feature the workers don't have (an import error in the UI, never silent).
  Documented: pin the local `schedr` to the worker image's version in the DAG repository's dev
  dependencies (e.g. `schedr==1.4.*`). `schedr dags check --against <server URL>` asks the
  server which worker versions are running and warns when the DAGs need a newer one.
- **Unit tests of task functions.** Outside `with Dag(...)` a `@task` function is **the plain
  function**: calling it runs it and returns the real value (inside a DAG the same call returns
  an `Output[...]`). For code reading the run context or registering handles,
  **`schedr.testing.task_context(...)`** sets `dag_ctx` (`planned_for`, `since`, `params`, …),
  declared secrets and a handle collector; pure Python, no processes:

  ```python
  with task_context(planned_for=datetime(2026, 10, 15, 3, tzinfo=UTC),
                    params=ReportParams(region="us"),
                    secrets={"etl/db#password": "x"}) as ctx:
      export(date(2026, 10, 15))
  assert ctx.handles == [TempDir(...), ExportJob("j-1")]
  ```

  Outside a task and outside `task_context`, `dag_ctx` / `cleanup_handle` fail as decided ("must
  be run inside a schedr task").
- **One whole run: `schedr dags test <dag> [--for <time>] [-p …] [--secret 'path#key=value']
  [--task <id>]`.** One **real run** in standalone mode with a temporary SQLite database: every
  task is its own process, exactly as in production (runner, rendering, quoting, masking,
  outputs, outcomes, routing, cleanups, resources). The terminal shows each task's state,
  outcome and duration, logs streamed with a task-name prefix, outputs as JSON at the end; exit
  code 0 if the run ended `success`, 1 if `failed`. `--task <id>` runs that task and its
  upstream tasks only. Not chosen: running the tasks in one Python process like Airflow's
  `dag.test()` (faster, but skips what differs in production).
- **From Python: `schedr.testing.run_dag(path, dag_id, for_=..., params=..., secrets=...)`**
  starts the same thing as a subprocess. Every task can be inspected and asserted separately:

  ```python
  result = run_dag("dags/sync.py", "sync_orders", for_=datetime(2026, 10, 15, 3, tzinfo=UTC))
  assert result.state == "success"
  fetch = result.tasks["fetch_primary"]
  assert fetch.outcome == "Rows" and len(fetch.output.rows) == 120
  assert result.tasks["report_outage"].skip_reason == "edge not taken: on Unavailable"
  load = result.tasks["load"]
  assert [a.status for a in load.attempts] == ["failed", "success"]
  assert "inserted 120" in load.log and load.cleanup.result == "ok"
  ```

  - **Per task:** `state`, `outcome`, `output` (decoded by the task's annotation, as a
    downstream task would; `output_json` raw), `skip_reason`, `attempts` (status, exit code,
    duration, peak memory, error summary), `log`, `cleanup` (result, attempts, log),
    `handles`. **Mapped tasks:** `result.tasks["process"][3]`, `len(...)`, `.outputs` (map-index
    order). **Resources:** `result.resources["cluster"]` (value, state, cleanup log). **The
    run:** `state`, `planned_for`, `since`, `params`, the one-line reason of its result.
  - **Step by step:** `with run_dag(..., step=True) as run: for task in run: ...` yields each
    task as it reaches a final state; the master **starts no new task until the test
    continues**, so a test sees each result (and outside state, e.g. "the file is in the
    bucket") before any downstream task runs. Parallel tasks still start together and are
    yielded one at a time, in the order they finish. Leaving the loop (`break`, a failed
    `assert`) **cancels the run**; cleanups and resource cleanups still run. Implemented as a
    test-only setting of the standalone master ("hold assignments until released"), driven
    through the command mechanism; no hooks, no user code in the master.
  - **Replacing a task's result:** `run_dag(..., replace={"fetch_primary":
    Unavailable("down")})`: the master completes that task with the given value, like an
    `empty` task; the value must match the task's return type and its outcome follows from the
    value. Tests routing (`.on(...)`) without network access. Only in `run_dag`, never in a
    deployed DAG.
- **Secrets locally:** `--secret 'path#key=value'` / `run_dag(secrets={...})` gives values
  directly (in memory, masked as usual). For standalone used as a dev environment, a **`file`
  secret storage** (`type = "file"`, a local TOML file of paths and values), one more
  implementation behind the generic abstraction; it **refuses to start outside standalone**, so
  it can't reach production.
- **Airflow:** `dag.test()` runs `run_dag` for that DAG (`conf` → `params`, `logical_date` →
  `for_`).

## Airflow compatibility

### Approach: look-alike API under the `schedr.airflow` namespace

- schedr is **not** Airflow and does not depend on `apache-airflow`.
- The Python API **copies Airflow's names, signatures and module layout** so existing DAG
  code needs as few changes as possible. **Behaviour underneath is entirely ours.**
- Migrating a DAG = **changing its imports only**. Everything below the imports stays the same.

### Thin translating classes

The Airflow layer consists of **thin classes that translate onto the native API**; they don't
build the model themselves.

**Principle: the Airflow layer can only use what the native API has.** No Airflow-only
structures, rules or behaviours: everything a compatibility layer produces must be expressible
in the native API **as it is**. Otherwise people would use the Airflow API just to get
functionality they can't express in the native API, and the native API would stop being the
primary one.

- **Allowed:** different *syntax* for the same thing: Airflow's names, signatures and argument
  types, the `@dag` decorator, task IDs as strings, `default_args` dicts, combinations of
  native constructs (e.g. an extra `empty` task or dropped edges, which a native DAG could
  write by hand).
- **Not allowed:** a model rule, task kind, edge type or run-time behaviour that the native API
  doesn't offer. An Airflow feature that needs one is either **added to the native API first**
  (then translated onto it) or is an **error** in the Airflow layer.
- **The native API is never changed to accommodate Airflow**, only the other way round: an
  Airflow feature maps onto what the native API has (directly, or as a combination of native
  constructs), or it is an error. A native feature is added only for a native need.
- Applies to every compatibility API, not only `schedr.airflow`.
- Earlier decisions that didn't follow it were all reviewed and brought in line.

- **Arguments with a native equivalent are translated**, including renamed ones and different
  types. Examples:

  | Airflow | Native |
  |---|---|
  | `DAG(dag_id=..., ...)` | `Dag("...", ...)` |
  | `schedule="0 3 * * *"` / `"@daily"` / `timedelta(...)` / `"@once"` / `None` | `Cron(...)` / `DAILY` / `Every(...)` / `Once(start_date)` / `None` |
  | `start_date=` / `end_date=` | `start=` / `end=` |
  | `max_active_runs=` (default 16, Airflow's `max_active_runs_per_dag`) | `max_active_runs=` (set explicitly to 16 when not given, so migrated DAGs keep Airflow's default) |
  | `tags=`, `description=` | `tags=`, `description=` |
  | `max_active_tasks=` (default 16, `max_active_tasks_per_dag`) | `max_active_tasks=` (set explicitly to 16 when not given) |
  | `max_active_tis_per_dagrun=` / `max_active_tis_per_dag=` | `max_active_per_run=` / `max_active=` |
  | `default_args={...}` | `defaults=TaskDefaults(...)` (keys translated the same way) |
  | `BashOperator(bash_command=...)` | `CommandOperator(command=...)` |
  | `PythonOperator(python_callable=f, op_args=..., op_kwargs=...)` | a `@task` wrapping `f` |
  | `execution_timeout=` | `timeout=` |

- **Arguments without a native equivalent are filtered out**, with a **parse warning** listed
  with the DAG in the UI (not silently), except:
  - **Metadata** (`owner`, `doc`, `doc_md`, `doc_rst`, `doc_json`, `doc_yaml`, `owner_links`,
    `access_control`): kept as DAG/task metadata and shown in the UI, no warning.
  - **An error when set to a non-default value**, because dropping it would change what the
    DAG does or hide failures: `depends_on_past=True`, `wait_for_downstream=True`,
    `ignore_first_depends_on_past`, `sla`, `run_as_user`, `user_defined_macros`,
    `user_defined_filters`, `template_searchpath`, and **all callbacks** (below). Default
    values (`depends_on_past=False`, `on_failure_callback=None`, …) are accepted silently.
- **Unknown arguments** (not Airflow arguments at all) are an error, as in Airflow.
- **`email_on_failure=True` is translated** (it is data, not code): with `email=[...]` it becomes
  a hidden native **`EmailChannel`** named after the DAG, with those addresses, for the event
  `TASK_FAILED` (see Outcomes, failures and joins: DAG-defined email
  channels). If no email transport is configured, the master's DAG validation shows a
  **warning** on the DAG ("email_on_failure ignored: no email transport configured").
  `email_on_retry=True` is translated the same way, with the event `TASK_RETRY`.
- **Callbacks are a parse error** (`on_failure_callback`, `on_success_callback`,
  `on_retry_callback`, `on_execute_callback`, `on_skipped_callback`, DAG-level `on_*_callback`,
  `sla_miss_callback`): they are arbitrary code with nowhere sound to run (not on the master;
  not as a failure branch; in the task's own process they would miss timeouts, OOM kills and
  lost workers, and command/script tasks have no Python process). Message: "not supported:
  configure notifications with `notify=`". The migration tool reports them as a manual step.
  Possibly later: Airflow's ready-made notifier objects (`SlackNotifier`, `SmtpNotifier`)
  recognized and translated to channels.
- **Internal builder API.** A **private builder API** of the `schedr` package (e.g.
  `schedr._model`) may be used where Airflow's *syntax* can't go through the public native API
  directly (e.g. the `@dag` decorator, task IDs given as strings), never to build behaviour the
  native API doesn't have (see the principle above).
- **Last-line `.output`** (`BashOperator`, `@task.bash`, `SchedrScriptOperator`'s `LAST_LINE`
  mode): where a DAG uses it, the factory inserts a **hidden native Python task** that takes the
  whole `ProcessResult` and returns the last non-empty line of stdout (without the trailing
  newline); downstream tasks get its output. A native DAG could write the same by hand.
  - Exact even when stdout was truncated: truncation keeps the tail, and a task taking the
    whole `ProcessResult` gets the text and its flags, not an error.
  - Added only where `.output` is actually used; the UI shows the hidden task folded into the
    command task.
  - Cost: one extra Python process per such reference and a short delay before its consumer.
  - Not chosen: a `last_line` field in the native `ProcessResult` (the native API isn't changed
    for Airflow); translating `.output` to the whole stdout (a silent difference: trailing
    newline, multi-line output); an error (breaks the common "Bash produces a value, Python
    consumes it" pattern).
- **`skip_on_exit_code`** (`BashOperator`, `@task.bash`, `SchedrScriptOperator`) is
  translated onto **outcomes**, so it needs nothing beyond the native API:
  - Default (`99`): the task gets `outcomes=AirflowCommandOutcome`, an ordinary `Outcome` enum
    internal to `schedr.airflow`:

    ```python
    class AirflowCommandOutcome(Outcome):
        SUCCESS = 0
        SKIPPED = 99
    ```

    (A separate enum, not a subclass of `DefaultOutcome`: Python doesn't allow extending an
    enum that has members.)
  - Every outgoing edge becomes `.on(<enum>.SUCCESS)`. On a skip code the edges aren't taken,
    so the downstream tasks see a skipped upstream: `all_success` (`AllSucceeded`) skips them,
    `none_failed` (`NoneFailed` + start anchor) runs them, as in Airflow.
  - `skip_on_exit_code=None` (disabled): `DefaultOutcome`.
  - Another code or a list of codes: an enum **generated for that task**, `SUCCESS = 0` plus
    one member per code (`SKIPPED_100`, …; one exit code per member, as decided for
    outcomes). Edges are still `.on(SUCCESS)` only.
  - Only difference from Airflow: the task itself shows as *success, outcome `SKIPPED`*, not
    *skipped* (display only; the run's result is the same).
  - By the `.on(DefaultOutcome.SUCCESS)` rule in Task results and outcomes, it is a parse
    error on such a task; the translation writes the edges itself.
- **Missing annotations** in Airflow-style callables are treated as `Any` (only the run-time
  output check applies), the same as an explicit `-> Any` in the native API.
- One translation path means the **same semantics** for both APIs: whatever the native API
  checks (types, references, outputs) is checked for Airflow-style DAGs too.

### Branching, task groups and mapping

All translated onto the native API (`@branch`, `group`, `.partial()` / `.expand()`). Where
Airflow behaviour differs from the native API, see the principle in Thin translating classes;
each case is described below.

**Branching:**

- **`@task.branch` / `BranchPythonOperator`: supported.** They return task ID strings (one, a
  list, or `None`), computed at run time, so they can't be checked at parse time; at run time
  an ID that isn't a **direct downstream** task fails the branch task. `None` skips all direct
  downstream tasks, as in Airflow: it is translated to the native `[]` (the empty subset).
- **`ShortCircuitOperator` / `@task.short_circuit`: supported.** A falsy return value skips
  downstream tasks; a truthy one continues. `ignore_downstream_trigger_rules=True` (default):
  skip everything downstream; `False`: skip only direct downstream tasks and let trigger rules
  decide the rest.
  - **Translation:** a native Python task with outcomes: a truthy result gives
    `Continue(value)`, a falsy one `Stop(value)` (dataclasses carrying the returned value, so a
    pull of it still works); its direct downstream edges become `.on(Continue)`.
  - **`ignore_downstream_trigger_rules=False`:** exact.
  - **`ignore_downstream_trigger_rules=True`:** native skips spread only through trigger rules,
    so it is exact where they give the same result, and a **parse error** otherwise (naming
    the task; suggesting `ignore_downstream_trigger_rules=False` or changing that task's
    rule):

    | Downstream shape | Skip on `Stop` | Exact? |
    |---|---|---|
    | a chain or tree below the short-circuit | a task whose only upstream was skipped is skipped, under either rule | yes |
    | a join with an upstream **outside** the short-circuit's subtree, rule `all_success` (→ `AllSucceeded`) | any skipped upstream → skipped | yes |
    | such a join with `none_failed` / `none_failed_min_one_success` (→ `NoneFailed`) | runs if its other upstream succeeded (Airflow skips it) | no → parse error |
    | an `always` task downstream (edges dropped) | no longer downstream, so not skipped (Airflow skips it) | no → parse error |

  - Not chosen: emulating "skip regardless" for `none_failed` joins with hidden join tasks
    (not exact either: data edges count in the join's rule).
- **Not supported:** `BranchExternalPythonOperator`, `BranchPythonVirtualenvOperator`,
  `BranchDayOfWeekOperator`, `BranchDateTimeOperator`, `@task.branch_external_python`,
  `@task.branch_virtualenv`. They don't exist in `schedr.airflow`; importing them is an import
  error naming them as unsupported.

**Trigger rules:**

- Translated onto the two model rules, `NoneFailed` and `AllSucceeded` (see Outcomes,
  failures and joins), or an error.
- **Translation by a factory** (private builder API) that returns a model rule and may also
  **rewrite the task's incoming edges**, so some rules with no direct equivalent are emulated
  exactly. Both rewrites are combinations a native DAG could write by hand, so they follow the
  principle in Thin translating classes:
  - **Start anchor:** a hidden `empty` root task of the DAG, completed by the master when the
    run starts (it is never skipped or failed), added as one more upstream. With `NoneFailed`,
    the task then can't be skipped for "all upstream skipped".
  - **Dropped edges:** the task's incoming dependencies are removed from scheduling (kept for
    display only).

  | Airflow 3 rule | Airflow behaviour | Translated to | Exact? |
  |---|---|---|---|
  | `all_success` (default) | all upstream succeeded; any failed → `upstream_failed`; any skipped → skipped | `AllSucceeded` | yes |
  | `none_failed_min_one_success` | no failure and ≥1 success; all skipped → skipped | `NoneFailed` | yes |
  | `none_failed` | no failure; runs even if all upstream skipped | `NoneFailed` + start anchor | yes |
  | `always` | runs at any time, doesn't wait for upstream | dropped edges | yes; a data reference to an upstream output is an error (no edge, so no guaranteed output) |
  | `one_success` | as soon as one succeeded; none succeeded: all skipped → skipped, else `upstream_failed` | **error** | `AtLeastOneSuccess` rejected (see Outcomes, failures and joins); the message suggests `none_failed_min_one_success`, usually what was meant |
  | `all_failed`, `one_failed`, `one_done`, `all_done`, `all_done_min_one_success`, `none_skipped` | run, or keep waiting, when upstream tasks fail | **error** | not possible: no failure branches (see Outcomes, failures and joins). Exception: a **leaf** `all_done` task (cleanup) translates onto a resource (see Cleanup and resources (Airflow)) |
  | `all_skipped` | runs only if all upstream skipped | **error** | not possible: no rule turns a skip into a run |
  | `all_done_setup_success` | setup/teardown tasks | **error** as a trigger rule | `@teardown` tasks translate onto resources instead (see Cleanup and resources (Airflow)) |

- **Why `AllSucceeded` exists:** `NoneFailed` runs a task when *some* upstream succeeded, so no
  combination of it with empty tasks and anchors expresses "skipped if *any* upstream was
  skipped". Translating `all_success` to `NoneFailed` with a check instead doesn't work either:
  `BashOperator`'s `skip_on_exit_code` defaults to 99, so almost every Bash task is skippable,
  and a warning or error on "join with a skippable upstream" would fire on the **default rule**
  in most DAGs. By the principle, the native API offers it too.
- Consequence: alert/rollback tasks (`one_failed`) and `all_done` tasks that aren't leaves
  must be reworked when migrating (the migration tool reports them; notifications replace
  alert tasks). Failure-aware rules evaluated by the master were rejected (see Outcomes,
  failures and joins). Leaf `all_done` cleanup tasks and setup / teardown translate onto
  resources (see Cleanup and resources (Airflow)).

**Task groups:**

- **`TaskGroup` (context manager) and `@task_group`: supported.** `group_id.task_id` names,
  `prefix_group_id=False`, dependencies on a group, nested groups, per-group `default_args`
  (→ group `TaskDefaults`); `tooltip`, `ui_color`, `ui_fgcolor` kept as metadata.
- **Mapped task groups** (`@task_group(...).expand(...)`): **not supported for now** (an
  error). The native API has none either; if added later, in both APIs together.

**Dynamic task mapping:**

- **`.partial(...).expand(...)` over literals and upstream outputs: supported.** Several mapped
  arguments give the **cartesian product**, as in Airflow (same in the native API).
- **`expand_kwargs`** (a list of dicts, one per instance): **supported, in both APIs**; pure
  data, evaluated by the master.
- **`.zip()`: not now** (pure data; could be done by the master later).
- **`.map(fn)`: not supported** (runs a Python function on the output; would need Python where
  there is none).
- **Limit on mapped instances per task:** `[mapping] max_instances`, default 1024 (like
  Airflow's `max_map_length`); more instances fail the mapped task with a clear error. Applies
  to both APIs.
- **Empty expansion in the Airflow layer: skipped**, as in Airflow (the native API: succeeds
  with `[]`, unchanged), so migrated DAGs keep their behaviour.
  - **Translation:** in front of a mapped task, the factory inserts a **hidden native Python
    task** that takes the mapped arguments and returns the outcome `Empty` or `NotEmpty`
    (`Empty` if any mapped argument is empty, as the cartesian product then is); the mapped
    task's edge becomes `.on(NotEmpty)`. On `Empty` the mapped task is skipped and its
    downstream tasks follow their rules (under `all_success` → `AllSucceeded`: skipped, as in
    Airflow). Mapping over a literal list uses the same hidden task (one mechanism).
  - Cost: one extra Python process per mapped task; the list counts against the hidden task's
    input cap.
  - Not chosen: changing the native rule (the native API isn't changed for Airflow); keeping
    "succeeds with `[]`" in the Airflow layer (a silent difference: downstream `all_success`
    tasks would run where Airflow skips them).

### Cleanup and resources (Airflow)

Translated onto the native per-task cleanup and resources (see Cleanup and
resources).

- **`on_kill()`** exists only in custom operators, which aren't supported (see Scope): nothing
  to translate.
- **Setup / teardown translate onto resources when possible**, the rest is a parse error:
  `@setup` → `@resource`; `@teardown` / `.as_teardown(setups=s)` → `@cleanup(s)`, receiving the
  setup's output; the tasks between them → users. `all_done_setup_success` is covered by this.
  **Parse errors:** teardowns with tasks downstream of them, teardowns taking other data, a
  teardown for several setups.
- **A leaf task with `all_done`** (the classic Airflow cleanup task) translates onto a
  `Resource` without setup code, named after the task: **users** = its **direct upstream
  tasks** (exactly what `all_done` waits for; it may run while other parts of the graph still
  run, as in Airflow); **cleanup** = the task itself (a Python callable → `@cleanup`, a
  `BashOperator` → `CommandCleanup`), with the task's `retries`. **Parse errors:** the task has
  downstream tasks; it takes data (e.g. `XComArg`). An `xcom_pull` inside it fails at run time
  ("a cleanup has no upstream outputs").
- **Compared with Airflow's setup/teardown:** the same idea and the same "runs if the setup
  succeeded" rule, but users are explicit (not every task between setup and teardown in the
  graph), the cleanup is not a graph node (nothing can wait for it, it can't decide the run's
  result as a leaf), it receives only the setup's output, and there is one cleanup per resource
  (Airflow: a teardown may serve several setups). schedr has no "clear" (see Run result), so
  re-creating a resource for re-run tasks never arises.
- **Differences in results:** a failed cleanup **fails the run** in schedr; Airflow by default
  doesn't fail the run when a teardown fails (`on_failure_fail_dagrun=False`). A successful
  `all_done` leaf can turn an Airflow run green after a failure; never in schedr (see Run
  result).

### Reference version: Airflow 3

Module paths mirror **Airflow 3** (the `airflow.sdk` Task SDK and the `standard` provider).
Migration is a mechanical rewrite of the import prefix, `airflow.` → `schedr.airflow.`:

| Airflow 3                                     | schedr                                               |
|-----------------------------------------------|------------------------------------------------------|
| `airflow.sdk`                                 | `schedr.airflow.sdk`                                 |
| `airflow.providers.standard.operators.bash`   | `schedr.airflow.providers.standard.operators.bash`   |
| `airflow.providers.standard.operators.python` | `schedr.airflow.providers.standard.operators.python` |
| `airflow.providers.standard.operators.empty`  | `schedr.airflow.providers.standard.operators.empty`  |

Airflow 2 import paths (`airflow.operators.*`, `airflow.decorators`) are **not** mirrored.

Example:

```python
from datetime import datetime, timedelta

from schedr.airflow.sdk import DAG, task
from schedr.airflow.providers.standard.operators.bash import BashOperator
from schedr.airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="example",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["demo"],
):
    start = EmptyOperator(task_id="start")
    extract = BashOperator(task_id="extract", bash_command="./extract.sh")

    @task
    def transform() -> None: ...

    start >> extract >> transform()
```

### Scope

**Tier 1 (DAG authoring):**

| Area                 | Included                                                                     |
|----------------------|------------------------------------------------------------------------------|
| `schedr.airflow.sdk` | `DAG` (context manager), `@dag`, `@task`, `@task.bash`, `chain`, `TriggerRule` |
| `DAG` arguments      | `dag_id`, `schedule` (cron, `@daily`-style presets, `@once`, `timedelta`, `None`), `start_date`, `end_date`, `catchup`, `default_args`, `tags`, `description`, `max_active_runs`, `max_active_tasks`, `params` |
| Operators            | `BashOperator`, `PythonOperator` (`python_callable`, `op_args`, `op_kwargs`), `EmptyOperator` |
| Per-task arguments   | `task_id`, `retries`, `retry_delay`, `execution_timeout`, `trigger_rule`, `max_active_tis_per_dagrun`, `max_active_tis_per_dag`, `email`, `email_on_failure`, `email_on_retry` |
| Dependencies         | `>>`, `<<`, `set_upstream` / `set_downstream`, `chain()`, lists on either side |

**Also in scope (beyond Tier 1):**

| Feature | Airflow API mirrored | Decided in |
|---|---|---|
| Branching | `@task.branch`, `BranchPythonOperator`, `ShortCircuitOperator`, `@task.short_circuit` | Branching, task groups and mapping |
| Trigger rules | all Airflow 3 rules: translated or an error | Trigger rules |
| Task groups | `TaskGroup` (context manager), `@task_group` | Branching, task groups and mapping |
| Dynamic task mapping | `.partial(...)`, `.expand(...)`, `expand_kwargs` | Branching, task groups and mapping |
| Setup / teardown | `@setup`, `@teardown`, `.as_teardown(setups=...)` | Cleanup and resources (Airflow) |
| Triggering another DAG | `TriggerDagRunOperator` | Dependencies between DAGs |
| Passing values between tasks | outputs, `ti.xcom_pull` (restricted, translated onto inputs) | Output serialization; `ti.xcom_pull` below |
| Connections | `Connection` in **Airflow's JSON format**, stored in **secret storage** (Vault first) | Secret storage (Connections) |

**Naming: "outputs", not "XCom".** XCom is Airflow's name for passing values between tasks.
schedr's shared model and native API call them **outputs**:

- A task produces **one output and one outcome** (see Task results and outcomes). There are no
  named outputs: Airflow's `multiple_outputs` maps onto references into parts of that one
  output (see Output serialization).
- A downstream task takes an **output reference** as an input (`extract.output`, or a part of
  it: `output["rows"]`).
- `.expand()` maps over an output; a branch task's output is the list of task IDs to follow.

Airflow-specific names (`ti.xcom_pull`, `do_xcom_push`, …) appear only inside `schedr.airflow`,
where they map onto outputs.

**`ti.xcom_pull(...)`: translated onto inputs, restricted.** Supported because the migration
promise (change only the imports) would otherwise break for many DAGs: pulling inside a
`PythonOperator` callable is very common. By the principle in Thin translating classes it uses
only native data flow: there is **no run-time fetch** (the native API never gets one).

- **Which tasks:** Airflow passes context to a callable through parameters named `ti`,
  `**context` or `**kwargs`; the factory sees the callable's signature at parse time. Only
  such tasks get the input below; others get nothing extra.
- **What they get:** the **outputs of all their upstream tasks** (ancestors in the graph) of the
  same DAG run, as an ordinary data input added behind the scenes (task ID → output; mapped
  tasks per map index). A native DAG could write the same by passing each output as an
  argument.
- **How a pull is answered:** `ti.xcom_pull(task_ids=...)` is answered **by the runner,
  locally**, from that input; no call to the master while the task runs (the master needn't be
  reachable). Inputs travel as usual (inline in `AssignTask` up to 4 MB, in chunks above).
- **Only from upstream tasks**: they are guaranteed finished, so the result is deterministic.
  Pulling from any other task is an error at run time ("`extract` is not upstream of `load`";
  Airflow returns `None` or a value depending on timing).
- Only the default key (`return_value`, i.e. the task's output); other keys, `dag_id=` of
  another DAG and `include_prior_dates=True` are errors; `map_indexes=` is supported.
- **Cost:** the task receives every upstream output, even ones it never pulls; this counts
  against the task's **total-inputs cap** (64 MB). A task over the cap fails with a clear error
  suggesting the data-flow rewrite (`load(extract.output)`); the migration tool reports every
  `xcom_pull` with that suggestion.
- Not chosen: a run-time fetch through the agent and the master (a mechanism the native API
  doesn't have); adding a run-time pull to the native API (gives up "data flow is the only
  way", which makes native DAGs checkable at parse time); an error (breaks migration of many
  DAGs); only direct upstream tasks (cheaper, but more DAGs need changes; possible later as an
  option); scanning the callable's source for literal `xcom_pull` calls (fails with computed
  task IDs, helpers or aliases).
- **Priority:** after the core output mechanism (it is a compatibility layer on top of it).

**Templating: no Jinja, by design. Replaced by t-strings.** schedr will not include Jinja.
Airflow templates are replaced by Python template strings (t-strings, PEP 750) combined
with placeholder objects:

```python
from schedr.airflow.templates import ds, params, macros

BashOperator(task_id="x", bash_command="echo {{ ds }} {{ params.region }}")   # Airflow
BashOperator(task_id="x", bash_command=t"echo {ds} {params.region}")          # schedr
```

- Placeholders (`ds`, `params`, `macros`, task outputs, …) are lazy: an operation on them
  (`params.region`, `macros.ds_add(ds, 7)`) builds a small expression, not a value. Only
  operations schedr implements are allowed; anything else is an error when the DAG is parsed.
- Templates are part of the **shared model**: stored in the DAG JSON as literal text plus typed
  references, e.g. `["echo ", {"ref": "ds"}, " ", {"ref": "params.region"}]`. Rust validates
  references at parse time and can render templates itself (e.g. Bash commands without
  starting Python).
- Values from other tasks are referenced directly (`t"{extract.output}"`) instead of
  `ti.xcom_pull(...)`.
- The native API uses the same placeholder mechanism.
- **Secrets** have their own placeholder, resolved from secret storage on the worker (see
  Secret storage).
- Requires Python 3.14+ (the whole package's minimum version, see Repository and tooling: Python).
- **Any argument may contain a t-string.** Every operator and task argument is checked for
  t-strings and filled in at run time; there is no per-operator list of templated arguments
  (Airflow's `template_fields`).
- **Nested values are filled in recursively**, all the way down: t-strings inside lists, tuples
  and dicts (e.g. `env={"RUN": t"{run_id}"}`, `op_kwargs={"day": t"{ds}"}`) are found and
  filled in at any depth.
  - Traversed: lists, tuples, dict values. **Dict keys: never**; a t-string used as a dict key
    is a parse error (rendered keys could collide, and there's no real use).
  - **Not traversed:** sets, dataclasses and any other custom objects.
  - A t-string found inside a container that isn't traversed is **an error when the DAG is
    parsed**, so it can't silently reach a task unfilled. The error message tells the author
    to convert the container explicitly, e.g. a set into a list, before passing it as an
    argument.
- **Not needed for now:** template files (`.sh` / `.sql` files rendered as templates).
- **Later (M5; deferred: not designed until the first version of schedr, M1–M4, works, see
  Roadmap):** the set of placeholders and macros (`ds`, `params`, `macros.ds_add`, …). Rule for
  that design (the principle in Thin translating
  classes): every Airflow placeholder must be **expressible with native placeholders** (e.g. `ds` as
  `dag_ctx.run.planned_for` formatted `%Y-%m-%d`); one without a native equivalent is an error in
  the Airflow layer, unless the native API gets it for a native reason.
- Not planned: a restricted `"{{ ... }}"` string syntax as a fallback.
- **Safe substitution into shell commands.** A t-string in a `command` inserts values into text
  run by a shell; unquoted, a value with spaces, `;` or `$(…)` would break the command or allow
  shell injection.
  - **Quoted by default.** In a string `command`, every interpolated value is **shell-quoted**
    (POSIX single-quote quoting, the same result as Python's `shlex.quote`), e.g.
    `t"./notify.sh {out.report_url}"` with `https://x/a b;rm -rf ~` renders as
    `./notify.sh 'https://x/a b;rm -rf ~'`. The agent (Rust) renders commands, so it does the
    quoting; tests check parity with `shlex.quote`.
  - **Explicit opt-out with the format spec:** `t"./run.sh {flags:raw}"` inserts the value
    unquoted (t-strings keep the format spec, so no extra API). Unknown format specs are a
    parse error.
  - **Argument-list commands, no shell:** `command=["./notify.sh", out.report_url]` runs the
    program directly, each element one argument: no injection possible, no quoting needed.
    Recommended in the documentation whenever pipes, redirects or other shell features aren't
    needed. Script arguments (`schedr.toml`, `ScriptOperator`) are always argument lists.
  - **Secrets in a command line are a parse error**: `secrets[...]` in a `command` (string or
    list), in the `args` of a script / `ScriptOperator`, or in the `command` / `args` of a
    cleanup or resource setup. A command line is readable by every user on the node (`ps`,
    `/proc/<pid>/cmdline`, unless `/proc` is mounted with `hidepid`), including sidecars and
    monitoring agents, and ends up in tools' error messages, which masking doesn't cover. The
    error message points to the two ways below (see Secret storage). A tool that accepts a
    credential only as an argument still works, explicitly in the user's script
    (`--token "$(schedr task-ctx secret get 'ci/deploy#token')"`).
  - Not the main mechanism: passing every value through environment variables (`"$URL"` in
    the command); safe only if the user remembers the double quotes. Kept as the
    recommendation for secrets.
- **Later in the project:** a migration tool (`schedr migrate`, built on `libcst`) that rewrites
  imports and converts Jinja templates to t-strings, flagging what it can't convert. It also
  **reports each `ti.xcom_pull` call** and suggests the data-flow rewrite (passing
  `extract.output` as an argument) without forcing it, so migrated DAGs work at once and can be
  modernized gradually.

Out of scope for now: sensors, Assets, callbacks, pools, hooks. (`pool=` is filtered out with a
parse warning: dropping it doesn't hide failures; queues cover the need.)

**Custom operators are not supported** (as in the native API, by the principle in Thin
translating classes). A user class subclassing `BaseOperator` (or any `schedr.airflow`
operator) is an import error naming the class: "custom operators are not supported: move
`execute()` into a `@task` function or a `PythonOperator` callable". The migration tool reports
each one as a manual step. Provider operators other than the `standard` provider (and schedr's
own `providers.schedr`) don't exist in `schedr.airflow` either.

## Repository and tooling

### Layout

```
Cargo.toml              # Cargo workspace; shared lints
rust-toolchain.toml     # stable + rustfmt + clippy
crates/
  schedr-model/         # shared model (DAG JSON, states); JSON Schema
  schedr-proto/         # Protobuf + tonic code, master <-> agent
  schedr-config/        # configuration loading and validation
  schedr-db/            # schema, migrations, queries (SQLite, Postgres)
  schedr-master/        # scheduling loop, DAG validation, commands, gRPC server
  schedr-agent/         # worker agent
  schedr-api/           # webserver: REST API, UI
  schedr/               # the binary: roles, user CLI, in-task CLI
    src/<module>.rs     # unit tests in src/<module>/tests.rs (see Testing)
    tests/              # integration tests (later)
  xtask/                # development tasks: `cargo xtask schema [--check]`
schema/                 # generated, committed: JSON Schemas, OpenAPI
pyproject.toml          # Python package
python/
  schedr/               # Python API package (typed: py.typed)
  tests/unit/schedr/    # unit tests mirroring the package (see Testing)
  tests/integration/    # integration tests (later)
Makefile                # setup / fmt / fmt-check / lint / test / check
.pre-commit-config.yaml
.github/workflows/ci.yml
```

### Rust

- Edition 2024, stable toolchain pinned via `rust-toolchain.toml`.
- `rustfmt` (max width 100).
- `clippy` with `all` and `pedantic` lint groups at warn; CI fails on any warning (`-D warnings`).
- `unsafe_code = "forbid"`, `missing_docs = "warn"`.

### Python

- Supported versions: **Python 3.14+** (required by t-strings, see Airflow compatibility, Scope:
  Templating). CI tests 3.14.
- Dependency management: **uv** (lockfile `uv.lock`, dev deps in `[dependency-groups]`).
- Build backend: **maturin** with `bindings = "bin"` (see Packaging and releases). The Python
  package version is dynamic, taken from the Cargo workspace. `uv sync` rebuilds the project
  when Rust sources change (`[tool.uv] cache-keys`).
- `ruff` handles formatting and linting with a broad rule set; `mypy` runs in strict mode;
  `pytest` runs the tests with coverage (see Testing).
- Local environment: pyenv-virtualenv selected by `.python-version` (git-ignored). The Makefile
  points uv at that env via `pyenv prefix`. Pre-commit hooks call `ruff`/`mypy` directly.

### Packaging and releases

- **Python package: built with maturin**, replacing hatchling. The wheel contains the **`schedr`
  Rust binary** plus the Python package (as `ruff` and `uv` ship). `pip install schedr` gives a
  DAG author the API **and** `schedr standalone` for local development. If schedr's Python
  package later includes a compiled Rust extension (allowed, see Rust/Python integration), it
  goes into the same wheel.
- **Platform wheels:** Linux x86_64 and aarch64, and macOS (local development; Linux-only
  features such as the memory watchdog are disabled there).
- **Container images:**
  - Master/webserver image: the Rust binary only, **no Python at all**.
  - Worker base image: Rust binary + Python 3.14 + the `schedr` package. Users build their own
    worker image on top of it, adding their libraries.
  - Both run under an **arbitrary UID**.
- **Helm chart** for deploying.
- **Version pairing:** the binary and the Python package always share **one version number** and
  are released together. The runner reports its version when it connects to the agent; the
  agent **rejects a mismatched runner** with a clear error.

### Libraries

Rust crates chosen for the implementation (versions pinned when added):

| Need | Crate | Why |
|---|---|---|
| async runtime | `tokio` | required by `tonic` and `axum` |
| gRPC | `tonic` + `prost` | see Master–worker protocol |
| REST | `axum` | tokio-native, the same stack as `tonic` |
| OpenAPI from Rust types | `utoipa` | derives next to serde |
| JSON Schema | `schemars` | see Rust/Python integration |
| database | `sqlx` (SQLite and Postgres), **runtime-checked queries**, migrations per backend | compile-time `query!` checking needs one backend |
| time and zones | `jiff` | explicit handling of DST gaps and overlaps |
| cron | `croner` | 5/6-field cron and presets; DST behaviour tested against our rules, wrapped if needed |
| UUIDv7 | `uuid` (`v7`) | see Runs: identity |
| CLI | `clap` (derive) | standard |
| configuration | `figment` | file < env (`SCHEDR_CONFIG__`, `__` nesting) < flags out of the box |
| logging | `tracing` + `tracing-subscriber` | see Observability |
| processes and signals | `tokio::process` (`process_group`), `rustix` | safe APIs only (`unsafe_code = "forbid"`) |
| shell quoting | own implementation | must match `shlex.quote` exactly |

Not chosen: `chrono` + `chrono-tz` (weaker DST handling); `diesel` / compile-time `sqlx` macros
(two backends); `actix-web` (a second async stack). The Python package stays dependency-free.

### Testing

**Unit tests only for now**; integration tests (`python/tests/integration/`,
`crates/*/tests/`) start as the **last step of M1** (see PLAN.md), before M1 counts as done.

- **Every public function, method and class / struct has unit tests. Private ones are not
  tested directly**: testing them would break the point of making them private (an
  implementation detail, free to change). They are covered through the public code that uses
  them.
  - **Python:** private means a name starting with `_` (`def _parse_line`, `class _Cache`,
    `self._buffer`) or name-mangled `__x`. Special methods (`__init__`, `__eq__`, `__call__`, …)
    are public behaviour and are tested. An underscore on a **module or package**
    (`schedr/_serialization/`, `schedr/_model/`) means "internal to schedr, not user API": its
    non-underscore names are still public **within schedr** and are tested.
  - **Rust:** private means an item without any `pub` (visible only in its module). Anything
    with `pub`, `pub(crate)` or `pub(super)` is tested.
- **Coverage floor: 85 %** per language over the whole code base, private code included
  (reached through the public tests); `make check` and CI fail below it. Not counted: the test
  files themselves (`src/**/tests.rs`) and a binary's `main.rs`, which only calls into the
  crate's library (everything testable lives there). Python: `pytest-cov`
  with branch coverage, `--cov-fail-under=85`. Rust: `cargo-llvm-cov`, `--fail-under-lines 85`.
- **Never skip a test** (`pytest.mark.skip`, `skipif`, `xfail`, Rust `#[ignore]`) without
  asking first, with the reason stated clearly. A test that can't run is fixed, or the question
  is raised.
- **Structure mirrors the code:**
  - **Python:** `python/tests/unit/schedr/...` mirrors the package; one `test_<module>.py` per
    module (`schedr/runner/protocol.py` → `tests/unit/schedr/runner/test_protocol.py`).
    A package's `__init__.py` is tested in `test_init.py` in the package's test folder.
    `pytest --import-mode=importlib`, so test files with the same name in different folders
    don't clash and test folders need no `__init__.py`.
  - **Rust:** one test file per module, next to it: `src/scheduler.rs` ends with
    `#[cfg(test)] mod tests;` and its tests are in `src/scheduler/tests.rs` (nested modules
    the same way: `src/scheduler/fire_times/tests.rs`; the crate root `src/lib.rs` has its tests
    in `src/tests.rs`). As a child module, it reaches
    `pub(crate)` / `pub(super)` items, which a crate's `tests/` folder (integration tests,
    public API only) can't. Test code is compiled only for tests. Not chosen: a `mod tests { … }`
    block inside each source file (mixes code and tests in long files).
- **Tools:**

  | Language | Tools |
  |---|---|
  | Python | `pytest`, `pytest-cov` |
  | Rust | built-in `#[test]` run by **`cargo-nextest`**; **`rstest`** (fixtures, parametrized cases); **`proptest`** (property tests: shell quoting vs `shlex.quote`, fire times across DST, JSON round trips); **`insta`** (snapshots: DAG JSON, error messages, CLI output); **`pretty_assertions`**; **`cargo-llvm-cov`** |

### Workflow

- `make check` runs everything CI runs: format check, clippy, ruff, mypy, the Rust tests
  (`cargo nextest`) and pytest, both with the coverage floor.
- Pre-commit hooks run the same checks on commit.
- CI: GitHub Actions, one Rust job and a Python version matrix.

## Roadmap

Milestones, each ending with something that works; architectural risks (processes, agent,
gRPC, journal) come before breadth of features.

| Milestone | Contents | Done when |
|---|---|---|
| **M0. Foundations** | the shared model as Rust types (DAG JSON, JSON Schema, Protobuf); configuration loading; database schema and `schedr db migrate` (SQLite and Postgres); CLI skeleton | — |
| **M1. Standalone runs a native DAG** | native API core (`Dag`, `@task`, `CommandOperator`, `EmptyOperator`, data flow and `>>`, output serialization, `Cron` / `Every` / `None`); runner `parse` / `task`; agent (process groups, log files, timeouts, cancellation); master loop (`planned_for` / `since`, `catchup`, `max_active_runs`, retries, `NoneFailed`); `schedr standalone` over gRPC on localhost; the webserver's REST API subset (`/api/v1`) the CLI needs (the UI comes in M3); CLI `dags list/pause/unpause`, `runs trigger/list/cancel`, `dags check`, `dags test`; the runner and its protocol independent of how the process was started (for M2.5) | a real DAG runs end to end on a laptop and its tests pass with `run_dag` |
| **M2. Distributed** | separate roles, mTLS, Postgres; worker lifecycle (register, heartbeat, reconnect, drain, journal); assignment and rejections, queues, slots; change detection, parsing per (root, hash, version), DAG conflicts; memory watchdog; ops port; images and Helm chart | runs on OpenShift with 2 workers and survives killing the master or a worker |
| **M2.5. Fork server** | the fork server as designed (How Python tasks are started), built on M2's watchdog and metrics; its value and preload list confirmed by M2's start-time metrics | Python tasks start from the fork server on Linux workers, with fallback to spawn |
| **M3. Web UI** | htmx pages, SVG graph, SSE, log proxy and live tail; command rows; auth modes; times in UTC and local | — |
| **M4. Native API complete** | outcomes and `.on(...)`, `AllSucceeded`, `@branch`, groups, mapping; `schedr.toml` and `ScriptOperator`; cleanup and resources; secrets (Vault, `file`) and connections; params and `dag_ctx`; notifications; `TriggerDagOperator`; concurrency limits; `Once`; backfill; retention; upgrade rules | — |
| **M5. Airflow layer** | thin translating classes (Tier 1 first, then the rest of the scope), trigger-rule translation, `xcom_pull`; the placeholder and macro design (deferred until the first version works); the migration tool | — |
| **Later** | sensors and events (see Open questions), the "possibly later" items | — |

- **The fork server is M2.5**, after M2 and before M3: everything it builds on (the watchdog,
  metrics, a stable agent on Linux pods) is finished in M2, M2 stays focused on running and
  surviving on OpenShift, and M2's metrics show its value just before it is built. It is part
  of the first version.
- **The first version of schedr is M1–M4:** native schedr, complete and distributed, with its
  UI. The Airflow layer (M5) follows in a later release. Not chosen: including Airflow Tier 1
  in the first version (delays using schedr for new DAGs).

## Open questions

Questions still open. Once decided, they move into the sections above. Deliberately deferred
items ("possibly later") are noted where they belong.

### Sensors and events

**Deferred: not revisited until the first version of schedr (M1–M4, see Roadmap) works.**
Until then, sensors stay out of scope (see Airflow compatibility, Scope).

Whether and how tasks can wait for a condition or an external event (Airflow's sensors,
`ExternalTaskSensor`, deferrable operators, Assets). Outline from the first discussion, as a
starting point:

- **Three kinds of waiting**, built in stages:
  1. **Waiting for a time** ("start at 06:00", "2 hours after upstream"): done by the master
     alone, like `empty`; a task state `waiting`. Airflow's `TimeDeltaSensor`,
     `DateTimeSensor`, `TimeSensor`.
  2. **Polling a condition** (a file, a row, an HTTP status): a `@sensor` returning
     `Ready[T] | NotYet` (commands and scripts: a reserved "not yet" exit code); each check is
     one short process on a worker, **no slot held between checks** (Airflow's
     `mode="reschedule"`; `mode="poke"` would hold scarce slots for hours). A `NotYet` uses no
     retry; a crash is a failure with the usual retries. Timeout: a failure, or an outcome such
     as `TimedOut` to route on (Airflow's `soft_fail`). Airflow's `PythonSensor`,
     `@task.sensor`, `FileSensor`.
  3. **Events pushed in**: `POST /api/events/<name>` / `schedr events send`; an `EventSensor`
     matched by name and equality on payload fields (no user code), the payload as output; an
     `OnEvent(...)` schedule starting runs (overlaps with Assets); internal events emitted by
     runs and tasks, replacing `ExternalTaskSensor` without polling. Needs event storage and
     retention, idempotency keys, authentication, rules for events arriving before anything
     waits, UI views.
- **Constraints:** the master never runs user code (conditions needing code are worker
  checks); nothing waits while holding a slot; a sensor's result is its output (a re-run checks
  again); every Airflow sensor maps onto one of these or stays an error.
- To settle then: the new task state and the engine rules for checks (`max_active*`, cancel,
  pause, drain), timeouts as failure or outcome, and the whole event model.
