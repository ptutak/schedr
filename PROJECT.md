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
- Remaining architecture details are open (see Open questions).

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

### Task logs

- Task logs are **stored on the worker's local disk** and **served by the worker itself** over
  HTTP (a small endpoint inside the worker agent, including live tail of running tasks).
- What backs the log directory (volume type, StatefulSet, …) is a deployment concern, outside
  schedr: the worker just serves local files.
- Each task attempt records which worker ran it, so its logs can be found.
- The **webserver proxies** log requests to the worker; the browser never talks to workers
  directly.
- The worker's log endpoint accepts only **short-lived tokens signed with a shared secret**.
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
    history shows again. (A future retention policy may clean it up.)

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
  (master ↔ worker, webserver → worker logs, webserver → master).

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
   API + port), content hashes per DAG/script root, and the journal summary (running attempts,
   unacknowledged results).
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
- The worker's **liveness probe** (`/healthz` on the log port) checks only the agent's own
  health, **never** whether the master is reachable (otherwise a master outage would restart
  every worker and kill all running tasks). Workers need no readiness probe.
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
  own memory spike).
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

- **`Finished` contains:** the attempt status, the exit code or signal, **stdout and stderr**
  (which are the task's **outputs** for scripts), the **defined outputs** (for Python API
  tasks), the duration and the peak memory.
  - When stdout/stderr exceed the output size limit, the output keeps **the last part (up to the
    limit) and is marked `truncated`**. A downstream task that reads a truncated output **gets
    an error**, so partial data is never used silently. The full stdout/stderr are always in
    the task's log file.
  - **Inline commands** (`command` kind) produce outputs **the same way as scripts** (stdout and
    stderr). The Airflow layer exposes the **last line of stdout** through `.output` for
    compatibility.
  - Failed Python tasks include a structured **error summary**: exception type, message and the
    last lines of the traceback, so the UI can show it without opening the log.
- **Task instance states** (where the task stands in its DAG run, decided by the master from
  attempt statuses, retries and the DAG structure), initial set to be refined with the API
  design: `scheduled`, `queued`, `unschedulable`, `running`, `up_for_retry`, `success`,
  `failed`, `cancelled`, `skipped`, `upstream_failed`.
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
  user retries it.
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
  `<interpreter> -m schedr.runner parse` or `... task` (`-m`, so it works with any configured
  interpreter executable, whatever virtualenv it belongs to). Only the mode and the socket path
  are passed on the command line / environment; everything else comes from the agent over the
  channel, so no secrets appear in the command line.
  - `parse`: gets the root and file list from the agent, puts every DAG root on `sys.path`,
    imports the files one at a time (native API calls only record DAGs), and sends back the
    DAG JSON and per-file import errors.
  - `task`: gets the task specification from the agent, imports the DAG file (latest code on
    disk), finds the task by ID, fills in t-strings (secrets fetched by the agent), calls the
    function, and sends back the output or a structured error summary; exit code 0 or non-zero.
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
- **Later: a fork server** as an optional start method. A parent process imports libraries
  once and forks one child per task attempt, so each task is still its own process (the memory
  watchdog, timeouts, signal-based cancellation and the journal work unchanged). The runner
  and the agent ↔ runner protocol must not depend on how the process was started, so this can
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

### Default deployment size

- **2 workers** by default, **2 slots each** (4 concurrent tasks). To be tuned per deployment.

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
  **parsed once**, by any `ACTIVE` worker that reported that hash. The master derives each
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
- Changes are detected by **polling on a schedule** (interval set in configuration), not by filesystem notifications (which are
  unreliable on network volumes and with git-sync's symlink switching).
- Defaults: **poll every 30 s**, **settle time 5 s**.
- After a change is seen, schedr waits for a **settle time** (configurable; no further changes) before
  computing the new content hash, so half-written files are never parsed.

### End-to-end flow: running a native-API DAG

What happens from the moment a DAG file written with the native Python API lands in
`SCHEDR_DAG_PATH` (already on the worker) until its run finishes. Each step is tagged:
**[D]** decided elsewhere in this file, **[open]** not decided yet (Open question 12).

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
   worker; it creates only one parse job per (root, hash) (see DAG ID conflicts). [D]

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
    runs follow the `catchup` rules. [D] A run can also be started manually from the UI or CLI.
    The master creates a **DAG run** in Postgres.
12. **Task instances.** The master creates `extract` and `load` in state `scheduled`. `extract`
    has no upstream tasks, so it is ready and moves to `queued`. [D, states to be refined]

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

23. **`Finished`** goes to the master: status, outputs, duration, peak memory. Delivery is at
    least once; the attempt ID makes repeats harmless. [D]
24. **Store and acknowledge.** The master stores the attempt result and the output under
    (run, `extract`), then sends `FinishedAck`; the agent removes the attempt from its journal.
    [D]
25. **Decide.**
    - On success, `extract` → `success`. `load`'s upstream condition is now met, so it goes
      through steps 13–24; its `AssignTask` carries `extract`'s output as input.
    - On failure, the master either retries (`up_for_retry` after `retry_delay`) or marks the
      task `failed`, and downstream tasks become `upstream_failed`. [D]
26. **Run complete.** When every task instance is in a final state, the DAG run is marked
    `success` or `failed`. The UI shows the run; logs come from the worker through the
    webserver proxy. If tasks in the run used different code hashes, the UI shows a warning.
    [D]

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

## Secret storage

- **Secret storage is built into schedr**, not an optional add-on or plugin.
- The core defines a generic **secret storage** abstraction, so several kinds of secret storage
  can be supported. **Only one implementation is built for now: HashiCorp Vault.**
- Core code, the shared model and the native API use the generic name (secret storage), never
  "Vault". Vault-specific names appear only in the Vault implementation.
- **Templates have a dedicated syntax for secrets**: a **general placeholder** (not a
  Vault-specific one) that refers to a value in secret storage (see Templating), e.g.
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
- Connections are stored in secret storage (see Scope).

## State storage

- **Production: PostgreSQL.** On OpenShift it is provisioned and managed by an operator.
  schedr only needs a connection string; it doesn't manage the database itself.
- **Local development: SQLite.**
- All persistent state lives in the database, so the master can restart at any time without
  losing anything.
- **Schema upgrades** are applied by a separate command, **`schedr db migrate`**, run as a
  Kubernetes Job or init container before the master starts. The master **refuses to start**
  against an outdated schema.

## Python API structure

schedr's Python package has two kinds of API:

- **Native API**: schedr's own DAG authoring API, at the top level of the `schedr`
  package. Its design is still open (see Open questions).
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

## Airflow compatibility

### Approach: look-alike API under the `schedr.airflow` namespace

- schedr is **not** Airflow and does not depend on `apache-airflow`.
- The Python API **copies Airflow's names, signatures and module layout** so existing DAG
  code needs as few changes as possible. **Behaviour underneath is entirely ours.**
- Migrating a DAG = **changing its imports only**. Everything below the imports stays the same.

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
| `DAG` arguments      | `dag_id`, `schedule` (cron, `@daily`-style presets, `timedelta`, `None`), `start_date`, `end_date`, `catchup`, `default_args`, `tags`, `description`, `max_active_runs` |
| Operators            | `BashOperator`, `PythonOperator` (`python_callable`, `op_args`, `op_kwargs`), `EmptyOperator` |
| Per-task arguments   | `task_id`, `retries`, `retry_delay`, `execution_timeout`, `trigger_rule`     |
| Dependencies         | `>>`, `<<`, `set_upstream` / `set_downstream`, `chain()`, lists on either side |

**Also in scope (beyond Tier 1):**

| Feature                  | Airflow API to mirror (exact subset still open)                         |
|--------------------------|-------------------------------------------------------------------------|
| Branching                | `@task.branch`, `BranchPythonOperator`; skipped-state propagation and trigger rules |
| Task groups              | `TaskGroup` (context manager), `@task_group`                            |
| Dynamic task mapping     | `.partial(...)`, `.expand(...)`                                         |
| Connections              | `Connection` in **Airflow's JSON format**, stored in **secret storage** (Vault first) |
| Passing values between tasks | Required: mapping and branching depend on it. Details open.         |

**Naming: "outputs", not "XCom".** XCom is Airflow's name for passing values between tasks.
schedr's shared model and native API call them **outputs**:

- A task produces outputs: one default output, plus optional named outputs (replacing
  Airflow's `multiple_outputs`).
- A downstream task takes an **output reference** as an input (`extract.output`,
  `output["rows"]`).
- `.expand()` maps over an output; a branch task's output is the list of task IDs to follow.

Airflow-specific names (`ti.xcom_pull`, `do_xcom_push`, …) appear only inside `schedr.airflow`,
where they map onto outputs.

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
- Requires Python 3.14+ (the whole package's minimum version, see Python tooling).
- **Any argument may contain a t-string.** Every operator and task argument is checked for
  t-strings and filled in at run time; there is no per-operator list of templated arguments
  (Airflow's `template_fields`).
- **Nested values are filled in recursively**, all the way down: t-strings inside lists, tuples
  and dicts (e.g. `env={"RUN": t"{run_id}"}`, `op_kwargs={"day": t"{ds}"}`) are found and
  filled in at any depth.
  - Traversed: lists, tuples, dict values. **Dict keys: possibly** (to be confirmed).
  - **Not traversed:** sets, dataclasses and any other custom objects.
  - A t-string found inside a container that isn't traversed is **an error when the DAG is
    parsed**, so it can't silently reach a task unfilled. The error message tells the author
    to convert the container explicitly, e.g. a set into a list, before passing it as an
    argument.
- **Not needed for now:** template files (`.sh` / `.sql` files rendered as templates).
- **Later:** the set of placeholders and macros (`ds`, `params`, `macros.ds_add`, …) is
  designed and implemented later in the project.
- Not planned: a restricted `"{{ ... }}"` string syntax as a fallback.
- **Later in the project:** a migration tool (`schedr migrate`, built on `libcst`) that rewrites
  imports and converts Jinja templates to t-strings, flagging what it can't convert.

Out of scope for now: sensors, Assets, callbacks, pools, Variables, hooks.

## Repository and tooling

### Layout

```
Cargo.toml              # Cargo workspace; shared lints
rust-toolchain.toml     # stable + rustfmt + clippy
crates/
  schedr-core/          # scheduling engine (library); unit tests inline, integration in tests/
  schedr/               # scheduler binary
pyproject.toml          # Python package
python/
  schedr/               # Python API package (typed: py.typed)
  tests/{unit,integration}/
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

- Supported versions: **Python 3.14+** (required by t-strings, see Templating). CI tests 3.14.
- Dependency management: **uv** (lockfile `uv.lock`, dev deps in `[dependency-groups]`).
- Build backend: **maturin** with `bindings = "bin"` (see Packaging and releases). The Python
  package version is dynamic, taken from the Cargo workspace. `uv sync` rebuilds the project
  when Rust sources change (`[tool.uv] cache-keys`).
- `ruff` handles formatting and linting with a broad rule set; `mypy` runs in strict mode;
  `pytest` runs the tests with coverage. Tests that spawn processes are marked `integration`.
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

### Workflow

- `make check` runs everything CI runs: format check, clippy, ruff, mypy, cargo test, pytest.
- Pre-commit hooks run the same checks on commit.
- CI: GitHub Actions, one Rust job and a Python version matrix.

## Open questions

1. **Native Python API design**: shape of the top-level `schedr` API (how close to Airflow
   it is, and what it does differently).
2. **Outputs: details.** Required (see Scope). Should be a feature of the shared model, with the
   Airflow layer mapping onto it. Proposed mechanism: a task writes its return value as JSON to
   a result file (not stdout); it is stored per (DAG run, task, key); Rust passes stored values
   to downstream tasks as inputs. Sub-questions, each with the proposal:
   - a. Format: JSON plus `datetime`, `date`, `timedelta`, `tuple`; no pickle.
   - b. `multiple_outputs=True` and `out["key"]`: support.
   - c. `PythonOperator` return value and `.output`: support.
   - d. `BashOperator` pushes the last line of stdout by default, like Airflow: support.
   - e. `ti.xcom_pull(...)` and custom keys: leave out (Tier 2).
   - f. Size limit: about 1 MB per value; the task fails clearly if exceeded.
3. **Unsupported Airflow arguments** (`pool`, `sla`, callbacks, …): proposed: accept and log a
   warning for real Airflow parameters; raise an error for unknown ones.
4. **Rust/Python integration.** *Decided:* see Rust/Python integration, How Python tasks
   are started, and Packaging and releases.
5. **Interface details:** web UI and CLI are required (see Architecture). Still open:
   - Frontend technology.
   - Live updates to the browser. Proposed: Server-Sent Events.
   - UI authentication: OIDC, or trusting a reverse proxy (e.g. oauth-proxy) header.
   - What the CLI talks to. Proposed: the webserver's API, the same one the UI uses; the
     master's gRPC API stays internal to workers.
   - How the webserver gets state. Proposed: reads Postgres directly; actions (trigger, clear,
     …) are written to Postgres for the master to pick up, or sent to the master.
6. **Branching subset**: `@task.branch` and `BranchPythonOperator` only, or also
   `ShortCircuitOperator` / `@task.short_circuit`?
7. **Task groups subset**: plain grouping (`group_id.task_id` names, dependencies on a group,
   per-group `default_args`), or also mapped task groups (`@task_group(...).expand(...)`)?
8. **Dynamic task mapping subset**: `expand` over literals and over upstream output (proposed
    minimum); also `expand_kwargs`, `.map()`, `.zip()`? Proposed: an upper limit on mapped
    instances per task.
9. **Connections in secret storage: details** (the rest is decided, see Secret storage).
    Vault path layout (Airflow's Vault backend uses `connections/<conn_id>` in KV v2),
    `conn_uri` support, which access API to mirror (`Connection.get(conn_id)`,
    `BaseHook.get_connection(conn_id)`).
10. **Variables in secret storage too?** Airflow's Vault backend also stores Variables.
11. **Plain scripts:**
    - `schedr.toml` keys (schedule, script, arguments, environment, working directory,
      interpreter, retries, timeout, …).
    - Is `schedule` optional in a declaration? Proposed: yes; with a schedule it is a
      standalone job and can also be used from DAGs, without one it is only used from DAGs
      (or triggered manually).
    - What `ScriptOperator` takes from the declaration. Proposed: everything except
      `schedule`; the operator can override arguments, environment, retries, ….
    - Exact syntax of a root-relative reference in the API (e.g. `"etl:team_a/schedr.toml"`
      or a separate argument).
    - **Later (API design):** names of the other script operators (`ScriptOperator` is
      decided) and operator arguments; whether these operators also
      exist in `schedr.airflow`; script outputs (none, last line of stdout, or a result file).
12. **Gaps found in the end-to-end flow** (see End-to-end flow: running a native-API DAG):
    - a. **First scheduled run:** how `start_date` and the first run behave in the native API
      (depends on question 1).
13. **Custom operators** (users subclassing an operator base class, e.g. Airflow's
    `BaseOperator`, with their own `execute()`). Not in scope yet. Proposed if added: a
    custom operator runs as a `python` task: the runner rebuilds the operator from the DAG file
    and calls `execute()`; its return value is its output. It can't change scheduling behaviour
    (skips, branching stay with the master; see How the Python API works with the engine).
    Open: whether to support them at all, in the native API, in `schedr.airflow`, or both.
