# Workers

A worker is the Rust **agent** (`schedr worker`) plus the subprocesses it starts. The agent
never runs user code itself: each task and each parse job runs in a child process, so a
crash or a memory spike in user code doesn't take the agent down. A worker needs no
database. All it needs is `[worker] master_url`.

## The agent's loop

The agent runs one event loop per session with the master. Each turn handles one of these:

- a message from the master: `Registered`, `AssignTask`, `AssignParse`, `CancelAttempt`,
  `FinishedAck`;
- a task or parse job that ended (its result goes to the journal, then to the master);
- the heartbeat timer (`[master] heartbeat_interval`, 10 s, set by the master): free slots and
  running attempts;
- the scan timer (`[dags] poll_interval`, 30 s): a root whose files changed and then stayed
  unchanged for `[dags] settle_time` (5 s) gets a new hash, sent as `CodeVersionChanged`;
- SIGTERM, which starts a drain.

Messages to the master go into an unbounded channel, so the loop never waits for a slow
master. The loop beats at every turn, and at least every 5 s while idle; liveness watches
that beat.

## Connecting

On start the agent recovers its journal (below), waits for each root's first hash, then
connects and sends `Register`. That message carries its worker ID and incarnation, versions,
queues, slots, log endpoint address, the hash of each root, and what its journal holds. It
then sends `Warnings`, the whole list of its warnings, and sends that list again whenever
the warnings change (see [The fork server](#the-fork-server)).

If the connection drops, **running tasks keep running**. The agent journals what ends
meanwhile, and reconnects after a backoff that doubles from 200 ms to 30 s, with jitter, so
workers that lost the master together don't come back in step. After a session in which it
was registered, the backoff starts from 200 ms again.

## Running a task

1. **Checks** (before anything starts): a free slot, not draining, the root and file present,
   the code at the hash the master expects, the interpreter defined. A failed check is a
   `Rejected` with a reason; most reasons are temporary, and the master tries again.
2. **Journal, then `Started`.** The attempt is written to the journal before `Started` is
   sent. That way a restarted agent always knows about an attempt the master believes is
   running. `Started` carries `start_method`: `spawn` for a new process (the interpreter, or
   the command itself), or `fork` for a process from the fork server. The first `Started` is
   `spawn`, sent before the process exists. A Python task that was forked sends a second
   `Started` with `fork` once its process exists. The master keeps the first start time,
   stores `fork`, and the attempts API shows `fork` from then on (see
   [The fork server](#the-fork-server)). A command, and a Python task that was spawned, stay
   `spawn`.
3. **The process.** A Python task runs `python -m schedr.runner task`, forked from a fork
   server when the task allows it and a server can, otherwise in a new process. A command runs through
   the shell when it is a string, or directly when it is a list of arguments. Each starts in
   its own process group, so cancelling or killing reaches the whole
   task, and runs in its root's directory (Python) or its DAG file's directory (commands).
   stdout and stderr go to `<data_dir>/logs/<worker ID>/<attempt ID>.log`.
   The runner's first message is `hello`. The agent sends the task, and the runner answers
   `task_imported` (a message of no fields) once the DAG file is imported and the arguments
   are built, then calls the function. It sends no `task_imported` when that fails. The agent
   times the two gaps on its own clock (see [Observability](09-observability.md)).
4. **While it runs:** the memory watchdog samples the group's memory, and `execution_timeout`,
   a cancel or a drain can stop it. Stopping is SIGTERM to the group, then SIGKILL after
   `[master] cancel_grace` (30 s), or after `[worker] drain_terminate_period` (3 s) when a
   drain's time is up (see Draining below), also for a task already being stopped then: its
   SIGKILL comes no later than the drain's. The reason the agent stopped it is the attempt's status
   (`timed_out`, `cancelled`, `interrupted`), however the process then exits.
5. **Result.** It is written to the journal, then sent as `Finished` until the master
   acknowledges it with `FinishedAck`: again on a reconnect, and while the connection stays up
   (the master may have failed to store it) after one heartbeat interval, then ever less often
   (2, 4, ... up to 32 intervals); after 8 such sends the agent reconnects, and the master's
   answer to its journal settles the result. `Finished` includes `usage` and `core_dumped` (below).

## The fork server

The runner has a third mode, `python -m schedr.runner fork_server`. It imports a list of
libraries once, then starts one process per request. The process runs the same `task` or
`parse` code a spawned runner runs, with its own channel, log and process group. On Linux
the worker starts this mode for the interpreter `[dags] interpreter` names, when that
interpreter has `fork_server = true`, and forks Python tasks and parse jobs from it.
Command tasks are always a new process. A Python task can ask for the bare server only,
or for a new process every time (below).

**Turning it on.** Off by default in the configuration. An interpreter written as a path spawns every task:

```toml
[interpreters]
python = "/usr/bin/python3"
```

To fork, that same name is a table. `path` is the executable. `preload` is the libraries
the preloaded server imports, in that order. Preload a library that is slow to import and
that most tasks use. Leave a library that starts a thread off the list: the preloaded
server is then disabled until the worker process starts again, and the log's `reason`
names the threads.

```toml
[dags]
interpreter = "python"

[interpreters.python]
path = "/usr/bin/python3"
fork_server = true
preload = ["pandas", "sqlalchemy"]
```

`fork_server` on any other interpreter does nothing. Python tasks use `[dags] interpreter`
only. With no `preload` list, or an empty one, the worker starts only the bare server. On
macOS the setting is ignored. The worker logs `fork_server = true is ignored: fork servers
run on Linux only; tasks are spawned`, and every task is spawned. Forking is unsafe on
macOS.

**The chart.** The chart turns the fork server on by default for the distributed workers
and for standalone's own worker. `worker.forkServer` has `enabled`, `path` (the image's
Python) and `preload`. The preload list is the image's libraries except `polars`, kept in
`images/schedr/preload.txt`. `polars` starts threads on import, so preloading it would
disable the preloaded server; a task that uses it imports it in the child. The image's
build fails if that list leaves a thread running. `make chart-check` fails if the chart's
default list differs from the file. To use an interpreter of your own in `config`, turn
`worker.forkServer.enabled` off: the chart refuses to render when both set the DAG
interpreter. With a renamed DAG interpreter (`config.DAGS__INTERPRETER`) the chart sets
that one, not `python`. The values, and sizing the pod for the two long-lived processes,
are in [Deployment](10-deployment.md#the-charts-mode-distributed). A thread found when the
server starts disables that server, as above.

`[worker] fork_server_import_retry` (default `20m`) is how long, from the first failure, a
server whose preload fails to import is started again. `0s` means that failure is not
retried. A failed import is often something that is not ready when the pod starts (a
sidecar's network, a mount). A failure still there after the window is lasting (a missing
package, a typo in the list).

**The servers.** On Linux the worker starts them when the worker starts. With a `preload`
list there are two: a preloaded one, sent that list, and a bare one, sent no modules (it
has `schedr`, and nothing else preloaded). Without a list there is only the bare one. Each
server's own output is appended to `fork-server-preloaded.log` or `fork-server-bare.log`
in the worker's log directory, beside the task logs.

The handshake is the usual one: `hello` (the mode is `fork_server`, and the message carries
the runner's schedr version), then `welcome`. The next message is `fork_server`, the modules
to import, in order. The server imports them, tells the garbage collector to leave those
objects alone (a collection in a child would otherwise copy their pages), and checks that it
has no thread but its own. It answers `fork_server_ready`, or `fork_server_disabled` with the
reason and the other threads' names. A disabled server stays up and answers every later
`fork` with `fork_failed`, until the other side closes the connection: exiting while a
request is still unread would reset the connection and can drop `fork_server_disabled`. The
worker reads the answer and stops the process. The disabled answer is also what anything but
Linux gets, and what a preload that raises or exits gets. A thread that appears later is
answered `fork_server_disabled` for that request. The worker stops the process and does not
start that server again.

A `fork` request carries an id, the mode (`task` or `parse`), the process's environment and
working directory, and four open files sent with the message: the process's own channel, its
stdout, its stderr, and a status socket. The server answers `forked`, or `fork_failed` with
the reason. Before the fork it flushes its output buffers, including C's, so text a preloaded
library printed and never flushed is not copied into the new process.

**The shim.** The server forks a shim, and the shim forks the process. The shim is not in the
process's group, so a signal to that group does not reach the shim. The shim puts the process
in its own group, and the process does the same. The shim sends `process_started` only after
its own call. The message is the process ID, which is the group's ID. The shim then waits
without collecting the process. When the process ends, the shim sends `process_exited` and
collects the process only after the other end of the status socket closes. Until then the
process ID stays taken, so a signal to that group cannot hit a different process that was
later given the same ID. The shim writes nothing. If it dies, the status socket closes and
there is no `process_exited`.

`process_exited` carries the exit code or the signal, and `core_dumped` (whether the kernel
reported a core dump). It also carries `usage`, when that can be read: user and system CPU
time in microseconds (`user_cpu_micros`, `system_cpu_micros`), the peak resident set size in
bytes (`peak_rss_bytes`), major page faults (`major_faults`), and how often the process gave
up the CPU or was preempted (`voluntary_context_switches`,
`involuntary_context_switches`). The shim reads all of those fields from the kernel's
`rusage` for the process. The resident size includes pages shared with the fork server. The
watchdog's PSS counts only a share of those pages (see [Memory](#memory)). The figures
include children the process had already collected. If they can't be read, `usage` is null
and the exit status is still sent. The worker puts this `usage` and `core_dumped` on
`Finished`. The master stores them with the attempt, and the attempts API shows them.
`usage` is null while the attempt is still running, when it was not read, and when the
master ended the attempt itself. `core_dumped` is false unless the process dumped core.

A spawned process has no shim. Before the worker collects it, it reads the zombie's
`/proc/<pid>/stat`. `usage` then has the user and system CPU time, including children the
process had already collected, and the major faults, its own and those children's.
`peak_rss_bytes` is null, because a zombie's memory is gone. Both context-switch counts are
null, because its `status` has its leader thread's switches only, not its other threads' or
those children's. If the zombie can't be read, `usage` is null. `core_dumped` is still
whether the kernel reported a core dump.

**The process.** stdin is `/dev/null`. stdout and stderr are the pipes it was given. Those
descriptors are blocking, as a spawned process's pipes are. The environment is replaced with
the request's. The time zone, the character-type locale and the temporary directory are then
taken from that environment again, as they are when an interpreter starts. The other locale
categories are `C`, and an unknown locale stays `C`. The working directory is the request's.
The standard streams are the interpreter's own, even where a preloaded library had replaced
them. Python reseeds `random` in the child. Files a preloaded library already had open stay
open. `logging` is left as the preload set it: a spawned process that imported the same
library would have the same handlers, and Python rebuilds logging's locks in the child. A
handler that writes a file keeps that file, and the forked processes share it. The process
then runs the normal runner on its own channel. On the way out it does what an interpreter's
shutdown does, in the same order: it waits for its threads that aren't daemons, then runs its
own exit handlers (`atexit`, `weakref.finalize`, and `logging`'s shutdown, which flushes
buffered handlers). The handlers the fork server registered while preloading don't run: they
belong to the server, and one could act on the server's state. A preloaded library's exit
handler therefore doesn't run per task, where a spawned process would run it.

**If the fork server or the shim dies.** The shim is its own process. It closes the server's
connection and then only uses the status socket, so once it exists it still sends
`process_started` and `process_exited` if the fork server dies. The process keeps running
too: the fork server dying does not signal it. A shim that dies does not itself stop the
process. The process is in its own group, so a signal to the shim's group does not reach it.
The status socket closes, and there is no `process_exited`. What the worker does then is
below.

**What the worker does with a task.** It asks the preloaded server to fork the runner, then
the bare one (only the bare one, when that is the only server). A task with `preload` false
asks only the bare server. That is for code that misbehaves next to the preloaded libraries.
The skipped server is not counted in `schedr_worker_fork_fallbacks_total`: the task never
asked it. A task with `fresh_process` true is always a new process, never forked, for code
that misbehaves after a fork, and `start_method` stays `spawn`. Left unset, `preload` is
true and `fresh_process` is false, so the task asks the preloaded server. When both are
set, `fresh_process` true wins. The DAG author sets them on `@task(...)`, `.override(...)`
and `TaskDefaults(...)`. The task's own argument wins, and they are stored as `preload`
and `fresh_process` in the task's settings. They apply to a Python task. A command is
always a new process. `CommandOperator` does not take them. A `TaskDefaults` value is
stored on every task and used only when that task is Python.

A server that does not fork the task counts in
`schedr_worker_fork_fallbacks_total{interpreter,reason}`, once per server:

| `reason` | Why that server was skipped |
|---|---|
| `disabled` | It is disabled: a preload failed to import, it has another thread, or its schedr version is not the agent's. |
| `down` | It is starting, waiting to be started again, or it did not answer the fork within 30 s (or the answer could not be paired with the request). |
| `fork_failed` | It answered `fork_failed`. The log `fork failed` carries the reason. |
| `shim_died` | The shim ended, or sent nothing for 30 s, before `process_started`. The worker has no process ID yet, so it does not kill anything. |

When no server forks the task, the worker spawns it and `start_method` stays `spawn`. When
one does, the worker takes the process ID from `process_started` and sends the second
`Started` with `fork`. From then on the attempt stays with that process. A runner that
never says hello fails the attempt with error kind `start_failed`. The worker does not
spawn another process for it.

A parse job forks one fresh child when the worker has fork servers: the preloaded server,
then the bare one, then a new process. Each server that does not fork it counts in the
same metric, with the same reasons. `schedr dags check` starts a new process and does not
ask the fork servers.

A forked task or parse job that the worker drops before the process has ended is killed,
as a spawned process is.

If the status socket then closes with no `process_exited`, the worker sends SIGKILL to the
process group. There is no exit code. When the runner sent no result, the attempt ends
`failed` with error kind `process_status_unknown`. The error says the shim that reports the
process ended first, and what was left of the group was killed. A task the worker had
already stopped keeps that status (`timed_out`, `cancelled`, `interrupted` or
`oom_killed`); the error kind is still `process_status_unknown`.

**When a server stops.** A server that exits, or that cannot be started, is started again.
The wait is 1 s, then doubles, up to 5 min. After the server has been up for 10 min, the
next wait is 1 s again. Meanwhile a task or a parse job uses the other server, then a new
process. One already forked keeps running: its shim still reports, and the server's exit
does not signal it.

A preload that fails to import disables that server. The worker starts it again on those
same waits, while the time since the first failure is still under
`[worker] fork_server_import_retry`. The metric stays `disabled` for the whole of that,
including the waits. After the window the server stays disabled until the worker process
starts again. Threads after preloading, and a schedr version that is not the agent's, are
never started again: they are the same on every start until the image changes. So is a
server that disables itself while handling a fork.

The worker logs each of these. `kind` is `preloaded` or `bare`, and the disabled lines
include `reason`.

| Log | When |
|---|---|
| `fork server up` | It finished its preload and is taking requests. |
| `fork server ended` | A running server exited. It is started again. |
| `can't start the fork server` | It did not start, or its preload did not answer within 5 min. It is started again, and it is not disabled. |
| `fork server disabled; retrying` | A preload import failed, and it will be tried again. Logged on the first failure, and again only when the reason changes. |
| `fork server disabled until restart` | It will not be started again until the worker process starts again. For an import it has stopped retrying, `gave_up` is how many minutes it had been failing. |
| `fork server disabled` | It disabled itself while handling a fork. Not started again. |
| `fork failed` | One fork request failed. The task or parse job tries the next server, or is spawned. |
| `fork server out of step; restarting it` | An answer could not be paired with its request. The server is dropped and started again. |

`schedr_worker_fork_servers{interpreter,kind,state}` is 1 for a server's current state and
0 for its other states (`up`, `down`, `disabled`). `interpreter` is the DAG interpreter's
name. The series are present from the moment the worker starts the server, so one that is
still starting is `down`. See [Observability](09-observability.md).

**Warnings.** A disabled fork server is a warning on the worker. After every registration,
and again whenever that list changes, the worker sends `Warnings`: the whole list, so a
server that is no longer disabled drops out, and a worker with none sends an empty list.
One warning per disabled server. Its kind is `fork_server_disabled`, its interpreter is the
DAG interpreter's name, and its message names the server and why it is disabled, then what
that means for tasks. A parse job uses those same servers. The message is `the preloaded
fork server is disabled: <reason>; <what tasks do>`, or the same with `bare` in place of
`preloaded`:

| What is disabled | The message ends |
|---|---|
| The preloaded server, while the bare one isn't disabled | `tasks are forked by the bare fork server, or spawned` |
| The bare server, while the preloaded one isn't disabled | `tasks the preloaded fork server can't fork are spawned` |
| Both servers, or the bare server when it is the only one | `tasks are spawned` |

A server that is down or still starting is not a warning. The log lines above, and
`disabled` on `schedr_worker_fork_servers`, show the same servers. The master replaces the
worker's stored list with each report and keeps that list while the worker is gone. The
worker in the API has `warnings` (`kind`, `interpreter`, `message`). `schedr workers list`
has a `WARNINGS` column with the count, and prints each warning's message under the table
as `<worker>: <message>`.

A forked task shares pages with its fork server, so the watchdog does not kill it on
resident set size when PSS cannot be read (see [Memory](#memory)).

## The journal

One file per attempt under `<data_dir>/journal/<worker ID>/`, written atomically (a temporary
file, fsync, rename) before the message it backs is sent. It holds `running` attempts, with
their process group, and `finished` results not yet acknowledged.

When the agent starts, it reads the journal before connecting:

- **Finished results** are reported in `Register` and sent again. Nothing a task produced is
  lost to an agent restart.
- **Running attempts** belonged to the previous incarnation, and their processes can no
  longer report to this agent. So it stops their process groups (SIGTERM, SIGKILL after 10 s)
  and leaves them out of `Register`. The master then ends them `lost` (using a retry), with
  the summary "worker restarted while the attempt ran (possibly out of memory)".

A process group is identified by its ID and its leader's start time, so a recycled PID is
never signalled. Without `/proc` (macOS) that start time can't be read: the agent warns once
that orphaned tasks aren't stopped after a crash, and leaves them running. One agent per
journal: a second agent with the same ID and data directory exits at once.

## Memory

All tasks on a worker share the pod's memory limit. If the kernel's out-of-memory killer
fires, it kills everything in the container, the agent included. So the agent enforces
**budgets** itself:

- The limit is the smallest cgroup v2 `memory.max` from the agent's cgroup up. Each task's
  budget is `[worker] memory_budget`, else (limit − 10 %) ÷ slots. The parse runner gets the
  same budget.
- Every `[worker] memory_check_interval` (1 s) the agent measures each task's process group
  (PSS, so shared pages aren't counted twice). A spawned task uses resident set size (RSS)
  for a process whose PSS can't be read. A forked task shares pages with its fork server, so
  its RSS counts those pages and would look far bigger than the task's own memory. If a
  process in its group has no PSS, that sample is skipped and the task is not killed for it.
  A zombie (a child not yet reaped) holds no memory and counts as nothing.
- A group over its budget gets **SIGKILL at once**: a spike can't wait for a grace period.
  The attempt ends `oom_killed` (using a retry), with "exceeded memory budget … (… used)".
- With no limit found (cgroup v1, a Linux laptop), memory is measured but never limited.
- Without `/proc` (macOS) there is no cgroup limit and a task's memory can't be read, so the
  watchdog neither measures nor kills.

Every attempt reports its peak memory, shown in the API, and its `start_method` once
`Started` has been stored. `usage`, stored with the result, is separate. For a forked
process its peak resident size includes the fork server's pages. This peak is the watchdog's
PSS, which counts only a share of those pages (see [The fork server](#the-fork-server)).

## Draining

**On SIGTERM** (a rolling update, a node drain) the agent sends `Draining`. The master stops
assigning to it, and the agent rejects anything sent before the master knew. Running tasks get
`[worker] termination_grace_period` (30 s, which should match the pod's
`terminationGracePeriodSeconds`) minus the last part, which the drain keeps for itself:

| From the end of the grace period | What happens |
|---|---|
| − (`drain_terminate_period` + `drain_report_period`), 5 s by default | Whatever still runs gets SIGTERM. |
| − `drain_report_period`, 2 s by default | What still runs gets SIGKILL (`drain_terminate_period`, 3 s, after the SIGTERM). |
| until the end | The interrupted tasks are reported `interrupted` (run again elsewhere without using a retry); the agent waits for the master's acknowledgements, sends `Deregister` and exits 0. |

So the worker is gone before the pod is killed, and no task ends `lost` because it was slow
to stop. The two times must fit in the grace period, else the worker doesn't start. A second
SIGTERM interrupts at once.

**For maintenance**, `schedr workers drain <id>` is a command. The worker gets no new tasks or
parse jobs, its running tasks finish normally, and it stays connected and idle until
`schedr workers resume <id>`. The drained state is stored, so it survives restarts of both the
master and the worker.

## Logs

The agent serves its task logs on its **log endpoint** (`[worker] log_port`, 8081):
`GET /logs/{attempt_id}?from=&limit=`, at most 1 MiB per request. Response headers give the
current size, whether the attempt still runs, and which worker answered. The webserver is
the only client. It finds the worker that ran the attempt and that worker's current address in
the database, and passes the log on (`GET …/attempts/{try}/log`). If another worker answers at
that address (a stale address after a restart), the webserver says so, instead of claiming
the log is gone.

Logs older than `[worker] log_retention` (30 days) are deleted, and beyond
`[worker] log_max_size` the oldest go first. The sweep runs at start and every hour; a log
whose attempt is still in the journal is never deleted. The run page shows the end of one
try's log, follows the rest while the try runs, and shows the reason when the log can't be
read (see [The task panel](12-ui.md#the-task-panel)).
