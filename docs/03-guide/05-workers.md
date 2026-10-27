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
queues, slots, log endpoint address, the hash of each root, and what its journal holds.

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
   the command itself), or `fork` from the fork server (M2.5; until then every attempt is
   `spawn`). The master stores
   it, and the attempts API shows it from then on.
3. **The process.** A Python task runs `python -m schedr.runner task`. A command runs through
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
   acknowledges it with `FinishedAck`.

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
  (PSS, so shared pages aren't counted twice).
- A group over its budget gets **SIGKILL at once**: a spike can't wait for a grace period.
  The attempt ends `oom_killed` (using a retry), with "exceeded memory budget … (… used)".
- With no limit found (cgroup v1, a Linux laptop), memory is measured but never limited.
- Without `/proc` (macOS) there is no cgroup limit and a task's memory can't be read, so the
  watchdog neither measures nor kills.

Every attempt reports its peak memory, shown in the API, and its `start_method` once
`Started` has been stored.

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
whose attempt is still in the journal is never deleted.
