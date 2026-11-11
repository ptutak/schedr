# Workers

A worker is the Rust **agent** (`schedr worker`) plus the subprocesses it starts. The agent
never runs user code itself: tasks and parse jobs run in child processes, so a
crash or a memory spike in user code doesn't take the agent down. A worker needs no
database. All it needs is `[worker] master_url`.

## The agent's loop

The agent runs one event loop per session with the master. Each turn handles one of these:

- a message from the master: `Registered`, `AssignTask`, `AssignParse`, `CancelAttempt`,
  `FinishedAck`, `HeartbeatAck`;
- a task or parse job that ended (its result goes to the journal, then to the master);
- the heartbeat timer (the 10 s [heartbeat interval](14-timings.md#fixed-timings), sent by the
  master): free slots and
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
queues, slots, log endpoint address, the hash of each root, and what its journal holds,
and, for the [Deployments](12-ui.md#deployments) page, its process ID and its configuration,
masked before it is sent (no secret leaves the process in clear). It
then sends `Warnings`, the whole list of its warnings, and sends that list again whenever
the warnings change (see [The fork server](#the-fork-server)).

If the connection drops, running tasks initially keep running. After a worker lease (60 s
by default) without word from the master, the agent stops those with `cleanup_anywhere` as
`lost`, using a retry; sooner when the master's last message was held on its way: the agent
also stops them once master time may have passed that message's stamp plus the lease, over
a reconnect too (see [Master time](17-master-clock.md)). Their cleanup waits for the master's decision, including if a task
ends on its own while disconnected. A master outage longer than the lease has the same effect.
The agent ends any stream silent for a lease, whatever it runs, and acts on no message
arriving past that limit. This also applies while fetching inputs: a late chunk is ignored,
the task is not started and no rejection is sent. On its first stream, before it knows the
lease, it drops a `Registered` at least as old as the lease stated in that reply and registers
again. The master takes back an unstarted assignment two heartbeat intervals after that lease
(decision 450).

The agent journals what ends meanwhile, and reconnects after a backoff that doubles from 200 ms
to 30 s, with jitter, so
workers that lost the master together don't come back in step. After a session in which it
was registered, the backoff starts from 200 ms again. Once the worker knows its lease, each
reconnect wait is capped at half that lease. A restarted master gives it a fresh lease from
the master's start plus the time a master it replaced may still answer it (20 s by default).
A worker that has heard a master of a newer lease epoch ends a stream from an older one, a
deposed master still answering, unless the newer has been silent for a whole lease.

If the worker returns after its lease and its `cleanup_anywhere` move is still waiting, the
master gives the cleanup back when the worker has handles it did not receive. If the move
was assigned elsewhere or ended, `CLEAN_REST` asks the original worker to clean those
remaining handles. See [State and failures](07-failures.md).

## Checking connections

For each root, the master picks a connected, active worker that is not drained and has the
root at the current parse's exact hash. It picks the one running the fewest attempts, then
by worker name. A worker on older code, or newer code not parsed yet, isn't picked: the
connection definitions come from that parse. `--worker NAME` asks for a particular one, which
must
meet the same conditions. If no suitable worker is found, the command is rejected.

A worker runs one connection check or test at a time. Its time starts when the job arrives,
including its wait for its turn and secret reads: 30 s for a check, or the test's `--timeout`
(60 s by default). A test is a one-off attempt of the connection's file beside the tasks; it
uses no task slot. Its process is journalled like an attempt's, so an agent restarted after
being killed stops the process it left behind. See [Secret
storage](13-secrets.md#checking-a-deployments-connections)
and [State and failures](07-failures.md#connection-jobs).

`[worker] pass_env` passes the named environment variables to parsing and the fork server
as well as tasks, so imports see the same settings. Secrets do not belong in `pass_env`:
DAG code can read them during parsing, and parse tracebacks are not masked.

## Running a task

Sensors share a watcher host and have different process and cancellation behavior; see
[The watcher host](#the-watcher-host).

For [built-in connections](../02-dags/14-connections.md#built-in-types), install the client
libraries in the interpreter where tasks run: `schedr[http]`, `schedr[ssh]`,
`schedr[mongo]`, `schedr[postgres]` and `schedr[mysql]`, as needed. A library is imported
when its connection is opened; without it the attempt fails, naming the extra to install.
For commands, install the tools the DAGs call in the worker image: `psql`, a MySQL or
MariaDB client (`mysql` and the dump tools), MongoDB database tools and/or `mongosh`,
`curl`, and `ssh`, as needed. schedr does not supply these clients. See
[In a command](../02-dags/14-connections.md#in-a-command) for their use.
The worker image also needs CA certificates: HTTP, SQL and Mongo use the system's CAs by
default. Give a private CA as the connection's `ca_cert` (PEM text).

1. **Checks** (before anything starts): a free slot, not draining, the root and file present,
   the code at the hash the master expects, the interpreter defined. For a script with a
   script cleanup, the cleanup script and its interpreter must also be available. A moved or
   recovered cleanup is planned for what the cleanup runs, without requiring the task's
   interpreter. A failed check is a `Rejected` with a reason; most reasons are temporary, and
   the master tries again.
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
   server when the task allows it and a server can, otherwise in a new process. A command runs
   through
   the shell when it is a string, or directly when it is a list of arguments. It inherits
   the attempt's context channel, `SCHEDR_CTX_FD`
   ([A command's context channel](#a-commands-context-channel)). Each starts in
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
   SIGKILL comes no later than the drain's. The reason the agent stopped it is the attempt's
   status
   (`timed_out`, `cancelled`, `interrupted`, or `lost` on cut-off), however the process then
   exits. For an async task, SIGTERM cancels its coroutine so `finally` and `async with`
   exits can run; the process then ends by SIGTERM as a sync task's does. A sync call that
   blocks the loop prevents cancellation from running and the group is killed after the grace.
   The [event loop](../02-dags/02-tasks.md#the-event-loop) is selected by the task's Python
   environment.
5. **Cleanup.** If the task process started and has a cleanup, the agent reports `TaskEnded`
   and runs it on the same worker. The attempt holds its slot through the cleanup and its
   retries; the instance is `cleaning_up` meanwhile. The task's secrets and connections remain
   available. Handles are journaled locally and survive an agent restart with the journal disk;
   the master also stores them and can move cleanup after worker loss when `cleanup_anywhere`
   is enabled. See [Cleaning up after a task](../02-dags/15-cleanup.md).
6. **Result.** It is written to the journal, then sent as `Finished` until the master
   acknowledges it with `FinishedAck`: again on a reconnect, and while the connection stays up
   (the master may have failed to store it) after one heartbeat interval, then ever less often
   (2, 4, ... up to 32 intervals); after 8 such sends the agent reconnects, and the master's
   answer to its journal settles the result. `Finished` includes `usage` and `core_dumped`
   (below).

### The watcher host

Sensors for one interpreter and set of root copies run as coroutines in a spawned,
long-lived host. A new code set uses another host; each stops when its last wait step ends.
They take watcher slots (`[watchers] slots`, 500 by default), not task slots; with none free a wait
step stays `queued` and says why. A draining worker hands its pokes to other workers at once, using
no retry, and full-control wait steps get the drain's grace time. The host imports each DAG file once, shares module globals, and
uses the worker's log directory as its working directory and the allowed environment plus
`pass_env`. Run-specific values come from `dag_ctx` and `task_ctx`; use absolute paths.
Blocking the event loop holds up the host's other wait steps.
Wait steps on a host share memory and can read each other's secrets; separate teams that
must be isolated with separate queues and workers.
[`wait_for(RunOf(...))`](../02-dags/21-wait-for.md) is ended by the master and does not
use a watcher host or slot.

A wait step's `max_wait` counts from its first start, across retries and moves. The agent
sets that first start on the first try; the master sends it with a later try, so `delay`
counts from the same moment.

A poke can move between calls to new code after the new host imports it and builds its call.
It keeps its deadline and uses no retry. If preparation fails, it stays on the old code and
logs why. A full-control `@sensor` keeps its host and code copy until it ends; frequent
deploys with long waits can retain many copies. Retries start on the newest code. See
[Wait steps](../02-dags/18-sensors.md#running-and-deploying-wait-steps).

Cancellation cancels the coroutine. If it does not end within its grace, the agent kills the
host; the other wait steps start again on a new host in the same attempt, keeping their time
and using no retry. The same happens to the others when a step blocks the loop past
`[watchers] block_limit`; the blocking step fails `blocked_event_loop` without a retry.

A host crash, memory kill, or host that stops answering ends its wait steps `lost` with
`watcher_host_lost`, each using a retry. No wait step is switched off: none can be blamed, so
the error names the wait steps that were on the host.
`[watchers] memory_budget` limits all of a worker's watcher hosts together (default 1 GiB);
when they exceed it, the largest host is killed. Asyncio tasks left by a step are
cancelled and awaited when it ends. A wait step can't start a process
([Wait steps](../02-dags/18-sensors.md#running-and-deploying-wait-steps)); one C code started runs
until the host stops, and a host that loses the agent's socket kills its process group.
Step output, a thread's it started included, goes to its attempt log; output from C code, or
from a thread outliving its step, goes to the host's own log, masked using the secrets of every
wait step the host has held.

### Event triggers

A DAG's [event trigger](../02-dags/19-triggers.md) runs on a watcher slot in its code's
watcher host, with the same event-loop and blocking rules as a wait step. Its events start
runs; a manual run of that DAG must also supply an event. The host gathers a batched
poke's window and sends one ordinary event to the master.

`[triggers] max_queued_events` limits queued event runs per trigger (default 1000; 0 means
no limit). At the limit, schedr stops the trigger and starts it again from its bookmark
once no event runs are queued. `[triggers] failing_after` (default 5) sets the consecutive
failure count at which it is counted as `failing` in the master metric.

The master handles [`RunOf` triggers](../02-dags/21-wait-for.md#starting-a-dag-after-another)
without a watcher slot. Their queued runs also count against `max_queued_events`; at the limit,
the master waits for room before looking for more runs.

If a trigger repeatedly gets its watcher host killed, it is switched off like a sensor;
`schedr sensors switched-off` lists it. Use `schedr triggers switch-on <dag>` to clear its
strikes, or `schedr triggers restart <dag>` to restart it from its bookmark. If its host is
killed for another watcher, it restarts from the bookmark after the failure backoff. See
[Wait steps](../02-dags/18-sensors.md#running-and-deploying-wait-steps).

### Commands' connection files

By default the worker writes these files under
`/dev/shm/schedr-<uid>/<hash of data dir>/<worker>/` when `/dev/shm` is a directory.
Otherwise it uses `<data_dir>/connections/<worker>/`. Set `[worker] connection_dir` to
choose the base directory; the worker ID is appended. The hash separates workers of the
same user with different data directories. The fallback or a configured directory can be on
disk, so mount memory-backed storage there if the files must stay in memory.

Before starting, the worker creates the worker folder and its parent, and checks the whole
path. Those last two must be real directories owned by the worker user, with no group or
other permissions. Other directories must be owned by that user or root, and not writable by
others (or by another group), unless sticky; symlinks there must be root-owned. An unsafe
path stops startup, naming the path and reason, with `connection files (secrets) can't go
below it` and advice to remove it or set `[worker] connection_dir`.

Each attempt gets its own folder, with connection and prefix folders beneath it (0700;
files 0600). The worker removes it when the attempt ends; a removal failure is logged. On
restart it sweeps folders left by earlier attempts, except those of processes it could not
stop. See [Secret storage](13-secrets.md) for what the files hold.

### A command's context channel

A command or script attempt gets one Unix datagram socket. The process inherits one end, and so
does every process it starts. The descriptor's number is `SCHEDR_CTX_FD`. There is no
socket file, so only a process that holds the descriptor can reach the channel. The
agent serves it until the attempt ends. A call after that is told the task has ended. A
Python task has `dag_ctx` in its function, and programs it starts find `SCHEDR_CTX_FD`
unset.

Each call opens a connection of its own. The caller makes a fresh stream socket pair,
sends one end over the datagram socket, and talks on the other end. The agent answers
on that connection, so concurrent calls keep their own answers.
It serves at most 64 of those connections at once and tells the next caller so. A call
waits up to 30 s to be taken, and 30 s to be answered.

Tasks' API calls go from the agent to the master on the Control stream. Under the master's
lease, the master runs them in process through the API router as the task or trigger, only
for an open attempt or trigger on that worker and only on the allow-list (currently `GET`
and `POST /api/v1/events`). The agent keeps unanswered calls and resends them after a
reconnect, giving up after a minute (so a command's API call waits up to that, not 30 s).
`[grpc] max_message_size` limits requests (413 before
sending) and answers (502 if too large); a command's context has a separate 1 MiB limit.

The same channel carries `resource_set` (a command or script resource setup reports its typed
value
once) and `resource_get` (a resource's command or script cleanup reads its setup's value); see
[Resources](../02-dags/16-resources.md#a-command-setup).

The agent accepts a caller by the protocol it speaks (protocol 1), whatever schedr
version sent it. Within that protocol a change is a new request or a new field. Either
side ignores a field it does not know, and a request the agent does not know is answered
with an error. A new value of `kind` is a new protocol: an older caller cannot read it. What
the commands print, and the messages when a call cannot be done, is
[Reading the run from a command](../02-dags/06-context.md#reading-the-run-from-a-command).

## The fork server

The runner has a third mode, `python -m schedr.runner fork_server`. It imports a list of
libraries once, then starts one process per request. The process runs the same `task` or
`parse` code a spawned runner runs, with its own channel, log and process group. On Linux
the worker starts this mode for the interpreter `[dags] interpreter` names, when that
interpreter has `fork_server = true`, and forks Python tasks and parse jobs from it.
Command tasks are always a new process. A Python task can ask for the bare server only,
or for a new process every time (below).

**Turning it on.** Off by default in the configuration. An interpreter written as a path spawns
every task:

```toml
[interpreters]
python = "/usr/bin/python3"
```

To fork, that same name is a table. `path` is the executable. `preload` is the libraries
the preloaded server imports, in that order. Preload a library that is slow to import and
that most tasks or DAG files use; the list speeds parsing too. Leave a library that starts a
thread off the list: the preloaded server is then disabled until the worker process starts
again, and the log's `reason` names the threads.

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
list there are two: a preloaded one, sent that list, and a bare one, sent no modules. Both
load all of `schedr` first (but its runner and `schedr.testing`), with what it imports,
such as pydantic: a child then imports none of it again, about 200 ms a task. Without a
list there is only the bare one. Each
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
and `StepDefaults(...)`. The task's own argument wins, and they are stored as `preload`
and `fresh_process` in the task's settings. They apply to a Python task. A command is
always a new process. `CommandOperator` does not take them. A `StepDefaults` value is
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

A parse job starts a fresh child for each file, asking the preloaded server, then the bare
one, then spawning a process. Each server that does not fork it counts in the same metric,
with the same reasons. `schedr dags check` also uses its configured fork servers when usable.

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

Under `<data_dir>/journal/<worker ID>/`, an attempt's record holds its process group, its
`cleaning` task result and cleanup process, or its unacknowledged `finished` result. Records
are written atomically (temporary file, fsync, rename). Results that end together are written
side by side, the directory flushed once for the group, before any of them is sent (up to 32). An attempt with cleanup also keeps
`<attempt_id>.attempt` (assignment, inputs, code copies and hash, including the script root
copy when present) and `<attempt_id>.handles` (one handle per line). Handles are also sent to
and stored by the master.

At startup the agent stops orphaned process groups (SIGTERM, SIGKILL after 10 s). A finished
result is reported in `Register`: `Ack` removes it, `Continue` sends it again, and `Kill`
discards it. A running attempt without recoverable cleanup is left out of `Register`; the
master ends it lost. With recoverable cleanup, the agent holds a slot and reports it as
running.
It waits for `Continue` before reporting `TaskEnded` and restarting cleanup with its handles,
all tries and freshly read secrets. A task interrupted while running gets a `lost` result; one
already cleaning keeps its task result. `Kill` discards the waiting cleanup; `CLEAN_REST`
stops any still-running task and cleans the handles the moved cleanup did not have. Even a
record
without a process identity gets cleanup, since the agent cannot tell whether it started. A
journal write failure is logged; a result not written can be lost on restart before its ack.

A process group is identified by its ID and its leader's start time, so a recycled PID is
never signalled. Without `/proc` (macOS) that start time can't be read: the agent warns once
that orphaned tasks aren't stopped after a crash, and leaves them running. One agent per
journal: a second agent with the same ID and data directory exits at once.

## Memory

All tasks on a worker share the pod's memory limit. If the kernel's out-of-memory killer
fires, it kills everything in the container, the agent included. So the agent enforces
**budgets** itself for task and parse processes and watcher hosts:

- The limit is the smallest cgroup v2 `memory.max` from the agent's cgroup up. Each task's
  budget is its own `memory=`, else `[worker] memory_budget`, else (limit − reserve) ÷ slots.
  The parse runner gets the worker's budget for each file process. Parsing is separate from
  task slots, so concurrent parsers can together exceed the pod limit; see [Parsing capacity](06-parsing.md#imports-and-parsing-capacity).
- Every [second](14-timings.md#fixed-timings) the agent measures each task's process group
  (PSS, so shared pages aren't counted twice). A spawned task uses resident set size (RSS)
  for a process whose PSS can't be read. A forked task shares pages with its fork server, so
  its RSS counts those pages and would look far bigger than the task's own memory. If a
  process in its group has no PSS, that sample is skipped and the task is not killed for it.
  A zombie (a child not yet reaped) holds no memory and counts as nothing.
- A group over its budget gets **SIGKILL at once**: a spike can't wait for a grace period.
  The attempt ends `oom_killed` (using a retry), with "exceeded memory budget … (… used)".
- With no limit found (cgroup v1, a Linux laptop), the worker accepts any task budget and
  enforces its own `memory=` if set. Otherwise memory is measured but not limited.
- Without `/proc` (macOS) there is no cgroup limit and a task's memory can't be read, so the
  watchdog neither measures nor kills.

A task's own budget replaces the worker's share. A worker rejects a task whose budget exceeds
its ceiling, the memory limit less the reserve, so another worker can take it. If none can,
the task waits with a reason saying so. A cleanup moved to another worker is never turned away
for its budget; its watchdog budget is at most that worker's ceiling.

For `hard_memory_limit=True`, the worker starts the internal `schedr task-exec`, which sets
`RLIMIT_DATA` and runs the task's program in its place. It remains the same process and process
group, with the same descriptors. See [Task memory](../02-dags/02-tasks.md#memory) for the
allocation behavior and the cost of starting a capped Python task.

Task process attempts report their peak memory, shown in the API, and their `start_method` once
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
| Earlier, by one cleanup try | A task with cleanup gets SIGTERM early enough to leave room for that try, when possible. |
| − the termination and report periods, 5 s by default | Other tasks still running get SIGTERM. Cleanups already running are left alone. |
| − the fixed [report period](14-timings.md#fixed-timings), 2 s | What still runs gets SIGKILL (`drain_terminate_period`, 3 s, after the SIGTERM). |
| until the end | The interrupted tasks are reported `interrupted` (run again elsewhere without using a retry); the agent waits for the master's acknowledgements, sends `Deregister` and exits 0. |

A running cleanup may outlast the grace period: the agent waits for it, but a pod killed
meanwhile
is lost. A cleanup held for the master's answer does not hold up a drain; the journal keeps it.
At its start the agent logs if `[worker] cleanup_timeout` cannot fit in the grace period, and it
logs once for each task whose own `cleanup_timeout` cannot. The two times must
fit in the grace period, else the worker doesn't start. A second
SIGTERM interrupts at once.

**For maintenance**, `schedr workers drain <id>` is a command. The worker gets no new tasks or
parse jobs, its running tasks finish normally, and it stays connected and idle until
`schedr workers resume <id>`. The drained state is stored, so it survives restarts of both the
master and the worker.

## Logs

The agent serves its task logs on its **log endpoint** (`[worker] log_port`, 8081):
`GET /logs/{attempt_id}?from=&limit=`, at most 1 MiB and 64 KiB per request (a part and the
bytes before it, see below). Response headers give the
current size, whether the attempt still runs, and which worker answered. The webserver is
the only client. It finds the worker that ran the attempt and that worker's current address in
the database, and passes the log on (`GET …/attempts/{try}/log`). If another worker answers at
that address (a stale address after a restart), the webserver says so, instead of claiming
the log is gone. The webserver passes whole lines only, masked (a task may print an address
with its password; see [Secrets](12-ui.md#secrets)). A part asked from inside a line starts
at the next one, and a running attempt's last line waits until it ends. A line longer than
the part is shown to its last whitespace, then a note that the rest of the line is not
shown, and that rest is passed over. Each part is masked together with the 64 KiB before it,
which is not shown, so a value that began there (a quoted one continuing over lines) is
hidden in the part too.

Logs older than `[worker] log_retention` (30 days) are deleted, and beyond
`[worker] log_max_size` the oldest go first. The sweep runs at start and every hour; a log
whose attempt is still in the journal is never deleted. The worker creates its logs 0600,
readable by its own user only, as the journal; a log shipper running as another user needs
`[worker] log_mode` (`"0640"` for one in the worker's group, at most `"0644"`). The mode is set on
each log as it is opened, whatever the umask; at its start the worker also gives it to the logs
already in its log folder (written before an upgrade, or under a looser mode), and the folder the
matching mode (0700, 0750 or 0755). The run page shows the end of one
try's log, follows the rest while the try runs, and shows the reason when the log can't be
read (see [The task panel](12-ui.md#the-task-panel)).
