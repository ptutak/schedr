# Life of a run

This page follows one manual run of a two-task DAG `sync` (`extract` → `load`) from the CLI to
its result. A scheduled run goes the same way, except that the scheduler creates it in step 3
with the DAG's default parameters, instead of a command.

```mermaid
sequenceDiagram
    autonumber
    participant CLI
    participant Web as webserver
    participant DB as database
    participant M as master
    participant W as worker
    CLI->>Web: POST /api/v1/dags/sync/runs
    Web->>DB: insert command trigger_run (+ NOTIFY)
    Web-->>CLI: 202, the command
    DB-->>M: notification: wake up
    Note over M: round: apply the command
    M->>DB: insert run, mark command applied
    Note over M: same round: dispatch
    M->>DB: create task instances, attempt for extract
    M->>W: AssignTask (extract)
    W->>W: checks, journal "running"
    W->>M: Started
    W->>W: run the task (a subprocess)
    W->>W: journal "finished"
    W->>M: Finished (status, output, outcome)
    M->>DB: store result and outcome, load becomes ready
    M->>W: FinishedAck
    Note over M: next round: assign load
    M->>W: AssignTask (load, with extract's output)
    CLI->>Web: GET /api/v1/commands/{id} (every 0.5 s)
    Web-->>CLI: applied: run ID
```

## 1. The request becomes a command

`schedr runs trigger sync` calls the webserver's REST API. The webserver checks nothing about
scheduling. It stores a `trigger_run` command row and answers `202` with the command. On
Postgres the insert also sends a notification on a channel the master listens to.

The CLI sends the request with an `Idempotency-Key` header, a new key for each command. If
no answer comes, or the webserver fails, it sends the request again under the same key, up
to 3 times in all. The webserver stores one command per key and person. A request sent
again gets the command stored the first time, so a lost answer never starts a second run.
The same key sent with a different command is refused (`422`). When no answer comes after the third try, the CLI says the
command **may have arrived** and to look (`schedr runs list`) before running it again: run
again, it is a new command with a new key.

The CLI then reads the command back every 0.5 s, for up to 30 s, until the master has decided.

## 2. The master wakes up

The master's loop runs a [round](04-master.md) every [second](14-timings.md#fixed-timings), and at
once when a notification arrives. On SQLite there are no notifications, so the command waits
for the next round.

## 3. The round applies the command

The round's first step applies every pending command, oldest first. For `trigger_run` the
scheduler checks that the DAG can run, then inserts a run. The run is `running` if the DAG is
under `max_active_runs`, otherwise `queued`. The command is then marked `applied`, with the run
ID as its result. A command that can't apply (an unknown DAG, a time in the future, or a DAG
whose file fails to import) is marked `rejected` with the reason, and the CLI prints it and
exits 1.

A trigger may carry parameters ([Run parameters](../02-dags/12-params.md)). The webserver
stores them on the command and does not check them. While the master applies the command it
fills in the DAG's defaults, checks the values against the DAG's schema, and inserts the run
only when they fit. A refusal marks the command `rejected` with the reason, and no run is
stored. The values that were accepted stay with the run: every task is sent them, a queued
run keeps the ones stored when it was created, and a later version of the DAG does not
replace them. A scheduled run has no command. It is stored with the defaults.

A DAG whose file fails to import, other than a timeout or a runner that died, is held. The
scheduler creates no run of it, a fire time during the hold is missed as while it is paused,
and a queued run is not started. A run that has already started is not cancelled. A timeout
or a runner that died holds nothing: the file is parsed again.
[Parsing](06-parsing.md#a-held-dag) is the whole of it.

## 4. The same round dispatches

Later in the same round the dispatcher:

1. creates one row per task, at `map_index` -1: `extract` has no upstream task, so it is
   `queued`; `load` is `scheduled`. A mapped task's instances are created when it expands
   ([A mapped task](#a-mapped-task));
2. picks a worker for `extract`: among `active` workers that serve its queue, run a recent
   enough schedr and have a free slot, the one with the most free slots. Workers that
   reported the root's current code come first; one that said it lacks the code is picked
   only when no other can take the task (it may have synced since, else it rejects again);
3. stores the attempt in the database **before** sending anything, then builds `AssignTask`:
   attempt ID, what to run, the expected code hash, the inputs, the run's parameters, timeouts
   and memory budget. A `resource_cleanup` flag marks a resource cleanup so its worker can
   supply the default timeout when it has none.

When the round ends, the master sends the round's messages down the workers' streams.

## 5. The worker accepts and runs it

The agent checks the assignment: it has a free slot, it isn't draining, the file and the
interpreter are there, and its code is at the expected hash. If a check fails it sends
`Rejected` with a reason, and the master tries elsewhere or waits.

If the checks pass, it records the attempt as running in its journal on disk, then sends
`Started`. A `Started` that doesn't arrive within 10 s makes the master take the assignment
back (`assignment_expired`) and queue the task again, without using a retry; one that arrived
in time counts even if a busy master handles it later.

For a Python task, the agent starts a runner in a new process group, with stdout and stderr going to the
attempt's log file. The runner says `hello`. The agent sends the task. Once the DAG file is
imported and the arguments are built, the runner sends `task_imported` (no fields) and calls
the function; it does not send that message when the import or the arguments fail. The agent
times both gaps on its own clock (`schedr_worker_task_start_seconds`, see
[Observability](09-observability.md)). The result comes back over the same local socket.
The task's timeout counts from the moment the agent starts launching it, including reading
its secrets and connections before the process starts. The process gets only the time left:
a 1 s timeout with a 500 ms secret read leaves 500 ms for the process. A cancel or the timeout
arriving during those reads ends the attempt `cancelled` or `timed_out`, without starting a
process. The runner's start is part of the same timeout: a cancel or the timeout arriving
before its `hello` stops the runner then (SIGTERM, the grace, SIGKILL). The same holds while
it waits for a fork server and while it sends the task's request. A runner that doesn't say
`hello` within 30 s fails the attempt with `start_failed`.
The memory watchdog samples the group's memory every second. With `fork_server = true` on
the DAG interpreter, a Linux worker starts that Python runner from a fork server (see
[The fork server](05-workers.md#the-fork-server)), and the attempts API shows `start_method`
`fork`. A task with `preload` false asks only the bare server. A task with `fresh_process`
true, and a task those servers can't fork, is a new process (`spawn`). A forked task the
worker drops before it has ended is killed, as a spawned one is. A forked task whose PSS
can't be read is not killed on resident set size, because that size includes the pages it
shares with the fork server.

A [wait step](../02-dags/18-sensors.md) instead runs in a shared watcher host and holds a
watcher slot. Its memory, cancellation and deployment behavior are
described in [The watcher host](05-workers.md#the-watcher-host).

## 6. The result comes back

If the task has a [cleanup](../02-dags/15-cleanup.md) and its process started, the agent
first reports `TaskEnded` with the task result and runs the cleanup on the same worker. The
instance is `cleaning_up`, and the attempt keeps its slot; downstream tasks and retries wait.
The cleanup gets its own tries and timeout.

Once cleanup ends, the agent records the result in its journal, then sends `Finished`: the
cleanup result as well as the status, the output,
the outcome (on success), the duration, the peak memory, `usage` and `core_dumped`. The
master handles it as soon as it arrives, between rounds:

- it stores the attempt's result and the task's new state (`success`, or `up_for_retry` /
  `failed` by the retry rules);
- on success it stores the output with its outcome (the name the worker reported, or
  `SUCCESS` when the worker named none);
- a failed cleanup fails the run, even if the task succeeded;
- it moves the run forward: each downstream task is decided by its incoming edges and
  `run_if`, as soon as that result is certain, so `load` becomes `queued`;
- it answers `FinishedAck`, and the agent deletes the journal file.

The output is JSON, written by the runner. Dates, durations and tuples are stored tagged
(`{"$date": "2026-10-19"}`, `{"$tuple": [1, "a"]}`), so the agent renders them without
Python and a task taking `Any` gets the real type back. A dict of the user's own shaped like
a tag (one key, a tag's name) is stored escaped as `{"$dict": {...}}` and reads back as
written; a path into the output goes through the escape.

Delivery is at least once. A worker that doesn't get the acknowledgement sends `Finished`
again, and the master acknowledges the repeat without storing it twice.

## 7. The next task, and the end

The next round assigns `load`. Its `AssignTask` carries `extract`'s output as input, the
run's parameters, and the outcomes of the upstream tasks `load` uses (`""` for a skipped
one). A task whose
edges and `run_if` are already decided is skipped or `upstream_failed` without being
assigned: an edge not taken (`.on(...)` whose outcome didn't happen, a `@branch` that
didn't choose that target, or a skipped upstream task) counts as skipped;
`RunIf.NONE_FAILED` (the default) still runs when another edge succeeded, and
`RunIf.ALL_SUCCEEDED` skips. A failure on any edge makes the task `upstream_failed` as
soon as that failure is known. [The master](04-master.md#when-a-task-is-ready) is the
rule; [Routing by outcome](../02-dags/08-outcomes.md) is how a DAG declares it.

For [first_of and at_least](../02-dags/20-first-of.md), the master instead ends the node
as soon as enough options succeed. It needs no worker or slot. The other options keep running.

When every task instance has a final state, the run gets its result: `failed` if a task
failed after its last retry, `cancelled` if a user cancelled it and nothing failed,
otherwise `success`. A skipped task leaves the run `success`. `upstream_failed` is a
consequence of the original failure, which already makes the run `failed`. Independent
parts of the graph keep running after a failure.

A success can later become a **yellow success**, which asks for attention while its run state
remains `success`: something may already have acted on that result. Its one reason now is
`late_cleanup_failed`, when the original worker returns and fails to clean handles missing
from a moved cleanup after the run ended. Handles may be left behind. The UI shows the success
in yellow with the reason; the CLI shows `success (late_cleanup_failed)` and the API exposes
`late_failure`. The task instance shows its failed cleanup, the master logs an error, and
`schedr_master_runs_late_failed_total` counts it for alerts. Notifications, when added, will
send this as an event. See [cleanup](../02-dags/15-cleanup.md#cleanup-on-another-worker).

Meanwhile `schedr tasks logs` reads each attempt's log through the webserver, which fetches
it from the worker that ran the attempt (see [Workers](05-workers.md)). The
[run page](12-ui.md#a-run) shows that run: its state, its graph, its tasks, a task's
attempts, cleanup, output and log, its resources, and the commands about it. The API's run
detail includes each task's cleanup result and the resources, their parts, users, blocking
setting and state. `schedr resources list <run>` and `GET /api/v1/runs/{run}/resources`
list the run's resources. Each attempt can include its cleanup result, tries, duration, error,
moves and handles; the shown handles are masked as task data.

## A mapped task

The walkthrough's tasks are not mapped. Every task, mapped or not, gets one row at the
start of the run, `map_index` -1, `scheduled`. An unmapped task's row is the one the master
assigns, and it is that task's only row. A mapped task's `-1` row is never sent to a
worker. It stays `scheduled` until the task expands. With instances it becomes
`running`, and then takes the state those instances settle to. When it cannot expand it
becomes `failed`. The instances are further rows, `0` to `N-1`, created when it expands.
The API lists the `-1` row first and then
those instances. `?map_index=-1` means the same as leaving the index off
([Mapping](../02-dags/10-mapping.md#following-the-instances)).

When the task's edges are ready, the master expands it and stores the instances in the same
transaction as the `-1` row's new state. One instance per element, numbered from 0. An empty
list ends the task `success`, with output `[]` and no instances. The task fails, with a
reason and with nothing run, when what it maps over is not a list, when an
`.expand_kwargs()` list it waits for as a whole has an element that is not a dict, or when
it would have more than `[mapping] max_instances` instances (1024). On a lane, an
`.expand_kwargs()` element that is not a dict fails that instance when it is assigned, as
`input_mismatch`, which is not retried. The master doesn't render an element written with
a t-string or a `dag_ctx` value: it counts it, and sends it to its instance in its stored
form, under `$expand_values`, for the runner to build like an argument.

A lane is an element-wise data edge: the task is mapped over the whole output of a mapped
task, and it uses that task nowhere else. A lane counts as ready once that upstream task
has expanded, so the downstream task can expand before those instances finish, and instance
`i` then waits only for upstream instance `i`. Any other edge waits for the upstream task
as a whole: an order edge, `.partial(all=…)`, or `results[0]`. If that upstream task
failed, nothing of the downstream task runs. A whole mapped task's own outcome only says
that its instances ended, so `.on(…)` over its output is still decided per element: once
the task expands, an instance whose upstream instance ended with another outcome is
skipped, as on a lane. An order edge narrowed with `.on(…)` from a mapped task counts as
one edge per instance in the downstream task's `run_if` (any instance for `none_failed`,
every one for `all_succeeded`).

One failed instance, or a lane whose upstream instance failed, fails the mapped task at
once. The task is `failed` when one of its own instances failed, and `upstream_failed` when
the only failures are lanes. Instances that have not started are cancelled. Ones that are
running are stopped. A result that arrives after the `-1` row is already final does not
change that row. The attempt is recorded as the worker reported it, the instance ends
`cancelled` whatever that report was, and its output is not stored. Instances that had
already succeeded keep their results. The tasks after a failed mapped task are
`upstream_failed`.

When every instance has finished and none failed, the task's `run_if` applies to the
instances. `NONE_FAILED` (the default) succeeds unless every instance was skipped.
`ALL_SUCCEEDED` skips the task if any instance was skipped. Any `cancelled` instance makes
the whole task `cancelled`. A failure is decided first, so it wins over a cancellation.
The list a downstream task reads is built then, in `map_index` order, from the instances.
It is not stored on the `-1` row. An instance that did not succeed is JSON null in that
list, which the next task reads as `None`.
[Mapping](../02-dags/10-mapping.md) is how a DAG writes this.
