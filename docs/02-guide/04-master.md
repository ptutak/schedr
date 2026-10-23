# The master

The master is the only process that makes decisions. It holds an in-memory view of the
workers (who is connected, their slots, queues and code) and keeps everything else in the
database. There is one master; when it restarts, it reads its state back from the database.

All of its work runs under **one lock**: a round, and the handling of each message from a
worker. Each sees a consistent view, and two decisions never interleave.

## Rounds

The scheduling loop runs a **round** (a "tick" in the code and the metrics) every
`[master] tick_interval` (1 s). On Postgres it also runs one as soon as a new command is
announced. Every notification that has arrived by then is taken by that one round, so a
burst of commands costs one round, not one each. The 1 s timer starts again after a round
woken this way.

A round does six things, in order:

| # | Step | What it does |
|---|---|---|
| 1 | **Commands** | Applies every pending command, oldest first (trigger, cancel, pause, retry, drain, …), then records its decision: `applied` with what it produced, or `rejected` with the reason. A cancel produces a message to the worker running the attempt. |
| 2 | **Drains and resumes** | A worker just drained has its parse jobs moved to other workers; one just resumed may get parse jobs again. |
| 3 | **Scheduler** | For each active DAG: works out the schedule's fire times since the last scheduled run and inserts the runs that are due (unless the DAG is paused). Then it starts queued runs while the DAG is under `max_active_runs`. |
| 4 | **Dispatcher** | Creates the task instances of runs that just started; queues failed tasks whose retry delay has passed; takes back assignments with no `Started` after 10 s; assigns ready tasks to workers (oldest ready first), storing each attempt before its `AssignTask` is built. A task no worker can take gets a reason ("no active worker serves queue `gpu`", "waiting for a free slot …"), shown in the API. |
| 5 | **Parse jobs** | Moves parse jobs that have run longer than 10 minutes to another worker with the same code. |
| 6 | **Lost workers** | Marks a worker not heard from for `[master] lease_timeout` (60 s) as `lost`, and ends its attempts (`lost`, each using a retry). |

When the round ends, the master sends the messages it built (`AssignTask`, `CancelAttempt`,
`AssignParse`) down the workers' streams. A message for a worker without a live stream is
dropped, because each kind is recovered another way. An assignment expires in a later round
and the task is assigned again. A cancel is sent again when the worker reconnects and reports
the attempt.

A round that fails (the database returns an error) is logged, and the next one goes on. What
it wrote before the error stays done, and **the messages it had built are still sent**: each
is built only after the write it stands for is stored, so none announces something the
database doesn't know.

### A round is never cut short

Each step is a series of separate writes, so stopping a round partway is like a crash in the
middle of it, and a round can't be undone halfway: it always runs to its end, or to the first
error the database returns. What such an error leaves behind is made safe instead:

- **A command is safe to apply again.** It is marked "being applied" before its first write,
  and decided after its last. One found still pending with the mark was cut off partway, and
  is applied again: a manual run takes its command's ID, so the repeat finds the run the
  first try made, and a repeat that finds what it asks for already holding (the run or task
  already cancelled, the task already queued, the worker already resumed) is decided
  applied, not rejected. Anything else wrong with it (an unknown task, say) is still a
  rejection.
- **Messages already built are delivered** (above). A cancel that is lost anyway (the master
  crashed before sending it, or the connection broke) is sent again when the worker
  reconnects, as every worker does after either.

Every query has a time limit (`[database] query_timeout`, 30 s), enforced by the database, so
a slow query ends with an error rather than holding the round. Only a database that stops
answering altogether (a network partition) can hold a round. The loop then stops beating, and
liveness fails after `[ops] loop_stall_timeout` (2 min). Kubernetes restarts the master,
which reads its state back (see [State and failures](07-failures.md)).

## Between rounds: messages from workers

Messages from workers are handled as they arrive, each under the same lock (in order for
each worker; messages from different workers take the lock in turn):

| Message | What the master does |
|---|---|
| `Register` | Checks the protocol version, takes over from a dead earlier stream, decides about each attempt the worker reports (continue, kill or acknowledge), and answers `Registered` with the timing settings. |
| `Heartbeat` | Nothing more: any message renews the lease when it arrives (below). The registry's "last seen" is written at most once per heartbeat interval. |
| `CodeVersionChanged` | Records the root's new hash, which can make it the current code and create a parse job. |
| `Started` | Marks the task `running`. |
| `Rejected` | Puts the task back to wait, or tries another worker, depending on the reason. A rejection never uses a retry. |
| `Finished` | Stores the result once, moves the run forward (downstream tasks become ready), finishes the run when every task has, and answers `FinishedAck`. |
| `Draining`, `Deregister` | Stops assigning to the worker; records it `draining`, then `offline`. |

**The lease counts from when a message arrives, not when it is handled.** Each worker's stream
is read by a task of its own, which records the arrival time and queues the message; messages
are then handled in order, under the lock. A reconnecting worker's `Register` counts as it
arrives too. So while the master is stuck in a slow database
write, the heartbeats of its workers still count, and a round that runs after the stall
doesn't declare live workers lost.

Parse results arrive as separate unary calls (`UploadParseResult`). So do large task inputs,
which workers fetch with `FetchInputs`.

## Starting up

At start the master checks that the database schema matches its version, then reads back the
worker registry. Every worker that isn't gone counts as `disconnected` from that moment, so its
lease starts again from zero: the master's own downtime never counts against workers. A worker
that doesn't come back within the lease is lost in a round, with its attempts. Parse jobs live
only in memory, and workers report their hashes again when they reconnect.

Then the master listens for workers and starts its loop, and `/readyz` turns ready.

## Gauges

Every 15 s the loop also counts runs, tasks and workers by state with one read-only query, for
`/metrics` (see [Observability](09-observability.md)). Unlike a round, this refresh only reads,
so it has a time limit of its own (half of `loop_stall_timeout`). If it runs out, the gauges
keep their previous values.
