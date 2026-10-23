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
it wrote before the error stays done, but **the messages it had built are dropped**: an
assignment expires and is made again, while a cancel waits until the worker reconnects
(see the note below).

### A round is never cut short

Each step is a series of separate writes, so stopping a round partway is like a crash in the
middle of it. If a round stopped between applying a command and recording its decision, the
next round would apply the command again. If it stopped after storing a cancel but before
sending it, the task would go on running. So a round always runs to its end, or to the first
error the database returns.

Every query has a time limit (`[database] query_timeout`, 30 s), enforced by the database, so
a slow query ends with an error rather than holding the round. Only a database that stops
answering altogether (a network partition) can hold a round. The loop then stops beating, and
liveness fails after `[ops] loop_stall_timeout` (2 min). Kubernetes restarts the master,
which reads its state back (see [State and failures](failures.md)).

```{note}
Two known limits, both B7 in `BUGS.md`. Applying a command and recording its decision are
separate writes, so a crash or a database error between them makes the next round apply the
command again. And a cancel that isn't delivered (the round failed later, or the master
crashed first) isn't sent again until the worker reconnects. The planned fix: one transaction
for applying and deciding, and resending pending cancels every round.
```

## Between rounds: messages from workers

Messages from workers are handled as they arrive, each under the same lock:

| Message | What the master does |
|---|---|
| `Register` | Checks the protocol version, takes over from a dead earlier stream, decides about each attempt the worker reports (continue, kill or acknowledge), and answers `Registered` with the timing settings. |
| `Heartbeat` | Renews the lease. Any message renews it; the registry's "last seen" is written at most once per heartbeat interval. |
| `CodeVersionChanged` | Records the root's new hash, which can make it the current code and create a parse job. |
| `Started` | Marks the task `running`. |
| `Rejected` | Puts the task back to wait, or tries another worker, depending on the reason. A rejection never uses a retry. |
| `Finished` | Stores the result once, moves the run forward (downstream tasks become ready), finishes the run when every task has, and answers `FinishedAck`. |
| `Draining`, `Deregister` | Stops assigning to the worker; records it `draining`, then `offline`. |

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
`/metrics` (see [Observability](observability.md)). Unlike a round, this refresh only reads,
so it has a time limit of its own (half of `loop_stall_timeout`). If it runs out, the gauges
keep their previous values.
