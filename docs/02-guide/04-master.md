# The master

The master is the only process that makes decisions. It holds an in-memory view of the
workers (who is connected, their slots, queues and code) and keeps everything else in the
database. There is one master per database (below); when it restarts, it reads its state
back from the database.

All of its work runs under **one lock**: a round, and the handling of each message from a
worker. Each sees a consistent view, and two decisions never interleave.

## Rounds

The scheduling loop runs a **round** (a "tick" in the code and the metrics) every
`[master] tick_interval` (1 s). On Postgres it also runs one as soon as a new command is
announced. Every notification that has arrived by then is taken by that one round, so a
burst of commands costs one round, not one each. The 1 s timer starts again after a round
woken this way.

**Notifications behind a connection pooler.** The master listens (Postgres `LISTEN`) on a
connection of its own. A pooler in **transaction** mode (PgBouncer and similar) can't carry
that: it lends the server connection the master listens on to other clients, whose queries
then fail on notifications they didn't expect. So before listening, the master checks the
connection it would listen through, from inside its own database (no other rights needed):
it reads the Postgres process behind one connection, opens a transaction on a second one, and
reads the first's again. A second client on the first one's process, or the first moved to
another process, means a pooler in transaction mode; the check repeats five times. Found, the
master warns and doesn't listen: new commands then wait for its next round (up to 1 s). For
immediate commands behind such a pooler, `[database] notifications_url` gives the master a
connection straight to Postgres for listening only (checked the same way);
`[database] notifications = false` turns notifications off, and the warning with them. A
database error that shows a notification where an answer was expected says that a pooler is
the likely cause.

A round does six things, in order:

| # | Step | What it does |
|---|---|---|
| 1 | **Commands** | Applies every pending command, oldest first (trigger, cancel, pause, retry, drain, …), then records its decision: `applied` with what it produced, or `rejected` with the reason. A cancel produces a message to the worker running the attempt. |
| 2 | **Drains and resumes** | A worker just drained has its parse jobs moved to other workers; one just resumed may get parse jobs again. |
| 3 | **Scheduler** | Reads every DAG's running count, last scheduled run and queued runs at once (three queries, however many DAGs). Then for each active DAG: works out the schedule's fire times since the last scheduled run and inserts the runs that are due (unless the DAG is paused), and starts queued runs while the DAG is under `max_active_runs`. |
| 4 | **Dispatcher** | Creates the task instances of runs that just started; queues failed tasks whose retry delay has passed; takes back assignments with no `Started` after 10 s (not one whose `Started` has arrived and waits to be handled); assigns ready tasks to workers (oldest ready first), storing each attempt before its `AssignTask` is built. A task no worker can take gets a reason ("no active worker serves queue `gpu`", "waiting for a free slot …"), shown in the API. |
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
database doesn't know. The one exception is the dispatcher's assignments: a round's
assignments are stored in one transaction, **together or not at all**. A failure among them
stores none, sends none of their `AssignTask` messages and frees the slots they took; the next
round assigns those tasks again. One transaction costs one lease check and one commit, where
each assignment used to cost its own.

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

Every query has a time limit (`[database] query_timeout`, 10 s; a write's transaction, a few
of these), enforced by schedr itself and, for writes on Postgres, by Postgres too, so a slow
query ends with an error rather than holding the round. If the round still stops turning, the
loop stops beating, and liveness fails after `[ops] loop_stall_timeout` (2 min). Kubernetes
restarts the master, which reads its state back (see [State and failures](07-failures.md)).

## Between rounds: messages from workers

Messages from workers are handled as they arrive, under the same lock, in order for each
worker. A worker's messages that are already waiting are handled together, up to 10 in one turn
of the lock; then the round and other workers get their turns, and the rest follow in the
worker's next turn. (Each task sends two: `Started` and `Finished`.)

Each message's writes are one transaction too, stored **whole or not at all**: a `Finished`
whose last write fails leaves nothing of it stored (the worker sends it again when it
reconnects), never an ended attempt with its run left behind.

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

**A task's start counts when its `Started` arrives, too.** The reader also notes each
`Started` (and `Finished`) as it arrives, and a round doesn't take back an assignment whose
`Started` is waiting in the queue: a busy master would otherwise take back tasks that did
start, assign them again, and get busier still with the extra writes (B11, found by the stress
suite).

Parse results arrive as separate unary calls (`UploadParseResult`). So do large task inputs,
which workers fetch with `FetchInputs`; these are read from the database without the master's
lock, so a busy master doesn't hold a task's start up.

## One master per database: the lease

A second master on the same database would schedule every run twice, so a master leads only
while it holds the database's **lease**: a one-row table (`master_lease`) saying who holds it,
until when by the **database's** clock, and how many times it changed hands (the takeover
counter).

- **Taking it:** a master takes the lease before anything else. One that finds it held waits
  as a **standby**: it says so in its log once, `/readyz` answers "starting: master", and it
  tries again every third of the period until the lease is free or expired.
- **Keeping it:** a task of its own renews the lease every third of `[master] lease_period`
  (30 s), so a long round doesn't hold renewals up. A renewal only extends the master's own
  lease, at its own takeover counter: it never takes the lease back from another master. The
  master leads only while its last renewal is recent enough by its own clock: until the period
  less one `[database] query_timeout` (10 s) after the renewal began, so it pauses before the
  lease can expire. The period must be at least three query limits.
- **Losing it for a while** (the database slow or unreachable): the master **pauses**. No
  rounds; worker messages, registrations and parse uploads wait in their queues. Running
  tasks go on. When a renewal succeeds again, it resumes. `schedr_master_leading` on
  `/metrics` is 0 meanwhile.
- **Taken over** (the lease expired and another master took it): the paused master's view is
  stale, so it exits; restarted, it waits as a standby.
- **Stopping cleanly** gives the lease up, so a standby takes over at once instead of after
  the lease expires.

**What the database rules out.** The master's own clock can't tell when its writes arrive: a
write sent while it led can be held up by the network or a connection pooler's queue until
after its lease expired. So every write of the master runs in a transaction that first checks
the lease is still its own (holder and takeover counter), and is refused once another master
took it over. On Postgres that check also holds the lease's row until the write commits, so a
takeover waits for writes in progress; SQLite runs one write at a time anyway. On Postgres the
transaction's start, its time limit and the check go to the database as one message, so a
master's write costs one round trip more than its statements alone (the commit). Two masters
can't both change the database, whatever their clocks or the network do.

```{mermaid}
sequenceDiagram
    participant A as master A (leading)
    participant DB as database
    participant B as master B (standby)
    A->>DB: write: check lease (A, counter 1), lock its row
    B->>DB: take the lease (A's expired)
    Note over DB: B waits for A's write
    A->>DB: commit
    DB-->>B: taken: B, counter 2
    A->>DB: write: check lease (A, counter 1)
    DB-->>A: refused: not A's any more
```

Each step needs nothing of the database session, so the lease works through any connection
pooler (PgBouncer in session or transaction pooling), and on SQLite. The same goes for the
query limit: schedr cuts a query off itself, and on Postgres each write's statements are also
limited in Postgres (`statement_timeout` for that transaction only), so a statement cut off
stops there instead of running on. **A write's transaction left idle** that long (its master
stalled between two statements: a network partition, a frozen process) is ended by Postgres
too (`idle_in_transaction_session_timeout`), with its lock on the lease's row: without that, a
standby couldn't take over for as long as the stall lasted. On SQLite a write takes the write
lock at its start (`BEGIN IMMEDIATE`), waiting for another writer instead of failing.

## Starting up

At start the master takes the lease (above), then checks that the database schema matches its version, then reads back the
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
