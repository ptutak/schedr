# The master

The master is the only process that makes decisions. It holds an in-memory view of the
workers (who is connected, their slots, queues and code) and keeps everything else in the
database. There is one master per database (below); when it restarts, it reads its state
back from the database.

All of its work runs under **one lock**, taken for one piece of work at a time: each part of a
round (below), and each transaction of a worker's messages. Each piece sees a consistent view
and two never interleave; between them the lock goes to whoever waited first, so a worker's
`Started` doesn't wait for a whole round, nor a round for another worker's whole turn
(decisions 469, 480).

## Rounds

The scheduling loop runs a **round** (a "tick" in the code and the metrics) every [second](14-timings.md#fixed-timings). On Postgres it also runs one as soon as a new command is
announced. Every notification that has arrived by then is taken by that one round, so a
burst of commands costs one round, not one each. The 1 s timer starts again after a round
woken this way.

A worker's `Finished` also wakes a round, on SQLite and Postgres alike. Its downstream tasks
are then ready and dispatched at once, not at the next tick, which spared a chain of tasks
up to a second on each step. A round woken this way starts at least 50 ms after the previous
round ended. An idle master runs it at once. Under a stream of finishes it runs at most 20
rounds a second, and each round takes every finish that arrived meanwhile. That leaves the
lease's renewals and the workers' messages their turns. Other messages (a `Started`, a
heartbeat, a new worker registering) wait for the next tick.

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

The webserver listens the same way, for the UI's live pages (see
[Live updates](12-ui.md#live-updates)). It uses the same check and the same
`[database] notifications_url`. When notifications are off, or the check finds a pooler in
transaction mode, the webserver logs `the database's changes can't be followed (notifications
off, or a pooler in transaction mode): live pages refresh every 15 s` and those pages get
`refresh` on that timer instead of hearing each change. The number of seconds is
the [refresh interval](14-timings.md#fixed-timings) (15 s by default, at least 1 s). `[database]
change_notifications = false` turns the change triggers off, and the pages refresh on that
same timer. On SQLite neither side listens. The master's commands wait for the next round.
The writes append a row to `changes`, and the webserver reads the new rows every second, so
the pages still follow each change while those triggers are on.

A round does six things, in order. Each handles at most so many items a round (commands 50,
new runs' task instances 2,000, assignments 50, expiries 50, retries 100, scheduled runs 10 a
DAG, waiting reasons 100), the rest first in the next round, so that no round takes seconds
and holds up the workers' messages and the lease:

| # | Step | What it does |
|---|---|---|
| 1 | **Commands** | Applies the pending commands, oldest first (at most 50 a round) (trigger, cancel, pause, retry, drain, …), then records its decision: `applied` with what it produced, or `rejected` with the reason. A cancel produces a message to the worker running the attempt. Each command is one transaction, its decision with it, and the round takes the lock for each command on its own (decision 483): a command whose transaction fails leaves nothing stored and sends nothing, and stays pending. |
| 2 | **Drains and resumes** | A worker just drained has its parse jobs moved to other workers; one just resumed may get parse jobs again (if finding them fails, it is tried again each round). |
| 3 | **Scheduler** | Visits the DAGs [due](#busy-and-quiet-dags): scheduled ones whose stored next fire time has come or isn't known, those with queued runs, and those a command, an ended run or a new version marked (every DAG, when `[master] quiet_dags` is off, and on a term's first round). Reads their running count, last scheduled run and queued runs, inserts the runs that are due unless the DAG is paused (at most 10 a DAG a round), starts queued runs while the DAG is under `max_active_runs`, and stores when to visit each scheduled DAG next. Its trigger passes also start runs for matching `RunOf` and posted named events. |
| 4 | **Dispatcher** | Creates the task instances of runs that just started (tasks with no upstream task `queued`, empty ones completed, the rest waiting for their edges); queues failed tasks whose retry delay has passed; takes back assignments with no `Started` after the worker lease (60 s by default) (not one whose `Started` has arrived and waits to be handled, nor one whose worker is disconnected within its lease or, after a restart, not registered again yet: it may come back with the task done); assigns ready tasks to workers (resource cleanups first, then oldest ready first), storing each attempt before its `AssignTask` is built. A task over one of its [concurrency limits](../02-dags/05-defaults.md#concurrency-limits) waits first, whatever the workers ("waiting: `max_active_per_run` (10) reached"). [Resource cleanups](../02-dags/16-resources.md) bypass those limits. A task no worker can take gets a reason ("no active worker serves queue `gpu`", "waiting for a free slot …"), shown in the API; a round writes at most 100 of these, the rest in the next rounds. Once a round has made its 50 assignments it stops going through the ready tasks: the rest show "pending: waiting for a round with room" (up to 500 a round) and go in the next rounds; a wait step among them switched off or out of `max_wait` still ends in that round (decision 481). The round reads its ready wait steps' switch-off marks and first starts once, and claims its assignments together, in a few statements (decision 482). |
| 5 | **Parse jobs** | Moves parse jobs that have run longer than `[master] parse_job_timeout` (10 minutes by default) to another worker with the same code. |
| 6 | **Lost workers** | Marks a worker not heard from for the [worker lease](14-timings.md#fixed-timings) (60 s) as `lost`, and ends its attempts: those it started `lost`, each using a retry; those it never started `assignment_expired`, to run again without one (a task with `cleanup_anywhere` only after the cancel grace and the cleanup move wait: the worker may have started it and be stopping it). |

A backfill command creates the backfill and all its queued runs at once, in one
transaction. They start oldest first as places free, after other kinds of queued runs and
within `max_active_runs`; they can start while the DAG is paused.

The named-event trigger pass reads events of the DAG's name from one minute before the
newest event it acted on, bounded by when the DAG became live. The lookback
(`SCHEDR_HIDDEN__MASTER__EVENT_LOOKBACK`) covers posts whose time was recorded before a
later post but whose transaction committed after it. The pass skips keys the DAG already
has and respects `[triggers] max_queued_events`.

For `@trigger(wait_for=...)`, the trigger pass hands one ended run or named event to a
call on a watcher. For named events it records passed items in `trigger_progress_passed` and reads
back a minute behind the last, bounded by when the DAG became live, to find late commits.
When a call returns an event, its input is passed when the returned event is stored; if the
return is dropped, the input is handed again.

The limits are counted in the master's memory: the open attempts (assigned and not yet
started, running, cleaning up) by DAG, by task and by a run's task. A new master reads them
from the database before its first assignment, and keeps them as it assigns and as attempts
end; a change that rolls back puts them back too, as it does the workers' slots. Nothing is
counted per assignment in the database (one master holds the lease), so the check costs no
query. `max_active_tasks` and `max_active` are the DAG's **current** version's (one value at
a time across its runs), `max_active_per_run` the run's own version's.

When the round ends, the master sends the messages it built (`AssignTask`, `CancelAttempt`,
`AssignParse`) down the workers' streams. A message for a worker without a live stream is
dropped, because each kind is recovered another way. An assignment expires in a later round
and the task is assigned again. A cancel is sent again when the worker reconnects and reports
the attempt.

A round that fails (the database returns an error) is logged, and the next one goes on. The
scheduler does not keep that round's place: its wake marks are put back, and the next fire
times it didn't store are still due, so the next round tries that visit again (a term's
first round visits every DAG again). What it wrote
before the error stays done, and **the messages it had built are still sent**: each
is built only after the write it stands for is stored, so none announces something the
database doesn't know. The one exception is the dispatcher's assignments: a round's
assignments are stored in one transaction, **together**, which costs one lease check and one
commit where each assignment used to cost its own. If that transaction fails, none of it is
stored, sent or kept (slots given back), and the round then assigns the same tasks **one at a
time**, each in its own transaction: a task whose assignment fails on its own (data that won't
read, a statement the database refuses) is **set aside** for a minute, with a warning naming
it, and the others go on; then it is tried again. When every task fails, or the failure is the
database's (unreachable, too slow, the lease lost), the round fails as any other and nothing is
set aside.

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

Every query has a time limit (the [query time limit](14-timings.md#fixed-timings), 10 s; a write's transaction, a few
of these), enforced by schedr itself and, for writes on Postgres, by Postgres too, so a slow
query ends with an error rather than holding the round. If the round still stops turning, the
loop stops beating, and liveness fails after the [loop stall limit](14-timings.md#fixed-timings) (2 min). Kubernetes
restarts the master, which reads its state back (see [State and failures](07-failures.md)).

### Busy and quiet DAGs

Which DAGs a round visits comes from the database, not from the master's memory, so a
standby that takes over decides the same way. A scheduled DAG (`cron`, `every`, `once`)
stores **when to visit it next**, `next_fire_at` on its trigger's row:

- after a visit, its next fire time, or none ahead (a far date, 9999-01-01);
- the first fire time still due while due runs **wait**: more than the 10 a round creates, a
  fire time waiting for a free place under `max_active_runs`, or one behind a run of another
  kind that may still fail (a run that succeeded takes the time for good). It has come, so
  every round visits the DAG, and it stays the same until that run is created: no write a
  round;
- a time that has come while the DAG is **held** (its file fails to import), so every round
  sees whether the hold lifted;
- **unknown** (empty) when its switch changes: a new or changed trigger, pausing, unpausing.

A round visits the scheduled DAGs whose time has come or is unknown, and the DAGs with a
run in `queued`, whatever stored it (a manual, chained, event or backfill run). The ones
something marked in this master are visited too:

- **A run ended.** Its DAG, when a queued run can take the freed place under
  `max_active_runs`. Cancelling or retrying needs no mark of its own: the run wakes the DAG
  when it ends.
- **A command** about the DAG (trigger, pause, unpause), applied earlier in the same round.
- **A new version**, or a removed DAG that came back, stored by discovery between rounds.

The rest are **quiet**: not visited. A paused DAG is never due by its schedule. The
leadership term's first round visits every DAG and stores each one's time. A visit that
finds nothing to do creates nothing: a scheduled run is unique for its `planned_for`, so a
needless visit costs time, never runs.

`[master] quiet_dags` is on by default. Set it to `false` and every round visits every DAG.
A failed scheduler pass puts its marks back ([above](#rounds)), and the times it didn't store
are still due, so the next round makes that visit again.

### An idle round reads one statement

Each round begins with one statement, the **probe**, that asks the database, for each read
the round would make, whether that read would find anything: commands to apply, queued
runs, scheduled DAGs due, runs just started, retries due, tasks ready, attempts to take back
or move, triggers to look after. A read whose answer is no isn't made. A write the round makes
for its later parts (a run started, then its tasks, then their assignment) takes the probe
again before the next read, so those parts happen in the same round.

A round's parts each run at their own time, a little later than the last. The probe looks
one tick ahead: what comes due before the next tick (a task ready, a retry, a fire time) is
found, so the parts share one probe. A read for a time past that, in a round that ran long,
takes the probe again for its own time: work a part stores is found by the next part.

Each check is its read's own condition, so the two can't disagree. A read that takes a
value the probe doesn't have (a worker's cutoff, say) is checked without it: the check
answers yes more often than the read finds something, never less. Only the master's rounds
are cut this way; a worker's messages and the API read as before.

An idle standalone (the examples, SQLite, a release build, measured over 10 minutes) makes
about two statements a second, 22.5 before the probe; it uses 0.3 % of a core (2.0 %), switches
context about 23 times a second (200) and writes about 4 MiB in 10 minutes, as before: the
writes are the lease, the node row and heartbeats. The statements are the probe and the loops
that run on their own intervals, each needed while idle:

| Loop | Interval | Why it runs while idle |
|---|---|---|
| Lease renewal and its check | 10 s | Keeps the lead; a standby takes over without it. |
| The master's node row | 10 s | The UI's list of nodes; stale rows removed. |
| Outbox | 5 s | Delivers notifications written by other processes, and retries failed ones. |
| Gauges | 15 s | Run, task and worker counts for metrics. |
| A worker's last heartbeat | at most every 10 s | Shown in the UI's workers list (liveness itself is kept in memory). |

A switched-on `RunOf` or event trigger still has the trigger pass read every round, until the
triggers' round of their own (M5.1f2).

## When a task is ready

Trigger rules count **edges**. An edge counts as its upstream task's state, except one
whose upstream succeeded with an outcome the edge isn't taken on (`.on(...)`), or a
selection edge whose `@branch` did not choose that target: those count as skipped. A
skipped upstream task skips every edge from it.

`RunIf.NONE_FAILED` (the default): the task is ready when every edge that wasn't skipped
succeeded and at least one did (or it has none); skipped when all its edges were skipped;
`upstream_failed` as soon as one failed. `RunIf.ALL_SUCCEEDED`: ready when every edge
succeeded; `upstream_failed` as soon as one failed; otherwise skipped when one was
skipped, once no other edge can still fail. The first failure wins without waiting for
the rest. [Routing by outcome](../02-dags/08-outcomes.md) is how a DAG sets the rule.

An empty task the rule marks ready is completed by the master (`success`, outcome
`SUCCESS`); no worker is assigned. The master also ends
[`first_of` and `at_least`](../02-dags/20-first-of.md) itself when enough options succeed,
without waiting for the others. They need no worker or slot. Which options ended first is the
order the master recorded their results; results handled in one turn count in the order they
arrived.
It starts and advances [`TriggerDagOperator`](../02-dags/23-chained-runs.md) attempts,
creating their target runs and waiting for them when requested, without a worker or slot.
It also ends [`wait_for(RunOf(...))`](../02-dags/21-wait-for.md) when the other run or
task ends, or the wait times out. It also resolves the key of each queued [`wait_for(EventNamed(...))`](../02-dags/22-events.md#waiting-in-a-run) each round and ends it with the first matching event posted since the run was created, or `TimedOut`. While waiting, the step is `queued` with a reason. For an `@sensor(wait_for=...)`, the
dispatcher waits without a slot, then assigns a call with the matching run or the first
matching event posted since the run was created and no later than its deadline. A retry
gets the same item; `NOT_YET` waits for a later match.

## Between rounds: messages from workers

Messages from workers are handled as they arrive, under the same lock, in order for each
worker. A worker's messages that are already waiting are handled together, up to 80 in one turn
(`SCHEDR_HIDDEN__MASTER__MESSAGES_PER_TURN`), the rest in the worker's next turn. A turn takes
the lock for each of its transactions on its own (decision 480), and checks each time that its
stream is still the worker's: the round and other workers' turns get in between. (A task sends `Started` and
`Finished`. A forked Python task sends `Started` twice, the second with `start_method` `fork`.)

In a turn, the `Started`s are stored together in one transaction, then the `Finished`s in
another: one lease check and one commit for each group instead of one per message. Heartbeats
and warnings are handled after them. Any other message (`Rejected`, `Draining`, a code
report, a trigger's) is a barrier: what came before it is stored first, then it is handled
alone, as before.

```{mermaid}
flowchart LR
    turn[A turn: up to 80 waiting messages] --> sort{Message}
    sort -->|Started| startGroup[Started group]
    sort -->|Finished| finishGroup[Finished group]
    sort -->|Heartbeat, Warnings| after[After the groups]
    sort -->|anything else| barrier[Barrier]
    barrier --> flush[Store the groups so far, then handle it alone]
    startGroup --> tx1[One transaction]
    finishGroup --> tx2[One transaction, after the first]
```

The `Finished`s of a group are written together too: their attempts read in one query, their
ends in one statement, their outputs in another, and each run read and moved forward once for
all of its results (B184). A result off the common path (a moved cleanup's, a rest of handles,
another worker's, code gone missing) is handled alone in its place, in the same transaction,
after the results before it are stored, as one by one. Two common results of one mapped task
arriving together are taken in the order that keeps both: one instance's success beside
another's failure stays a success.

A group's writes are stored **whole or not at all**: a `Finished` whose last write fails leaves
nothing of the group stored, never an ended attempt with its run left behind. The group is then
redone one message at a time, as every message was before: at most 10 in this turn, the rest
first in the next, before anything newer, so a `Finished` is never stored before its `Started`.
A message the database refuses for its data is handled as before, alone (a `Finished` stored as
the attempt's failure). Attempt ends and run results are counted once the group is stored, so a
group rolled back and redone counts nothing twice. A commit whose answer is lost is read back
once for the group, by the first attempt its writes started or ended: a group lands whole or
not at all. A group that started or ended none is redone one message at a time, which changes
nothing twice.

Tasks' API calls arrive from the agent on the Control stream. Under the master's lease, the
master runs them in process through the API router as the task or trigger, only for an open
attempt or trigger on that worker and only on the allow-list (currently `GET` and `POST`
`/api/v1/events`). The agent keeps unanswered calls and resends them after reconnecting,
giving up after a minute. `[grpc] max_message_size` limits requests (413 before sending)
and answers (502 if too large); a command's context has a separate 1 MiB limit.

| Message | What the master does |
|---|---|
| `Register` | Checks the protocol version, takes over from a dead earlier stream, records the worker's node for the Deployments page (this master, the worker's process, its configuration masked again), decides about each attempt the worker reports (continue, kill or acknowledge), and answers `Registered` with the timing settings. |
| `Warnings` | Replaces the worker's stored warnings with this list. Each one has a kind (`fork_server_disabled`), an interpreter and a message (see [Workers](05-workers.md#the-fork-server)). If the write fails, the master logs it and leaves the stored list unchanged. The list stays while the worker is gone. |
| `Heartbeat` | Nothing more: any message renews the lease when it arrives (below). The registry's "last seen" is written at most once per heartbeat interval. |
| `CodeVersionChanged` | Records the root's new hash, which can make it the current code and create a parse job. |
| `Started` | Marks the task `running` and stores `start_method` (`spawn`, or `fork`). A later `Started` for the same attempt, while it has not ended, replaces `start_method` and keeps the first start time. |
| `TaskEnded` | Stores the task result in `attempts.task_result`, marks the instance `cleaning_up` and leaves the attempt open. If the worker is lost, that result is retained; cleanup can move when `cleanup_anywhere` is enabled, otherwise it ends `worker_lost`. |
| `Rejected` | Puts the task back to wait, or tries another worker, depending on the reason. A rejection never uses a retry. |
| `Finished` | Stores the result once, including the cleanup result, the outcome on success, `usage` and `core_dumped`, moves the run forward (downstream tasks become ready, skipped or `upstream_failed` by their edges and `run_if`), finishes the run when every task has, and answers `FinishedAck`. `usage` is null when the worker did not read it. |
| `Draining`, `Deregister` | Stops assigning to the worker; records it `draining`, then `offline`. |

When the original worker returns with handles the master lacks, a `cleanup_anywhere` move
still waiting is given back to it. If the move was assigned elsewhere or ended, the master
answers `CLEAN_REST` so that worker cleans the remaining handles. A late failure can mark an
ended success yellow without changing its state (see [Life of a run](03-life-of-a-run.md#7-the-next-task-and-the-end)).

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
- **Keeping it:** a task of its own renews the lease every third of the [master lease period](14-timings.md#fixed-timings)
  (30 s), so a long round doesn't hold renewals up. A renewal only extends the master's own
  lease, at its own takeover counter: it never takes the lease back from another master. The
  master leads only while its last renewal is recent enough by its own clock: until the period
  less the [query time limit](14-timings.md#fixed-timings) (10 s) after the renewal began, so it pauses before the
  lease can expire. The period must be at least three query limits.
- **Losing it for a while** (the database slow or unreachable): the master **pauses**. No
  rounds; worker messages and parse uploads wait in their queues. `/readyz` returns 503
  ("starting: master"), and a new control stream is refused with `UNAVAILABLE` ("not leading"),
  including a `Register` that waited for the lock as it paused; the refusal names the master
  holding the lease and when its lease runs out (or that none holds it), for the worker's log
  (decision 452). Workers reconnect. Running
  tasks go on. When a renewal succeeds again, it resumes. `schedr_master_leading` on
  `/metrics` is 0 meanwhile.
- **Taken over** (the lease expired and another master took it): the paused master's view is
  stale, so it exits; restarted, it waits as a standby.
- **Stopping cleanly** gives the lease up, so a standby takes over at once instead of after
  the lease expires.
- **Its row on the Deployments page:** each master, standby or leading, writes a row of its
  own to the database every 10 s (the `masters` table): its ID (the holder it takes the
  lease as), host, version, its configuration, masked before it leaves the process, and
  its problems now: its lease renewed late (a warning; it pauses until it is renewed), its
  last scheduling round failed (an error), its command notifications failing (a warning;
  commands then wait for the next round). Each clears once it no longer holds. The page
  colours the master by them and lists them in its panel. The page shows those heard from in the last 30 s, the lease's holder as the leader (see
  [Deployments](12-ui.md#deployments)). A row not written for an hour is removed. These
  writes don't go through the lease, so a standby writes too.

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
worker registry. Every worker that isn't gone counts as `disconnected` from that moment, and
its lease starts again from zero once a master it may have replaced can no longer answer it:
`lease_period` less `query_timeout` (20 s by default), how long that master may still believe
it leads (decision 444). The master's own downtime never counts against workers. A worker that
doesn't come back within that wait and the lease is lost in a round, with its attempts. Parse jobs live
only in memory, and workers report their hashes again when they reconnect.

Then the master listens for workers and starts its loop, and `/readyz` turns ready.

## Retention

Retention is on by default. In the configuration file, `[retention]` sets how long to keep
finished runs, their outputs, events and commands:

```toml
[retention]
runs = "90d"
outputs = "90d"
events = "90d"
commands = "365d"
```

Each value is an age such as `90d`, or `"forever"` to keep that kind. Zero is refused.
`outputs` and `events` default to `runs`, so setting only `runs` changes all three. Commands
default to 365 days. An upgraded deployment starts deleting older data at once; set the ages
before upgrading if it must be kept longer.

Runs and outputs age from the run's end. Deleting a run also deletes its tasks, attempts,
outputs and cleanup handles. An old backfill left without runs is deleted too. Commands about
that run remain as an audit trail until their own age expires: decided commands age from when
they were requested; pending commands are kept.

An unfinished run is never deleted because it still has work to do. For each DAG still defined,
retention also keeps:

- Its newest scheduled run, so the scheduler can continue its chain without creating catchup
  runs again from the start.
- Its newest run of any kind, for a manual run's `since`.
- The last run to end in success or failure, for `RUN_RECOVERED`.
- Successful manual and backfill runs planned after its newest scheduled run, and, if it has
  no scheduled run, its successful backfill runs. Catchup counts their fire times as done.

Retention also keeps a run an open `TriggerDagOperator` attempt is waiting for, so it is not
started again, and a run or posted event handed to a waiting step or trigger, so a retry gets
the same item. Protected runs keep their outputs too. A removed DAG (its file no longer
defines it) keeps these protections until the removal is as old as `runs`, so a file missing for
a while, such as after a bad sync, costs no history. After that, unfinished runs and runs still
being read are still kept; once its last run is gone, the DAG and its remaining history are
deleted. If that ID is defined again afterwards, it starts with no history. When a parent run
is deleted, a child run loses its parent link; `triggered_by` still names the parent.

With `outputs` shorter than `runs`, eligible outputs can go while the run, tasks and outcomes
stay. The output API then answers `410 Gone`, and the UI says "Output deleted by retention".
An output age longer than the run age makes no difference: outputs go with the run.

Events found by triggers age from when they were received; events posted through the API age
from when they were posted. What triggers passed of a deleted posted event goes with it;
trigger bookmarks stay. DAG versions no run, trigger or DAG uses are deleted when run
retention is enabled. An explicit backfill over an old range sees deleted runs as missing and
creates them again.

The leading master runs retention every hour beside its main loop, deleting batches of runs
from one DAG per transaction. `schedr_retention_deleted{kind}` counts what it deletes; for
`outputs`, it counts runs whose outputs were removed. Worker task logs have their own
[retention](05-workers.md#logs), independent of these ages.

## Gauges

Every 15 s the loop also counts runs, tasks and workers by state with one read-only query, for
`/metrics` (see [Observability](09-observability.md)). Unlike a round, this refresh only reads,
so it has a time limit of its own (half of the [loop stall limit](14-timings.md#fixed-timings)). If it runs out, the gauges
keep their previous values.
