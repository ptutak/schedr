# The master clock

Master time is the leading master's monotonic clock. A deadline the master and a worker both
have to honour is a time on that clock, so the two machines never compare wall clocks to
decide whether work is still alive. The numbers in the diagrams are the defaults (a 60 s
worker lease, a 10 s heartbeat, a 30 s master lease, a 10 s query timeout, and the 20 s lead
those last two leave). The settings are in [Timings](14-timings.md#fixed-timings). The
messages are in [Master–worker protocol](15-protocol.md). What each failure does to a run is
in [State and failures](07-failures.md#when-something-stops).

## What master time is

The process reads the wall clock once, the first time it reads master time. After that,
master time is that reading plus the monotonic time since. The timeline is this process's
own: another master's times are a different timeline and are never compared with it
(decision 442).

```{mermaid}
sequenceDiagram
    participant Wall as Wall clock
    participant MT as Master time
    Note over Wall,MT: The process starts and reads the wall clock. That reading is the origin of this timeline.
    Wall->>MT: origin
    Note over MT: Thirty seconds of monotonic time later, master time is the origin plus 30 s.
    Note over Wall: The wall clock jumps forward an hour.
    Note over MT: Master time is still the origin plus the monotonic time since. The jump moves no deadline.
    Note over MT: A restarted process, or the other master, has a new timeline.
```

Every master message carries a `MasterClock`: the timeline, master time as the message left,
the latest heartbeat heard on that stream, the wall clock (for people), and the lease epoch
(decision 444). Heartbeat 0 is the worker's `Register`.

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    W->>M: Register (heartbeat 0)
    W->>M: Heartbeat 1, sent at boot time T
    M-->>W: HeartbeatAck, and every later message
    Note over M,W: timeline, master_ms, heard_sequence 1, wall_ms, lease_epoch
```

The worker does not trust its own guess of when the master sent the message. The heartbeat
the stamp names was sent first, so master time `master_ms` is already past that send.
Master time `X` is no earlier than the send plus `X - master_ms`, less 0.1 % (one part in a
thousand: each clock slews by at most 500 ppm, so the two by at most 1000 ppm). That is
Cristian's bound, from the worker's boot clock, which keeps counting through a suspend. The
worker keeps the tightest sample of the last 10 minutes (decision 447,
`SCHEDR_HIDDEN__WORKER__MASTER_SAMPLE_WINDOW`): an older one, however tight, has drifted its
0.1 % from further back. An equally tight later sample replaces an old one, so the 0.1 % is
taken from a recent sample. A looser sample counts once the tighter ones have left the
window, or a pause has made them unusable. With no sample yet, every deadline counts as
already passed.

A pause the boot clock did not count (a frozen VM: the wall clock ran ahead of the boot
clock, past a second of slack and 0.2 % of the span) makes every deadline passed. The next
sample replaces the old one.

```{mermaid}
sequenceDiagram
    participant W as Worker boot clock
    participant M as Master
    Note over W: Heartbeat 1 leaves at boot time T
    W->>M: Heartbeat 1
    M-->>W: master_ms = M, heard_sequence = 1
    Note over W: Master time X is no earlier than T + (X - M), less 0.1 percent
    Note over W: The tightest such sample wins. Hours later, a new sample just as tight replaces it.
    Note over W: No sample, or the wall clock ran ahead of the boot clock: the deadline has passed.
```

```{mermaid}
flowchart LR
    newSample[A stamped answer to a heartbeat] --> tighter{At least as tight as the newest kept?}
    tighter -->|yes| dropLoose[Drop the looser ones before it] --> keep[Keep it last]
    tighter -->|no| keep
    keep --> old{Oldest kept sent more than 10 min ago?}
    old -->|yes| dropOld[Drop it] --> old
    old -->|no| best[The first kept is the tightest of the window]
```

`AssignTask.start_by_ms` is master time on that timeline: the start timeout (the worker
lease, 60 s) after the master stored the assignment, from the same reading the master's
expiry keeps.

## The races it closes

### An assignment started after the master took it back (B225)

Before, the master expired an assignment from the wall-clock time it stored (`assigned_at`
plus the lease). The worker's lease was its own timer, and the assignment's arrival refreshed
it. A late delivery and a slow journal then started the task on a worker the master had
already replaced. The review that found it: stored at 0 s, delivered at 55 s, journaled for
10 s, expired at 61 s, given to another worker at 62 s, started on the first at 65 s.

```{mermaid}
sequenceDiagram
    participant M as Master wall clock
    participant W as Worker
    Note over M: t = 0 s. Assignment stored. Expiry is assigned_at plus 60 s.
    M->>W: AssignTask arrives at 55 s
    Note over W: Arrival refreshes the worker's own lease. The journal write takes 10 s.
    Note over M: t = 61 s. Expired from assigned_at, and taken back.
    Note over M: t = 62 s. Assigned to another worker.
    Note over W: t = 65 s. Journal done, still inside the worker's lease. The task starts.
```

After, the worker starts the task only while its bound of `start_by_ms` is more than two
heartbeat intervals ahead (20 s). Otherwise it rejects the assignment `TOO_LATE` and does not
leave it in the journal. The master takes it back two heartbeat intervals (20 s) after its own
master time passes the same deadline, unless `Started` has arrived by then (decision 450): a
`Started` sent at the last moment has the worker's reserve and that grace, 40 s, to arrive.

```{mermaid}
sequenceDiagram
    participant M as Master time
    participant W as Worker
    Note over M: Stored at master time S. start_by_ms is S plus 60 s. Expiry keeps S.
    M->>W: AssignTask, stamp names the heartbeat sent at T
    Note over W: Start only while T + (start_by - master_ms) - 0.1 percent is more than 20 s ahead.
    W-->>M: Otherwise Rejected TOO_LATE, and the journal entry is dropped
    Note over M: Past S plus 60 s plus 20 s grace, with no Started waiting, the attempt is taken back.
```

### A start budget that outlived the expiry

The first patch put a remaining budget on the assignment, fixed when it was sent. The worker
counted that budget from when the assignment arrived. The wait after the send sat outside the
master's expiry, which still ran from when the attempt was stored, so the worker could start
after the master had taken the attempt back (decision 442: a budget spent at delivery).

```{mermaid}
sequenceDiagram
    participant M as Master
    participant W as Worker
    Note over M: Budget fixed at send: 60 s minus the age since the attempt was stored.
    M->>W: AssignTask carries that budget, then waits on the way
    Note over W: The whole budget is counted from arrival.
    Note over M: Expiry still runs from storage. The trip after the send is outside it.
    Note over W: The worker can still be inside its budget after the master took the attempt back.
```

Master time closes it by sending the deadline itself, the same reading `expire` keeps, rather
than a duration the worker starts counting later.

### Heartbeats left unanswered

Numbering heartbeats and treating the count as the clock left the worker behind whenever an
ack never went out (the master not leading, or the stream buffer full). The master's expiry
moved on; the worker's count did not.

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    W->>M: Heartbeat 3
    Note over M: No ack (not leading, or the send buffer is full)
    Note over W: A clock made of answered heartbeats stays at 2
    Note over M: Expiry has moved on
    Note over W: The worker still has start budget left
```

The master records the heartbeat's number when it arrives, ack or no ack, and puts that
number on every later message. The worker dates master time from when it sent that
heartbeat. A run of messages with no sample at all counts every deadline as passed, so the
task is not started.

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    Note over W: Heartbeat 3 leaves at boot time T. T is what the worker will use.
    W->>M: Heartbeat 3
    Note over M: Heard sequence becomes 3 as the heartbeat arrives
    M-->>W: A later message names heard_sequence 3 and master_ms
    Note over W: Master time is read from T. No sample at all means the deadline has passed.
```

### A `Started` held up (B227)

A `Started` sent in the last moment of the budget could arrive after the master had taken
the attempt back and given it to someone else. The worker keeps two heartbeat intervals (20 s
of the 60 s) so that `Started` can reach the master first. The master notes it as it is read,
before the message waits for the lock.

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    Note over W: Before: start at the end of the 60 s, then send Started
    W->>M: Started, held on the way
    Note over M: 60 s from storage, and Started is not here yet. Taken back, given to another worker.
    Note over W: The first task is already running
```

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    Note over W: After: the last 20 s (two heartbeats) is kept. The worker does not start inside it.
    W->>M: Started, sent with 20 s of master time still to go
    Note over M: The arrival is recorded before the lock. Expiry leaves the attempt alone.
```

A `Started` held longer than those two intervals is a known limit, below.

### A wall-clock jump on the master (B228)

Expiry, worker loss and the other liveness timers used to compare stored wall-clock times
with the wall clock the master was handed. A step of that clock made a live assignment look
due. Holding liveness after a jump was the next patch, and it was dropped: nothing that
decides liveness reads the wall clock. A jump of two minutes while master time moves five
seconds takes no assignment back and loses no worker. Both go only once master time itself
passes the lease.

```{mermaid}
sequenceDiagram
    participant Wall as Wall clock
    participant Live as Liveness
    Note over Wall: Jumps forward 2 minutes
    Note over Live: Before: stored wall times compared with this clock. The assignment looks expired.
    Note over Live: The hold after a jump was the patch. It was dropped.
    Note over Live: After: master time moved 5 s during the jump. The assignment and the worker stay.
```

### The minimum lease (B226)

The worker keeps two heartbeat intervals for `Started`, reads master time from a heartbeat
sent up to one interval earlier, and still needs an interval in which to start. A lease under
four heartbeat intervals cannot cover that, and a lease under one interval loses a worker
between two heartbeats. The load check is below.

```{mermaid}
sequenceDiagram
    participant L as Worker lease, four heartbeats at least
    Note over L: 1. The heartbeat the stamp names, up to 10 s old
    Note over L: 2. At least one interval, 10 s, left to start
    Note over L: 3 and 4. Two intervals, 20 s, kept so Started arrives before expiry
    Note over L: Under 40 s the process refuses to start
```

### A worker cut off by the clock of its own receipts (decision 445)

A worker counted its cutoff (the time it stops `cleanup_anywhere` tasks and triggers, as the
master may move them) only from when it received the master's last message, by its own
clock: a message held on its way left that cutoff late in master time. Now the cutoff is the
earlier of the two: a lease after the last receipt, or the moment master time may have
passed the last stamp plus a lease, by the bound above.

```{mermaid}
sequenceDiagram
    participant M as Master time
    participant W as Worker
    M->>W: A message stamped at master time 100 s, held 30 s on its way
    Note over W: Received at 130 s by master time. By receipt, cut off at 190 s.
    Note over W: By the stamp, master time may pass 160 s (stamp plus lease) at 160 s. Cut off then.
    Note over M: The master counts its lease from the worker's last message it received.
```

### A worker cut off one way (decision 453)

The master counts a worker lost a lease after the last message it received from it, not from
its own last send. With the worker's heartbeats lost on the way while the master's messages
still arrive, each stamped later than the one before, the stamp alone would keep pushing the
worker's cutoff past the moment the master moves its cleanup. So the cutoff has a third bound:
every master message names the last heartbeat it heard (`heard_sequence`), the worker knows when
it sent that heartbeat, and the master received it no earlier. The worker stops a lease after
that send, less the drift allowance (0.1 %), on its own boot clock.

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    W->>M: Heartbeat 7, sent at 10 s (worker's clock)
    Note over M: Heard at 10.02 s. Lost from 70.02 s (lease 60 s).
    W-xM: Heartbeats 8, 9, ... lost on the way
    M->>W: Assignment stamped at 50 s, heard_sequence 7
    Note over W: By the stamp alone: stop near 110 s.
    Note over W: By heartbeat 7's send: stop at 10 + 60 - 0.06 = 69.94 s. Stops then.
    Note over M: Moves the cleanup a grace and a move wait after 70.02 s.
```

When the master answers each heartbeat, this bound and the stamp's are within a few
milliseconds of each other; it only matters when the worker's messages stop getting through.

## Checked by a simulator (decision 446)

The worker's real `MasterTime` runs against a master on a virtual clock in a property test
(`crates/schedr-agent/tests/clock_sim.rs`): rate drift up to 0.1 %, delays both ways up to
5 s, answers lost, assignments delivered up to 30 s late, and stalls the boot clock missed.
Heartbeats are also lost on their way to the master while its messages still arrive, naming
the last heartbeat it heard. At every event it checks that a start is at least the reserve
before its deadline in master time, and that a worker not yet cut off is within a lease of when
the master last heard it (so of the last stamp too). Removing the third bound above, or the
drift allowance makes it fail at once.

The master's side runs in the same test (`clock_sim/master.rs`): the real master core over an
in-memory database, and two workers, each with its own `MasterTime` on a drifting clock,
connected by streams that delay messages, hold them up and break. Workers are cut off one way
or both, or replaced by a new incarnation while the old process runs on. Masters restart, or are
taken over while the deposed one still answers heartbeats. Its DAG has two tasks: one with a
`cleanup_anywhere` cleanup, which a worker cut off stops, and an ordinary one, which it keeps
running. The test checks three things:

- no worker starts an attempt the master has ended;
- no two processes of one task instance run at once, so no retry within a replaced
  incarnation's hold and no moved cleanup beside the worker it left (the ordinary task may run
  again only beside an attempt the master ended `lost`, at least once);
- once the faults are over, every attempt and run has ended.

Switching off the take-back's grace, the replaced incarnation's hold or the cleanup's move
wait makes it fail, each in scenarios aimed at it.

```{mermaid}
flowchart LR
    gen[Random scenario: drift, delays, losses, stalls, late assignments] --> sim[Worker MasterTime on virtual clocks]
    sim --> rule1{Started at least the reserve before start_by?}
    sim --> rule3{Not cut off only within a lease of the last heartbeat the master heard?}
    rule1 -->|no| fail[Fails, with the smallest scenario]
    rule3 -->|no| fail
    faults[Random faults: cuts, lags, replaced workers, takeovers] --> both[Master core and two workers on one clock]
    both --> alone{One process per task instance at a time?}
    both --> ended{Nothing started after the master ended it; all ended at the end?}
    alone -->|no| fail
    ended -->|no| fail
```

## Watching it (decisions 448 and 449)

| Metric | What it shows |
|---|---|
| `worker_master_sample_age_seconds` | The age of the heartbeat master time is read from; growing past a few heartbeats, the master isn't answering. |
| `worker_master_round_trip_seconds` | That heartbeat's round trip: what a deadline loses to the link. |
| `worker_start_slack_seconds` | How long before its deadline (less the reserve) each assignment started; near zero, assignments arrive late. |
| `worker_too_late_rejections` | Assignments rejected `TOO_LATE`. |
| `worker_older_epoch_refusals` | Streams ended for a lower lease epoch: a deposed master still answering. |
| `master_replaced_attempts_held` | Attempts of replaced incarnations held for a lease (decision 443). |

A worker compares the master's wall clock on each stamped answer with its own over the round
trip. More than 2 s outside it (`SCHEDR_HIDDEN__WORKER__CLOCK_SKEW_WARNING`), it logs a
warning once a session and reports a `clock_skew` warning, shown with the worker on the
Deployments page. Wall clocks only touch what people read (cron times, logs, shown times);
no deadline uses them.

```{mermaid}
sequenceDiagram
    participant W as Worker wall clock
    participant M as Master
    Note over W: Heartbeat sent at wall time A
    W->>M: Heartbeat
    M-->>W: Answer stamped with the master's wall time
    Note over W: Arrives at wall time B. Inside A to B: no skew. 30 s past B: 30 s skew.
    Note over W: Past 2 s: one clock_skew warning this session.
```

## One clock, and what still uses the wall clock

Before, a deadline was a wall-clock time stored in the database and compared with whatever
clock the master or the worker had. After, each liveness deadline is a master time, and the
master's own expiries count on its monotonic clock.

```{mermaid}
sequenceDiagram
    participant DB as Stored wall time
    participant M as Master
    participant W as Worker
    Note over DB,W: Before
    M->>DB: Expire when the master's wall clock passes assigned_at plus the lease
    W->>W: The worker's lease is its own timer, refreshed when the assignment arrives
    Note over M,W: The two clocks, and the two starting points, disagree
```

```{mermaid}
sequenceDiagram
    participant M as Master monotonic clock
    participant W as Worker
    Note over M: After
    M->>W: The deadline is a master time, on the stamp's timeline
    W->>W: The worker bounds that same master time from the heartbeat the stamp names
    Note over M: Expiry counts on this clock, from the reading the deadline was built with
```

| Deadline | Counts on master time from |
|---|---|
| Assignment expiry | When this master stored the assignment. After a restart, when it first sees one it did not store: later, never earlier. |
| Worker loss | The later of the last message it handled and the last arrival, recorded as the bytes are read, before they wait for the lock. |
| A parse job | When the job was given to the worker, until `[master] parse_job_timeout` (10 min). |
| A connection job | When the round sent it, plus the job's own limit and 30 s. |
| A moved cleanup's wait | When it was set aside to move, plus the cancel grace and the cleanup move wait. After a restart, a whole wait from when this master first sees it. |

A worker read back at startup counts as last heard a lead time later (20 s), so its lease
starts once a deposed leader can no longer be answering. That wait is in
[Starting up](04-master.md#starting-up).

The wall clock the master is handed is what people see and what the database keeps:
schedules, the times shown, retention. These four stay on it as well. Each is a decision one
master makes about a time of its own, or a time a person set. None of them is an agreement
with a worker about whether a process is still alive.

| Still the wall clock | Why it stays there |
|---|---|
| Schedules | Fire times are the times people set, stored with the run. |
| Set-aside | How long this master leaves an assignment that failed on its own before it tries again, compared with the clock the round was handed. |
| A sensor's `max_wait` | How long the wait may last, from the time stored with the wait. |
| The script-root grace | How long this master has gone without a report of a script root, from its start and from the last report. |

## Identity built on it

### A replaced incarnation (decision 443)

A new incarnation registers at once. The attempts the old process had started, and that the
new journal does not report, end `lost` only after a worker lease of master time from that
registration. The old stream is answered no more, so its heartbeats do not keep it alive.
The hold is stored on each attempt. A master that restarts keeps the hold and counts the
lease afresh from when it first sees the attempt.

```{mermaid}
sequenceDiagram
    participant Old as Old process
    participant M as Master
    participant New as New incarnation
    New->>M: Register
    Note over M: Accepted at once. The old stream gets no more heartbeat answers.
    Note over M: Unreported started attempts are marked. The mark is stored with each attempt.
    Note over M: They end lost 60 s of master time after the registration.
    Note over M: A restart keeps the mark and counts the 60 s afresh.
    Note over Old: Its cleanup_anywhere tasks stop at its own lease. An ordinary task may still be running.
```

Refusing the new incarnation until the old one was lost would make a fast restart wait a
lease. Stopping every task at the lease would kill a worker's tasks on a network blip.
Neither was chosen.

### A new leader, and the lease epoch (decision 444)

A deposed master answers heartbeats until its own monotonic lead runs out: the master lease
minus a query timeout (30 s − 10 s = 20 s). The new leader, for a worker it has not heard
from, starts that worker's lease only after the same lead on the new leader's master time.
The worker is lost after the lead plus the 60 s lease. The
[lease](04-master.md#one-master-per-database-the-lease) itself is still what stops two
masters writing.

Every master message carries the leader's lease epoch. A worker that has seen an epoch
refuses a lower one and reconnects. The fence expires: a worker lease, on the worker's own
clock, with no word from the highest epoch, and the lower epoch is followed. A database
restored from a backup sets the epoch back; restarting every worker was not chosen. Fencing
by timeline was not chosen either (timelines are not ordered). Counting the lead on a clock
that includes suspend was not chosen: that covers only a paused master.

```{mermaid}
sequenceDiagram
    participant Old as Deposed master
    participant W as Worker
    participant New as New leader
    Note over Old: Answers heartbeats for its lead, 20 s on its monotonic clock
    New->>New: Restored workers count as heard 20 s of master time from now
    Note over New: A worker still unheard after that 20 s plus the 60 s lease is lost
    New-->>W: Messages stamped with the new lease epoch
    W->>W: A lower epoch ends the stream. The worker connects again.
    Note over W: A whole lease with no word from the highest epoch, and the lower epoch is followed
```

### The load checks

Both are refused when the configuration is loaded. The lead check is what makes the fence's
lease long enough: a deposed master must stop answering before a worker gives up on the
higher epoch. Sending the lead to workers, for the fence to wait it out, was not chosen.

```{mermaid}
flowchart TD
    startNode[Load] --> leaseOk{Worker lease at least four heartbeats?}
    leaseOk -->|no| refuseLease[Refuse to start]
    leaseOk -->|yes| leadOk{Lead shorter than the worker lease?}
    leadOk -->|no| refuseLead[Refuse to start]
    leadOk -->|yes| runNode[Start]
```

The defaults pass: 60 s is six heartbeats, and the lead is 20 s. A 90 s master lease with a
10 s query timeout is an 80 s lead against a 60 s worker lease, and is refused.

The lease period must also be at least three query timeouts, so the lead is at least two: the
worker lease must be longer than twice the query timeout. A deployment that shortened its worker
lease to 20 s or less (to notice a dead worker sooner), keeping the default lease period and
query timeout, is refused at start until it also shortens those. Schedr's own tests run a 5 s worker lease with
a 6 s lease period and a 2 s query timeout: a 4 s lead.

## Known limits

A `Started` held up for more than four heartbeat intervals (40 s: the worker's reserve and the
master's grace), the stream still up, can arrive after the master has taken the attempt back. The master answers it with a cancel. The
task may already have been given to another worker (decision 442).

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    Note over W: Started inside the budget, with the 20 s reserve
    W->>M: Started, held more than 40 s
    Note over M: The attempt was taken back at 80 s and given to another worker
    M-->>W: CancelAttempt
    Note over W,M: Both may have run it
```

A jump of the master's monotonic clock (some moves of a virtual machine) is not caught.
Wall-clock steps are.

The worker's timers run on a clock that a suspend stops; the cutoff is read again from the
boot clock at least every 5 s (the worker's alive tick), so a worker resumed after a suspend
stops its `cleanup_anywhere` tasks up to 5 s late.

A cutoff the last session's stamps set stands over a reconnect, until the new session's first
stamped message: a new stream doesn't move it later.

A stall that even the worker's boot clock missed shows only once its wall clock has run 1 s
(plus 0.2 % of the time since the sample) ahead, so that a wall-clock step isn't taken for
one. A shorter stall goes unseen and leaves the worker's reading that much late; the
simulator allows exactly that.

At the end of a replaced incarnation's lease, the old process stops its `cleanup_anywhere`
tasks and its triggers. An ordinary task still running there can overlap the retry, as it can
for a worker the master has lost (decision 443).

```{mermaid}
sequenceDiagram
    participant Old as Old process
    participant M as Master
    Note over M: 60 s of master time after the new incarnation registered
    M->>M: The unreported attempt ends lost, and may be retried
    Note over Old: cleanup_anywhere tasks and triggers are stopped
    Note over Old: An ordinary task can still be running beside the retry
```

A worker of an earlier build ignores the deadline. During a rolling upgrade, master first, it
keeps the old race until every worker is new. That stays with the protocol's minor version
until 1.0 (B187). An assignment from an older master leaves `start_by_ms` unset, and the
worker then has no limit of this kind.
