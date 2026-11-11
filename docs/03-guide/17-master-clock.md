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
worker keeps the tightest sample. An equally tight later sample replaces an old one, so the
0.1 % is taken from a recent sample. A looser sample is kept only when a pause has made the
old one unusable. With no sample yet, every deadline counts as already passed.

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
leave it in the journal. The master takes it back when its own master time passes the same
deadline, unless `Started` has already arrived and is waiting to be handled.

```{mermaid}
sequenceDiagram
    participant M as Master time
    participant W as Worker
    Note over M: Stored at master time S. start_by_ms is S plus 60 s. Expiry keeps S.
    M->>W: AssignTask, stamp names the heartbeat sent at T
    Note over W: Start only while T + (start_by - master_ms) - 0.1 percent is more than 20 s ahead.
    W-->>M: Otherwise Rejected TOO_LATE, and the journal entry is dropped
    Note over M: Past S plus 60 s, with no Started waiting, the attempt is taken back.
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

## Known limits

A `Started` held up for more than two heartbeat intervals (20 s), the stream still up, can
arrive after the master has taken the attempt back. The master answers it with a cancel. The
task may already have been given to another worker (decision 442).

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    Note over W: Started inside the budget, with the 20 s reserve
    W->>M: Started, held more than 20 s
    Note over M: The attempt was taken back at 60 s and given to another worker
    M-->>W: CancelAttempt
    Note over W,M: Both may have run it
```

A jump of the master's monotonic clock (some moves of a virtual machine) is not caught.
Wall-clock steps are.

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
