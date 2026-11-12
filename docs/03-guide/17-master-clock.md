# The master clock

Master time is the leading master's monotonic clock. A deadline the master and a worker both
have to honour is a time on that clock, so the two machines never compare wall clocks to
decide whether work is still alive. The numbers in the examples are the defaults (a 60 s
worker lease, a 10 s heartbeat, a 30 s master lease, a 10 s query timeout, and the 20 s lead
those last two leave). The settings are in [Timings](14-timings.md#fixed-timings). The
messages are in [Master–worker protocol](15-protocol.md). What each failure does to a run is
in [State and failures](07-failures.md#when-something-stops).

## What master time is

The process reads the wall clock once, the first time it reads master time. After that,
master time is that reading plus the monotonic time since. The timeline is this process's
own: another master's times are a different timeline and are never compared with it
(decision 442).

| When | Wall clock | Master time |
|---|---|---|
| The process starts | Read once: the origin of this timeline | The origin |
| 30 s of monotonic time later | | The origin plus 30 s |
| The wall clock jumps forward an hour | An hour ahead | Still the origin plus the monotonic time since: the jump moves no deadline |
| A restarted process, or the other master | | A new timeline |

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
already replaced. The review that found it:

| Wall clock | Master | Worker |
|---|---|---|
| 0 s | Stores the assignment. Expiry is `assigned_at` plus 60 s. | |
| 55 s | | `AssignTask` arrives and refreshes the worker's own lease. The journal write takes 10 s. |
| 61 s | Expired from `assigned_at`: takes it back. | |
| 62 s | Assigns it to another worker. | |
| 65 s | | Journal done, still inside its own lease: starts the task. |

After, the worker starts the task only while its bound of `start_by_ms` is more than two
heartbeat intervals ahead (20 s). Otherwise it rejects the assignment `TOO_LATE` and does not
leave it in the journal. The master takes it back two heartbeat intervals (20 s) after its own
master time passes the same deadline, unless `Started` has arrived by then (decision 450): a
`Started` sent at the last moment has the worker's reserve and that grace, 40 s, to arrive.

| When | Master | Worker |
|---|---|---|
| Master time S | Stores the attempt. `start_by_ms` is S plus 60 s; expiry keeps S. | |
| On `AssignTask` | | Its stamp names the heartbeat sent at boot time T. The worker starts only while T + (`start_by_ms` − `master_ms`), less 0.1 %, is more than 20 s ahead. |
| Otherwise | | Rejects `TOO_LATE`, and drops the journal entry. |
| Past S + 60 s + 20 s, no `Started` waiting | Takes the attempt back. | |

### A start budget that outlived the expiry

The first patch put a remaining budget on the assignment, fixed when it was sent. The worker
counted that budget from when the assignment arrived. The wait after the send sat outside the
master's expiry, which still ran from when the attempt was stored, so the worker could start
after the master had taken the attempt back (decision 442: a budget spent at delivery).

Master time closes it by sending the deadline itself, the same reading `expire` keeps, rather
than a duration the worker starts counting later.

### Heartbeats left unanswered

Numbering heartbeats and treating the count as the clock left the worker behind whenever an
ack never went out (the master not leading, or the stream buffer full). The master's expiry
moved on; the worker's count did not.

The master records the heartbeat's number when it arrives, ack or no ack, and puts that
number on every later message. The worker dates master time from when it sent that
heartbeat. A run of messages with no sample at all counts every deadline as passed, so the
task is not started.

### A `Started` held up (B227)

A `Started` sent in the last moment of the budget could arrive after the master had taken
the attempt back and given it to someone else. The worker keeps two heartbeat intervals (20 s
of the 60 s) so that `Started` can reach the master first. The master notes it as it is read,
before the message waits for the lock.

| | Before | After |
|---|---|---|
| When the worker may start | Up to the end of the 60 s | Not inside the last 20 s (two heartbeats) |
| Its `Started` | Sent at the very end, and held on the way | Sent with 20 s of master time still to go |
| The master | At 60 s from storage, no `Started` yet: takes the attempt back and gives it to another worker, while the first task already runs | Records the arrival before the lock, so expiry leaves the attempt alone |

A `Started` held longer than those two intervals is a known limit, below.

### A wall-clock jump on the master (B228)

Expiry, worker loss and the other liveness timers used to compare stored wall-clock times
with the wall clock the master was handed. A step of that clock made a live assignment look
due. Holding liveness after a jump was the next patch, and it was dropped: nothing that
decides liveness reads the wall clock. A jump of two minutes while master time moves five
seconds takes no assignment back and loses no worker. Both go only once master time itself
passes the lease.

### The minimum lease (B226)

The worker keeps two heartbeat intervals for `Started`, reads master time from a heartbeat
sent up to one interval earlier, and still needs an interval in which to start. A lease under
four heartbeat intervals cannot cover that, and a lease under one interval loses a worker
between two heartbeats. The load check is below.

A worker lease covers, at the least, four heartbeat intervals:

- one: the heartbeat the stamp names, up to 10 s old;
- one: at least 10 s left to start;
- two: 20 s kept so `Started` arrives before the expiry.

Under 40 s the process refuses to start.

### A worker cut off by the clock of its own receipts (decision 445)

A worker counted its cutoff (the time it stops `cleanup_anywhere` tasks and triggers, as the
master may move them) only from when it received the master's last message, by its own
clock: a message held on its way left that cutoff late in master time. Now the cutoff is the
earlier of the two: a lease after the last receipt, or the moment master time may have
passed the last stamp plus a lease, by the bound above.

A message stamped at master time 100 s is held 30 s on its way:

| Bound | The worker stops its `cleanup_anywhere` tasks at |
|---|---|
| By receipt: a lease after it arrived, at 130 s | 190 s of master time |
| By the stamp: when master time may pass 100 s + 60 s | 160 s: this one, the earlier |

The master counts its own lease from the worker's last message it received.

### A worker cut off one way (decision 453)

The master counts a worker lost a lease after the last message it received from it, not from
its own last send. With the worker's heartbeats lost on the way while the master's messages
still arrive, each stamped later than the one before, the stamp alone would keep pushing the
worker's cutoff past the moment the master moves its cleanup. So the cutoff has a third bound:
every master message names the last heartbeat it heard (`heard_sequence`), the worker knows when
it sent that heartbeat, and the master received it no earlier. The worker stops a lease after
that send, less the drift allowance (0.1 %), on its own boot clock.

| When | Worker | Master |
|---|---|---|
| 10 s on the worker's clock | Sends heartbeat 7. | |
| 10.02 s | | Hears it. Counts the worker lost from 70.02 s (a 60 s lease). |
| After that | Heartbeats 8, 9, … are lost on the way. | |
| Master time 50 s | | Sends an assignment stamped 50 s, `heard_sequence` 7. |
| On that assignment | By the stamp alone it would stop near 110 s. By heartbeat 7's send it stops at 10 + 60 − 0.06 = 69.94 s, and does. | |
| 70.02 s plus the cancel grace and the move wait | | Moves the cleanup. |

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

| Side | What it varies | What it checks |
|---|---|---|
| The worker's `MasterTime` on virtual clocks | Drift, delays, lost answers, stalls, late assignments | A start is at least the reserve before `start_by_ms`; a worker not cut off is within a lease of the last heartbeat the master heard |
| The master core and two workers on one clock | Cuts, lags, replaced workers, restarts, takeovers | One process per task instance at a time; nothing started after the master ended it; all ended once the faults are over |

A failure is reported with its smallest scenario.

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
Deployments page. A heartbeat sent at the worker's wall time A, its answer arriving at B: a
master wall time between A and B is no skew, one 30 s past B is 30 s of it. Wall clocks only touch what people read (cron times, logs, shown times);
no deadline uses them.

## One clock, and what still uses the wall clock

Before, a deadline was a wall-clock time stored in the database and compared with whatever
clock the master or the worker had. After, each liveness deadline is a master time, and the
master's own expiries count on its monotonic clock.

| | Before | After |
|---|---|---|
| A deadline is | A wall-clock time stored in the database (`assigned_at` plus the lease) | A master time, on the stamp's timeline |
| The master expires it | When its wall clock passes that time | On its monotonic clock, from the reading the deadline was built with |
| The worker counts | Its own lease timer, refreshed when the assignment arrives | A bound of that same master time, from the heartbeat the stamp names |
| So | Two clocks, and two starting points, that disagree | One clock, and one starting point |

Each liveness deadline, and where its master time counts from:

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

| When | Master | Old process | New incarnation |
|---|---|---|---|
| The registration | Accepts it at once, and stops answering the old stream's heartbeats. Marks the started attempts the new journal does not report, the mark stored with each. | | Sends `Register`. |
| Its own lease | | Its `cleanup_anywhere` tasks stop. An ordinary task may still be running. | |
| 60 s of master time after the registration | Ends the marked attempts `lost`. | | |
| After a restart | Keeps the mark, and counts the 60 s afresh. | | |

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

| When | Deposed master | New leader | Worker |
|---|---|---|---|
| For its lead, 20 s on its monotonic clock | Answers heartbeats. | | |
| On taking over | | Counts restored workers as heard 20 s of master time from now. | |
| 20 s + 60 s later, on its master time | | Counts a worker still unheard as lost. | |
| On the new leader's messages | | Stamps them with the new lease epoch. | A lower epoch then ends a stream, and the worker connects again. |
| A lease with no word from the highest epoch, on its own clock | | | Follows the lower epoch. |

### The load checks

Both are refused when the configuration is loaded. The lead check is what makes the fence's
lease long enough: a deposed master must stop answering before a worker gives up on the
higher epoch. Sending the lead to workers, for the fence to wait it out, was not chosen.

At load, the process refuses to start when:

1. the worker lease is under four heartbeat intervals, or
2. the lead is not shorter than the worker lease.

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

Every worker honours the deadline: a worker of the 0.1 preview, which ignored it, is refused
by a 1.0 master (another protocol major). An assignment without `start_by_ms` sets the worker
no limit of this kind.
