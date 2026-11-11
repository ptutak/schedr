# Guarantees

What the master–worker protocol guarantees, and how master time sets every deadline a worker
shares with the master. The message flows are in
[Master–worker protocol](15-protocol.md). How a worker reads master time, and the races that
reading closes, are in [The master clock](17-master-clock.md). What a stop does to a run is
in [State and failures](07-failures.md#when-something-stops). The numbers below are the
defaults; the settings are in [Timings](14-timings.md#fixed-timings). The inequalities a
change has to keep are in
[Invariants of master time](../_generated/05-design/topics/workers.md#invariants-of-master-time).

## The protocol, and which clock

One master leads, the one that holds the database lease. Standbys wait. Workers keep a
journal on their own disk and open the streams. Every message the master sends carries a
`MasterClock`.

```{mermaid}
flowchart TB
    subgraph masters [Masters on one database]
        standby["Standby: refuses a stream, not leading"]
        leader["Leading master"]
        db[("Lease row: holder and epoch")]
        standby -->|takes the lease when it is released or expired| db
        leader -->|renews every 10 s; each write checks holder and epoch| db
    end
    worker["Worker, with its journal"]
    worker -->|"Control: Register, heartbeats, results"| leader
    leader -->|"Control: assignments, acks, cancels"| worker
    worker -->|UploadParseResult| leader
    worker -->|FetchInputs| leader
    stamp["MasterClock on every master message: timeline, master_ms, heard_sequence, wall_ms, lease_epoch"]
    leader --> stamp --> worker
    mt["Master time: this process's monotonic clock"]
    lead["That same monotonic clock, as a duration from the last renewal"]
    boot["The worker's boot clock: its bound of a stamped master time"]
    timer["The worker's own timer"]
    wall["The wall clock"]
    dbclock["The database clock"]
    leader --- mt
    leader --- lead
    worker --- boot
    worker --- timer
    leader --- wall
    db --- dbclock
    mt --> live["Assignment expiry, worker loss, a parse job, a connection job, a moved cleanup's wait, a replaced incarnation's hold, a new leader's wait"]
    lead --> deposed["How long a deposed master still answers"]
    boot --> bound["start_by_ms, and the master-time half of the cleanup_anywhere cutoff"]
    timer --> own["A stream silent for a lease, the epoch fence, the receipt half of the cutoff"]
    wall --> people["Schedules, set-aside, a sensor's max_wait, the script-root grace, the skew warning"]
    dbclock --> who["When a standby may take the lease"]
```

`heard_sequence` is the latest heartbeat this stream has shown the master, and `0` is the
worker's `Register`. `wall_ms` is there for people. `timeline` is this master process's own;
another master's times are a different timeline. A deadline in the live list is a master
time on that timeline.

## How master time limits a worker

The worker's bound of a master time falls before that master time. The bound is what the
timelines below call the worker's side. How it is taken from a stamp is in
[The master clock](17-master-clock.md#what-master-time-is).

### A start deadline

`start_by_ms` is the worker lease, 60 s, after the master stored the attempt, from the same
reading the master's expiry keeps.

```{mermaid}
sequenceDiagram
    participant M as Master time
    participant W as Worker bound
    Note over M: 0 s. The attempt is stored. start_by_ms is 60 s.
    Note over W: The worker starts only while its bound of those 60 s is more than 20 s ahead.
    Note over W: The 20 s is two heartbeats, kept so Started can arrive. Otherwise it rejects TOO_LATE and drops the journal entry.
    Note over M: A Started or Finished already read stays, including one still waiting for the lock.
    Note over M: Held at 80 s. A round once master time is past 80 s, with none waiting, takes the attempt back.
    Note over M,W: The bound is earlier than master time, so the last start falls before the take-back.
```

The 80 s is the lease plus the same two heartbeats. While that worker may still come back
with the attempt, the master leaves it out of the take-back. The lease must be at least four
heartbeats, 40 s, or the process refuses to start.

### The worker lease, and the cutoff

The master marks a worker lost once master time is past 60 s after the later of two
readings: the last message's arrival, recorded as the bytes are read, and the last message
it has handled. The worker has three limits of its own, and the earliest wins for a
`cleanup_anywhere` task.

```{mermaid}
sequenceDiagram
    participant M as Master time
    participant R as Worker timer
    participant B as Worker bound
    Note over R: The stream ends 60 s after the last master message, or after the stream opened.
    Note over R: Messages that arrive after that are ignored.
    Note over B: The master-time cutoff is the bound of the last stamp's master_ms plus 60 s.
    Note over B: And 60 s less 0.1 % after it sent the last heartbeat the stamp says the master heard.
    Note over R,B: A cleanup_anywhere task stops at the earliest of the three. Its cleanup waits.
    Note over M: The worker is still held at 60 s after its last message. A round past that marks it lost.
    Note over B,M: The master heard that heartbeat no earlier than it was sent, so the stop falls before the loss.
```

The receipt half is what stops a message held on its way from pushing the cutoff later. A
reconnect keeps the cutoff the last session computed until the new session has a stamp. A
worker with no `cleanup_anywhere` task does not arm this cutoff. An ordinary task keeps
running after the stream ends.

### A moved cleanup's wait

The wait starts when a round marks the worker lost. It is the cancel grace plus the cleanup
move wait, 30 s + 10 s.

```{mermaid}
sequenceDiagram
    participant M as Master time
    participant W as Worker
    Note over M: The round that marks the worker lost.
    Note over W: Its cleanup_anywhere tasks have already stopped, whatever the master sent since.
    Note over M: For 40 s of master time the cleanup stays put.
    Note over M: A round once that 40 s has passed may assign the cleanup to another worker.
    Note over M: After a restart the wait is a whole 40 s from when this master first sees it.
```

A cut one way, the worker's messages lost while the master's still arrive, is covered by the
third limit: each master message names the last heartbeat it heard
([A worker cut off one way](17-master-clock.md#a-worker-cut-off-one-way-decision-453)).

### A replaced incarnation's hold

A new incarnation registers at once. The old stream gets no further heartbeat answers, so
its heartbeats do not push the hold out.

```{mermaid}
sequenceDiagram
    participant Old as Old process
    participant M as Master time
    participant New as New incarnation
    New->>M: Register
    Note over M: 0 s. Started attempts the new journal does not report are marked.
    Note over Old: Its last stamp is at or before that mark. Its cleanup_anywhere tasks stop by the bound of that stamp plus 60 s.
    Note over M: Those attempts end lost at 60 s of master time after the mark.
    Note over M: A restart keeps the mark and counts the 60 s from when this master first sees the attempt.
    Note over Old: An ordinary task can still be running at 60 s.
```

A journal that reports the attempt clears the mark, so a continue is not ended when the hold
runs out.

### A new leader's wait, and the lease epoch

Each lane is that process's own clock. The durations are the same settings: the lead is the
master lease minus a query timeout, 30 s − 10 s = 20 s, and it has to be shorter than the
60 s worker lease or the process refuses to start.

```{mermaid}
sequenceDiagram
    participant Old as Deposed master, monotonic clock
    participant W as Worker, its own timer
    participant New as New leader, master time
    Note over Old: Answers heartbeats while this clock is before the last renewal plus 20 s.
    Note over Old: A clean stop answers nothing more, and gives the lease up.
    Note over New: A worker it has not heard from counts as heard 20 s ahead.
    Note over New: Still held at 80 s from the restore. A round after that, still unheard, marks it lost.
    Note over W: A lower lease epoch is refused while the highest has been silent for under 60 s.
    Note over W: At 60 s of that silence the lower epoch is followed.
```

The process refuses to start when that 20 s lead is at least the 60 s worker lease, so the
time a deposed master may still answer is shorter than the time a worker keeps the higher
epoch. The new leader starts the worker lease only after its own 20 s, and marks the worker
lost only after the 60 s that follow. Writes are a different fence: the database refuses a
write once the lease row is another holder's, or the same holder's at a newer epoch.

## The guarantees

### Starting

| Guarantee | What it rests on | Where it stops |
|---|---|---|
| A task is not started after the master took the attempt back | `start_by_ms` is 60 s of master time after the attempt was stored. The worker starts only while its bound of that deadline is more than 20 s ahead, and rejects `TOO_LATE` otherwise. The master takes the attempt back only once master time is past 80 s, and a `Started` or `Finished` already read keeps it. While the worker may come back, the attempt is left out of the take-back. | A `Started` held longer than 40 s, the stream still up, can arrive after the take-back. The master answers `CancelAttempt`, and both workers may have run the task. An older worker ignores `start_by_ms`. |

The 40 s between the last start and the take-back is the worker's two heartbeats and the
master's two. Both limits are drawn under
[A start deadline](#a-start-deadline). The late `Started` and the older worker are in
[Known limits](17-master-clock.md#known-limits).

### One leader

| Guarantee | What it rests on | Where it stops |
|---|---|---|
| One master leads at a time | The lease row names the holder. A standby takes it when it is released, or when the database clock says it has expired. The leader renews every 10 s and answers workers while its monotonic lead, 20 s from the last renewal, still has time. | A deposed master answers heartbeats until that 20 s runs out. A clean stop stops at once. |
| A deposed master's writes are refused | Each write checks the holder and the epoch in the write's transaction, and is refused once the row has moved on. | The lease row expires on the database's clock. A step of that clock can hand the lease over while the old process still has lead left; its writes then fail the check. |

The lease itself is in
[One master per database](04-master.md#one-master-per-database-the-lease). The 20 s wait and
the epoch fence are drawn
[above](#a-new-leaders-wait-and-the-lease-epoch).

### A result

| Guarantee | What it rests on | Where it stops |
|---|---|---|
| A result is delivered at least once | The worker journals the result before it sends `Finished`, and sends it again until `FinishedAck`. After eight resends it reconnects, and the journal is reconciled. | A result not yet acknowledged stays in the journal across an agent restart. Losing that disk loses it. |
| A result is stored once | The end is recorded only while the attempt has no status yet. A second `Finished` is acknowledged, and the stored end stands. | A `Finished` from a worker that does not own the attempt is acknowledged and not stored. A result the database refuses is stored as the failure `result_not_stored`, and is not retried. |

Repeats are in [Delivery](07-failures.md#delivery). The refused result is in
[When something stops](07-failures.md#when-something-stops).

### Silence, and a cleanup that moves

| Guarantee | What it rests on | Where it stops |
|---|---|---|
| A stream silent for a lease ends | 60 s after the master's last message, on the worker's timer, the worker ends the stream and ignores what arrives later. A `Registered` at least that old on the first stream is ignored, and the worker registers again. | An ordinary task keeps running. A `cleanup_anywhere` task stops here, or sooner, at the cutoff below. |
| A `cleanup_anywhere` task stops before its cleanup moves | The task stops at the earliest of 60 s after the last message received, the bound of the last stamp plus 60 s, and 60 s less 0.1 % after it sent the last heartbeat the master says it heard. The master loses the worker only once master time is past 60 s after the worker's last message it received, then waits 40 s before the cleanup can move. The stop falls first, a cut one way included. | A cleanup already running when the cut starts keeps running. A frozen VM does not stop. |

The two limits are drawn under
[The worker lease, and the cutoff](#the-worker-lease-and-the-cutoff) and
[A moved cleanup's wait](#a-moved-cleanups-wait). A cleanup already running, and a frozen VM,
are in [Cleanup on another worker](../02-dags/15-cleanup.md#cleanup-on-another-worker).

### Clocks

| Guarantee | What it rests on | Where it stops |
|---|---|---|
| A wall-clock jump moves no liveness deadline | Assignment expiry, worker loss, a parse job, a connection job and a moved cleanup's wait count on master time. The worker reads that time from `MasterClock`. | A jump of the master's monotonic clock is not caught. Four decisions still read the wall clock: schedules, set-aside, a sensor's `max_wait`, and the script-root grace. A jump can fire one of those early, or hold it. |

Which four, and why they stay on the wall clock, is in
[One clock, and what still uses the wall clock](17-master-clock.md#one-clock-and-what-still-uses-the-wall-clock).

## What is not guaranteed

Each line is a limit that is kept on purpose, or open for now.

- A `Started` held longer than 40 s can arrive after the take-back, and both workers may have run the task ([Known limits](17-master-clock.md#known-limits)).
- A worker of an earlier build ignores `start_by_ms`, and an assignment that leaves it unset has no master-time start limit, until protocol 1.0 ([Known limits](17-master-clock.md#known-limits)).
- A cleanup already running at the cut, and a task on a frozen VM, can overlap the moved cleanup ([Cleanup on another worker](../02-dags/15-cleanup.md#cleanup-on-another-worker)).
- After a suspend, the cutoff is read again on the alive tick, every 5 s, so a `cleanup_anywhere` task can stop up to 5 s late ([Known limits](17-master-clock.md#known-limits)).
- A stall under 1 s, plus 0.2 % of the time since the sample, leaves the worker's bound late by that much ([Known limits](17-master-clock.md#known-limits)).
- The cutoff the last session computed stands over a reconnect, until the new session's first stamp ([Known limits](17-master-clock.md#known-limits)).
- An ordinary task keeps running after the stream ends, and can overlap its retry once the worker is lost or a replaced incarnation's 60 s hold ends ([When something stops](07-failures.md#when-something-stops)).
- A jump of the master's monotonic clock is not caught ([Known limits](17-master-clock.md#known-limits)).
- Schedules, set-aside, a sensor's `max_wait` and the script-root grace move with the wall clock ([One clock](17-master-clock.md#one-clock-and-what-still-uses-the-wall-clock)).
- The lease row expires on the database's clock ([One master per database](04-master.md#one-master-per-database-the-lease)).
