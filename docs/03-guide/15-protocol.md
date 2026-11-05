# Master–worker protocol

Workers open the `Control` stream; their stable worker ID and per-process incarnation ID distinguish a reconnect from a restart. Times and their settings are in [Timings](14-timings.md).

## Registering and heartbeats

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    W->>M: Control: Register (identity, roots, journal, slots)
    alt Compatible version and no live stream for this ID
        M-->>W: Registered (timings, journal decisions, size limit)
        W->>M: Warnings (current complete list)
        loop Heartbeat interval: 10 s default
            W->>M: Heartbeat (free slots, running attempts)
            M-->>W: HeartbeatAck (while leading)
        end
        Note over W,M: Worker lease: 60 s default without any message; master marks worker lost
    else Incompatible version or duplicate live stream
        M-->>W: Registration refused
    end
```

Each `JournalEntry.handles` in `Register` counts the handles in that attempt's worker journal.
A `JournalDecision` can answer `CLEAN_REST` (`ATTEMPT_DECISION_CLEAN_REST`) with
`cleanup_from`, the first handle index to clean. The worker stops a still-running task,
cleans from that index and sends `Finished` with `CleanupResult.rest` set. That result is
added to the attempt once; it never represents the whole cleanup.

Any worker message renews the master's worker lease at arrival, even if it waits to be handled.
The master answers heartbeats before they wait for its lock, while it leads. The worker notes
messages from the master as word from it. The worker ends a stream silent for a lease,
whatever it runs, and ignores messages arriving past that limit. On its first stream, a `Registered` at least as old as the lease it states is
ignored and the worker registers again. A non-leading master refuses a stream with
`UNAVAILABLE` ("not leading"), including a `Register` held at its lock as it paused.

## A normal run

```{mermaid}
sequenceDiagram
    participant M as Master
    participant W as Worker
    participant P as Task process
    M->>M: Store attempt and reserve slot
    M->>W: AssignTask
    W->>W: Journal running attempt
    W->>M: Started (spawn)
    W->>P: Start process
    opt Forked Python task
        W->>M: Started (fork; first start time retained)
    end
    P-->>W: Result
    W->>W: Journal finished result
    W->>M: Finished
    M->>M: Store result and advance run
    M-->>W: FinishedAck
    W->>W: Remove journal entry and free slot
    Note over W,M: No ack: resend after one heartbeat interval (10 s default), then back off; reconnect after 8 resends
```

A command resource setup reports its value to the agent with `resource_set` over the
[context channel](05-workers.md#a-commands-context-channel), and on success that value is
its `Finished` output, which the master passes to the resource's command cleanup for
`resource_get`.

## A rejected assignment

```{mermaid}
sequenceDiagram
    participant M as Master
    participant W as Worker
    M->>W: AssignTask
    W->>W: Check slot, code and specification
    W-->>M: Rejected (reason and detail)
    M->>M: Return task to waiting or try another worker; no retry used
    Note over M: Code wait: 10 min default for missing eligible code; then task is unschedulable
```

## An assignment never started

```{mermaid}
sequenceDiagram
    participant M as Master
    participant W as Worker
    M->>W: AssignTask (may be lost in transit)
    alt Started arrives in time
        W->>M: Started
        Note over M: Arrival counts even while the message waits to be handled
    else No Started arrives
        Note over M: Assignment expiry: worker lease, 60 s default; take it back in a round
        M->>M: End assignment_expired; queue again without using a retry
    end
```

## Large inputs

```{mermaid}
sequenceDiagram
    participant M as Master
    participant W as Worker
    M->>W: AssignTask (inputs_external)
    W->>M: FetchInputs (attempt ID)
    M-->>W: InputChunk stream
    alt Fetch succeeds
        W->>M: Started
    else Fetch fails or does not finish in time
        Note over M: Assignment expiry: worker lease, 60 s default without Started; assignment is taken back
    end
```

A chunk arriving after the worker has heard nothing for a lease is ignored: the worker
does not start the task or send a rejection.

## Cancel

```{mermaid}
sequenceDiagram
    participant M as Master
    participant W as Worker
    participant P as Task process group
    M->>W: CancelAttempt
    alt Task is still running
        W->>P: SIGTERM
        Note over W,P: cancel_grace: 30 s default; SIGKILL the remaining group when it expires
        P-->>W: Process ends
        W->>M: Finished (cancelled, after any cleanup)
        M-->>W: FinishedAck
    else Result already finished
        W->>M: Finished (original result wins)
        M-->>W: FinishedAck
    end
```

## A task with cleanup

```{mermaid}
sequenceDiagram
    participant P as Task
    participant W as Worker
    participant M as Master
    P->>W: Register handles (journal before returning)
    W->>M: HandlesAdded
    P-->>W: Task result
    W->>W: Journal cleaning record
    W->>M: TaskEnded (task result)
    M->>M: Keep result; instance cleaning_up
    loop Cleanup tries and retry delays; slot remains held
        W->>W: Run cleanup with handles and secrets
        Note over W: cleanup_timeout: 5 min default per try; stop a timed-out try
    end
    W->>M: Finished (task and cleanup results)
    M-->>W: FinishedAck
```

## Reconnect while the agent lives

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    Note over W,M: Stream drops; task and cleanup continue
    Note over W: Reconnect backoff starts at 200 ms, up to 30 s default
    W->>M: Register (same incarnation, journal)
    alt Attempt still open
        M-->>W: Registered (Continue)
        W->>M: TaskEnded or Finished again if needed
        opt Cancel was requested while disconnected
            M->>W: CancelAttempt
        end
    else Master already ended attempt
        M-->>W: Registered (Kill for running, Ack for finished)
        W->>W: Stop or discard entry as directed
    end
    Note over M: Worker lease: 60 s default without messages; worker and attempts become lost
```

## Agent restart and journal decisions

```{mermaid}
sequenceDiagram
    participant W as New agent
    participant M as Master
    W->>W: Read journal; stop orphan process groups
    Note over W: Orphan grace: 10 s default before SIGKILL
    W->>W: Hold slot for recoverable cleanup
    W->>M: Register (new incarnation, journal)
    alt Open attempt: Continue
        M-->>W: Registered (Continue)
        alt Task was running
            W->>M: TaskEnded (lost task result)
        else Cleanup was running
            W->>M: TaskEnded (saved task result)
        end
        W->>W: Restart cleanup with saved handles, all tries and freshly read secrets
        Note over W: cleanup_timeout: 5 min default per try; stop a timed-out try
        W->>M: Finished (task and cleanup results)
        M-->>W: FinishedAck
    else Attempt already ended: Kill
        M-->>W: Registered (Kill for running entry)
        W->>W: Discard waiting cleanup
    else Result already stored: Ack
        M-->>W: Registered (Ack for finished entry)
        W->>W: Remove finished journal entry
    end
```

A finished entry for an open attempt gets `Continue` and is sent again. Without a recoverable cleanup, an orphaned running entry is omitted and reconciled as missing.

## Lost worker

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    opt Task ends before worker is lost
        W->>M: TaskEnded (task result)
    end
    Note over M: Worker lease expires; worker is lost
    alt Started task without cleanup_anywhere
        M->>M: Keep reported task result or mark task lost
        opt Task has cleanup
            M->>M: End cleanup worker_lost
        end
    else Unstarted assignment
        M->>M: End assignment_expired; retry without using a task retry
    else Started task with cleanup_anywhere and moves remaining
        M->>M: Keep task result or mark task lost; queue cleanup to move
    else Cleanup has exhausted its moves
        M->>M: End cleanup worker_lost
    end
```

A restart that omits an attempt is reconciled similarly; a saved task result is retained.

## Lost worker with cleanup_anywhere

```{mermaid}
sequenceDiagram
    participant O as Original worker
    participant M as Master
    participant W as Another worker
    O->>M: HandlesAdded (journaled handles)
    Note over M: Worker lease expires; original worker is lost
    M->>M: Keep task result or mark task lost; queue cleanup to move
    Note over M: Wait cancel_grace + cleanup_move_wait (30 s + 10 s default) after worker lost
    alt No eligible worker
        M->>M: Remain cleaning_up; no worker for the cleanup
    else Worker available
        M->>W: AssignTask (cleanup_only, handles)
        alt Refused
            W-->>M: Rejected
            M->>M: Take back; prefer another worker next time
        else No Started
            Note over M: Assignment expires; take back without another move
        else Started
            W->>M: Started
            W->>M: TaskEnded
            W->>W: Run cleanup
            W->>M: Finished (cleanup result)
            M->>M: Keep original task result and finish attempt
            M-->>W: FinishedAck
        end
    end
```

Each move counts as a cleanup try; if the receiving worker is lost after
`cleanup_retries + 1` moves, cleanup ends `worker_lost`. A refused worker can be asked again
when no other is available. A handle not delivered before the original worker is lost cannot
accompany the cleanup. If the original worker returns with more handles while the move
still waits, the master takes the move back and gives cleanup to that worker. If the move
was assigned elsewhere or ended, the master sends `CLEAN_REST` with its contiguous handle
count as `cleanup_from`. A handle received past a gap can therefore be cleaned again. The
rest result can arrive after the run ends; a failure after success makes it a [yellow success](03-life-of-a-run.md#7-the-next-task-and-the-end).

If the worker from which cleanup was first scheduled to move reconnects with a finished cleanup before the
move takes place, its journaled `Finished` receives `Continue`, so it sends the result again;
the master stores it and drops the move. A report from a later cleanup-only worker keeps the
task result already stored.

A lost worker may only be cut off from the master. A cleanup already running when the cut
starts can overlap a moved one, and a frozen VM cannot stop its task. See
[Cleanup on another worker](../02-dags/15-cleanup.md#cleanup-on-another-worker).

```{mermaid}
sequenceDiagram
    participant O as Cut-off worker
    participant M as Master
    participant W as Another worker
    Note over O: Task runs; connection lost, cleanup held
    Note over O: No word for worker lease (60 s default)
    O->>O: CutOff stops task (lost); cleanup waits
    alt Move waiting and original worker has more handles
        O->>M: Register (attempt and handle count)
        M->>M: Take back move; give cleanup to original worker
        M-->>O: Registered (Continue)
        O->>O: Run cleanup with all handles
    else Move assigned or ended and original worker has more handles
        M->>W: AssignTask (cleanup_only, known handles)
        O->>M: Register (attempt and handle count)
        M-->>O: Registered (CLEAN_REST, cleanup_from)
        O->>O: Stop task if running; clean remaining handles
        O->>M: Finished (CleanupResult.rest)
    else Master still owns the attempt
        O->>M: Register (attempt)
        M-->>O: Registered (Continue)
        O->>O: Run held cleanup
    end
```

## Drain

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    participant P as Task process
    W->>M: Draining
    M->>M: Stop assigning work
    Note over W,P: termination_grace_period: 30 s default; reserve 3 s termination and 2 s reporting
    alt Task has cleanup
        Note over W,P: Stop early enough to leave one cleanup try before the stop deadline, when possible
        W->>P: SIGTERM, then SIGKILL if needed
        P-->>W: Interrupted
        W->>M: TaskEnded (interrupted)
        W->>W: Run cleanup; do not stop cleanup at drain deadline
        Note over W: cleanup_timeout: 5 min default per try; timeout stops that try
    else Other task still running at stop deadline
        W->>P: SIGTERM
        Note over W,P: drain_terminate_period: 3 s default; SIGKILL remaining process group
    end
    W->>M: Finished
    M-->>W: FinishedAck
    W->>M: Deregister
    Note over W: Report period: 2 s default before grace ends; a cleanup may outlast it
```

The agent waits for an ongoing cleanup; if its container is killed first, the worker is lost.

## Master failover

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant A as Old master
    participant DB as Database
    participant B as Standby master
    Note over A: Master crashes; worker tasks continue
    Note over DB,B: Master lease: 30 s default; standby can take over after expiry (clean stop releases it)
    B->>DB: Take lease and read state
    B->>B: Mark workers disconnected; restart worker leases
    Note over W: Reconnect backoff: 200 ms first, up to 30 s default and half known lease
    W->>B: Register (journal and roots)
    B-->>W: Registered (Continue, Kill, Ack or CLEAN_REST per entry)
    Note over B: Worker lease: 60 s default from startup; absent worker becomes lost
```

## Parse job

```{mermaid}
sequenceDiagram
    participant M as Master
    participant W as Worker
    M->>W: AssignParse (root and job ID)
    W->>W: Parse files
    Note over W: parse_file_timeout: 60 s default per file; timed-out file becomes an import error
    W->>M: UploadParseResult (header, then one file per part)
    M-->>W: ParseResultAck
    Note over M: parse_job_timeout: 10 min default; unfinished job is reassigned
```

A `ParsedFile` carries `script_roots`, the script roots its `ScriptOperator`s asked for,
including a missing root that caused an import error. During a DAG parse, the runner asks the
agent `script_declaration` for a reference. The agent answers with the checked declaration
or an error explaining why it could not resolve it.

A file exceeding the master's message limit is reported as an import error; the upload runs separately from heartbeats.

## Connection job

```{mermaid}
sequenceDiagram
    participant M as Master
    participant W as Worker
    M->>W: ConnectionJob (check or test)
    alt Test
        W->>W: Open connection and run check
        Note over W: connection_test_timeout: 1 min default; stop a timed-out test
    else Check
        W->>W: Render connection reports
    end
    W->>M: ConnectionJobDone (reports)
    Note over M: Job time plus 30 s: no answer means lost; a late answer is dropped
```

## Code changes and warnings

```{mermaid}
sequenceDiagram
    participant W as Worker
    participant M as Master
    Note over W: poll_interval 30 s and settle_time 5 s defaults; wait for stable code before reporting
    W->>M: CodeVersionChanged (root hash)
    M->>M: Record hash; possibly create parse job
    W->>M: Warnings (replacement list when changed)
    M->>M: Replace stored warnings
```

## Message reference

The timing links point to [Timings](14-timings.md); a dash means no dedicated message deadline. Repeats are described for the same ID or state, not as a promise that every operation has identical effects.

### Worker to master on `Control`

| Message | Sender and when | Receiver action | Repeat safe? | Timing |
|---|---|---|---|---|
| `Register` | Worker on every connection | Validate, reconcile journal and reply | New connection reconciles again; duplicate live stream refused | [worker lease](14-timings.md#fixed-timings): `SCHEDR_HIDDEN__MASTER__LEASE_TIMEOUT` |
| `Heartbeat` | Worker periodically | Renew lease on arrival | Yes | [`SCHEDR_HIDDEN__MASTER__HEARTBEAT_INTERVAL`](14-timings.md#fixed-timings) |
| `CodeVersionChanged` | Worker after root settles or withdraws it | Record hash and possibly plan parse; an empty hash withdraws the root | Yes, same hash | [`[dags] settle_time`](14-timings.md#public-timing-keys) |
| `Started` | Worker before spawn; again for fork | Mark running; update method, keep first time | Yes | [assignment expiry](14-timings.md#fixed-timings) |
| `Rejected` | Worker unable to accept assignment | Requeue or select another worker | Stale rejection is ignored | [assignment expiry](14-timings.md#fixed-timings) |
| `TaskEnded` | Worker as cleanup begins | Keep task result, mark cleaning up | Yes, for open attempt | [`[worker] cleanup_timeout`](14-timings.md#public-timing-keys) |
| `HandlesAdded` | Worker as each handle is journaled; all again on `Continue` | Store handles from the assigned worker for a moved cleanup; first at each position stands, up to 1 MiB per attempt | Yes | — |
| `Finished` | Worker after attempt and cleanup | Store result and reply with `FinishedAck` | Yes, stored result is acknowledged again | [`SCHEDR_HIDDEN__MASTER__HEARTBEAT_INTERVAL`](14-timings.md#fixed-timings) governs resend |
| `Draining` | Worker on shutdown | Stop assignments, mark draining | Yes | [`[worker] termination_grace_period`](14-timings.md#public-timing-keys) |
| `Deregister` | Worker after drain | Mark offline | Yes | [`SCHEDR_HIDDEN__WORKER__DRAIN_ACK_WAIT`](14-timings.md#fixed-timings) |
| `Warnings` | Worker after registration and on change | Replace warnings | Yes | — |
| `ConnectionJobDone` | Worker after connection check or test | Store reports if job is still open | Late or already settled answer is dropped | [`[webserver] connection_test_timeout`](14-timings.md#public-timing-keys) plus job allowance |

### Master to worker on `Control`

| Message | Sender and when | Receiver action | Repeat safe? | Timing |
|---|---|---|---|---|
| `HeartbeatAck` | Master as each `Heartbeat` arrives, while it leads | Note the master as heard; silence for the worker lease ends the stream and stops `cleanup_anywhere` tasks | Yes | [`SCHEDR_HIDDEN__MASTER__LEASE_TIMEOUT`](14-timings.md#fixed-timings) |
| `Registered` | Master after `Register` | Apply timing, journal decisions and `script_uses` (`RootUses` per DAG root); ignore an answer at least a lease old and register again | Decisions reconcile entries | [`SCHEDR_HIDDEN__MASTER__LEASE_TIMEOUT`](14-timings.md#fixed-timings) |
| `ScriptUses` | Master when the script roots sent for DAG roots change | Replace the uses (`RootUses` per DAG root); fold the used script roots' hashes into each DAG root's reported hash, withdrawing a DAG root that uses a script root this worker lacks (decisions 301, 303, 304) | Yes, it replaces; an omitted root uses none | — |
| `AssignTask` | Master after storing assignment | Validate and start attempt; `cleanup_only` runs moved cleanup with its handles; `resource_cleanup` marks a resource cleanup so the worker supplies its default timeout if needed | Existing attempt ID is ignored | [assignment expiry](14-timings.md#fixed-timings) |
| `AssignParse` | Master when root needs parsing | Parse root and upload result | Job results are matched to the assigned job | [`[master] parse_job_timeout`](14-timings.md#public-timing-keys) |
| `CancelAttempt` | Master on cancellation or reconnect | Stop running task, including a resource cleanup task; leave per-task cleanup alone | Yes | [`[master] cancel_grace`](14-timings.md#public-timing-keys) |
| `FinishedAck` | Master after storing `Finished` | Forget journaled result | Yes | — |
| `ConnectionJob` | Master for a check or test | Run one job, return reports | Do not rely on repeated execution being harmless for a test | [`[webserver] connection_test_timeout`](14-timings.md#public-timing-keys) |

### RPCs

`UploadParseResult` is client-streaming and `FetchInputs` is server-streaming; they run alongside `Control`, despite being separate calls.

| RPC | Caller and when | Receiver action | Repeat safe? | Timing |
|---|---|---|---|---|
| `Control` | Worker on connection | Register and exchange control messages | Reconnection reconciles journal; a live duplicate is refused | [`SCHEDR_HIDDEN__WORKER__RECONNECT_BACKOFF_FIRST`](14-timings.md#fixed-timings), `SCHEDR_HIDDEN__WORKER__RECONNECT_BACKOFF_MAX` |
| `UploadParseResult` | Worker after parse | Accept header and file parts, acknowledge job | Stale job results are ignored | [`[master] parse_job_timeout`](14-timings.md#public-timing-keys) |
| `FetchInputs` | Worker when `AssignTask` says inputs are external | Stream inputs for attempt | Read-only; repeat while assignment is available | [assignment expiry](14-timings.md#fixed-timings) |
