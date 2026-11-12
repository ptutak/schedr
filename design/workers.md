# workers

Worker lifecycle, memory protection, environment, drain.

Hand-written parts and generated blocks: `make records` writes the blocks between the marker lines from the decisions whose `topic` (or `also`) is this one; edit the decisions for those, the rest by hand.

## Invariants of master time

Rules a change has to keep. The reader's account, with the defaults drawn out, is
[The master clock](/03-guide/17-master-clock.md) (`docs/03-guide/17-master-clock.md`). Master time is `clock::monotonic`: the
wall clock at the process's first reading, plus `Instant` since, on `clock::timeline`.
`service::stamp_of` puts that timeline, master time rounded down to a millisecond (`master_ms`),
the latest heartbeat heard, the wall clock (for people) and the lease epoch on every master
message.

### The worker's bound is earlier than master time

A deadline is reached only once the worker's bound says so. That stops a late message, a drifting
clock or a frozen VM from leaving start budget, or a `cleanup_anywhere` task, after the master has
moved on. With no sample, `past` is already true, so nothing starts against a master that never
stamps a heartbeat this session sent. The cutoff treats a missing sample differently, below.

Master time `X` ms may be reached at `sent + d`, where `sent` is when this session sent heartbeat
`heard_sequence` and `d` is `(X − master_ms − 1)` ms with `d / 1000` taken off. The `− 1` ms is the
stamp rounded down (`Timestamp::as_millisecond`): the true instant is up to 1 ms later. The
thousandth is `DRIFT_PER_MILLE` (1): each clock slews by at most 500 ppm, so the two by at most
1000 ppm, 0.1 %. `past` is true at that instant, and with no sample, a negative span, or a pause.
A pause is this machine's wall clock ahead of its boot clock (`CLOCK_BOOTTIME`) by more than 1 s
plus 0.2 % of the boot time since the sample (`PAUSE_SLACK`, `PAUSE_PER_MILLE`). The bound uses the
tightest sample whose send is at most the window before the newest sample's send
(`DEFAULT_SAMPLE_WINDOW`, 10 min, `SCHEDR_HIDDEN__WORKER__MASTER_SAMPLE_WINDOW`,
`MasterTime::with_window`); a sample at least as tight replaces
the looser ones before it, so the 0.1 % is counted from a recent send. Another timeline drops the
samples. The next sample after a pause replaces them.

Kept by `master_time::MasterTime::heard` and `MasterTime::past` (the bound itself is `reached_at`,
private, in that file); the stamp by `service::stamp_of` and `clock::monotonic`. Decisions 442 and
447. Breaks `master_time::tests::a_deadline_counts_from_the_send_of_what_the_master_had_heard`,
`an_equally_tight_later_sample_replaces_an_hours_old_one`,
`a_pause_the_boot_clock_didnt_count_makes_every_deadline_passed`,
`window_tests::the_tightest_sample_is_taken_over_the_window`, and
`clock_sim::a_worker_keeps_master_time_rules_whatever_the_delays_drift_and_stalls` (the file says
removing the drift allowance fails it at once). A stall shorter than the pause slack is unseen and
leaves the bound that much late; the simulator allows exactly that (`run`'s `unseen`). No test
measures a stall just under the slack.

### No start after the master has taken the attempt back

The worker starts only while its bound of `start_by` is strictly more than two heartbeat intervals
ahead. The master takes an unstarted assignment back only once its own master time is strictly past
the moment it stored the attempt, plus the worker lease, plus two heartbeat intervals. Those four
intervals are the room for a `Started` sent at the last moment. Inside them, a late delivery or a
slow journal (B225) and a `Started` held on the way (B227) cannot put the task on a second worker.

`start_by` is `as_millisecond(stored + lease)` from the same reading `expire` keeps, so the sent
deadline is at most 1 ms before `stored + lease`. The worker starts only while
`bound(start_by) − 2·heartbeat` is strictly ahead (`slack` subtracts the reserve after the bound,
so the drift applies to the whole span). The take-back is after `stored + lease + 2·heartbeat`,
which is after `start_by + 2·heartbeat`. Drift, the stamp's millisecond and an old sample only move
the worker's side earlier. A `Started` or `Finished` already noted is left alone, so a message waiting
for the lock is not taken back and given out again.

The worker's side is `agent::journal_start` (`heartbeat * 2`, then `MasterTime::slack`; `TOO_LATE`
drops the journal entry). The master's is `dispatch::unstarted`, `dispatch::expire`
(`duration_since(stored) > start_timeout + start_grace`) and `core::MasterCore::new`
(`with_start_grace(heartbeat_interval * 2)`). The arrival is `service::read_stream` calling
`Arrivals::start_arrived` before the message is queued. The worker copies the interval from
`Registered.timing`, the same key the grace uses. A lease under four heartbeats cannot cover a
sample up to one interval old, one interval to start, and the two kept: `loader::check_lease`
refuses `lease_timeout < 4 * heartbeat_interval`. Decisions 442, 450 and 281 (the receipt lease:
`journal_start` also refuses once `gone_silent`). While the worker may still come back,
`dispatch::may_come_back` keeps the attempt out of `expire` (F14): taking it back then would race
the journal.

Breaks `agent::tests::an_assignment_past_its_start_budget_isnt_started_and_is_rejected_too_late`,
`an_assignment_starts_by_its_deadline_in_master_time`,
`an_assignment_journaled_past_the_lease_isnt_started`,
`dispatch::tests::an_unstarted_assignment_is_taken_back_only_its_grace_after_start_by`,
`loader::tests::a_lease_under_four_heartbeats_is_refused`,
`core::tests::a_restarted_master_takes_back_no_assignment_within_its_workers_lease`. The simulator
checks the worker's half (a start it would allow is at least the reserve before `start_by`, plus
the unseen-stall allowance) and, in `clock_sim/master.rs`, the master's: the real core and two
workers on one clock, where no worker starts an attempt the master ended, and removing the grace
(`Weakened::StartGrace`) makes it fail.

### The worker's cutoff is the earliest of the receipt, the stamp and what the master heard

A `cleanup_anywhere` task stops at the earliest of the worker lease after the last message received,
the bound of the last stamp's `master_ms` plus the lease, and the lease (less 0.1 %) after this
session sent the latest heartbeat a stamp's `heard_sequence` named. The cleanup gate waits on that same
instant, and a reconnect keeps it until the new session has a sample. A message held on the way
cannot push the stop later than the receipt bound (decision 252); the stamp pulls it earlier
(decision 445); the heard heartbeat's send pulls it to before the master's loss (decision 453).

`cutoff = min(heard + lease, bound(last.master_ms + lease), heard_sent + lease - lease/1000,
carried)`, with `bound` the first rule's; the third is zero after a pause. With no sample and nothing carried, `cutoff_in` is `None`, not zero, and only
`heard + lease` remains. A `Registered` that names heartbeat 0 is a sample. The master loses the
worker when `master_time > last_worker_message + lease`: the later of the last arrival
(`service::read_stream`, `Arrivals::arrived`, before the lock) and the last handled message
(`core::heard_from`). `core::tick_into` compares them. It hands the cleanup on when
`master_time >= that loss + cancel_grace + cleanup_move_wait` (`core::MasterCore::new` adds the
two into `with_cleanup_move_wait`; `dispatch::move_cleanup`, `assign_cleanups`). After a restart
the wait is a whole one from when this master first sees it. Where the last stamp is the answer to
that last worker message, `bound(stamp + lease)` is before the loss and the move is a grace and a
move-wait later, so the worker has stopped. A later master send is stamped at send
(`service::deliver`) and moves the worker's `heard` and `last.master_ms` without moving
`last_worker_message`; it carries the same `heard_sequence`, so `heard_sent` stays, and the master
received that heartbeat no earlier than its send: `heard_sent + lease` (on a clock at most 0.1 %
fast) is at or before the loss, and the worker has stopped a grace and a move wait before the
move, whatever the master sends. `stop_cut_off` also stops every trigger. The timer is armed only while `gates` is
non-empty, so a worker with no `cleanup_anywhere` task does not run this cutoff.

`agent::lease_deadline` takes the min; `agent::master_cutoff` and `master_time::cutoff_in` are the
master-time side (`MasterTime::heard` sets `heard_sent`); `agent::open_session` stores it in `carried_cutoff` before `MasterTime::open`
forgets the samples; `agent::heard_master` drops the carried instant once `cutoff_in` is `Some`.
`agent::publish_deadline` is what `attempt::CleanupGate::passes` reads, and `agent::on_alive`
recomputes it. Decisions 445 and 252. Breaks
`master_time::window_tests::the_cutoff_counts_from_the_last_stamp_on_master_time`,
`the_cutoff_counts_from_the_send_of_what_the_master_last_heard`,
`agent::tests::a_reconnect_keeps_the_cutoff_the_last_stamps_set`,
`the_cleanup_gates_read_the_master_time_cutoff`,
`a_master_gone_silent_stops_a_task_whose_cleanup_may_move`, and the simulator's check that a worker
still inside `cutoff_in` is within a lease of when the master last heard it (heartbeats lost one
way included). The move wait breaks
`dispatch::tests::a_lost_workers_cleanup_waits_for_a_task_it_cant_see_to_stop`, and
`clock_sim/master.rs` runs the worker's cutoff and the master's move on one clock
(`Weakened::MoveWait` makes it fail).

### A replaced incarnation's attempts stay held for a lease

A new incarnation registers at once. The attempts the old process had started, and that the new
journal does not report, end `lost` only once a worker lease of master time has passed since the
mark. That stops a retry, or a moved cleanup, from overlapping the old process while its own
cutoff is still ahead. The old stream is answered no more, so its heartbeats do not push the mark
out.

The mark is at registration: `dispatch::incarnation_replaced` keeps `started && !moving &&` not
reported, `attempts::mark_replaced` stores `replaced_at`, and `replaced_since` records master time.
`dispatch::end_replaced` ends one when `master_time >= mark + lease` (`duration_since < lease`
waits). After a restart the row is still marked and the lease is counted from when this master
first sees it (`or_insert`), so the hold is never shorter than a lease here. `HeartbeatAcks::answer`
sends nothing once `is_current` is false, so the old stream's last stamp is at or before the mark.
A journal that reports the attempt clears the mark (`dispatch::reported`), so a continue is not
ended when the hold runs out. While the old process still has a sample, it cuts off by
`bound(last_stamp + lease)`, at or before `mark + lease`, which is when `end_replaced` may end the
attempt; the suspend and unseen-stall limits below can make that stop late. Decision 443. Breaks
`dispatch::tests::a_replaced_incarnation_loses_only_its_started_unreported_attempts` (still open at
one second under the lease, ended on it),
`core::tests::a_replaced_incarnations_hold_survives_a_master_restart`,
`service::tests::a_replaced_stream_gets_no_more_heartbeat_answers`, and `clock_sim/master.rs`:
an incarnation replaced while the old process runs on, no two processes of a task instance at
once; without the hold (`Weakened::Hold`) it fails, as without the move wait
(`Weakened::MoveWait`).

### A new leader waits out the deposed one, and a worker fences an older epoch

A worker the new leader has not heard from is lost only after a lead plus the worker lease, the
lead being as long as a deposed master may still answer. A worker that has seen a lease epoch
refuses a lower one until a worker lease passes with no word from the higher one. Together they
stop the deposed master, still answering, from keeping a worker alive past the moment the new
leader retries its work, and they stop a restored database (its epoch goes backwards) from being
followed while the deposed master is still sending. Two masters writing at once is the database
lease, not this clock: `lease::Leadership::fenced_db` refuses a write once the lease moved on.

The deposed master leads while its monotonic clock is strictly before
`renewal_start + (lease_period − query_timeout)` (`lease::lead_time`, `Leadership::leads`). It
acks only while `leads` is true (`HeartbeatAcks::answer`). The new leader sets a restored worker's
`heard` to `now + takeover_wait` with that same lead (`roles::master_settings`,
`core::MasterCore::restore`) and loses it when `master_time > heard + lease` (`tick_into`): at
`restore + lead + lease` it is still held, and the next instant it is lost. Every stamp carries
`lease_epoch` (`service::with_epoch`). `agent::older_leader` refuses while
`epoch < highest` and the silence from the highest is strictly under the lease (or the lease is not
known yet); at a lease of silence the lower epoch is followed. That holds only while the lead is
shorter than the worker lease, so the higher epoch has stopped sending before the fence drops:
`loader::check_lease` refuses `lease_period − query_timeout >= lease_timeout`. Decision 444.
Breaks `core::tests::a_restarted_master_restores_workers_and_counts_their_lease_from_its_start`
(restored at 100 s, still held at 180 s, lost at 181 s: 20 s lead + 60 s lease),
`agent::tests::a_lower_epoch_is_followed_once_the_higher_has_been_silent_for_a_lease`,
`loader::tests::a_master_lead_not_shorter_than_the_worker_lease_is_refused`. No test runs a deposed
master's lead clock against the new leader's `takeover_wait`.

### A wall clock never decides whether work is still alive

Assignment expiry, worker loss, a parse job (`parse_jobs::expire`), a connection job (the deadline
`tick_into` stores) and a moved cleanup's wait (`move_not_before`) count on master time. A step of
the wall clock therefore takes nothing back and loses no worker
(`core::tests::a_wall_clock_jump_takes_nothing_back_and_loses_no_one`). Decision 442.

The handed wall clock is still read for four decisions, each one this master makes about a time it
stored or a person set. A jump can fire one early or hold it; it cannot agree with a worker that a
process is alive.

| Still the wall clock | Where | Why it is safe |
|---|---|---|
| Schedules | `scheduler::Scheduler::pass` | Fire times are the times people set, stored with the run. |
| Set-aside | `dispatch::assign_into` (`set_aside` against `now`) | How long this master waits before retrying an assignment that failed on its own. |
| A sensor's `max_wait` | `dispatch::deadline`, from the stored start | How long the wait may last, on the time stored with it. |
| The script-root grace | `core::script_uses_at` | How long this master has gone without a report, from its own start and the last report. |

The stamp's `wall_ms` is compared with the worker's wall clock over the round trip
(`master_time::wall_skew`, warned once a session in `agent::heard_master`, decision 449). That
warning is for people. The worker's wall clock is also read against its boot clock to see a pause,
inside the bound above. No test pins these four staying on the wall clock.

### Known limits, kept on purpose

Each is one line; the reader's page has the diagrams.

- A `Started` held longer than four heartbeat intervals (the worker's two and the master's two), the stream still up, can arrive after the take-back; the master answers `CancelAttempt`, and both workers may have run it (the guide's Known limits; decisions 442 and 450). The journal entry for 442 still says two intervals, from before 450.
- A jump of the master's monotonic clock, some moves of a virtual machine, is not caught. Wall-clock steps are (the guide's Known limits). No test.
- The worker's timers stop in a suspend. `on_alive` re-reads the cutoff from the boot clock every `alive` (5 s, `ALIVE_INTERVAL`), so a `cleanup_anywhere` task can stop up to 5 s late (the guide's Known limits). No test drives a suspend.
- A stall under 1 s plus 0.2 % of the boot time since the sample is unseen, and the bound stays late by that much (the guide's Known limits; `clock_sim` allows it and does not fail on it).
- The cutoff the last session computed stands over a reconnect until the new session's first sample (the guide's Known limits; `a_reconnect_keeps_the_cutoff_the_last_stamps_set`).
- At the end of a replaced incarnation's lease the old process has stopped only its `cleanup_anywhere` tasks and, when the cutoff fired, its triggers. An ordinary task can still overlap the retry, as with a lost worker (the guide's Known limits; decision 443; `agent::tests::a_master_gone_silent_ends_the_stream_and_leaves_other_tasks_running`).
- An assignment that leaves `start_by_ms` unset has no master-time start limit (`journal_start` checks only `Some`); a 0.1 worker, which ignored it, is refused by protocol 1.0 (the guide's Known limits; B187, decision 456). No test of an unset `start_by_ms` on purpose.


## Current

<!-- generated by scripts/records.py: current -->
### Memory and environment

- **Limited tasks start through schedr task-exec** (D50): An internal `schedr task-exec --data-limit
  N -- <program>` sets the data limit (`RLIMIT_DATA`, D438) on its own process and executes the
  task's program in its place (built in M4.15a). A capped Python task is spawned through it, never
  forked: a fork-server child setting its own limit was not built (Claude's choice in M4.15a: one
  path, the fork's faster start given up for capped tasks only).
- **Workers and the DAG code running on them are trusted** (D101): Tasks run as the agent's user and
  can act as their worker; sender checks guard against confused workers. Teams that must be kept
  apart use separate schedr deployments.
- **Worker pass_env reaches parsing and the fork server** (D333): Parsing and the fork server
  receive all of [worker] pass_env, as tasks do. DAG code can read these variables at parse time.
  pass_env must not contain secrets; parse tracebacks are not masked.
- **A task's own memory budget is `memory=`; the hard cap `hard_memory_limit=`** (D436):
  `@task(memory="2GiB")` (and on operators: `CommandOperator(..., memory=...)`) replaces the
  worker's default budget for that task; the watchdog kills it above. `hard_memory_limit=True` also
  sets the hard cap at that size through `schedr task-exec` (D50). Not chosen: the engine's names
  `memory_budget=`/`setrlimit=`.
- **A task's budget is placed within a worker's pod limit** (D437): A worker takes a task whose
  budget fits its pod limit minus the agent's reserve, whatever its default share; none fits: the
  task waits queued ("no worker has memory for 8 GiB"), as archived (rejection 7). Slots aren't
  counted in memory. Not chosen: memory tracked as a resource.
- **The hard memory cap is `RLIMIT_DATA`** (D438): `hard_memory_limit=True` sets `RLIMIT_DATA` (heap
  and anonymous mappings) to the task's budget. Not chosen: the whole address space limit (breaks
  programs reserving big ranges up front: Go, the JVM).

### Identity and connection

- **Database trouble ends the worker stream; data faults are handled separately** (D108): On
  database trouble, undo the failed message’s effects and end the worker stream; the worker
  registers again with its roots and journal. Under D115, data faults keep the stream open: a
  refused Finished becomes an acknowledged result_not_stored failure, other refused messages are
  logged and dropped, and registration adopts open attempts whose start record was refused.
- **Only database trouble ends a worker stream** (D115): Transient database errors end the stream. A
  refused Finished becomes an acknowledged, non-retried result_not_stored failure, retaining
  duration and usage; other data-fault messages are logged and dropped. During registration, adopt
  an open journal attempt whose start record is refused, reserve its slot, and let the worker send
  Finished.
- **Offline workers have no problem on the Deployments graph** (D130): An offline worker is grey and
  dashed; stale warnings and import errors do not colour it. A lost worker remains an error and a
  disconnected worker a warning.
- **A disconnected or frozen worker can cause a task to run twice** (D280): A disconnected worker
  stops only `cleanup_anywhere` tasks at its lease; other tasks may overlap retries elsewhere until
  reconnect. A frozen worker may permit any task to overlap, including `cleanup_anywhere` tasks
  outlasting the lease and cleanup move wait. Chaos scenarios record these permitted overlaps as
  `tries_run_twice` and fail on all others. Tasks should be idempotent.
- **An assignment cannot start after its worker lease expires** (D281): The agent closes a stream
  silent for the worker lease and ignores messages arriving after it. The master takes back
  unstarted assignments only after the lease; input fetches and first registration obey the same
  limit.
- **One monotonic clock for the cluster: master time** (D442): Every liveness deadline is a master
  time: the leading master's monotonic clock, stamped with its timeline on every master message
  along with the latest heartbeat heard. `AssignTask` carries `start_by_ms`; the worker reads master
  time from when it sent that heartbeat, keeps two heartbeat intervals for `Started` and rejects
  `TOO_LATE` past it. The master's expiries count on its own clock; a lease under four heartbeats is
  refused. Not chosen: an acknowledged `Started` before launch; a hold after a wall-clock jump.
- **A replaced incarnation's lost attempts wait out its lease** (D443): When a worker registers as a
  new incarnation, the started attempts its journal doesn't report end `lost` only once the old
  incarnation's lease is over in master time, counted from the registration (the old stream gets no
  more heartbeat answers). The hold is stored with each attempt, so a master restart keeps it,
  counting afresh. Ordinary tasks of an old process still alive keep the at-least-once guarantee of
  a lost worker. Not chosen: stopping every task at the lease; refusing the new incarnation until
  the old one is lost.
- **A new leader waits out the deposed one; workers fence older epochs** (D444): After a takeover, a
  worker the new leader hasn't heard from counts as lost only a lead time (`lease_period -
  query_timeout`) plus the worker lease after it began leading. Every master message carries the
  leader's lease epoch; a worker that has seen an epoch refuses messages and registrations from a
  lower one, and reconnects, until a worker lease passes without word from that epoch (a database
  restored sets it back); a configuration whose lead isn't shorter than the worker lease is refused
  at load. Not chosen: the master's lead clock counting suspend (`CLOCK_BOOTTIME`); fencing by
  timeline only; restarting workers after a restore.
- **The worker's cutoff on master time as well** (D445): After a worker lease without word from the
  master, the worker stops its tasks with `cleanup_anywhere` at the earlier of two moments: a lease
  after the last message it received (decision 252), and when master time may have passed the last
  stamp's master time plus the lease, by the same bound as a start deadline. Never later than
  before. Not chosen: master time only; leaving the cutoff on receipt.
- **A simulator checks the liveness rules on virtual clocks** (D446): A deterministic simulator in
  `crates/schedr-agent/tests/clock_sim.rs` runs the master's core and dispatcher and the worker's
  master-time bound and cutoff on virtual clocks, proptest choosing delays, pauses, drift, restarts,
  takeovers and incarnations. It checks four rules: no attempt starts after the master expired it;
  no attempt stays open for ever; no retry within a replaced incarnation's hold; a moved cleanup
  never runs beside the worker it left. Not chosen: the full stack under tokio's paused time; fewer
  rules.
- **The worker's best clock sample is taken over a window** (D447): The worker's best master-time
  sample is the tightest of the last 10 minutes (`SCHEDR_HIDDEN__WORKER__MASTER_SAMPLE_WINDOW`), so
  the drift allowance comes from a recent sample by construction. Not chosen: a fixed constant.
- **A clock-skew warning from the master's wall clock** (D449): A worker warns once per session when
  the master's wall clock, as stamped, differs from its own by more than 2 s beyond the round trip's
  bound (`SCHEDR_HIDDEN__WORKER__CLOCK_SKEW_WARNING`): for people, never a deadline (B232). Not
  chosen: leaving B232 for later.
- **The master expires an assignment two heartbeats after its deadline** (D450): The master takes
  back an unstarted assignment two heartbeat intervals after its `start_by` (one whose task has
  `cleanup_anywhere` later still, decision 455), while the worker still refuses to start it two
  intervals before: four intervals between the last start and the take-back, for a `Started` held on
  its way. A reassignment comes 20 s later by default. Not chosen: one interval; none.
- **A worker's cutoff counts from the send of the last heartbeat the master heard** (D453): The
  worker's cutoff is also at most a lease, less 0.1 %, after it sent the latest heartbeat a master
  stamp names as heard (`heard_sequence`): the master received it no earlier and counts the worker
  lost from it, so a worker whose messages are lost one way stops before its cleanup moves, whatever
  the master still sends (B233). Not chosen: the master counting its lease from its own last send
  too (a silent worker kept alive while the master writes to it).
- **A lost worker's unstarted attempt whose cleanup may move waits as its cleanup would** (D455):
  When a worker is lost, an attempt of a task with `cleanup_anywhere` that it was never heard
  starting is taken back (`assignment_expired`, no retry used) only once the wait a started one's
  cleanup gets before it moves is over (the cancel grace and the cleanup move wait, 40 s by
  default): the worker may have started it as it was cut off, its `Started` held up, and be stopping
  it (B234). A replaced incarnation's are held the same way from the new one's registration: a lease
  (the old process's cutoff), then that wait; and a restarted master, which doesn't keep the wait,
  takes one back no sooner than the start timeout plus that wait after it first sees it. Other
  tasks' are taken back at the loss, as before: a worker cut off keeps running them (decision 252).
  Not chosen: waiting for every task; only the deadline plus the grace of decision 450 (short of a
  task's stop when the cancel grace is longer than two heartbeats).

### Drain

- **Retry a resumed worker's failed follow-up each round** (D109): A resume command is applied once;
  a failed search for its parse jobs is retried each round. Retries stop when the search succeeds or
  the worker is drained again.

### Messages and transactions

- **B184 is fixed in M4B: set-based writes, a run settled once per turn, the journal in groups**
  (D396): With a turn's messages in groups (D397), M4B writes a turn's `Finished`s with set-based
  statements, reads and settles each run once per turn, and writes the agent's journal in groups.
  B184 counts as fixed when stress 23 records its 500 ends within 5 s on Postgres; a unit test
  counts a turn's statements.
- **A turn's messages are stored in groups by type; savepoints later** (D397): A worker's turn (up
  to 40 messages, `SCHEDR_HIDDEN__MASTER__MESSAGES_PER_TURN`) is reordered, `Started`s then
  `Finished`s, and each group is stored as one batch in one transaction; a failed group is redone
  one by one, at most 10 in a turn. The turn in one transaction with savepoints per group comes
  later, only if needed.

### From other topics

- A cancelled process group keeps its remaining grace period (D100, `task-processes`; built M4.3e)
- Show node problems on the Deployments graph (D129, `observability`; built M4.4b)
- Teams requiring isolation run separate deployments (D173, `secrets`; rule)
- Connection checks and tests run on a selected worker (D193, `connections`; built M4.7c)
- A connection test runs without a task slot (D199, `connections`; built M4.7c)
- Connection tests default to a 60-second timeout (D200, `connections`; built M4.7c)
- Command connection files default to memory-backed storage (D201, `connections`; built M4.7b2)
- Cancellation does not stop a running per-task cleanup (D219, `cleanup`; built M4.8b)
- A lost worker fails the run when its declared cleanup cannot move (D220, `cleanup`; built M4.8b2)
- Cleanup anywhere waits when no worker can take it (D221, `cleanup`; built M4.8b2)
- Task and cleanup results are reported separately (D229, `cleanup`; built M4.8b2)
- Per-task cleanup defaults to the worker cleanup timeout (D235, `configuration`; built M4.8f)
- Inline server timings use the hidden configuration (D240, `configuration`; built M4.8u)
- `cleanup add` returns after journaling and sending the handle (D242, `cleanup`; built M4.8b)
- A cleanup moved to another worker keeps the same attempt (D243, `cleanup`; built M4.8b2)
- Each cleanup move counts as a try (D247, `cleanup`; built M4.8b2)
- A journaled cleanup waits for the master’s `Continue` (D248, `cleanup`; built M4.8b)
- A cleanup can outlast the pod grace period (D249, `cleanup`; built M4.8b)
- Warn about cleanup timeout at assignment once per task (D250, `cleanup`; built M4.8b)
- A disconnected worker stops tasks with `cleanup_anywhere` (D252, `cleanup`; built M4.8b3)
- A disconnected task’s cleanup waits for the master (D253, `cleanup`; built M4.8b3)
- A task ending while disconnected holds its movable cleanup (D254, `cleanup`; built M4.8b3)
- A cleanup already running continues after disconnection (D255, `cleanup`; built M4.8b3)
- A moved cleanup waits after the worker lease (D256, `cleanup`; built M4.8b3)
- A task stopped on disconnection is `lost` (D257, `cleanup`; built M4.8b3)
- The master acknowledges each worker heartbeat (D258, `protocol`; built M4.8b3)
- A moving cleanup's result from the worker it moved from counts (D282, `cleanup`; built M4.8z)
- A master that is not leading is unready and refuses streams (D297, `components`; built M4.8z)
- Workers withdraw DAG roots when required script roots are missing (D303, `scripts`; built M4.9b)
- Worker reconnect waits are capped at half the master's lease (D307, `cleanup`; built (B168 fixed))
- The returning worker cleans handles the master never received (D308, `cleanup`; built (B169 fixed))
- A watcher host serves one interpreter and code set (D320, `wait-steps`; built M4.11a2)
- Watcher hosts stop when their last watcher ends (D334, `wait-steps`; built M4.11a2)
- The UI shows watcher hosts and switched-off sensors (D343, `observability`; built M4.11a4)
- Running cleanup continues past the drain deadline (D346, `cleanup`; built M4.11a4)
<!-- end of current -->

## Replaced

<!-- generated by scripts/records.py: decisions -->
- D395 (superseded by D397): M4B without a measuring step; 20 messages a turn
<!-- end of decisions -->

## Where in code

<!-- generated by scripts/records.py: code -->
- `crates/schedr-agent/src/agent.rs` (D193, D243, D248, D254, D255)
- `crates/schedr-agent/src/agent.rs::NO_WORD` (D252)
- `crates/schedr-agent/src/agent.rs::connection_jobs` (D199)
- `crates/schedr-agent/src/agent.rs::cut_off` (D252)
- `crates/schedr-agent/src/agent.rs::cut_off_at` (D445)
- `crates/schedr-agent/src/agent.rs::cut_off_if_late` (D281)
- `crates/schedr-agent/src/agent.rs::drain_running` (D346)
- `crates/schedr-agent/src/agent.rs::heard_master` (D449)
- `crates/schedr-agent/src/agent.rs::kept_cleanup` (D229)
- `crates/schedr-agent/src/agent.rs::older_leader` (D444)
- `crates/schedr-agent/src/agent.rs::on_done_group` (D396)
- `crates/schedr-agent/src/agent.rs::reconnect_cap` (D307)
- `crates/schedr-agent/src/agent.rs::report_changes` (D303)
- `crates/schedr-agent/src/agent.rs::send_handles` (D242)
- `crates/schedr-agent/src/agent.rs::silent_at` (D281)
- `crates/schedr-agent/src/agent.rs::warned_room` (D250)
- `crates/schedr-agent/src/attempt.rs::CleanupGate` (D253)
- `crates/schedr-agent/src/connection_env.rs::default_dir` (D201)
- `crates/schedr-agent/src/connection_env.rs::prepare` (D201)
- `crates/schedr-agent/src/connection_jobs.rs` (D193, D199, D200)
- `crates/schedr-agent/src/env.rs::task_environment` (D333)
- `crates/schedr-agent/src/fork.rs::pass_env` (D333)
- `crates/schedr-agent/src/jobs.rs` (D297)
- `crates/schedr-agent/src/journal.rs::write_many` (D396)
- `crates/schedr-agent/src/master_time.rs::MasterTime` (D442)
- `crates/schedr-agent/src/master_time.rs::cutoff_in` (D445, D453)
- `crates/schedr-agent/src/master_time.rs::heard` (D447, D453)
- `crates/schedr-agent/src/master_time.rs::wall_skew` (D449)
- `crates/schedr-agent/src/master_time.rs::with_window` (D447)
- `crates/schedr-agent/src/status.rs` (D252)
- `crates/schedr-agent/src/supervise.rs::left_after_leader` (D100)
- `crates/schedr-agent/src/watchers.rs::WatcherHosts` (D320, D334)
- `crates/schedr-agent/tests/clock_sim.rs::run` (D446)
- `crates/schedr-agent/tests/clock_sim/master.rs::simulate` (D446)
- `crates/schedr-api/src/routes.rs` (D129)
- `crates/schedr-api/src/routes.rs::test_connection` (D200)
- `crates/schedr-api/src/routes.rs::worker_problems` (D130)
- `crates/schedr-api/src/views.rs::ProblemView` (D129)
- `crates/schedr-api/src/views.rs::SensorView` (D343)
- `crates/schedr-api/src/views.rs::WatcherHostView` (D343)
- `crates/schedr-config/src/config.rs` (D303)
- `crates/schedr-config/src/config.rs::cleanup_move_wait` (D256)
- `crates/schedr-config/src/config.rs::connection_dir` (D201)
- `crates/schedr-config/src/config.rs::connection_job_cancel_grace` (D240)
- `crates/schedr-config/src/config.rs::connection_test_timeout` (D200)
- `crates/schedr-config/src/config.rs::stop_poll` (D240)
- `crates/schedr-config/src/config.rs::vault_renew_check` (D240)
- `crates/schedr-config/src/loader.rs::check_lease` (D442)
- `crates/schedr-db/migrations/postgres/0001_initial.sql` (D247)
- `crates/schedr-db/migrations/sqlite/0001_initial.sql::cleanup_moves` (D247)
- `crates/schedr-db/src/attempts.rs` (D229, D282)
- `crates/schedr-db/src/attempts.rs::cleanup_moves` (D247)
- `crates/schedr-db/src/attempts.rs::end_attempts` (D396)
- `crates/schedr-db/src/attempts.rs::mark_replaced` (D443)
- `crates/schedr-db/src/attempts.rs::move_cleanup` (D243)
- `crates/schedr-db/src/database.rs::is_data_fault` (D108, D115)
- `crates/schedr-db/src/instances.rs::store_outputs` (D396)
- `crates/schedr-db/src/nodes.rs::NodeProblem` (D129)
- `crates/schedr-master/src/clock.rs::monotonic` (D442)
- `crates/schedr-master/src/commands.rs::check_connections` (D193)
- `crates/schedr-master/src/core.rs` (D229, D282)
- `crates/schedr-master/src/core.rs::CoreError` (D108)
- `crates/schedr-master/src/core.rs::DEFAULT_SCRIPT_ROOT_GRACE` (D303)
- `crates/schedr-master/src/core.rs::handle_group` (D397)
- `crates/schedr-master/src/core.rs::take_back` (D308)
- `crates/schedr-master/src/core.rs::takeover_wait` (D444)
- `crates/schedr-master/src/dispatch.rs` (D242, D243, D255, D282)
- `crates/schedr-master/src/dispatch.rs::NO_CLEANUP_WORKER` (D221)
- `crates/schedr-master/src/dispatch.rs::RESULT_NOT_STORED` (D115)
- `crates/schedr-master/src/dispatch.rs::Weakened` (D446)
- `crates/schedr-master/src/dispatch.rs::cleanup_move_wait` (D256)
- `crates/schedr-master/src/dispatch.rs::connection_worker` (D193)
- `crates/schedr-master/src/dispatch.rs::end_lost` (D220)
- `crates/schedr-master/src/dispatch.rs::end_replaced` (D443)
- `crates/schedr-master/src/dispatch.rs::expire` (D450, D455)
- `crates/schedr-master/src/dispatch.rs::finished_group` (D396)
- `crates/schedr-master/src/dispatch.rs::forget_worker` (D455)
- `crates/schedr-master/src/dispatch.rs::incarnation_replaced` (D443, D455)
- `crates/schedr-master/src/dispatch.rs::resume` (D109)
- `crates/schedr-master/src/dispatch.rs::with_start_grace` (D450)
- `crates/schedr-master/src/dispatch/tests.rs` (D220, D221)
- `crates/schedr-master/src/execution.rs::attempts_finished` (D396)
- `crates/schedr-master/src/metrics.rs::stage` (D397)
- `crates/schedr-master/src/service.rs::HeartbeatAcks` (D258)
- `crates/schedr-master/src/service.rs::NOT_LEADING` (D297)
- `crates/schedr-master/src/service.rs::is_current` (D443)
- `crates/schedr-master/src/service.rs::stamp_of` (D442)
- `crates/schedr-master/src/service.rs::turn` (D397)
- `crates/schedr-master/src/service.rs::with_epoch` (D444)
- `crates/schedr-model/src/commands.rs::MAX_CONNECTION_TEST_MS` (D200)
- `crates/schedr-model/src/runner.rs::WatchersRequest` (D320)
- `crates/schedr-proto/proto/schedr/protocol/v1/master.proto` (D242)
- `crates/schedr/src/cli.rs` (D199)
- `crates/schedr/src/ops.rs::readyz` (D297)
- `crates/schedr/src/roles.rs` (D201)
- `python/schedr/runner/watchers.py` (D320)
<!-- end of code -->

## Open

<!-- generated by scripts/records.py: questions -->
None.
<!-- end of questions -->

<!-- generated by scripts/records.py: details -->
Details and edge cases, with the reasons; read them before changing this:

- JOURNAL.md: D395, D396, D397, D436, D442, D443, D444, D445, D453, D455
- archive/PROJECT.md: Cleanup and resources, DAG code delivery, Master–worker protocol, Web UI and CLI, Worker lifecycle, Worker memory protection and crash recovery
<!-- end of details -->
