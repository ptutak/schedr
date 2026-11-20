# The master's round

A round is the leading master's serial pass over everything that moves runs forward: the
workers' results, commands, the scheduler, preparing runs, expiry and assignment. Only a round
changes an instance's or a run's state (decision 505), so its length bounds how fast tasks
follow each other. This page lists what a round does, part by part and statement by statement,
for profiling. Keep it up to date with the code: a change to a part's reads or writes changes
its section here.

The code is in `crates/schedr-master/src/service.rs` (the loop, the lock, delivery),
`core.rs` and `core/round.rs` (the parts), `dispatch.rs` (prepare, expire, assign, results),
`execution.rs` (what a result changes in its run), `scheduler.rs`, and `crates/schedr-db`
(the statements). Names below are functions, so they can be searched for.

## Words used here

| Word | Meaning |
|---|---|
| The lock | The master's core lock (`core_for`). A round takes it once per part; a worker's turn, a registration or the trigger loop gets in between parts. |
| Own transaction | A write outside any transaction (`in_tx!`): its own start, the statement, `COMMIT`. |
| One transaction | `db.atomically`: everything inside joins it, a nested `atomically` too. |
| Start of a transaction | On Postgres one message: `BEGIN`, `statement_timeout` and `idle_in_transaction_session_timeout`, and for a master the fence (`set_config` of holder and epoch, then a `DO $fence$` block reading `master_lease FOR KEY SHARE`). `pg_stat_statements` counts these as separate statements, but they are one round trip. On SQLite `BEGIN IMMEDIATE` and the fence read. |
| Round trip | One message to the database and its answer: a transaction costs two (start and `COMMIT`) plus one per statement. |
| The probe | One statement at a round's first check: `SELECT EXISTS (...), ...`, 16 columns, one per read a round may skip (`schedr-db/src/probe.rs`, `Check`). A read whose column said nothing returns nothing without a statement. A probe is taken again when a read asks for a later time than it covered; a read skipped after a write marks the round stale, so another follows. Only reads inside a round are skipped. |
| Delivery | Messages a part built (`Outgoing`) put on the workers' streams after the lock is let go (`deliver`): memory only, no database. |

## When a round runs

`run_with` (service.rs) waits for the first of:

- **A tick:** every `tick_interval` (1 s by default).
- **A result or start queued** (`finished`): a worker's `Started` or `Finished` queued for
  the round wakes it, at most one round every `task_round_gap` (10 ms) after the last ended.
- **A command announced** (Postgres `LISTEN`): the commands part becomes due.
- **Another round asked for:** a round that skipped a read on a stale probe, or that put
  results back after a failure.

Not leading, or paused, it runs no round. Each round is timed whole
(`schedr_master_tick_duration_seconds`). `tick_ahead` runs one round looking ahead by the tick
interval (a part's time-bounded reads share one probe); tests' `tick` runs up to five.

A round decides two flags first (`round`):

- **Slow** (Expire and Rest run): once a tick interval has passed since they last ran, or in a
  full round.
- **Commands** (the commands part runs): when slow, or when a command was announced.

## The round, step by step

All of it runs inside `db.in_round_noting` (the probe's scope). `round_parts` does:

### 0. The trigger loop's wake

- `skipped_at(Check::TriggersDue, now)`: the round's first check, so **the probe statement
  runs here**.
- If a trigger's next look is due, or the last trigger pass failed, the trigger loop is
  notified (`triggers_due`). The trigger pass itself is not part of the round (below).

### 1. Results (`round_results`, service.rs), every round

The workers' `Started`s and `Finished`s, queued by their turns since the last round, and a
worker's other messages queued behind them (a `Rejected`, a `Deregister`, a stream's end).
Nothing queued: nothing done.

Before the lock (`ResultsReads::read`, dispatch.rs):

1. `attempts_by_ids`: one SELECT of every queued attempt (attempts JOIN task_instances).
2. `runs_tasks`: one SELECT of their runs joined to their instances (each run's DAG from a
   cache), for the batch.

Under the lock, one pass over the queue in arrival order:

3. Each message is checked against its stream (`queued_served`): a replaced stream's, or a
   lost worker's, is dropped.
4. `Started`s and `Finished`s gather into one batch across workers; any other message ends
   the batch (it is handled first), then that message is handled alone
   (`handle_in_round`), at most `failed_group_messages_per_turn` (10) alone a round; the rest
   go back to the queue for the next round.
5. A batch is **one transaction** (`handle_results_in`, core.rs), with a copy of the
   dispatcher's memory to put back if it fails:
   - **Starts** (`started_batch`): `attempts_by_ids` again, under the lock (a registration
     may have adopted one meanwhile); `mark_started_many`, one UPDATE of `attempts` per start
     method (`fork`, `spawn`): up to 2 statements for the whole batch.
   - **Results** (`finished_batch`): `batch_attempts` (the attempts, from the reads before
     the lock when they cover them); then for the common case (the worker's own attempt, its
     first result, not a moved cleanup) `finish_common`:
     - wait steps' strikes cleared (`clear_wait_steps`), or counted (`count_strike`), only
       for wait steps;
     - `end_attempts`: one UPDATE of `attempts` for all of them;
     - the batch's **runs together** (`runs_finished`, execution.rs, one transaction, the
       round's joined): each run from the reads before the lock (or `run`: 2 SELECTs) and
       its results planned; every run's outputs in one `store_outputs` (an INSERT ... ON
       CONFLICT into `outputs`), before any run moves forward; then per run `advance`:
       `chosen_of` (an `outputs_of` read only for branches), `settle_all` (in memory; reads
       only to expand a mapped task or settle a `first_of`), its expansions created at once
       (`create_instances_of`) and its instances' changes and its end **held**; then the
       held changes in one `update_instances_of_runs` (an UPDATE ... FROM VALUES of
       `task_instances` per 500 rows, every run's) and the ended runs in one `finish_runs`.
       A run whose DAG has notification channels stores what is held and ends at once
       (`finish_run` and its notifications), as they read its tasks and its DAG's earlier
       runs;
     - stops for what a failure cancels (`stop`: an `open_attempts` read, only when there are
       some).
     A result that isn't the common case goes through `finished` alone, in its place.
   - Acknowledgements (`FinishedAck`) for every result; a `CancelAttempt` for a `Started` of
     an attempt no longer running.
6. A failed batch: memory put back, then each worker's part of it as a group
   (`handle_group`, one transaction each), then message by message.
7. Timed as `schedr_master_round_part_seconds{part="results"}`; delivery after the lock.

Per batch: about 2 reads before the lock, then 1 transaction with roughly 2 + 3 per run
statements; per message nothing.

### 2. Commands (`commands`, service.rs), when due

The lock is taken for each command on its own (decision 483).

1. `commands_to_apply`: the script roots' uses sent; `pending_commands`, one SELECT of
   `commands` (skipped by the probe when none is pending).
2. Per command, `apply_command`: **one transaction each** (`apply_one`): a trigger, a
   cancel, a pause, a drain, a backfill, and its follow-ups (a drain's or resume's parse jobs
   moved). A `runs trigger` command reads its DAG (`active_dags_of`), then its context in one
   SELECT (`trigger_context`: the nearest earlier run, the running count and the queued
   runs), inserts its run (`INSERT INTO runs`), starts what `max_active_runs` lets
   (`start_queued`, the new run in the queue read before) and marks the command decided.
3. `commands_done`: overdue connection jobs marked lost; resumes whose follow-up failed tried
   again.

### 3. Scheduler (`Scheduler::pass`, scheduler.rs), every round

1. `dags_with_queued_runs`: one SELECT DISTINCT of `runs` (probe: `QueuedRuns`).
2. `scheduled_due`: one SELECT of `dag_triggers` due by now (probe: `Scheduled`).
3. In memory, which DAGs to visit (quiet DAGs): **none due, nothing more is read** (the idle
   path). The term's first round visits all: `next_fires`, every `dag_triggers` row.
4. For the visited DAGs: `run_overview_of` (one SELECT, a UNION ALL of the running counts,
   the last scheduled runs and the queued runs) and `active_dags_of` (a SELECT of `dags` and `dag_triggers`, `current_import_errors`,
   and each DAG's version from a cache, read on a miss), per 500 DAGs.
5. Per visited DAG:
   - due fire times in memory; only if any: `covered_times` (a SELECT of `runs`), then per run
     due, at most 10 a round: `insert_run`, **its own transaction each**; `succeeded_times`
     when a fire time still waits;
   - `start_queued`: per queued run that fits `max_active_runs`, `start_run` (an UPDATE of
     `runs`), **its own transaction each**;
   - `set_next_fire` (own transaction) only when the next fire time changed.
6. Nothing sent to workers.

### 4. Triggers: not in the round

`RoundPart::Triggers` is skipped by the round. The trigger loop (`start_triggers`,
`run_triggers`) runs the pass beside the rounds, taking the lock for it as a part does: at
once, then after any write ended in this master, when the round's probe says a trigger is due
(step 0), on a command or a trigger message, at most once a `task_round_gap`. Its lock time is
`round_part_seconds{part="triggers"}`, its wait `core_lock_wait_seconds{by="triggers"}`.

A pass: `exists(Check::Triggers)` (one EXISTS; nothing to look after, no pass), then under the
lock `claim_looks` (own transaction), `triggers` (every `trigger_progress` row; read 4 times
a pass), `active_dags` (every DAG), `queued_event_runs`, the triggers to stop
(`stop_trigger`: writes and a `CancelTrigger`), to start (`start_trigger`: writes, a
`StartTrigger` to a worker's watcher slot), `RunOf` and named-event DAGs' runs, and
`store_next_looks` (an UPDATE per DAG whose next look changed). Outside a round, the probe
skips nothing in it.

### 5. Prepare (`Dispatcher::prepare`, dispatch.rs), every round

1. The stops queued by earlier parts (`CancelAttempt`) to the outgoing messages.
2. `take_back_found`: per attempt a registration found unstarted, `attempt` (a SELECT), then
   if still unstarted **one transaction** ending it (`end_attempt`, and its run moved, as a
   result is) or giving a moved cleanup back. Usually none.
3. `prepare_runs` (execution.rs): `newly_running`, the running runs without instances (probe:
   `NewlyRunning`); per run, up to 2000 instances a round, **one transaction**:
   `create_instances` (an INSERT of `task_instances` per 500 tasks), `settle_all` (in memory),
   `store` (`update_instances` for the roots made ready, `queued` with `ready_at`), `finish`
   if the run ended at once.
4. `queue_due_retries`: `due_retries` (probe: `DueRetries`), then per retry due, at most 100
   a round, `update_instances`, **its own transaction each**.

### 6. Expire (`Dispatcher::expire_round`), slow rounds only

1. `unstarted_attempts` (probe: `Unstarted`) and `unstarted_cleanups` (probe:
   `UnstartedCleanups`): two SELECTs, every assigned and unstarted attempt or cleanup.
2. In memory, those the workers' echo proves never started (B240); at most 50 a round.
3. Per expired attempt **one transaction** (`end_attempt`, `attempt_finished`: `run_tasks`, 2
   SELECTs, and the run moved as for a result); per cleanup given back,
   `unassign_cleanup` (own transaction).
4. `end_replaced`: `replaced_attempts` (probe: `ReplacedAttempts`); per one past its hold,
   `attempt`, then one transaction ending it `lost` (`clear_replaced`, `run_tasks`,
   `task_result`, the run moved).
5. Its `CancelAttempt`s wait in the stops: sent by Assign or the next Prepare.

### 7. Assign, every round

Before the lock (`AssignReads::read`, dispatch.rs), on the pool, no transaction:

1. `ready_instances`: one SELECT of every ready instance (`task_instances` JOIN `runs` JOIN
   `dag_versions` JOIN `parse_results`, queued, `ready_at` passed, its run running and not
   cancelling), ordered by `ready_at` and ID, **no LIMIT**: every ready row is read, though a
   round assigns at most 50 (probe: `Ready`; nothing ready, the part reads nothing more).
2. At the same time, each on its own connection: `upstream_of_runs`, one SELECT of the
   upstream outcomes and outputs of the first 50 ready tasks; and, only if a wait step is
   ready, `switched_off_wait_steps` and `first_starts`.

Under the lock (`assign_round`):

3. `fail_waiting_users` (probe: `Unschedulable`): per task past its resource's
   `unschedulable_timeout`, one transaction failing it.
4. The stops to the outgoing messages.
5. `assign_cleanups` (probe: `WaitingCleanups`): per cleanup due, its worker picked,
   `assign_cleanup` (own transaction) and its message built (its inputs read).
6. `wind_down_removed` (probe: `RemovedDagsRuns`): runs of removed DAGs cancelled.
7. Nothing ready: done here, no transaction.
8. `held_and_versions` (one SELECT of `dags`, no probe: the DAGs held, and the others'
   current versions for step 9); chained tasks (`TriggerDagOperator`) started
   (own transaction each, then `chained_attempts`, probe: `ChainedAttempts`); wait steps
   waiting for a run or an event (`wait_for_runs`: reads and writes per wait step).
9. The concurrency limits from those versions; the counts
   (`active_attempts`) only the first time, then kept in memory.
10. **One transaction** for the round's assignments (`assign_each`):
    - per ready task, in memory: the DAG's version and limits (`held_back`), a worker picked
      (`workers.pick`: connected, eligible, a free slot; most free first, then least recently
      assigned; free slots on every worker before any waiting queue, decision 509), an
      attempt ID, the slot reserved. Wait steps first check whether they ended before their
      call (switched off, out of time). At 50 assignments the rest are counted waiting
      (`next_round`) and the loop ends;
    - `claim_attempts`: per 200 picks, one UPDATE of `task_instances` (`queued` to `running`,
      `RETURNING` what it claimed) and one INSERT of `attempts`;
    - the upstream read again only if the one before the lock doesn't cover every pick;
    - per pick, its `AssignTask` built (`build_one`): inputs from the upstream read (a mapped
      instance reads `map_sizes`, and its upstream's results), `run_params` only if the DAG
      has params, `run_event` only for an event run, the size checked;
    - `set_queued_reasons`: one UPDATE per reason why tasks wait (none waiting, none).
11. A failure: memory put back, then task by task, one transaction each; a commit whose
    answer was lost read back.

### 8. Rest (`round_rest`, core.rs), slow rounds only

1. Parse jobs past their time given to the next worker (memory; `AssignParse` messages).
2. Lost workers: a connection whose last handled or arrived message is older than
   `lease_timeout` (a `Register` counts only while it waits, B267). Per lost worker **one
   transaction** (`forget`): its state, and each open attempt ended `lost` or its cleanup
   taken back; then its parse jobs and connection jobs moved.

## The round's trace

M5.2g timed every step and point of the round with a trace of its own
(`SCHEDR_HIDDEN__MASTER__ROUND_TRACE`), removed once measured: its tables, two runs of
scenario 1 with each point's count, mean, median, p99 and total, are in note N1
(`notes/N1.toml`).

The trace is there for M5.2g's profiling only: its removal is prepared as commit `dec855c1`
(branch `trace-removal`), to cherry-pick once M5.2g's changes are measured; note N1 keeps its
tables and the register of its points.

## What a round's numbers mean

The metrics (`/metrics` on the master's ops port; the stress report's `round_*` keys):

| Metric | Is |
|---|---|
| `schedr_master_tick_duration_seconds` | A whole round, wall time: the parts, the waits for the lock between them, the reads before the lock, the probe, delivery. |
| `schedr_master_round_part_seconds{part}` | Each part's time under the lock (`results`, `commands`, `scheduler`, `prepare`, `expire`, `assign`, `rest`; `triggers` is the trigger loop's). |
| `schedr_master_core_lock_wait_seconds{by}` | The waits for the lock: `round`, `turn`, `register`, `triggers`, `stream`, `upload`, `restore`. |
| `schedr_master_turn_part_seconds{part}` | A worker's turn under the lock: `rest` (heartbeats, warnings), `triggers` (each run of trigger messages, one transaction). |

The round's time not in any part is the off-lock work: the results' and assign's reads before
the lock, the probe, delivery, and the waits for the lock.

### Scenario 1, measured (2026-11-19, release, quick x2)

32 slots, 2501 tasks at 81 a second; the round loop busy all the 31 s of load (32.7 s of
rounds), 208 rounds of about 12 tasks each:

| Part | Under the lock, per task |
|---|---|
| results | 3.9 ms |
| assign | 1.8 ms |
| commands | 1.4 ms |
| prepare | 0.7 ms |
| scheduler | 0.7 ms |
| expire | 0.1 ms |
| off the lock (reads before it, probe, delivery, waits) | 4.6 ms |
| **a round, per task** | **13.1 ms** |

The master used 18 % of one core and Postgres ran its statements for 9.1 s in all (about
3.6 ms a task): most of a round waits on round trips. The largest single read is
`ready_instances` (about 700 rows a call, 13.6 ms each: no LIMIT). Per task, about 3.5 round
trips: 9,012 statements counted, less the `set_config`s and fences sent in a transaction's
start, plus about 700 starts and 700 `COMMIT`s (whole run, parsing included).
