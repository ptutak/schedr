# Stress tests

Besides the unit and integration tests run on every change, a **stress suite** puts the real
system under load and trouble: the `schedr` binary as separate processes on a real Postgres,
many workers, killed processes, a frozen master, a stalled database, a connection pooler.
It is too slow for every push, so it runs **nightly in CI** (the whole suite when `main` changed since its last
passing run; scenario 21 alone with a fresh seed when it did not) and **locally with `make stress`**, at the end of each milestone and after a
change to scheduling, the database layer or the worker.

## What every scenario checks

Each scenario ends with the same **invariants**, and any of them broken fails it:

- nothing is still running, and no command is left undecided;
- no task succeeded twice, and every task of a successful run succeeded;
- every run ended in a state the scenario expects (`success`, or `cancelled` / `failed` where
  it cancels or overloads on purpose);
- **No unexpected overlap of tries**, checked twice: by the times the
  database recorded, and by the tasks themselves. Every stress task appends a `start` and an
  `end` line (its run, task, try, process and the time) to one shared file. A try the suite
  killed ends when it was killed; a try schedr stopped ends when the database recorded it.
  Two tries of one task running at once show as overlapping lines. In the chaos scenarios,
  expected overlaps are counted as `tries_run_twice` and listed in the bundle: a cut worker
  can keep running a task without `cleanup_anywhere` after the master retries it; a frozen
  worker cannot stop any task, including one with `cleanup_anywhere`. Other overlaps fail,
  including a `cleanup_anywhere` task on a cut (not frozen) worker. Tasks should be
  idempotent: tasks without `cleanup_anywhere` can run twice after a cut, and any task can
  run twice after a freeze.
- Chaos checks also fail if cleanup starts before its try ends, if a cleanup runs to its end
  more than once (except when its earlier worker was in trouble), or if the database says
  cleanup succeeded but its end mark is missing.

**Speed only fails on a gross regression.** Each scenario has generous limits (at least so many
tasks a second, the master's round under 5 s at the 99th percentile, a task starting within
30 s of its assignment, …). Shared CI machines vary too much for tight limits; the exact numbers
are in the report.

## The scenarios

| # | Scenario | What it shows |
|---|---|---|
| 1 | Throughput: many DAGs of short tasks and one wide DAG | every task once; tasks a second, latencies, round times |
| 2 | Command bursts: many triggers at once, a pause/unpause storm, cancelling running runs at once | each command decided once; no duplicate runs |
| 3 | Worker churn: workers killed with everything they run (`kill -9` of the process tree, as a pod dying) and back | lost tries retried; no task on two workers at once |
| 4 | Master failover: the leader killed; then the next one frozen past its lease and thawed | the standby takes over; the thawed master's writes are refused and it exits |
| 5 | The database stalled for the leader: shorter than its lease, then longer | it goes on; then the standby takes over, the stalled master exits, no worker is lost |
| 6 | Scenarios 1 and 4 through PgBouncer in transaction pooling | the lease and the commands need nothing of a database session |
| 7 | Rolling restart: workers stopped with SIGTERM one after another | draining under load; no task fails |
| 8 | Code churn: many DAG files, changed every few seconds while runs go on | parsing keeps up; runs succeed |
| 9 | Standalone on SQLite under load | no "database is locked"; the master's round time |
| 10 | Soak: a steady load (3 minutes quick, an hour full) | each role's memory and open files stay flat |
| 11 | Heavy output: big logs read through the API at once, outputs near their limit | logs complete and served in time |
| 12 | The memory watchdog: many tasks over their budget beside normal ones | only those are killed (`oom_killed`); the workers stay |
| 13 | Python tasks, spawned (`13_python_tasks`): chains of `@task` functions passing outputs on | outputs arrive right; a new interpreter for each task |
| 13 | Python tasks, forked (`13_python_tasks_forked`): the same load, the workers' fork servers on, a short preload list | every attempt forked (floor 99 %); how long the starts take |
| 14 | Quiet DAGs: a few busy DAGs alone, then beside many daily DAGs that do not fire | how much the master's rounds grow once the quiet DAGs are quiet |
| 15 | Change signals, on Postgres: scenario 1's load four times, the triggers that announce changes off, on, on, off | how the rounds and the tasks a second compare |
| 15b | The same, on SQLite through standalone (scenario 9's load) | the same comparison |
| 15c | The same with scenario 13's load (Python tasks, spawned): bound by the CPU, where the triggers' work at commit competes with the tasks' starts instead of hiding behind the disk | the same comparison |
| 16 | Concurrency limits: DAGs of two mapped Python tasks, two runs each at once, on many more slots than the limits allow | never more open at once than `max_active_per_run`, `max_active` or `max_active_tasks`; the DAG's limit reached; every run succeeds. Written in M4.4, its first result comes with the next run of the suite |
| 17 | Silent two-way cut, shorter and then longer than the worker lease and cleanup move wait | reconnection and moved cleanups |
| 18 | One-way cuts past the lease, first toward the master, then toward the worker | each direction's recovery |
| 19 | Slow links: 200 ms per chunk on every worker, then 1.5 s on one | no failures |
| 20 | Worker frozen with `SIGSTOP` past the lease and cleanup move, then thawed | its tasks continue while its agent is frozen |
| 21 | Seeded random cuts, slow links and freezes under load | recovery under a reproducible fault schedule |
| 22 | Parsing many files (`22_parse_many_files`): `schedr dags check` of files importing a shared helper and a slow library, spawned and forked, one at a time and four at once | seconds and peak memory for each; every file parsed |
| 23 | Many wait steps (`23_many_sensors`): 500 at once on one worker, its default `[watchers] slots`, 400 plain `@sensor`s awaiting a file and 100 pokes checking for it every 5 s | the worker's memory with its watcher hosts, idle and with all 500 in, per wait step, and what the hosts report; how soon each ends once the file is there; every one succeeds |

Masters are reached through a small proxy that sends new connections to whichever master is
ready, as a Kubernetes Service would; the database stall is the same proxy holding everything
back.

Each chaos worker has its own TCP proxy link. Scenario 21 chooses a fault every few seconds,
for 5–90 s, affecting at most half the workers at once. It uses `SCHEDR_STRESS_SEED` or a
fresh clock seed, recorded in the report and timeline. Replay the schedule with the same
profile and seed, for example `SCHEDR_STRESS_SEED=123 make stress STRESS=21_random_chaos`.
Timing of processes and the resulting execution can still vary.

Each chaos scenario leaves `target/stress/<scenario>/timeline.txt` and
`tries_run_twice.txt`. On failure the bundle also contains role logs, `marks.txt` and a
`database.json` snapshot; CI retains the bundle with the JSON report. The report counts
lost attempts and moved cleanups as well as overlaps.

**Scenario 13** is two tests of one load: chains of `@task` functions passing outputs on, a
Python process for each task. `13_python_tasks` spawns each task, a new interpreter every
time. `13_python_tasks_forked` is the same load with the workers' fork servers on and a short
preload list (`json` and `decimal`). A `total` other than 120 is broken. The forked test's
floor is the share of attempts that were forked, at least 99 %. The spawned test has the same
floor for attempts that were spawned. Each reads the start to the runner's hello on the first
worker (`schedr_worker_task_start_seconds`, phase `runner`). A scrape that fails, or that has
no such times, is broken.

**Scenario 14** measures quiet DAGs (M2.5.2): how much the master's rounds grow when many
quiet DAGs sit beside a few busy ones. Two workers of four slots run a few busy DAGs, each
an `Every` of 10 s with one `true` command (5 in the quick profile, times the machine's
scale, or 10 full). They are unpaused, and after a 20 s settle the master's rounds are
scraped for 1 minute (5 minutes full): the difference of
`schedr_master_tick_duration_seconds` between two scrapes. Then many daily DAGs are parsed
and unpaused (300 times the scale, or 1,000 full). The cron is minute 0 of the UTC hour
twelve hours ahead, `catchup` off and no `start`, so a fire before the DAG was stored does
not run and none fires while the scenario lasts. After the unpause the scenario waits until
`schedr_master_dags{state="quiet"}` reaches that count (up to 3 minutes): each idle DAG turns
quiet on its next visit. Then the same 20 s settle and window run again. The scenario fails
on a window with no counted rounds, a metrics scrape that is not 200, a busy DAG that did
not fire in that window, or a quiet DAG with a scheduled run. It also fails when the mean
round beside the quiet DAGs is over 2.0 times the mean alone, or the p99 over 1.5 times.
The p99 is a bucket edge, and those buckets are √2 apart, so 1.5 allows one bucket worse and
not two. The report `target/stress/14_quiet_dags.json` records `quiet_dags`,
`parse_quiet_s`, `quiet_after_unpause_s`, each window's round count, mean
(`round_mean_alone_ms`, `round_mean_beside_quiet_ms`) and p99 (`round_p99_alone_s`,
`round_p99_beside_quiet_s`), `round_mean_ratio` and `round_p99_ratio` (beside / alone), and
the busy and quiet gauges at the start and end of each window.

**Scenarios 15 and 15b** measure what announcing changes costs the master. The triggers that
tell the webserver what changed are part of the schema (see
[Live updates](12-ui.md#live-updates)). 15 runs scenario 1's load on Postgres, four times,
each on a database of its own, the triggers off, then on, then on, then off, so a drift
over the scenario weighs on both alike. With them on, a client follows `GET /api/v1/changes`,
as a live page does, so the webserver follows the changes during the load. 15b is the same
comparison on SQLite through `schedr standalone`, with scenario 9's load. There the triggers
append a row to the `changes` table. The runs without them leave the triggers in place and
turn off the one-row switch they read (`changes_switch`), as `[database]
change_notifications = false` does. The runs with them on have to deliver, on average, at
least one `run` event per run.

The two runs with the triggers on are compared with the two without: the average round
mean, the average round p99, and the average tasks a second. The scenario fails when the
mean with them on is over 1.3 times the mean without, the p99 over 1.5 times, or the tasks
a second under 0.9 times. The p99 is a bucket edge, and those buckets are √2 apart, so 1.5
allows one bucket worse and not two. The report `target/stress/15_change_signals.json`
(and `15b_change_signals_sqlite.json`) records each side's round mean
(`round_mean_without_ms`, `round_mean_with_ms`), round p99 (`round_p99_without_s`,
`round_p99_with_s`) and tasks a second (`tasks_per_s_without`, `tasks_per_s_with`), the
three ratios (`round_mean_ratio`, `round_p99_ratio`, `tasks_per_s_ratio`), the p99 from
assigned to started (`assigned_to_started_p99_without_s`,
`assigned_to_started_p99_with_s`), the `run` events per run
(`run_events_with_per_run`), and each of the four runs (`run1_without_round_mean_ms`,
`run2_with_tasks_per_s`, and the same for the other runs).

## Running it

```bash
make stress                          # all scenarios, the quick profile
make stress STRESS=failover          # scenarios whose name matches
make stress STRESS_PROFILE=full      # the large profile (an hour's soak)
make stress STRESS_STATEMENTS=1      # also count Postgres's statements
```

`make stress` starts Postgres and PgBouncer in containers and removes them afterwards. The
**quick** profile is sized for GitHub's smallest standard machine (2 CPUs, 7 GB) and grows with
the machine, at most four times; **full** is fixed and larger. Each scenario writes its numbers
to `target/stress/<scenario>.json` (in CI, an artifact of the run): the profile, the metrics,
and what failed, if anything. A scenario that fails also prints the logs of every process.
With `STRESS_STATEMENTS=1`, Postgres loads `pg_stat_statements` and the run ends by writing
`target/stress/statements.txt`: for each scenario's database its transactions and statements,
then the statements run most often and those that took longest. Run one scenario at a time
to read it: the databases are named at random.

## Baseline

The first run with every scenario passing (2026-10-24, quick profile ×2: a laptop under WSL,
Postgres and PgBouncer in containers; the disk's flush on commit dominates there, so a real
Postgres server does better). Throughput counts successful tasks a second over the scenario;
the round p99 is the bucket of `schedr_master_tick_duration_seconds` holding it. Rows 1–12
are the factor-of-two buckets of that day. Scenario 13's rows are a later run, below.
Scenario 14 is the M2.5.2 histogram, √2 apart. Scenarios 15 and 15b are the 2026-10-28
ratios of the runs with the change triggers on to the runs without, below.

| # | Scenario | Tasks | Tasks/s | Assigned → started p99 | Round p99 |
|---|---|---|---|---|---|
| 1 | throughput | 2,501 | 16.0 | 1.9 s | ≤ 4.1 s |
| 2 | command burst | 601 runs | — | commands decided p99 0.58 s | — |
| 3 | worker churn | 330 | 8.3 | 2.2 s | ≤ 2.0 s |
| 4 | master failover | 330 | 7.7 | 1.4 s | ≤ 1.0 s |
| 5 | database stall | 330 | 7.3 | 6.0 s | ≤ 1.0 s |
| 6 | through PgBouncer: failover / throughput | 330 / 2,501 | 7.2 / 14.9 | 2.3 s / 2.4 s | ≤ 2.0 s / ≤ 4.1 s |
| 7 | rolling restart | 270 | 6.4 | 2.2 s | ≤ 2.0 s |
| 8 | code churn | 70 runs | — | — | — |
| 9 | standalone on SQLite | 440 | 20.0 | 1.4 s | — (not measured; see below) |
| 10 | soak (3 minutes) | 1,020 | 5.6 | 0.9 s | ≤ 1.0 s |
| 11 | heavy output | 168 | 4.6 | 1.3 s (logs read p99 1.4 s) | ≤ 2.0 s |
| 12 | memory watchdog | 120 (12 killed over budget) | 5.7 | 1.3 s | ≤ 1.0 s |
| 13 | Python tasks, spawned (`13_python_tasks`) | 640 | 12.4 | run p50 48.2 s; hello p50 0.64 s | — |
| 13 | Python tasks, forked (`13_python_tasks_forked`) | 640 | 14.3 | run p50 41.7 s; hello p50 0.02 s, p99 0.08 s | — |
| 14 | quiet DAGs (M2.5.2, quick ×2, 600 quiet DAGs) | — | — | — | 0.36 s alone and beside |
| 15 | change signals, Postgres (2026-10-28, quick, four runs) | — | — | — | mean 1.02×, p99 1.00×, tasks/s 0.97× |
| 15b | change signals, SQLite through standalone (2026-10-28, quick, four runs) | — | — | — | mean 0.94×, p99 1.17×, tasks/s 0.98× |

Scenario 13's rows are the 2026-10-27 run (quick profile, this machine), not the 2026-10-24
one. Where the other rows give assigned → started, these give the run's p50 and the start to
the runner's hello. The hello times are the bucket of `schedr_worker_task_start_seconds`
(phase `runner`) on the first worker. The forked run forked every attempt. Its floor is 99 %.
A scrape that fails, or that has no such times, is broken. That scenario's first run found a
bug, and the step fixed it: a new worker's fork servers could not start until a task had made
its log directory.

Scenario 9's round time was not measured on 2026-10-24. Standalone's banner continues
after the ops address (`, data in …`), and the harness kept that comma, so the scrape of
`/metrics` read nothing. The comma is dropped now. Scenario 9 reads
`schedr_master_tick_duration_seconds` from standalone's ops port, as the other scenarios
read it from the master, and the round's p99 has to be at most 5 s.

Scenario 14's row is the M2.5.2 run, not the 2026-10-24 one. The mean round was 50 ms alone
and 51 ms beside the 600 quiet DAGs (ratio 1.04); the p99 was the same bucket, 0.36 s. All
600 were quiet 0.03 s after their unpause. Before M2.5.2, on the same shape of run, the mean
was 41 ms alone and 291 ms beside (ratio 7.1), and the p99 was ≤ 0.512 s alone and ≤ 1.024 s
beside.

Scenarios 15 and 15b's rows are the 2026-10-28 run (quick profile, four runs each), not
the 2026-10-24 one. Where the other rows give tasks a second and a round p99, these give
the ratio of the runs with the change triggers on to the runs without: the rounds' mean,
the rounds' p99, and the tasks a second. On Postgres that was 1.02, 1.00 and 0.97. On
SQLite, through standalone, it was 0.94, 1.17 and 0.98. The tasks a second were about 2-3%
lower with the triggers on, within the noise between runs. The triggers stay on by default
(see [Live updates](12-ui.md#live-updates)).

On the way there the suite found and the step fixed: assignments taken back though started
(scenario 13 went from 624 expired assignments and 11 successes to 11.5 tasks a second),
rounds costing queries per DAG and reading a wide DAG's definition per ready task, a stalled
master blocking the takeover, SQLite writes failing with "database is locked", the master's
`LISTEN` breaking other connections behind PgBouncer, a leaving worker's unstarted tasks
counted as lost, and slow rounds after a burst of runs (per-round limits, batched writes).

## After M3 (2026-10-28)

The whole suite after M3 and the round woken by a finished task (`1e75e64`, quick profile, this
laptop under WSL): every scenario passed.

| # | Scenario | Tasks | Tasks/s | Assigned → started p99 | Round p99 |
|---|---|---|---|---|---|
| 1 | throughput | 2,501 | 14.6 | 3.3 s | ≤ 2.9 s |
| 2 | command burst | — | — | commands decided p99 0.48 s | — |
| 3 | worker churn | 330 | 8.0 | 1.8 s | ≤ 1.4 s |
| 4 | master failover | 330 | 7.2 | 2.2 s | ≤ 1.0 s |
| 5 | database stall | 330 | 8.7 | 3.2 s | ≤ 0.7 s |
| 6 | through PgBouncer: failover / throughput | 330 / 2,501 | 7.0 / 15.0 | 2.3 s / 2.5 s | ≤ 1.0 s / ≤ 2.9 s |
| 7 | rolling restart | 270 | 6.8 | 1.6 s | ≤ 0.7 s |
| 8 | code churn | 70 changes | — | parsing caught up in 0.02 s | — |
| 9 | standalone on SQLite | 440 | 20.6 | 1.3 s | ≤ 0.7 s |
| 10 | soak (3 minutes) | 1,020 | 5.5 | 1.5 s | ≤ 0.7 s |
| 11 | heavy output | 168 | 3.9 | 1.6 s (logs read p99 1.5 s) | ≤ 2.0 s |
| 12 | memory watchdog | 120 (12 killed over budget) | 5.5 | 1.5 s | ≤ 1.0 s |
| 13 | Python tasks, spawned | 640 | 8.6 | run p50 70 s; hello p50 1.28 s | ≤ 2.0 s |
| 13 | Python tasks, forked | 640 | 13.4 | run p50 44.5 s; hello p50 0.04 s | ≤ 2.0 s |
| 14 | quiet DAGs (600) | — | — | — | mean 47 ms alone, 51 ms beside (1.09×); p99 0.36 s both |
| 15 | change signals, Postgres | — | — | — | mean 1.00×, p99 0.83×, tasks/s 0.99× |
| 15b | change signals, SQLite | — | — | — | mean 1.11×, p99 1.17×, tasks/s 0.98× |

**Only runs of one session compare.** Scenario 13 came out well below the 2026-10-27
baseline (spawned 8.6 tasks a second against 12.4), so it was run again at each change since
then, in the same afternoon, alternating:

| Code | Spawned tasks/s | Forked tasks/s |
|---|---|---|
| `f0405b4`, the baseline's own code | 10.3, 8.7 | 13.2, 13.5 |
| `0628b51`, M3.1 | 10.0, 9.7 | 13.5, 13.6 |
| `cc2ec1c`, M3.2 (its new indexes) | 9.3, 10.6 | 14.1, 13.9 |
| `04d083f`, M3.6 and fixes, before the round wake-up | 10.6, 9.2 | 13.2, 13.8 |
| `c172efd`, the round wake-up | 10.5, 10.3 | 13.4, 13.9 |

The baseline's own code gave the same as today's. The gap is the machine: Windows was using
much of the CPU beside WSL that afternoon (89 % in all), and spawned Python tasks, the
CPU-heaviest load here, lose the most. The suite's run of scenario 13 overlapped a
documentation build, which explains its lower 8.6. On a laptop, compare runs of one session
in alternating order; numbers from different days measure the machine as much as schedr.
The nightly CI run, on machines of one kind, shows the trend.

**The round woken by a finished task** (decision 42B) was measured the same way, before
(`04d083f`) and after (`c172efd`), in both orders (the ratio is after over before):

| Scenario | Tasks a second | Runs' p50 | Runs' p99 | Assigned → started p99 |
|---|---|---|---|---|
| 1, Postgres (10-task chains) | 0.92, 1.07 | 0.79, 0.93 | 1.09, 0.93 | 1.67, 0.81 |
| 9, SQLite through standalone | 1.00, 1.05 | 0.98, 0.93 | 0.99, 0.89 | 0.97, 0.63 |
| 13, Python tasks, spawned | 0.99, 1.11 | 1.00, 0.89 | — | 0.92, 0.85 |

The runs are shorter with it (scenario 1's chains by 7-21 %), and no cost held in both
orders: the first pair's slower starts and lower tasks a second on scenario 1 turned over in
the second.

Scenario 15c (the triggers on and off under the Python load) is new; its first result comes
from the nightly run.

## After M4B (2026-11-11)

M4B stores a worker's messages in turns of groups (its `Started`s in one transaction, then its
`Finished`s, set-based) and writes the agent's journal in groups. Measured the same evening on
this laptop under WSL (quick profile), the code before M4B's batching (`0da885f`) and after it
(`afb1b9e`), one tree after the other. Scenario 13's rows are its second rerun (each run
overwrites the tree's report); the first run gave spawned 7.8 → 7.9 tasks a second (start p99
1.8 s → 4.4 s) and forked 10.4 → 11.4 (1.7 s → 5.0 s). "Noise" marks a change under about 5 %,
or one the reruns showed swinging as far either way.

| Scenario | Metric | Better | Before | After | Change | Verdict |
|---|---|---|---|---|---|---|
| 1 throughput | tasks_per_s | more | 11.10 | 16.79 | +51 % | better |
| | run_p50_s | less | 139.2 | 91.4 | −34 % | better |
| | run_p99_s | less | 211.5 | 135.3 | −36 % | better |
| | wall_s | less | 251.5 | 173.0 | −31 % | better |
| | round_p99_s | less | 2.90 | 2.05 | −29 % | better |
| | assigned_to_started_p99_s | less | 4.44 | 3.98 | −10 % | better |
| | parse_all_s | less | 18.13 | 16.60 | −8 % | better |
| | tasks | (count) | 2,501 | 2,501 | | same |
| 2 command burst | command_decided_p99_s | less | 0.70 | 0.50 | −29 % | better |
| | trigger_calls_per_s | more | 13.76 | 14.20 | +3 % | better (noise) |
| | wall_s | less | 303.1 | 303.8 | 0 % | same (fixed length) |
| 6 through PgBouncer | tasks_per_s | more | 10.22 | 15.41 | +51 % | better |
| | run_p50_s | less | 150.2 | 99.3 | −34 % | better |
| | run_p99_s | less | 228.1 | 146.7 | −36 % | better |
| | wall_s | less | 270.8 | 187.8 | −31 % | better |
| | round_p99_s | less | 2.90 | 2.90 | 0 % | same |
| | assigned_to_started_p99_s | less | 4.62 | 4.41 | −4 % | better (noise) |
| | parse_all_s | less | 17.72 | 17.55 | −1 % | same |
| | tasks | (count) | 2,501 | 2,501 | | same |
| 9 standalone on SQLite | tasks_per_s | more | 16.93 | 18.04 | +7 % | better |
| | run_p50_s | less | 15.5 | 14.2 | −8 % | better |
| | run_p99_s | less | 21.9 | 19.9 | −9 % | better |
| | wall_s | less | 35.4 | 34.5 | −3 % | better (noise) |
| | round_p99_s | less | 1.02 | 1.02 | 0 % | same |
| | assigned_to_started_p99_s | less | 2.10 | 3.48 | +66 % | **worse** |
| | tasks | (count) | 440 | 440 | | same |
| 13 Python, spawned (rerun 2) | tasks_per_s | more | 5.46 | 6.17 | +13 % | better |
| | run_p50_s | less | 112.5 | 99.6 | −12 % | better |
| | run_p99_s | less | 114.5 | 101.2 | −12 % | better |
| | wall_s | less | 128.6 | 116.6 | −9 % | better |
| | round_p99_s | less | 4.10 | 2.05 | −50 % | better |
| | assigned_to_started_p99_s | less | 3.67 | 7.84 | +114 % | **worse** |
| | start_runner p50 / p99 | less | 1.28 / 2.56 | 1.28 / 2.56 | 0 % | same |
| | spawn_share | 1.0 (all spawned, as meant) | 1.0 | 1.0 | | same |
| | tasks | (count) | 640 | 640 | | same |
| 13 Python, forked (rerun 2) | tasks_per_s | more | 9.92 | 11.89 | +20 % | better |
| | run_p50_s | less | 60.8 | 50.7 | −17 % | better |
| | run_p99_s | less | 63.2 | 51.8 | −18 % | better |
| | wall_s | less | 72.0 | 60.9 | −15 % | better |
| | round_p99_s | less | 2.05 | 1.45 | −29 % | better |
| | assigned_to_started_p99_s | less | 3.12 | 3.39 | +9 % | worse (noise) |
| | start_runner p50 / p99 | less | 0.04 / 0.16 | 0.04 / 0.16 | 0 % | same |
| | fork_share | 1.0 (all forked, as meant) | 1.0 | 1.0 | | same |
| | tasks | (count) | 640 | 640 | | same |
| 23 many sensors | await_response_p50_s | less | 12.41 | 2.50 | −80 % | better |
| | await_response_p99_s | less | 26.32 | 4.80 | −82 % | better (B184 met) |
| | await_response_max_s | less | 26.56 | 4.80 | −82 % | better |
| | poke_response_p50_s | less | 27.96 | 7.64 | −73 % | better |
| | poke_response_p99_s | less | 34.98 | 11.09 | −68 % | better |
| | poke_response_max_s | less | 35.01 | 11.09 | −68 % | better |
| | wall_s | less | 76.8 | 63.3 | −18 % | better |
| | all_in_s (all 500 started) | less | 17.43 | 25.68 | +47 % | **worse** |
| | worker_held_mib | less | 94.9 | 94.2 | −1 % | same |
| | worker_idle_mib | less | 25.9 | 25.9 | 0 % | same |
| | per_wait_step_kib | less | 141.3 | 140.1 | −1 % | same |
| | hosts_reported_mib | less | 41.6 | 41.4 | 0 % | same |
| | wait_steps, watcher_hosts | (count) | 500, 1 | 500, 1 | | same |

Throughput rose by half on Postgres, with PgBouncer or without, and by 7 to 20 % elsewhere.
Runs are about a third shorter, and so are the rounds. Memory didn't move. B184 counted as
fixed when stress 23 records its 500 ends within 5 s on Postgres (decision 396): p99 4.8 s, met.

**What got worse is the time to a start**: assigned → started in scenarios 9 and 13, and
scenario 23's `all_in_s`. Over four runs of scenario 13 on each code, alternating, it ranged
3.1 to 4.8 s before and 3.4 to 7.8 s after: overlapping, the average higher. A `Started`
arriving while the master handles a turn of up to 40 `Finished`s waits for the whole turn,
where it waited behind one message before. It stays well inside the worker's reserve for a
start (two heartbeats, 20 s). The turn's size is `SCHEDR_HIDDEN__MASTER__MESSAGES_PER_TURN`
([Timings](14-timings.md)).

## Where a start's time goes (M5.1a, 2026-11-13)

Each side now times its part of a start ([Observability](09-observability.md#where-a-starts-time-goes)),
and every measured scenario reports p50 and p99 of each stage. `all_in_s` in scenario 23 is
now taken before its 5 s settling wait, and the worker sets its slot gauges as each wait step
starts, not at its 5 s alive tick: it measures the starts only. Measured on this laptop under
WSL (quick profile, Postgres, the test build), on the code after 1.0.0. A figure from a histogram
is the upper bound of its bucket: the stages' buckets double, the round's are √2 apart (so
1.02, 1.45 and 2.05 s are neighbouring buckets).

| Stage | 9 standalone (1 worker) | 13 spawned (2 workers) | 13 forked (2 workers) | 23 many sensors (1 worker) |
|---|---|---|---|---|
| assigned → started p50 / p99 (from the database) | 0.11 / 3.09 s | 1.04 / 4.35 s | 1.02 / 3.75 s | — |
| worker: `AssignTask` to `Started` sent, p99 | 0.03 s | 0.05 s | 0.05 s | 0.05 s |
| worker: start record on disk, p50 / p99 | 0.013 / 0.026 s | 0.013 / 0.05 s | 0.013 / 0.05 s | 0.013 / 0.05 s |
| master: a turn waiting for the lock, p50 / p99 | 0.0001 / 0.82 s | 0.82 / 3.28 s | 0.82 / 1.64 s | 0.10 / 6.55 s |
| master: a round waiting for the lock, p50 / p99 | 0.013 / 0.10 s | 0.41 / 0.82 s | 0.41 / 0.82 s | 0.0001 / 0.82 s |
| master: a round, p50 / p99 | — / 1.02 s | — / 2.05 s | — / 1.45 s | 0.13 / 4.10 s |
| master: a turn's `Started`s, p99 | 0.05 s | 0.20 s | 0.20 s | 0.82 s |
| master: a turn's `Finished`s, p99 | 0.10 s | 0.20 s | 0.20 s | 0.82 s |
| `all_in_s` (500 wait steps started) | | | | 14.5 s (25.7 s before, with the wait and the gauge's lag) |

**The time to a start is the wait for the master's core lock.** The worker's part is small:
under 50 ms from an assignment to its `Started`, the start record on disk included. A turn's
own work is under a second at p99. What a `Started` waits for is the lock, held by the rounds
and the other worker's turns: in scenario 13 a turn waited 0.8 s at p50 and up to 3.3 s,
matching the assigned → started p99. With one worker (scenarios 9 and 23) there is no other
turn to wait behind, so the wait is the rounds: scenario 23's rounds, assigning 500 wait steps,
took up to 4.1 s, and a turn waited up to 6.6 s. The master records `started_at` when it gets
to the `Started`, so this wait shows as a late `started_at`; the tasks themselves started when
the worker sent it.

**The worker's journal is a part of scenario 23's starts, not most of them.** 500 start
records at about 13 ms each are about 6.5 s of disk time, one after another on the agent's
loop, of the 14.5 s; the rest is the master assigning them in rounds of 50 behind a lock its
turns also want.

### What a round spends (M5.1c)

`master_round_part_seconds` times each part of a round. Before M5.1c1, scenario 23's rounds
spent their time assigning: of 17.6 s of assignment transactions over the run, the wait-step
checks (switched off? past `max_wait`?) took 10.8 s, one or two queries for each ready wait
step at every visit, 1,340 of those visits to tasks past the round's 50; the claims took 4.8 s
(9.6 ms each, three statements) and the deadlines 1.5 s. An assignment cost about 18 ms. After
M5.1c1 (the checks read once a round, the round stopping at its 50, the claims in a few
statements over the batch, the lock taken per part and per transaction):

| Scenario 23 | M5.1b | M5.1c1 |
|---|---|---|
| `all_in_s` (500 wait steps started) | 14.5 s | 4.2 s |
| the round's assign part, p99 | 3.3 s | 0.2 s |
| a turn waiting for the lock, p99 | 6.6 s | 1.6 s |

**Commands (M5.1c2).** A round's commands reach 1.6-3.3 s at p99 in scenarios 13 and 23,
every one a `TriggerRun` at about 64 ms in scenario 23: marked applying 10.0 ms, all the active
DAGs read to find one 10.4 ms, the nearest earlier run 4.5 ms, the run inserted 10.7 ms, the
running count 4.0 ms, the queued runs 4.3 ms, queued runs started 10.7 ms, the decision stored
9.6 ms (scenario 13 about twice that, its machine busy with tasks). No step dominates: the four
writes are each their own commit, and the reads 4-10 ms each.

**Commands (M5.1c3).** With each command one transaction (its mark, its work and its decision
committed together) and only the named DAG read, a round's commands part fell to the 0.8 s
bucket at p99 in scenario 23 and in scenario 13 with forked tasks, and to the 1.6 s bucket in
scenario 13 (3.3 s before). The part's length no longer holds workers back: the lock is taken
for each command on its own, and a turn waits at most one command.

| p99 | 13 | 13, forked | 23 |
|---|---|---|---|
| the round's commands part | 1.6 s | 0.8 s | 0.8 s |
| a turn waiting for the lock | 0.8 s | 0.4 s | 0.8 s |
| a round, all its parts | 2.9 s | 2.0 s | 2.9 s |

### Statements counted (M5.1d, 2026-11-14)

Counted with `STRESS_STATEMENTS=1` after M5.1c3, scenario 23 ran about 8,000 statements in 35 s,
2.3 s of Postgres's time: the database isn't busy, the cost is the round trips. Three things
were done one item at a time inside a turn's groups: each `Started` read its attempt and wrote
its start (1,000 statements for 500 starts), each wait step that succeeded read its run's DAG
twice and cleared its strikes, and a result sent again before its acknowledgement came (about
360 of the 500 here: the stress setup's 1 s heartbeat resends a result unacknowledged at two
heartbeats) was taken alone, read again and ended to no effect. Now a group's starts are one
read and a statement for each start method, its wait steps' strikes one read and a statement
for each DAG, and a repeat is acknowledged without a read or a write.

| | Before | After |
|---|---|---|
| 23: statements in the run | 7,978 | 5,523 |
| 23: a wait step's end recorded (`await` p99 less its 1 s check) | 2.9 s | 1.2 s |
| 23: all 500 started (`all_in_s`) | 3.2 s | 1.1 s |
| 23: a round, p99 | 2.9 s | 1.4 s |
| 1: tasks a second | 22.1 | 27.3 |
| 6 (through PgBouncer): tasks a second | 21.3 | 26.7 |
| 1 and 6: a round, p99 | 4.1 s | 4.1 s |

The pokes' figure (7.75 s, then 4.4 s) is mostly where the file opens in their 5 s cycle, so
the step is judged on the `await` steps (note 6).

### The turn's size (M5.1e, 2026-11-14)

How many of a worker's messages the master takes in one turn
(`SCHEDR_HIDDEN__MASTER__MESSAGES_PER_TURN`), measured on an idle machine with one run of
scenarios 1, 13 and 23 at each size (the variable passes through to the test cluster's master):

| Size | 1: tasks/s | 1: start p99 | 13 forked: tasks/s | 13 forked: start p99 | 23: `await` p99 |
|---|---|---|---|---|---|
| 10 | 28.2 | 0.80 s | 18.9 | 0.53 s | 3.86 s |
| 20 | 28.1 | 0.71 s | 19.6 | 0.45 s | 2.42 s |
| 40 | 27.6 | 0.98 s | 19.5 | 0.49 s | 2.03 s |
| 60 | 27.1 | 0.92 s | 19.1 | 0.48 s | 1.64 s |
| **80** | 28.1 | 0.77 s | 19.6 | 0.50 s | 1.64 s |
| 120 | 27.0 | 1.09 s | 18.6 | 1.24 s | 1.54 s |
| 160 | 27.1 | 0.88 s | 19.0 | 0.50 s | 1.74 s |

Steady throughput stays within about 4 % at every size, which is the runs' noise. The size
shows in bursts: 500 wait steps ending at once are recorded in 3.9 s at 10 and in 1.6 s from 60
up, while starts get slower again from 120. The default is 80 since then (decision 484).
