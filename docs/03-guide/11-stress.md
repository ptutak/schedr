# Stress tests

Besides the unit and integration tests run on every change, a **stress suite** puts the real
system under load and trouble: the `schedr` binary as separate processes on a real Postgres,
many workers, killed processes, a frozen master, a stalled database, a connection pooler.
It is too slow for every push, so it runs **nightly in CI** (when `main` changed since its last
passing run) and **locally with `make stress`**, at the end of each milestone and after a
change to scheduling, the database layer or the worker.

## What every scenario checks

Each scenario ends with the same **invariants**, and any of them broken fails it:

- nothing is still running, and no command is left undecided;
- no task succeeded twice, and every task of a successful run succeeded;
- every run ended in a state the scenario expects (`success`, or `cancelled` / `failed` where
  it cancels or overloads on purpose);
- **no try of a task started before the previous try ended**, checked twice: by the times the
  database recorded, and by the tasks themselves. Every stress task appends a `start` and an
  `end` line (its run, task, try, process and the time) to one shared file. A try the suite
  killed ends when it was killed; a try schedr stopped ends when the database recorded it.
  Two tries of one task running at once would show as overlapping lines.

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

Masters are reached through a small proxy that sends new connections to whichever master is
ready, as a Kubernetes Service would; the database stall is the same proxy holding everything
back.

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
over the scenario weighs on both alike. With them on, a client follows `GET /api/v1/events`,
as a live page does, so the webserver follows the changes during the load. 15b is the same
comparison on SQLite through `schedr standalone`, with scenario 9's load. There the triggers
append a row to the `changes` table. The runs without them leave the triggers in place and
turn off the one-row switch they read (`changes_switch`), as `[database]
change_notifications = false` does. The runs with them on have to deliver, on average, at
least one `run` event per run.

The two runs with the triggers on are compared with the two without: the average round
mean, the average round p99, and the average tasks a second. The scenario fails when the
mean with them on is over 1.15 times the mean without, the p99 over 1.5 times, or the tasks
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
```

`make stress` starts Postgres and PgBouncer in containers and removes them afterwards. The
**quick** profile is sized for GitHub's smallest standard machine (2 CPUs, 7 GB) and grows with
the machine, at most four times; **full** is fixed and larger. Each scenario writes its numbers
to `target/stress/<scenario>.json` (in CI, an artifact of the run): the profile, the metrics,
and what failed, if anything. A scenario that fails also prints the logs of every process.

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
