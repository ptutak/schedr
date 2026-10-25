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
| 9 | Standalone on SQLite under load | no "database is locked" |
| 10 | Soak: a steady load (3 minutes quick, an hour full) | each role's memory and open files stay flat |
| 11 | Heavy output: big logs read through the API at once, outputs near their limit | logs complete and served in time |
| 12 | The memory watchdog: many tasks over their budget beside normal ones | only those are killed (`oom_killed`); the workers stay |
| 13 | Python tasks: chains of `@task` functions passing outputs on | outputs arrive right; a Python process for each task |

Masters are reached through a small proxy that sends new connections to whichever master is
ready, as a Kubernetes Service would; the database stall is the same proxy holding everything
back.

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
the round p99 is the bucket of `schedr_master_tick_duration_seconds` holding it.

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
| 9 | standalone on SQLite | 440 | 20.0 | 1.4 s | — |
| 10 | soak (3 minutes) | 1,020 | 5.6 | 0.9 s | ≤ 1.0 s |
| 11 | heavy output | 168 | 4.6 | 1.3 s (logs read p99 1.4 s) | ≤ 2.0 s |
| 12 | memory watchdog | 120 (12 killed over budget) | 5.7 | 1.3 s | ≤ 1.0 s |
| 13 | Python tasks | 640 | 11.5 | 2.4 s | ≤ 2.0 s |

On the way there the suite found and the step fixed: assignments taken back though started
(scenario 13 went from 624 expired assignments and 11 successes to 11.5 tasks a second),
rounds costing queries per DAG and reading a wide DAG's definition per ready task, a stalled
master blocking the takeover, SQLite writes failing with "database is locked", the master's
`LISTEN` breaking other connections behind PgBouncer, a leaving worker's unstarted tasks
counted as lost, and slow rounds after a burst of runs (per-round limits, batched writes).
