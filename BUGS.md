# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that reproduces
it: shown failing before the fix and passing after; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

A bug found by reasoning but **not reproduced yet** is logged too, marked **Suspected**, with
how it would be confirmed; once a test shows it, the mark goes. If it turns out not to be a
bug, the entry is removed, saying why in the commit.

## B17 (Suspected): a rolling restart now and then loses the tasks of a drained worker

- **Seen:** 2026-10-24, stress scenario 7 (rolling restart, quick x2), once in three runs:
  4 attempts ended `lost` on `w03` ("stopped answering while the attempt ran"), so their runs
  failed (no retries). The worker had been stopped with SIGTERM (drained, it exited 0) and
  started again with the same ID; the next two runs of the scenario passed.
- **Not understood yet:** a drained worker's tasks should finish or be interrupted (and run
  again without a retry), never be lost. The logs of the worker's earlier run were lost (the
  harness restarted its log file); it now appends them.
- **Confirm:** rerun `make stress STRESS=stress_07` until it fails, then read the worker's
  whole log and the master's around the restart.

## B16: behind PgBouncer in transaction pooling, the master's `LISTEN` breaks other connections

- **Seen:** 2026-10-24, stress scenario 6 (both parts, through PgBouncer `v1.26.0-p0` in
  transaction pooling): within seconds the webserver answers "the database failed" to
  `runs trigger`; its log: `encountered unexpected or invalid data: execute: unexpected
  message: NotificationResponse`, and `command stored, notification failed`.
- **Cause:** the master listens for new commands with `LISTEN schedr_commands` (on a
  connection of its own, `PgListener`). Through PgBouncer in transaction pooling the `LISTEN`
  registers on whichever server connection PgBouncer lends at that moment, and that
  connection is lent to other clients afterwards: notifications then arrive in the middle of
  the webserver's (or the master's) queries, which sqlx doesn't expect, and the query fails.
  PgBouncer documents `LISTEN` as unsupported in transaction pooling. 2.10 made polling the
  command guarantee but kept the `LISTEN`.
- **Repro:** `make stress STRESS=stress_06`.
- **Fix (to decide):** options, the user's choice: a setting to turn notifications off
  (polling only, the guarantee anyway: a command waits up to a round); a separate URL for the
  listening connection only, straight to Postgres, past the pooler; detecting PgBouncer
  (`SHOW` commands it answers) and turning them off by itself.

## B15 (Suspected): a panic inside tokio in standalone on SQLite under load

- **Seen:** 2026-10-24, stress scenario 9 (standalone on SQLite, quick x2), in the standalone
  process's log, after many B14 errors: `thread 'tokio-rt-worker' panicked at
  tokio-1.54.0/src/sync/task/atomic_waker.rs:324:17: assertion failed: old_state == WAKING`.
  2 of 40 runs never finished (B14 may explain them alone).
- **Not understood yet:** an internal assertion of tokio's `AtomicWaker`; it can follow a
  panic inside a waker (a nested panic), misuse of a waker, or a bug in tokio or in what
  wakes through it (sqlx's SQLite worker thread). Debug builds only assert it.
- **Confirm:** rerun scenario 9 with `RUST_BACKTRACE=1` for the role processes, before and
  after B14's fix; find the panicking task's stack.

## B4: `runs trigger` in a roles integration test got no result once

- **Seen:** 2026-10-22, the first `make test-integration` of M2.3:
  `roles::a_worker_that_never_comes_back_is_lost_and_stays_listed` panicked in
  `Cluster::trigger` (`crates/schedr/tests/roles.rs`): `cli_json(["runs", "trigger", …])` was
  `None`, so the CLI exited non-zero, after 34.8 s; the whole run took 35 s instead of the
  usual 12 s.
- **Not reproduced** in the two runs right after (53/53 passed each time). Postgres had just
  been started by podman; `cli_json` doesn't keep the CLI's stderr, so the reason is unknown.
- **2026-10-23:** the role tests now keep every failed or slow (over 2 s) CLI call's stderr,
  and the system's I/O stall, in their failure messages. Most likely B6 (fixed the same day:
  a 7–8 s Postgres stall after a rebuild, and the master declaring live workers lost during
  it; the trigger's CLI waits 30 s for the master's decision, and the run took 35 s instead
  of 12 s, as in B6's stalls), but unproven: no test reproduces it.
- **Next:** if it happens again, read the CLI's stderr in the failure message; otherwise close
  it after a few weeks of clean `make check` runs.
