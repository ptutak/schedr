# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that reproduces
it: shown failing before the fix and passing after; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

A bug found by reasoning but **not reproduced yet** is logged too, marked **Suspected**, with
how it would be confirmed; once a test shows it, the mark goes. If it turns out not to be a
bug, the entry is removed, saying why in the commit.

## B15 (Suspected): a panic inside tokio's `AtomicWaker` aborts standalone (SQLite and Postgres)

- **Seen:** 2026-10-24, stress scenario 9 (standalone on SQLite, quick x2), in the standalone
  process's log, after many B14 errors: `thread 'tokio-rt-worker' panicked at
  tokio-1.54.0/src/sync/task/atomic_waker.rs:324:17: assertion failed: old_state == WAKING`.
  2 of 40 runs never finished (B14 may explain them alone).
- **Seen again:** 2026-10-25, `make check` for 2.12 (no Rust change in that step), the
  integration test `schedr::standalone the_same_run_works_on_postgres`: on **Postgres**, 2 s
  after start (the machine busy: two other worktrees building and testing, a kind cluster),
  right after `schedr master: leading`: `thread 'tokio-rt-worker' panicked at
  tokio-1.54.0/src/sync/task/atomic_waker.rs:336:17: assertion failed: state == REGISTERING
  || state == REGISTERING | WAKING || state == WAKING`, then `worker thread panicking;
  aborting process`, so the CLI found no server. Not reproduced by Grok's `make check` of
  the same tree a little earlier, nor by the docs worktree's. So not SQLite-specific; it
  ends the whole process, not one task, and it needs no B14 errors first.
- **Not understood yet:** an internal assertion of tokio's `AtomicWaker`; it can follow a
  panic inside a waker (a nested panic), misuse of a waker, or a bug in tokio or in what
  wakes through it (sqlx: its SQLite worker thread, or its Postgres connection). Debug
  builds only assert it.
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
