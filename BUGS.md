# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that reproduces
it: shown failing before the fix and passing after; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

## B9: a drain can't shorten a stop already under way

- **Seen:** 2026-10-23, in the review of the B8 fix (by reading the code, not in a run).
- **What:** a drain whose time is up interrupts the running attempts through their stop
  channel, used once. An attempt already being stopped (cancelled by the master, or past its
  `execution_timeout`) has used it, keeps the cancel grace (30 s) before SIGKILL, and can
  outlive the pod's deadline: the drain doesn't finish, the worker isn't deregistered (the
  master marks it lost later), and a timed-out task ends `lost` rather than `timed_out` (a
  cancelled one still ends `cancelled`). Both use a retry or none as before, so the effect is
  mostly the missed deregistration.
- **Next:** let a stop escalate (a second reason shortens the grace to
  `drain_terminate_period`), with a test: a task ignoring SIGTERM, cancelled just before the
  drain's time is up.

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
