# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that reproduces
it: shown failing before the fix and passing after; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

## B8: a drained task can outlive the pod's grace period

- **Seen:** 2026-10-23, in the M2.9b review (by reading the code, not in a run).
- **What:** when a SIGTERM drain's time is up (`termination_grace_period` minus 5 s, 25 s by
  default), the agent stops what still runs with `StopReason::Interrupted` through the
  supervisor, which sends SIGTERM and then SIGKILL only after the master's `cancel_grace`
  (30 s), at 55 s. Kubernetes kills the pod at 30 s. A task that takes more than 5 s to exit on
  SIGTERM never reports `interrupted`: the journal still says `running`, and the master ends it
  `lost`, using a retry (a task with `retries = 0` fails).
- **Next:** give the drain's interrupt its own grace that ends before the pod's deadline (e.g.
  the 5 s margin), or SIGKILL at the deadline; a test with a task ignoring SIGTERM.

## B7: a command can be applied twice when its decision isn't stored

- **Seen:** 2026-10-23, in the M2.9 review (by reading the code, not in a run).
- **What:** the master applies a command (e.g. `trigger_run` inserts the run) and then marks
  it decided (`decide_command`) in a separate statement. If the master crashes, or the
  database fails, between the two, the command is still `pending` and the next tick applies
  it again: a second manual run, a second cancel. Likewise a tick that ends between storing a
  cancel (`cancel_requested_at`) and delivering `CancelAttempt` leaves the task running until
  its worker reconnects; and a tick that **fails** (a database error in any later step)
  returns before delivering anything, so every message it had built is dropped: a cancel
  already decided `applied` is never sent while the worker stays connected (found again in
  the M2.9b review; assignments recover by expiring).
- **Next:** apply and decide in one transaction (the scheduler's and dispatcher's writes
  inside it), and resend the `CancelAttempt` of tasks marked for cancel on every tick (not
  only on reconnect). M2.9 decided not to abandon slow ticks partly because of this.

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
