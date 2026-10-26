# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that reproduces
it: shown failing before the fix and passing after; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

A bug found by reasoning but **not reproduced yet** is logged too, marked **Suspected**, with
how it would be confirmed; once a test shows it, the mark goes. If it turns out not to be a
bug, the entry is removed, saying why in the commit.

## B22: a draining worker once missed its 8 s grace under load

- **Seen:** 2026-10-26, one `make check` while other role tests were running.
  `roles::a_draining_worker_stops_a_task_already_being_cancelled_within_the_grace_period`
  (`crates/schedr/tests/roles.rs`) panicked: `terminate_within` for the worker, grace 8 s,
  was not success ("the worker didn't stop within the grace period"). 37 role tests had
  passed; that one failed and `make check` exited 2.
- **Not reproduced** on the next full `make check` (1257 Rust unit tests, 662 Python unit
  tests, 99 Rust integration tests, 27 Python integration tests, then `make docs`).
- **Suspect:** the 8 s grace under load, the same kind of limit as B21 (a bound that is
  long enough until the machine is busy). Not fixed.
- **Next:** if it happens again, look at the grace and what the worker was doing in those
  8 s. One miss is not enough to change the limit.

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
