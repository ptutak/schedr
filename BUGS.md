# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that would have
caught it; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

## B1: `dags test` shows a negative duration

- Found: 2026-10-22, while checking the example from PROJECT.md (Native API) before 1.15b.
- Symptom: the table's DURATION column showed `-0.8s` for `load` (the run succeeded).
- Repro: save the example as `sync.py`, then
  `schedr dags test sync_customers --path sync.py --for 2026-10-15T01:00:00Z`.
- Cause (reviewed 2026-10-22, M1 wrap-up): the master stamps `started_at` and `ended_at`
  with its wall clock when it handles `Started` and `Finished` (in order, under its lock), and
  the table subtracts them; a negative value means the system clock stepped back in between
  (WSL2 resyncs its clock with the host's, which is where it was seen; it didn't reproduce
  since). The agent already measures each attempt with a monotonic clock and sends it as
  `duration_ms` in `Finished`, but the master doesn't store it.
- Fix proposed: store `duration_ms` with the attempt (`0001_initial.sql` is still edited in
  place), show it in the API's attempt view, and use it in `dags test`'s table and
  `run_dag`'s `Attempt.duration`; the timestamps stay for "when". Test: a fake clock that
  steps back between `Started` and `Finished`.
- Plan: not scheduled yet.
