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
- Suspect: the attempt's start and finish times come from different sources (e.g. the
  master's clock and the agent's report), or the table subtracts the wrong pair.
- Plan: not scheduled yet.
