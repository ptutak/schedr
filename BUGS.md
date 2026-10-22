# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that would have
caught it; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

## B2: a runner that dies at start gives no hint why

- Found: 2026-10-22, while re-running the PROJECT.md example after the `trigger=` rename.
- Symptom: `schedr dags check sync.py` and `schedr dags test ...` print only
  `error: parsing failed: parse runner: the runner closed the connection` when the
  `[dags] interpreter` (`python3` on `PATH`) doesn't have `schedr` installed (here a Python
  3.13 virtualenv: `ModuleNotFoundError: No module named 'schedr'`, and too old besides).
- Repro: run `schedr dags check <file>` with a `python3` on `PATH` that lacks `schedr`.
- Expected: the runner's stderr (or its last lines) and a hint: which interpreter was used,
  that it needs `schedr` at this version and Python 3.14+, and how to choose another
  (`[dags] interpreter` / `[interpreters]`).
- Plan: not scheduled yet; matters for 1.16 (a fresh `pip install` user hits it first).

## B1: `dags test` shows a negative duration

- Found: 2026-10-22, while checking the example from PROJECT.md (Native API) before 1.15b.
- Symptom: the table's DURATION column showed `-0.8s` for `load` (the run succeeded).
- Repro: save the example as `sync.py`, then
  `schedr dags test sync_customers --path sync.py --for 2026-10-15T01:00:00Z`.
- Suspect: the attempt's start and finish times come from different sources (e.g. the
  master's clock and the agent's report), or the table subtracts the wrong pair.
- Plan: not scheduled yet.
