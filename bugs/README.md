# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that reproduces
it: shown failing before the fix and passing after; git history keeps the record. Finding a
bug doesn't mean fixing it at once: it is logged here and its fix scheduled with the user.
Not part of the documentation site.

**Where they live:** one file per bug, `bugs/B<n>.toml` (its title, rating, the date it was
found, whether it is only suspected, and its details in Markdown); this file is
`bugs/README.md`, and `BUGS.md` is generated from them all (`python3 scripts/bugs.py
render`; `make check` fails when it is out of date). A new bug: `python3 scripts/bugs.py new
"<title>"` writes the next number's file, to fill in, and moves `bugs/next-id` on. A fix
deletes the bug's file. The script's docstring shows a file's fields.

A bug found by reasoning but **not reproduced yet** is logged too, marked **Suspected**, with
how it would be confirmed; once a test shows it, the mark goes. If it turns out not to be a
bug, the entry is removed, saying why in the commit.


## Ratings

Every bug has three ratings, set when it is logged (by whoever finds it; Claude checks them)
and revised when more is known:

- **Severity**, what happens when it occurs: **critical** (a security hole, silent wrong
  results or wrong code run, a task run twice, the whole cluster stuck); **high** (in normal
  operation, with no mistake by the DAG's author, a task or run fails wrongly, stalls, runs on
  after a cancel, or a worker can't work); **medium** (a visible, repeatable failure that
  testing a DAG would catch, slowness growing with scale, a misleading UI or CLI); **low**
  (cosmetic, a message, a test-only flake, an author's type mistake caught at run time
  instead of at load).
- **Probability**, in a real deployment: **likely** (ordinary use or routine events),
  **possible** (a specific but plausible situation: a reconnect at the wrong moment, git-sync,
  a feature combination, scale), **rare** (unusual input, exact timing, extreme sizes, a
  hostile actor). For a security bug: how easily it is triggered, by accident or on purpose.
- **Kind**: security, data, correctness, reliability, performance, usability or tests.
  Security is a kind, not a third score: its worst case is already the severity.

**Score** = severity (critical 4, high 3, medium 2, low 1) × probability (likely 3,
possible 2, rare 1). The ranking below puts **every security bug first**, whatever its score
(the user, 2026-10-30), then the rest by score, then severity; fixes go in its order
unless the user schedules otherwise. The score is computed by the script, never written by
hand; each entry's first line in `BUGS.md` is its rating.

**What gets fixed when** (the user, 2026-10-30): every bug of severity **high or critical** is
fixed (security first, then the ranking), then features go on; a new high or critical bug
interrupts the feature work and is fixed next. Medium and low bugs wait here, fixed when a
step touches their code or the user schedules them.
