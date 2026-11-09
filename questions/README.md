# questions/

One TOML file per question not decided yet (or since decided), `Q<n>.toml`; `next-id` holds the
next number. The format is in `scripts/records.py`'s docstring; `python3 scripts/records.py
question "<title>"` starts one. Open questions are listed in `DECISIONS.md` and on their topic's
design page; a decided one keeps its file with `status = "decided"` and `decided_by`. One that
waits for a later step, milestone or someone asking is `status = "deferred"` with `until` (a
step or milestone ID, "if asked", "later"): out of the open list, listed apart in `DECISIONS.md`.
