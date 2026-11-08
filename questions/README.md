# questions/

One TOML file per question not decided yet (or since decided), `Q<n>.toml`; `next-id` holds the
next number. The format is in `scripts/records.py`'s docstring; `python3 scripts/records.py
question "<title>"` starts one. Open questions are listed in `DECISIONS.md` and on their topic's
design page; a decided one keeps its file with `status = "decided"` and `decided_by`.
