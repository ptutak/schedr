# decisions/

One TOML file per decision, `D<n>.toml`; `next-id` holds the next number. The format and the checks are in
`scripts/records.py`'s docstring; `make records` writes `DECISIONS.md` and the other views, `make records-check` fails on an
invalid file, a missing number, a decision named but not recorded, or a stale `DECISIONS.md`. A decision's reasons
and "Not chosen" are in `JOURNAL.md` (`archive/PROJECT.md` up to D366; its `history` names the anchor), never here.
