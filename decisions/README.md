# decisions/

One TOML file per decision, `D<n>.toml`; `next-id` holds the next number. The format and the checks are in
`scripts/decisions.py`'s docstring; `make decisions` writes `DECISIONS.md`, `make decisions-check` fails on an
invalid file, a missing number, a decision named but not recorded, or a stale `DECISIONS.md`. A decision's reasons
and "Not chosen" are in `archive/PROJECT.md` (its `history` says where), never here.
