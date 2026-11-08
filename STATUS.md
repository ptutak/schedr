# Status

Rewritten at the end of each session or step (never appended to; at most 60 lines). Start here, then read only what
the step needs: `PLAN.md` (the step's row), `DECISIONS.md` and `design/<topic>.md`, `BUGS.md`. Reasons and history
are in `PROJECT.md`; earlier hand-offs in `history/`.

## Now (2026-11-08, evening)

- **Milestone:** M4 (Native API complete). M4.1 to M4.11 done; **M4.12a next** (`TriggerDagOperator`), then M4.12b
  (backfill), M4.13 (notifications), M4.14 (retention), M4.15 (memory, upgrades). After M4: M4B, then Codex's
  F12-F14 review of the whole repository.
- **In progress:** restructuring the project's documents (the user's, 2026-11-08: option A, all phases before
  M4.12a): `STATUS.md` (this), `decisions/` and `DECISIONS.md`, `design/<topic>.md`, `PROJECT.md` kept as the
  history (by topic up to D366, then a dated Journal); then the `project-docs` skill and `AI_SETUP.md`.
  - Phase 1 (this file, `scripts/decisions.py`, `design/README.md`): committed.
  - Phase 2: `decisions/D1..D367.toml` and `DECISIONS.md`, each checked against the unclipped text by
    a fresh Codex session (sol, high) and corrected (`checked = 2026-11-08`); `decisions-check` in
    `make check` and CI: committed.
  - Phase 3: the `design/` topics. Phase 4: docs build, `AI_SETUP.md`, `AI_INSTRUCTIONS.md`, memory.
- **The `project-docs` skill** (the user's ask): `~/.claude/skills/project-docs`, private repository
  `ptutak/project-docs-skill`: the layout, `templates/` with `init.sh`, the routines, where skills live.

## Waiting for the user

Nothing.

## Rules in force

- Commits and pushes pre-accepted while nothing waits for the user's decision (M4.12 and this restructuring).
- Stress runs only nightly; never started by hand.
- Before stating how something behaves, read the code or run it, and say which.

## Bugs

Open: B185, B187 (`BUGS.md`, ranked). Next number: `bugs/next-id`.

## Colleagues

Codex weekly 36% used (resets Sat 14 Nov 21:47); Grok ~67% of the week used (resets Friday 9:00). Usage logs in the skills' `USAGE.md`.

Resume this session with:

claude --resume e45442ad-3ec7-4bce-9d45-b6191e505a11
