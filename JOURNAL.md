# schedr: Journal

How schedr's design goes on from 2026-11-08: dated entries, newest last, appended and never
rewritten. Each decision taken has an entry with the question, the options, the choice, why and
"Not chosen", and its anchor (`<a id="D<n>"></a>`), which its file `decisions/D<n>.toml` points
to. The history up to decision 366 is `PROJECT.md`, organized by topic and left as it is.
### <a id="D367"></a>2026-11-08: how the project keeps its documents (decision 367)

- **Question:** the agent kept making wrong assumptions as the project grew (PROJECT.md 776 KB, about
  200k tokens, read only in pieces; NEXT_SESSION.md 789 lines; a decision left unrecorded; a claim
  about `schedr dags test` from memory that was false, and a decision built on it). How should the
  project's knowledge be organized for minimal reads, easy tracking of changes and the current stage?
- **Chosen (the user's, option A, all phases before M4.12a):** each question gets one small file:
  `STATUS.md` (where things stand, rewritten, at most 60 lines), `design/<topic>.md` (how a part
  works now: Current, Decisions, Where in code, Open), `decisions/D<n>.toml` (one per decision:
  status, topic, a short summary, where its reasons are) with a generated `DECISIONS.md`, checked by
  `scripts/decisions.py` in `make check`; and **PROJECT.md kept as the history** (the user: humans
  think forward as a continuum): its topic part unchanged up to decision 366, then this Journal.
  Migrated in four phases, every decision's file checked against the unclipped text (a side note:
  a sample would let wrong summaries become the agent's first read). The layout, its tooling and
  routines are kept for every project in the `project-docs` skill (the user's ask), with where the
  skills live and how they are kept.
- **Not chosen:** B, keeping PROJECT.md whole with only an index of decisions and STATUS.md (its
  600-900-line sections stay, amendments keep piling up inside paragraphs); C, only STATUS.md and
  the rule to check code (the decisions stay hard to find); splitting PROJECT.md up by topic or
  reordering it by date (the continuum lost, or meaning shifted by cutting paragraphs apart).

### <a id="D368"></a>2026-11-08: one source for each record, the rest generated (decision 368)

- **Question:** after phase 2, the plan, the decisions' state and the open questions still lived in
  several hand-written places (PLAN.md's rows mixing the plan, the criterion and the state; each
  design page's own list of decisions; open questions in PROJECT.md, the design pages and PLAN.md;
  STATUS.md's "next"). Where else could duplication and drift go?
- **Chosen (the user's, all five of Claude's proposals):** structured records with generated views.
  `plan/<milestone>.toml` (steps with scope, "done when", status, date, commits, `part_of`) renders
  `PLAN.md`, whose "next" is the first planned step; a decision names its `step`, and built or
  planned is that step's status, never written by hand; `questions/Q<n>.toml` render into
  `DECISIONS.md` and the topic's page; each design page's lists of decisions, code paths and open
  questions, and `STATUS.md`'s top block (milestone, next step, open bugs), are generated between
  marker lines. One script, `scripts/records.py` (`make records`, `make records-check`), replacing
  `scripts/decisions.py`. **Generated views are indexes** (the user): one short line per item,
  enough to know where to go next; the record holds the content.
- **Not chosen:** a hand-written `built` field (it was being filled when this replaced it: the step's
  status already says it, and two places drift); generating the code map and keeping the rules in one
  place now (small; later).

### <a id="D369"></a>2026-11-08: decisions point to anchors (decision 369)

- **Question:** a decision's `history` named a PROJECT.md section, up to 933 lines, found by
  grepping the number, which early answer codes ("(30A)") make hard.
- **Chosen (the user's):** an anchor `<a id="D<n>"></a>` at the start of each decision's main passage,
  `history = "PROJECT.md#D<n>"` (or `JOURNAL.md#D<n>`); the check fails on a missing anchor. Placed
  by a script from the extracts, the weak matches read by hand; removing the anchors gives back the
  text exactly.
- **Not chosen:** keeping section names (slow to find); the section and the passage's first words (a
  reworded sentence would break it silently).

### <a id="D370"></a>2026-11-08: JOURNAL.md from now on; PROJECT.md left as it is (decision 370)

- **Question:** decision 367 kept PROJECT.md as the history and appended a Journal to it, and the
  restructuring rewrote PROJECT.md's opening lines to say so: that changed the history instead of
  adding to it.
- **Chosen (the user's):** the journal is a file of its own, `JOURNAL.md`; PROJECT.md goes back to
  its own text and gets nothing more but the anchors (added the same way as in the journal). Every
  new decision goes to JOURNAL.md.
- **Not chosen:** a Journal section at PROJECT.md's end (the first approach).

### <a id="D371"></a>2026-11-08: work reaches main through git; a guard against undone commits (decision 371)

- **Question:** B190: M4.11f's commit silently undid all of M4.11g, tests included, with CI green,
  most likely because a worktree started before M4.11g was copied over main.
- **Chosen (the user's, B190 fixed first):** work from a worktree comes to main only through git
  (merge, rebase, cherry-pick); a colleague's edits to records field by field; `git diff --cached
  --stat` read before every commit. `scripts/undone.py`: `make undone-check` (in `make check`) fails
  when uncommitted changes remove over 20 lines the last 10 commits added and don't add them back;
  CI runs it on each pushed commit; a meant removal says `Undoes: ...` in its message. A scan of the
  whole history found no other lost work.
- **Not chosen:** comparing with the last commit only (a copy over a main two commits ahead would
  pass); a review of the re-applied M4.11g (the user: it was reviewed when built).
