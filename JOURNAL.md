# schedr: Journal

How schedr's design goes on from 2026-11-08: dated entries, newest last, appended and never
rewritten. Each decision taken has an entry with the question, the options, the choice, why and
"Not chosen", and its anchor (`<a id="D<n>"></a>`), which its file `decisions/D<n>.toml` points
to. The history up to decision 366 is `archive/PROJECT.md`, organized by topic and left as it is.
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

### <a id="D372"></a>2026-11-09: PROJECT.md moves to archive/ (decision 372)

- **Question:** with JOURNAL.md holding every decision since 2026-11-08 and the design pages
  saying how schedr works now, where does the old design log belong?
- **Chosen (the user's):** `archive/PROJECT.md`, moved with `git mv` (its history follows it), its
  text unchanged; every pointer to it rewritten (decisions' `history`, code comments, docs, plan
  notes, the AI files); what records history as it happened (`history/`, CHANGELOG.md, the
  journal's entries) left as written.
- **Not chosen:** keeping it at the root (it reads as current; the first file a newcomer opens).

### <a id="D373"></a>2026-11-09: design pages generated from the decisions (decision 373)

- **Question:** the design pages' Current was written by hand from the decisions' summaries, and
  restated them: two texts to keep in step, which drift. Can more of the page be generated?
- **Chosen (the user's):** each decision gets an optional `section` (the subheading it goes under);
  `scripts/records.py` writes each topic page whole: Current (the topic's active decisions'
  summaries by section, the state shown unless built, then the other topics' decisions naming it in
  `also`), Replaced (superseded, dropped, untraced), the code list (`path` or `path::symbol`, the
  symbol checked to be in the file), Open, and Details (the archive/PROJECT.md sections and
  JOURNAL.md entries the anchors are in). The pages are for the agent, an index into the decisions
  and the code; the reader's documentation is `docs/` (the user: "this is only for you"). A summary
  is now written to be read on its page. The sections came from the drafted pages' subheadings (a
  script) and, for topics without a page, were grouped by Claude. `development.md` keeps its
  hand-written working rules, with generated blocks.
- **Not chosen:** hand-written Current pages checked against the decisions (the first drafts: 20
  pages, already drifting; kept on the local branch `design-drafts`); a hand-written Overview per
  page (15 lines each: drifts again, and the reader doesn't read these pages).

### <a id="D374"></a>2026-11-09: claims nobody found in the code (decision 374)

- **Question:** 13 Sonnet agents checked every active decision's summary against the code; 22
  summaries were wrong (corrected), two pointed at code problems, and 13 claims nobody could find
  in the code. How are they kept, so they aren't lost?
- **Chosen (the user's, Claude's recommendation):** the code problems are bugs (B194, B195); the
  claims go in a decision's optional `unverified` list, shown on its design page after the summary
  and listed in DECISIONS.md, cleared when someone confirms or corrects them.
- **Not chosen:** a decision status such as `doubtful` (a status is about the whole decision; one
  unchecked sentence would mark a checked decision doubtful).

### <a id="D375"></a>2026-11-09: summaries follow the code (decision 375)

- **Question:** the check of all 326 summaries against the code found 22 wrong: mostly a name that
  isn't in the code (`master_tasks_waiting`), or code that moved on without its decision (D43, D228,
  D250). What keeps that from happening again?
- **Chosen (the user's, all four of Claude's proposals):** `records-check` refuses a built summary
  naming in code font what no code file has; `records.py affected` lists the decisions a change
  touches, re-read before each step's commit; 401 `path::symbol` entries proposed by Sonnet agents
  (each symbol checked to be in its file; the ones the agents called loose dropped; a spot check of
  20 found 18 right, one loose, one wrong and corrected); every verified decision `checked` today,
  and `records.py stale` lists those whose code changed since. A summary names a key and leaves its
  value to the reference pages.
- **Not chosen:** generating defaults and limits into the summaries (the reference pages already
  have them).

### <a id="D376"></a>2026-11-09: STATUS.md generated (decision 376)

- **Question:** STATUS.md's hand-written part restated what the records already hold (what was
  done, the next step) and drifted: it still called the design pages "next" after they were built.
- **Chosen (the user's):** STATUS.md is written whole by `make records`: the next step, open
  questions, bugs, the latest JOURNAL.md entries and steps done come from the records; only what
  the records can't show is written by hand, in `status.toml` (in progress, waiting for the user,
  the rules in force, colleagues' usage, the resume line).
- **Not chosen:** generating colleagues' usage from the skills' `USAGE.md` (outside the
  repository: CI couldn't check it).

### <a id="N1"></a>2026-11-09: the design drafts deleted (note 1; corrects D373)

- D373's entry says the 20 hand-written design drafts were kept on the local branch
  `design-drafts`. The user: "we don't need those"; the branch was deleted (it was never pushed).

### <a id="D377"></a>2026-11-09: notes in the journal (decision 377)

- **Question:** an event that isn't a choice (the design drafts' branch deleted, contradicting what
  D373's entry says) had nowhere to go: the journal had only decisions, and its entries are never
  rewritten.
- **Chosen (the user's, A):** a second kind of entry, the note (`N<n>`, anchored like a decision,
  no TOML): an event, a finding, or a correction of an earlier entry (`corrects D<n>` in its heading,
  shown on that decision's design page). N1 is the first.
- **Not chosen:** a third kind for corrections (rare; a note naming what it corrects covers it);
  notes without numbers (nothing could link to one, and STATUS.md couldn't list them).

### <a id="D378"></a><a id="D379"></a><a id="D380"></a><a id="D381"></a>2026-11-09: TriggerDagOperator: tests, a missing target, limits, mapping (decisions 378-381)

- **Question:** what M4.12a still needed beyond decisions 357-362 and 365: what the operator does in
  `dags test` and `run_dag`, a target missing when it runs, whether it counts towards the limits
  (the earlier text: as a running task), and mapping.
- **Chosen (the user's, all as recommended):** a real run when the target is in the path tested,
  else the task fails with a hint (378); a missing or conflicting target fails with retries, as it
  may not be parsed yet, invalid params still without (379); not counted towards the limits, as
  `wait_for` nodes, though it has attempts for its retries (380); mapping first chosen, then left
  to M4.12c once Claude found no operator is mappable yet (381).
- **Not chosen:** a stub in tests (the target's behaviour untested); failing at once on a missing
  target (a deploy in progress would fail runs); counting it (a long wait would block the DAG's
  other tasks while holding nothing); mapping only this operator in M4.12a (operators would differ),
  or all operators in it (a much larger step).

### <a id="D382"></a><a id="D383"></a><a id="D384"></a><a id="D385"></a><a id="D386"></a><a id="D387"></a><a id="D388"></a><a id="D389"></a><a id="D390"></a><a id="D391"></a><a id="D392"></a><a id="D393"></a><a id="D394"></a>2026-11-09: the open questions answered (decisions 382-394)

- **Question:** 25 questions were open, most of them ideas parked for later; the user went through
  them one by one, Claude's recommendation first.
- **Decided (the user's):** Q1, one leading master per deployment with standbys, scaled by more
  deployments (D382); Q3, a slim image in 1.0.0 (D383, step R1.0a, against Claude's "full
  image only"); Q5, oldest-ready-first stays (D384); Q6, a worker's interpreters serve all its
  roots, another set is another worker (D385, the user's own answer); Q7, no OpenTelemetry
  (D386); Q8, no isolation after host losses (D387); Q16, a calendar is an event trigger
  (D388); Q18, no per-code retries (D389); Q19, no long-running notification (D390);
  Q20, email and a webhook (D391); Q22, no `by_deadline` (D392); Q23, no `cancel_rest`
  (D393); Q25, `trigger=RunOf` cycles uncaught, documented (D394). Q17 decided by D381
  (operator mapping, M4.12c).
- **Deferred** (status `deferred`, a new status with `until`): Q4, editable and shared limits, to
  M6 (as D95 said; first answered "hidden env in M4B", then revisited in full); Q9-Q14 (LDAP, skip
  or set a state, OIDC, `schedr login`) to M6; Q15, partitioning, to M4B; Q21, mapping wait steps,
  to M4.12c; Q2, NetworkPolicies, if asked; Q24, a manual rebalancing of triggers (the user's idea),
  later.

### <a id="D395"></a>2026-11-09: M4B without a measuring step; 20 messages a turn (decision 395)

- **Question:** M4B (a worker's turn of messages in one transaction, D54) started with a step to
  measure per-turn timings and record a stress baseline (M4B.1).
- **Chosen (the user's):** no measuring step (M4B.1 dropped); the design stays as planned, one
  transaction for all of a turn's messages, and `MESSAGES_PER_TURN` goes from 10 to 20. The
  throughput B184 lacks (500 wait steps ending at once recorded at about 22 a second) should come
  as a side effect, not as the goal. (Claude first wrote this as "M4B is B184's fix"; corrected by
  the user before it was committed.)
- **Not chosen:** measuring first.

### <a id="D396"></a>2026-11-09: B184 fixed in M4B (decision 396)

- **Question:** would M4B (a turn in one transaction, 20 messages a turn) fix B184? Only the
  commit (about 8 of the 15 ms a `Finished` costs on file SQLite) is shared by a turn; the
  statements (about 7 ms) stay per message, and the agent's journal (about 5 ms a result) comes
  next: roughly twice as fast, not enough to call it fixed. (The user had thought `Finished` writes
  were batched already: a turn shares the core lock since B12, but each message is still its own
  transaction.)
- **Chosen (the user's, all four of Claude's additions):** set-based writes for a turn's
  `Finished`s; each run read and settled once per turn; the agent's journal in groups; a target
  (stress 23's 500 ends within 5 s on Postgres) and a unit test counting a turn's statements.
  Steps M4B.4 and M4B.5.
- **Not chosen:** the journal in groups as a step only if stress 23 still missed the target.

### <a id="D397"></a>2026-11-09: a turn's messages in groups, savepoints later (decision 397)

- **Question:** M4B (D54) planned one transaction per turn with a savepoint and memory snapshot per
  message. Is there a cheaper way to get most of the gain?
- **Options:** A, as planned; B, the turn's `Finished`s (and `Started`s) each handled as one batch
  operation in one transaction, failing back to one by one, as T2 does for assignments; C, the whole
  turn in one transaction without savepoints (a bad message costs N+1); D, asynchronous commits
  (unsafe: an ack for a result a crash can lose). Claude found that grouping only consecutive
  messages makes tiny groups with short tasks, so a turn is reordered (`Started`s first; an
  attempt's own order holds; batch-ending messages are barriers).
- **Chosen (the user's):** B with the reordering first (M4B.3, M4B.2's savepoints dropped from it);
  A on top of B later, a savepoint per group (M4B.6), only if stress or real traffic shows it
  matters. 40 messages a turn (the user's; D395 had 20), a hidden setting, with a fallback of at
  most 10 messages redone in a turn so the core lock is held no longer than today. Replaces D54's
  design as the first step and D395's 20.
- **Not chosen:** C (N+1 transactions on a bad message); D (unsafe); more than 40 by default (the
  gain flattens, a fallback costs more).

### <a id="D398"></a>2026-11-09: a TriggerDagOperator starts only a `Chained` DAG (decision 398)

- **Question:** a side agent noticed that a TriggerDagOperator can't start a DAG whose runs come
  from events: each such run needs an event, and the operator gives none, so the task failed when
  it ran, with no warning before. What should happen?
- **Options:** A, refuse at run time and warn in `dags check`; B, the operator passes an event
  (`event_key=`, `event_data=`); C, chained runs exempt from the event rule; D, a target with any
  trigger refused.
- **Chosen (the user's):** "We allow only one way of triggering dags - period." A DAG is started by
  its own trigger (schedule, event, `RunOf`) or by other DAGs' TriggerDagOperators, never both; it
  says the second with `trigger=Chained()`, any DAG, or `Chained(by=[...])`, those named (`by=[]`
  is an error). `trigger=None` means manual runs only, the DAG otherwise off (its Pause is greyed
  out in the UI); manual runs are a fallback and for testing, allowed on every DAG. A target that
  isn't `Chained`, or whose `by` leaves the DAG out, fails the task when it runs, not retried; a
  paused `Chained` target fails it too, with retries (pausing switches its trigger off, as for a
  schedule; Claude's choice of retries: resuming fixes it). `schedr dags check` fails the
  operator's file when its target in the root refuses it (the user's); the master's parse
  doesn't (the user's, option A of a side agent's note: one DAG's edit would stop another DAG's
  file loading, all its DAGs held). A `Chained` DAG's params need no defaults (its runs take the
  operator's); `schedr dags test` unpauses the `Chained` DAGs of the path tested, so D378's real
  run happens (a new DAG is stored paused). It also removes a chained run landing on the same
  `planned_for` as one of the target's scheduled runs.
- **Not chosen:** A (the event problem only); B (a second way to start an event DAG); C (its tasks
  would fail reading `dag_ctx.event`); the check in the master's parse too. Differs from Airflow,
  whose `TriggerDagRunOperator` starts any DAG: a `schedule=None` DAG it starts becomes
  `trigger=Chained()`; one started both ways is split or started by hand.

### <a id="D399"></a>2026-11-09: cycles between DAGs allowed, inside a DAG refused (decision 399)

- **Question:** the archive's text had a TriggerDagOperator fail when its target was already in
  the chain of parent runs (a cycle), and Codex's review of M4.12a found that check bypassed by a
  `RunOf` trigger in the loop (Claude made `RunOf` runs record their parent so the check followed
  it, and logged a loop of two `RunOf` triggers as B198). Should cycles between DAGs be stopped?
- **Chosen (the user's):** no. "We allow cycles between dags, but we prevent cycles inside a
  dag." A cycle can always be built from events (a task posting an event its own DAG's
  `EventNamed` trigger takes), so a check across DAGs can't be complete in practice, and some
  loops are wanted ("the next batch until none is left"). The operator's chain check (with its
  limit of 64 runs) is removed before release, and B198 is no bug. Inside a DAG a cycle stays an
  error at load: tasks depending on each other, an operator naming its own DAG,
  `Chained(by=...)` naming itself. `RunOf` runs keep recording their parent, for the UI's links.
- **Not chosen:** a chain check per creation path (incomplete: events close any loop); a
  run-flood guard (a hidden limit on queued runs, or a metric for long chains), which can come
  later if runaway loops are seen, without forbidding cycles.

### <a id="D400"></a><a id="D401"></a><a id="D402"></a><a id="D403"></a><a id="D404"></a>2026-11-09: backfill's open points (decisions 400-404)

- **Question:** before M4.12b, four points the archive's backfill design (Runs: manual runs,
  backfill and pausing; decisions 363, 364) leaves open.
- **Chosen (the user's, all as recommended):** 400, a fire time whose run is queued or running is
  skipped too, not only a succeeded one (the archive counted only successes, so a day still going
  would get a second run); 401, a backfill's runs start on a paused DAG, as manual runs do
  (pausing switches the trigger off, decision 398); 402, `runs cancel --backfill` cancels its
  queued and running runs, finished ones stay; 403, the run's label says `backfill 3/31` and the
  DAG page lists its backfills with counts by state and a Cancel button.
- **Not chosen:** 400, a second run behind one still going; 401, refused while paused, or created
  and held until unpause; 402, only the queued ones; 403, the label only, progress through the API
  and CLI.
- **404 (the user's, asked which triggers):** any trigger with fire times: `Cron`, `Every`, and
  `Once` too, though it gives one run ("no need to make an exception"). Not chosen: `Cron` only;
  refusing `Once` in favour of `runs trigger --for`.

### <a id="D405"></a>2026-11-09: the scheduler skips fire times another run covers (decision 405)

- **Question:** Codex's review of M4.12b found a scheduled run and a backfill run for one fire
  time: D400 keeps a backfill off a fire time that has a run, but the scheduler looked only at
  earlier scheduled runs. An unpaused DAG's latest fire time backfilled before the pass reached
  it got a second, scheduled run; a paused DAG with `catchup` backfilled, then unpaused, would
  have run its backfilled days again.
- **Chosen (the user's):** the scheduler creates no scheduled run for a fire time with a
  successful, queued or running run of any kind, D400's rule the other way. Covered fire times
  are left out before the free places are counted, so a catch-up isn't held up behind one; the
  covered times are read only when a run is due. A covering run that fails later leaves its day
  without a scheduled run (the scheduler has moved on): "the user will have to rerun manually
  failed runs, the rule stays." A manual run for a fire time stands in for its scheduled run too.
- **Not chosen:** a backfill leaving out the fire times the scheduler will still create (misses a
  paused DAG unpaused later); only a successful run covering (a run still going would get a
  second one beside it); both runs, documented.

### <a id="D406"></a><a id="D407"></a>2026-11-09: operator mapping (decisions 406, 407; Q21 deferred)

- **Question:** M4.12c planned `.partial()`/`.expand()` on every operator, one design (decision
  381), and whether wait steps map with them (Q21).
- **Chosen (the user's):** 406, "We don't support Command, Script operators partial and expand"
  ("the syntax on .expand and .partial for Scripts and Commands was too confusing to keep it"):
  of the operators only `TriggerDagOperator` is mapped; `EmptyOperator` is refused too (nothing
  to vary); Q21 stays deferred, wait steps unmapped ("not yet"). 407, a mapped
  `TriggerDagOperator`, Airflow's form: `TriggerDagOperator.partial("fan", dag="region").expand(
  params=regions)`, `planned_for=` and `.expand_kwargs()` too, over a literal list or an upstream
  output: a chained run per element, each instance's outcome its own. This supersedes decision
  381's `for`-loop hint. Its output (the user's): "consistent with what a single
  TriggerOperator returns, just a list of those": each instance's value in element order (the
  run's ID, or its `Failed` or `TimedOut`); not narrowed to `.on(str)` as a single one's
  `.output` is, since each element has its own outcome (as for a mapped `@task`). Not chosen:
  always a run ID; `None` for the instances that didn't succeed.
- **Not chosen:** element references usable anywhere in a template (`schedr.element("path")`, a
  new reference kind); mapping command and script operators (with shell lines limited to literal
  strings); an `EmptyOperator` mapped over a list, a fan-out that only counts; mapping wait steps
  now, with `TimedOut` routed per element.
