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

### <a id="D408"></a><a id="D409"></a><a id="D410"></a><a id="D411"></a><a id="D412"></a><a id="D413"></a><a id="D414"></a><a id="D415"></a><a id="D416"></a>2026-11-09: notifications' open points (decisions 408-416)

- **Question:** before M4.13, what archive/PROJECT.md's design (Outcomes, failures and joins:
  notifications) leaves open.
- **Chosen (the user's, all as recommended):** 408, all eight events now (not `RUN_FAILED` and
  `LATE_CLEANUP_FAILED` alone); 409, the enum `Notify` (not `NotifyOn`, `NotificationEvent`);
  410, an outbox written with the event, delivered at least once and retried until it gives up
  (not sent from memory), and, the user asked, "after sending we clear it?": yes, a delivered
  row is deleted, one given up on logged, counted and deleted; 411, SMTP through `lettre` (not
  a client of our own); 412, a webhook's fixed JSON with a `text`, headers from the
  environment (not an HMAC signature, not JSON without `text`); asked whether a webhook takes
  any content: the receiver decides, so Teams, PagerDuty and Opsgenie go through a relay, and
  the user kept the generic webhook (not formats per service now); 413, `[notifications]
  ui_url` (not taken from the UI's settings); 414, DAG-defined `EmailChannel`s as designed (not
  server channels only); 415, no throttling (not N an hour per channel); 416, a job's `notify`
  stays refused (not per-job notifications).
- **Open:** the user's own email service takes a POST: an HTTP transport for email, beside
  SMTP, once its API is known.

### <a id="D417"></a><a id="D418"></a><a id="D419"></a><a id="D420"></a><a id="D421"></a><a id="D422"></a><a id="D423"></a>2026-11-09: webhook bodies, DAG webhooks, notifications from tasks (decisions 417-423)

- **Question:** the user's email service takes a POST of its own shape, plain or with a template
  stored in its database; the user wants to define the POST's content, then webhooks defined in
  DAGs, then to use a DAG's channel from its tasks and scripts.
- **Asked along the way:** "shouldn't we use python templates for this?": callbacks would run
  user code per notification (D410's outbox undone), `string.Template` adds nothing, Jinja
  (minijinja) gives loops at a new dependency; the user then proposed t-strings (`t"{var}"`,
  PEP 750; schedr requires Python 3.14): evaluated when the worker parses the file, into text
  parts and references, filled by the master. The user asked whether workers send them: no, the
  master sends every notification; the worker only turns the t-string into data.
- **Chosen (the user's, all as recommended):** 417, a body template, t-strings in DAGs and
  `"{name}"` strings in the config, D412's fixed JSON the default (not Jinja, not callbacks, not a
  fixed shape the service adapts to); 418, DAG webhooks within `dag_urls`, no secret headers (not
  `Secret(...)` headers the admin allows, not any URL), and redirects not followed (Claude's
  default, shown to the user); 419, a server webhook used as configured (not a DAG's body over
  it); 420, `timeout` a public key (not hidden); 421, `send`/`api.notify`/`schedr task-ctx
  notify` with `text` and `data`, in M4.13a, commands included (not a whole `body=` from the
  call, not commands later, not a step of its own); 422, a cap per attempt (not none); 423,
  paths into `data`, a missing field refusing the call, null in event notifications (not the
  whole `data` only, not refused at validation).
- **Corrected (a side agent's note, the user's choice A):** 418 first said a DAG webhook's
  secret goes in its URL; but a Slack or Teams webhook URL is the secret, and the URL would sit
  in the DAG file and its JSON. DAG webhooks are for URLs that aren't secret; secret ones are
  server webhooks (not the master reading secrets under a prefix, which gives up "the master
  never sees a value"; not dropping DAG webhooks).
- **Open:** the email service's request shape, for M4.13b (it fits 417's bodies); closed by 427.

### <a id="D424"></a><a id="D425"></a><a id="D426"></a><a id="D427"></a><a id="D428"></a><a id="D429"></a>2026-11-10: the notification config's layout, email in the config only (decisions 424-429)

- **Question:** before the night's M4.13b, its decisions asked ahead: email over HTTP, the switch
  for DAG channels (D414's `[notifications.email] dag_channels` against D418's), several
  recipients, the email's look.
- **Along the way:** the user asked "Can't we just create PythonAPI version of it?" for the email
  service: a `WebhookChannel` whose body is the service's request (427). On the switches: "a
  type= webhook, type=email inside to know which one is which", then "one email channel with
  type=email, and multiple other channels with type=webhook", then "multiple email channels
  (per-name) and webhook channels" (424). Asked what `dag_channels` is, then "we shouldn't allow
  defining EmailChannel in code. It's a standard, no need to have a class" (426, superseding 414);
  asked why `dag_urls` restricts: the master posts, from a more privileged place than a worker
  (SSRF), so a list empty by default (425, superseding 418; no `dag_channels`).
- **Chosen (the user's):** 424 channels with a `type` (not a table per kind); 425 `dag_urls` alone,
  empty by default (not any URL until set, not none); 426 email in the config, one email to all
  in To (not one each), plain text (not text and HTML); 427 a webhook for the email service (not
  a class of its own); 428 a 4xx dropped at once but 408 and 429 (not retried for a day); 429
  `[notifications] ca_file` (not the system's trust store only).

### <a id="D430"></a><a id="D431"></a><a id="D432"></a><a id="D433"></a><a id="D434"></a><a id="D435"></a>2026-11-10: retention's open points (decisions 430-435)

- **Question:** before M4.14, what the archived Retention design (archive/PROJECT.md) didn't
  cover: it predates backfills, chained runs and named events, and D232's hidden timings.
- **Along the way:** reading the scheduler showed that with `catchup` it creates runs after the
  newest scheduled run, so keeping only "the latest run of each DAG" could re-create a catchup
  DAG's old runs; and that `runs.parent_run_id` is a foreign key a deleted parent would break.
- **Chosen (the user's):** 430 the three newest runs kept (not the newest only); 431 a child's
  parent link cleared (not the parent kept, not deleted together); 432 the interval and batch
  hidden (not public keys); 433 on by default, as archived (not off until set); 434 named events
  with `events` (not a key of their own); 435 410 Gone for a deleted output (not 200 with a
  flag, not 404).

### <a id="D436"></a><a id="D437"></a><a id="D438"></a><a id="D439"></a>2026-11-10: per-task memory, M4.15's open points (decisions 436-439)

- **Question:** asked ahead, before working through M4.15 unattended (the user: "you can ask me
  about M4.15 if it's small and then work until end of M4.15"): the API names, placing a big
  budget, which rlimit, and what to do with gaps in the upgrade rules.
- **Chosen (the user's):** 436 `memory=` and `hard_memory_limit=` (not `memory_budget=` and
  `setrlimit=`); 437 within a worker's pod limit, as archived (not memory as a resource); 438
  `RLIMIT_DATA` (not `RLIMIT_AS`); 439 clear gaps fixed, choices stop the step (not a report
  only).

### <a id="D440"></a>2026-11-10: what retention keeps beyond the three newest runs (decision 440)

- **Question:** Codex's review of M4.14 showed D430's three newest runs aren't enough: with
  `catchup`, backfilled fire times ahead of the newest scheduled run are skipped only while their
  runs exist; a `TriggerDagOperator` waiting for a deleted child starts it again; a sensor's retry
  gets the run it was handed, gone. A side note to the user then pointed out a DAG never
  scheduled (paused, only backfilled).
- **Chosen (Claude's, to be overruled by the user):** keep those runs too: what catchup counts as
  done (after the newest scheduled run; all successful backfills while there is none), and what
  something unfinished still reads. Not chosen: storing the scheduler's position apart from the
  runs (a bigger change to the scheduler), documenting the re-runs as a consequence.

### <a id="D441"></a>2026-11-10: a DAG that no longer parses after a major upgrade (decision 441, Q26)

- **Question:** M4.15's check of the upgrade rules (D439) found the archived design ("the last
  good version keeps running") at odds with D72 (a file failing to import holds its DAGs).
- **Chosen (the user's):** held, as D72; the reason names the schedr version and says to fix the
  file or roll back. Not chosen: keeping the older parse in use.

### <a id="D442"></a>2026-11-11: one monotonic clock for the cluster (decision 442, B225-B228)

- **Question:** B225 (Codex, batch A's review): an assignment delivered late and journaled
  slowly started on a worker after the master had taken it back and given it to another.
- **Chosen (the user's):** a deadline in the assignment, counted by the worker conservatively
  against the link's delay; no extra round trip. Not chosen: acknowledging `Started` before the
  launch (a round trip per task); answering a late `Started` by stopping the task (both may
  already run).
- **Then, after Grok's three reviews found holes in each step (a budget spent at delivery,
  numbered heartbeats, a hold after a wall-clock jump):** the user: "one monotonic clock for
  all". Master time is the leading master's monotonic clock; every master message carries it
  with its timeline and the latest heartbeat heard (also its wall clock, for people); every
  liveness deadline (an assignment's start, worker loss, parse and connection jobs, a moved
  cleanup's wait) is a master time, and the master's own expiries count on it, never on stored
  wall-clock times. The worker reads master time from when it sent the heartbeat a message
  names (Cristian's bound, less 0.1 % drift, on its boot clock), keeps two heartbeat intervals
  for `Started` (B227), and treats a pause its boot clock didn't count (its wall clock ran
  ahead) as a passed deadline. A lease under four heartbeat intervals is refused at load
  (B226). The B228 hold after a jump was dropped: nothing reads the wall clock for liveness. An
  earlier build's worker ignores the deadline, left with B187 until 1.0.
- **Known limits:** a `Started` held up more than two heartbeat intervals; a master whose
  monotonic clock jumps (some VM migrations); the holes in leadership and incarnations Grok's
  design review found (B230, B231).

### <a id="D443"></a>2026-11-11: a replaced incarnation's lost attempts wait out its lease (decision 443, B230)

- **Question:** B230 (Grok's review of the liveness draft): a worker's new incarnation was
  accepted while the old process might still run within its lease, and the old one's
  unreported attempts were retried at once, so a retry or a moved cleanup could overlap it.
- **Chosen (the user's):** the new incarnation registers at once; the old one's unreported
  started attempts end `lost` only after its lease in master time from the registration, the
  old stream getting no more heartbeat answers. Stored with the attempts, the hold survives a
  master restart, counting afresh. Not chosen: refusing the new incarnation until the old one
  is lost (a fast pod restart would wait a lease); stopping every task at the lease (a network
  blip longer than the lease would kill all of a worker's tasks).
- **Known limit:** at the lease an old process stops only its `cleanup_anywhere` tasks and
  triggers; an ordinary task still running there may overlap its retry, as with a lost worker.

### <a id="D444"></a>2026-11-11: a new leader waits out the deposed one; workers fence older epochs (decision 444, B231)

- **Question:** B231 (Grok's review of the liveness draft): a deposed master still answering
  heartbeats kept a worker's lease alive past what the new leader counted.
- **Chosen (the user's, A + C):** the new leader counts a worker it hasn't heard from lost only
  after its leading began plus a lead time (`lease_period - query_timeout`, as long as the old
  leader may still believe it leads) plus the worker lease; and every master message carries
  the lease's epoch, a worker refusing a lower epoch than one it has seen (it reconnects).
  Not chosen: the lead clock counting suspend (`CLOCK_BOOTTIME`), which covers only a paused
  master; fencing by timeline (unordered).
- **Then (a side agent's note, the user's choice):** a database restored from a backup sets the
  epoch back, and every worker would refuse every master. The fence expires: a worker lease
  without word from its highest epoch (that leader has counted the worker lost by then; a
  deposed master stops answering within its lead time, shorter than the lease), and the lower
  epoch is followed. Not chosen: restarting every worker after a restore.
- **Then (Grok's review, the user's choice):** that holds only while the lead is shorter than
  the worker lease, which a legal configuration could break (a 90 s lease period, 10 s queries:
  an 80 s lead against a 60 s lease); such a configuration is refused at load. Not chosen:
  sending the lead to workers for the fence to wait it out.

### <a id="D445"></a>2026-11-11: the clock step before M4B (decisions 445-452)

- **Question:** after the bug round, how to harden master time (decision 442) instead of finding
  its holes one review at a time; Claude's list of eight ideas.
- **Chosen (the user's, "C7 B, the rest accepted recommendations"; ideas 1-7 as one step before
  M4B):** a simulator on virtual clocks checking four rules (446); the worker's cutoff also on
  master time, never later than before (445); the best sample over a 10-minute window (447);
  clock and fencing metrics (448); B232's skew warning (449); the master taking an assignment
  back two heartbeats after its deadline (450, option B: two intervals, not one); an invariant
  page in `design/`. For M4B: B48 fixed by M4B.3's one snapshot per group (451); a master that
  isn't leading names the leader (452); the takeover gap, a slow `Register` and reconnect storms
  checked by stress 05 and 23, no code; reviews by Grok while Codex is out.

2026-11-11, later (Claude): the first build of decision 446 covers the worker's side only: the
real `MasterTime` against a virtual master (`crates/schedr-agent/tests/clock_sim.rs`), the
master's rules taken from the dispatcher's tests. The core and dispatcher in the simulator,
with restarts and takeovers, are left open for the user to decide (built later, or the record
narrowed). Grok's review of the step: a reconnect forgot the master-time cutoff, the cleanup
gates read only the receipt's, the alive tick didn't recheck it after a suspend, the age gauge
froze between sessions, overlapping stalls in the simulator; all fixed, each with a test.

### <a id="D453"></a>2026-11-11: the cutoff from what the master heard (decision 453, B233)

Grok found, writing the invariants of master time, that a cut one way (the worker's heartbeats
lost, the master's messages arriving) left the worker's cutoff behind the master's later stamps,
while the master counts the worker lost from the last heartbeat it received: a send more than a
grace and a move wait (40 s by default) after that left a `cleanup_anywhere` task running beside
its moved cleanup. The user chose to count the cutoff from the send of the last heartbeat a stamp
names as heard, over the master counting from its own sends. With answered heartbeats the new
bound and the stamp's are within milliseconds; the simulator now loses heartbeats one way and
checks the worker against the master's loss, and fails without the bound.

### <a id="N2"></a>2026-11-11: B184 met; the stress comparison before and after M4B (note 2)

Stress run on this laptop, the code before M4B's batching (`0da885f`) and after (`afb1b9e`),
one after the other (the guide, Stress tests, After M4B). Stress 23 records its 500 ends with
p99 4.8 s (26.3 s before): decision 396's bar for B184, met; B184 closed. Throughput rose by
half on Postgres. The time to a start rose (assigned → started p99 in scenarios 9 and 13,
stress 23's `all_in_s`), within an overlapping range over four reruns: a `Started` waits for a
whole turn of `Finished`s. Not fixed before 1.0.0: the user put the start path into M5 as its
first steps, profile first (M5.0a-e).

### <a id="D454"></a>2026-11-11: images on glibc beside Alpine (decision 454)

The user, preparing 1.0.0: release x86_64 images on glibc alongside the Alpine ones. Asked
which, how named and on what: all three images, glibc the plain tag and Alpine `-alpine` (as
the official Python images name theirs), on `python:3.14.8-slim` (Debian). The chart's default
tag is then glibc. Not chosen: base and slim only; a suffix for glibc (nothing changing for
0.1.0's users); separate image names; Ubuntu; distroless.
A side note then: Debian 13's PostgreSQL client is 17, and `pg_dump` 17 refuses a Postgres 18
server, while the Alpine image has 18. The user chose PostgreSQL's own apt source
(apt.postgresql.org, its key pinned by hash) over copying the binaries from the `postgres:18`
image or documenting the gap.

### <a id="D455"></a>2026-11-12: a lost worker's unstarted attempts wait as a cleanup would (decision 455, B234)

The master's side of the clock simulator (R1.0b) found, at 60 scenarios with every safeguard on,
a worker cut off both ways just after an assignment reached it: it started the task, its
`Started` held up; the master counted it lost a lease later and queued the attempt again at once
(B17's rule: never started there), and the task ran on the other worker while the first, cut off
at the same moment, was still stopping it. The user asked for the fix; Claude chose the narrow
one: only tasks with `cleanup_anywhere` (the ones a worker cut off stops) wait, as long as a
started one's cleanup waits before it moves; ordinary tasks run at least once anyway (decision
252) and are taken back at the loss as before. The simulator's smallest scenario is now a test
of its own, and the simulator runs 600 fresh scenarios nightly (the user's suggestion). Grok's
review of the fix found two more ways to the same overlap, both fixed with tests: a new
incarnation registering before the lease left the old one's unstarted attempts on the start
timeout (the worker never lost), and a restart forgot the wait (a cancel grace over the lease
plus two heartbeats then too long); a scenario now runs with the defaults' ratios too.

### <a id="D456"></a>2026-11-12: from 1.0.0 the schema and the protocol change only additively (decision 456)

Decided by the user on 2026-10-29 with the 1.0.0 plan (`plan/1.0.0.toml`), recorded at the
release: decision 156's edits in place end with 1.0.0. Schema changes become new migrations,
the protocol is 1.0 and grows only additively within 1.x, and DAG JSON, the REST API and the
configuration keep what they accept. The protocol's major moving from 0 to 1 refuses a 0.1
worker under a 1.0 master, which closes B187 (the user, 2026-11-11: "Protocol 1.0"); the lesson
stays in the rule: a later minor's field an older worker would drop goes only to workers that
know it.

### <a id="D457"></a>2026-11-12: the UI names a DAG's trigger kind, shows descriptions, and has no Pause for a DAG without a trigger (decision 457)

The user, looking at the UI before 1.0.0. A DAG without a trigger showed a greyed-out Pause,
but a working Unpause while paused (paused before new code dropped its trigger, say): "the best
would be - remove those buttons entirely for DAGs without triggers". For the demo and the end
user, a DAG's `description`, already shown on its page, shows in the list too. And "Schedule"
became "Trigger", naming the kind, "Schedule is one of it": the user chose the plain names
(Schedule, Run of, Chained, Event, Manual) over the API's (Cron, Every, Once, RunOf, None).

### <a id="D458"></a>2026-11-12: a sensor poke tolerates overruns of `every` (decision 458, supersedes 322)

Two integration tests failed under a loaded check (B238): their poke ran every 100 ms, and one
call past it failed the run, as decision 322 said. A side agent pointed out the same would fail
real runs on a busy machine. The user chose to tolerate a few overruns rather than fail at
once, the count a public key beside `switch_off_after` (`[watchers] poke_overruns`, 3), and
added a hard limit: "a hard time limit to reach await mainly so not to block the loop, but also
a hard-limit on one poke-overrun I would say 3xevery interval". The first is `[watchers]
block_limit`, already there; the second is the cancel at three times `every`.

### <a id="D459"></a>2026-11-12: a DAG's trigger has its own row with its switch (decision 459)

A side agent noted that only the UI showed a DAG without a trigger as `manual`: `dags list`
still had PAUSED, and pause and unpause still took such a DAG. The user asked why a manual DAG
has a pause state at all, and then what "manual" (or a dash) would mean for a DAG with a
trigger: "the model is wrong". Pausing switches a trigger off, so the switch is the
trigger's, not the DAG's: "a different table called TRIGGER with one field PAUSED (bool) and
Foreign Key to the DAG". Its rule, chosen with the user: a trigger that appears, or changes
kind, starts paused (so an unpaused manual DAG gaining a cron in a later version doesn't fire
the moment it is deployed); a cron edit keeps the switch. The existing `triggers` table keeps
an event trigger's bookmark past its removal, a different lifecycle, so the switch has a table
of its own, `dag_triggers`. The user: "do everything now, there is no rush, let's do 1.0.0
properly", in 0001 (only rc1's test databases exist), `trigger_state` replacing `paused` in the
API, STATE in `dags list`, the UI's kinds; then rc2, a mandatory step.

The user also asked to rename the tables so their names say what they hold: an event
trigger's progress (bookmark, worker, errors) is `trigger_progress`, what it passed
`trigger_progress_passed`, beside the switch's `dag_triggers`. Building it, the DAG list first
lost its State column with only `paused` in the Trigger cell; the user wanted the trigger's
kind and whether it is paused visible, "it could be one column though": the Trigger column
shows the kind, the trigger and an `active` or `paused` badge, nothing for a manual DAG.

### <a id="D461"></a>2026-11-12: a DAG page's actions in one row; Restart where it does something (decision 461)

Reviewing the UI after decision 459, the user saw a schedule's Trigger, Backfill and Unpause in
one row and an event trigger's Restart in a row of its own, and Restart offered while paused,
where it changes nothing until Unpause. Both went to one row, Restart only for a trigger stopped
or failing. A side agent pointed out that Restart also restarts a running trigger, the fix for
one hanging on its worker; the user: "show restart when the trigger is 'stuck'", in addition to
failing or stopped. Only a poke trigger can be told stuck: its calls end within three times its
`every` (decision 458) and are reported within a heartbeat. A listener may be quiet for hours,
and `restart_if_idle` restarts it already.

### <a id="D460"></a>2026-11-12: both sides number their messages and echo the other's (decision 460, B240; supersedes 450 and 455)

The nightly simulator's first random runs of the master's side (R1.0b) found B240: a worker's
messages up cut while its `AssignTask` still arrived; it started the task, its `Started` held up,
and the master, losing the worker, ended the attempt `assignment_expired` (never started) and ran
it again beside the first, uncounted. Decision 442 had listed the limit ("a `Started` held up
more than two heartbeat intervals"). The master couldn't tell a heartbeat the worker sent before
an assignment from one sent after: it echoed the worker's heartbeat numbers, but the worker
echoed nothing. The user chose to fix it properly: every message both ways numbered and the
other side's latest echoed ("monotonic clock" in the user's words meant this sequence with an
echo, not a clock reading, which took a few rounds to see), then to simplify what master time
had decided by timeouts: no start deadline, an assignment taken back only once the worker's
messages show it never started it, a reconnecting process's journal deciding, a new incarnation
holding what it may have started; the protocol 2.0, so an rc1 worker is refused. All before
1.0.0: "more important right now, before 1.0.0 is to make things right, not to postpone them".

A moved cleanup follows the same rules: while assigned and not started (`moving`), it counts as
not started on its new worker, whatever its task did before; shown never started there, it waits
for another worker again with no move used, otherwise it is held or moved on like a started one.
The lease's lower bound of four heartbeats stays, its reason now only that a few lost beats
don't lose a worker.

### <a id="D462"></a><a id="D463"></a><a id="D464"></a><a id="D465"></a><a id="D466"></a><a id="D467"></a>2026-11-13: metrics in the UI (decisions 462-467)

After 1.0.0 the user set the next phase: M5.0's performance work, then metrics in the UI. What
existed: every process exports Prometheus metrics on its ops port, which the UI never reads (no
process records that address); the attempts keep their timestamps, peak memory and CPU for 90
days; nothing records a process's memory over time, and no one records a container's limit.
D386 rules out OpenTelemetry.

Claude proposed three layers: a Status block on the Deployments page (the user's idea of
2026-11-12, from what that page already fetches), history from the attempts on the DAG page, and
a Metrics page over a samples table the master writes. Reading the code corrected the idea's
note: the page's `GET /api/v1/deployment` carries no slots, so the block needs each worker's
`slots` and `running` added there (the Workers page's endpoint has them). The user asked for
memory per node, per master and worker, "and maybe even more, in some kind of a chart"; then
chose layers 1 and 3, asked what layer 1 needs (only that additive API change), and accepted
every recommendation and the layout:

- Depth (decision 463): processes, the container's limit from the cgroup (what gets a pod
  killed), and a worker's memory split into agent, fork servers, watcher hosts and tasks. Not
  chosen: processes only.
- History (decision 464): a sample a minute for 24 h, 15-minute averages and peaks for 30 days.
  Not chosen: one a minute for 7 days (more rows, shorter reach).
- CPU beside memory (decision 463): the same path at almost no cost.
- A task's peak memory over its attempts (decision 467), from data already stored: it shows a
  task creeping towards its limit before an OOM kill.
- Charts drawn by the server as SVG (decision 466), as the DAG graphs are: no JavaScript library,
  htmx and the live stream re-render them. Not chosen: uPlot (hover and zoom, but a vendored
  library and charts drawn in the browser).
- No Grafana dashboards in the Helm chart for now.
- Part of M5 (steps M5.1-M5.6), after M5.0's performance work and before the Airflow layer, so
  the Metrics page can show the start's stages M5.0a times. Not chosen: a milestone of its own;
  before M5.0.
- Placement (decision 465): history where it belongs (the task panel, the DAG page), the
  deployment-wide figures on a new Metrics page after Deployments. Not chosen: everything on
  one page.

The UI scraping each process's `/metrics` was not chosen: it needs every process to record its
ops address, and gives the present only. Runs and tasks by state stay off the Status block,
which describes a deployment's health; they belong to the Metrics page.

### <a id="D468"></a>2026-11-13: one inbox for all workers (decision 468); M5.0 reordered

Grok's study for M5 (session d12a2612) ranked the start path's costs: the worker's start journal
written one record at a time, each a file and a directory fsync awaited on its loop (about 4x
in groups on its disk); the master's core lock held for a worker's whole turn; per-item
statements inside the round's and the finishes' transactions. It also found stress 23's
`all_in_s` counting a fixed 5 s sleep and up to 5 s of a gauge's lag. The user reordered M5.0:
profile and fix that clock, the journal in groups, then the lock, the pooler, the turn size, and
the idle cost (a standalone measured at about 22 queries a second, 1.9 % of a core and 0.6 GiB
written a day with nothing to do).

Going through M5.0c, the user asked what else fights for the lock with one master. The code has
one `Mutex<MasterCore>` for the whole master: each worker's turn takes it, and so do the round,
registrations, stream ends and parse uploads; the REST API doesn't (its writes are commands the
round picks up) and heartbeats are answered before it. A `Started` waits for its own turn's
`Finished`s, for other workers' turns, and for a round. Reading on, `Started` carries no time:
the master records `started_at` when it handles the message, so the regression after M4B is
mostly the recording's lag, the tasks themselves starting no later (the worker launches right
after sending `Started`). Claude proposed the worker stamping its start time; the user: nice to
have, it won't improve the performance (planned as M5.7), and chose releasing the lock between
groups plus `Started`s first across workers (A+B). Then the user asked: instead of one worker's
messages per turn, gather all workers' messages and process them together. That is B's general
form: fewer commits and lock acquisitions under load, set-based statements across workers,
heartbeats in one statement; its costs are a failed phase reaching every worker (redone worker
by worker, then one by one), barriers and stream identity kept per worker, fairness by a limit
per worker, and a longer hold (the lock released between phases). Chosen as M5.0c, with A+B as
the fallback if M5.0a shows other workers' turns cost nothing. Not chosen: a turn per worker;
`Started` recorded outside the core lock (brittle); the round shortened (left to M5.0d).

### <a id="D469"></a>2026-11-13: a round lets go of the core lock between its batches (decision 469)

M5.0a's profile (stress guide, Where a start's time goes) showed a start's time is the wait for
the master's core lock, and with one worker (scenarios 9 and 23) there is no other worker's turn
to wait behind: the round is what holds it, up to 4.1 s in scenario 23 while it assigns 500 wait
steps, a turn waiting up to 6.6 s. D468's one inbox can't help a single worker, so M5.0c's target
for scenario 9 was out of its reach. Claude raised it before reaching M5.0c (the two modes: a
design that doesn't hold goes back to the user) with three options: shorten the round's hold in
M5.0c as well; narrow M5.0c's target to several workers and leave the round to M5.0d; or M5.0d's
set-based round statements first. The user chose the first. Not chosen: the round left to M5.0d
(scenario 9's start p99 would stay out of reach in M5.0c); M5.0d first (shorter statements still
leave the round holding the lock from start to end).

### <a id="D470"></a><a id="D471"></a><a id="D472"></a><a id="D473"></a><a id="D474"></a><a id="D475"></a>2026-11-13: the Airflow migration tool instead of the layer (decisions 470-475); M5 renumbered, 1.1.0 after M5.1

The user set the order: release 1.1.0 after M5.1 (performance and metrics; the steps renumbered
M5.1a-M5.1m, M5.1m, the worker's start time, before the release), then M5.2, the Airflow
migration tool. The archived plan was a look-alike layer (`schedr.airflow`, Airflow 3's names
translated at parse time, migration by changing imports); on 2026-11-12 the user had proposed
converting Airflow DAG files into native ones once instead. Claude took that as a change of
unit, from a runtime layer to a one-off source conversion, and put the layer's decisions back
on the table. The user accepted every recommendation:

- How it reads a DAG (decision 470): a LibCST rewrite of the source, keeping the user's code,
  with each result checked against the original imported through a recording stub. Not
  chosen: running the original to rebuild the DAG (loops unrolled, structure lost), or the
  rewrite unchecked.
- Input (decision 471): Airflow 2 and 3. Not chosen: Airflow 3 only, the layer's choice, since
  most people leaving Airflow are on 2.x.
- What converts (decision 472): the authoring core, then templates, XCom, sensors, chained
  runs and email where exact; the rest under TODO comments and in a report. Airflow `Param`
  becomes native params, so decision 136 (Param with the layer) is superseded.
- Connections and Variables converted from Airflow's JSON exports; provider operators counted
  in the report for a later version (decision 474). Not chosen: a set of provider operators
  now; DAG files only.
- The layer dropped (decision 475); its translation rules become the tool's, and the principle
  stays: the native API isn't changed for Airflow.

The user added (decision 473): the tool should emulate what Airflow has with native
constructs, e.g. the empty resource with a cleanup for a rule like `all_done`. Claude restated
it as "exact emulations by combinations of native constructs" (as the layer allowed) and asked
whether the user's "run_if rule" meant `all_done` or Airflow's `@task.run_if` decorator.

The user answered: yes, and more generally, something in Airflow may translate to something
else in schedr as long as the DAG takes the same paths, even though it is a different entity.
That changed the check of decision 470: it compares the paths a run can take (which tasks run,
after which, for each way their upstream tasks end), not entity kinds, or every emulation would
read as a difference. Decision 473 states the paths as what the tool keeps, with
`@task.run_if(condition)` (a short-circuit in front of the task) as a second example.

Then the user: the tool should have two modes, Airflow 2 and Airflow 3, so it parses without
guessing. Claude agreed and added a stronger reason: some differences aren't in the file at all,
defaults it doesn't state (from memory, to be checked in M5.2b: `catchup` true by default in 2,
false in 3; no schedule meaning daily in 2, manual in 3), and some imports exist in both. Decision
471 now says: `--airflow 2|3`, always given, an import contradicting it an error. Not chosen:
guessing the version from the imports.

### <a id="N3"></a>2026-11-13: M5.1b's 10 s target moved to M5.1c (note 3)


M5.1b wrote a burst's start records as one group, and stress 23's `all_in_s` stayed at 14.5 s
(M5.1a: 14.5 s): the worker's journal wasn't the limit on this machine, the master's rounds
were (50 assignments a round, a round's p99 4.1 s while it holds the core lock), so 500 starts
take about ten rounds whatever the worker does. Grok's estimate that the journal set the floor
didn't hold here: each group's write overlaps the master's next round. Starts in scenarios 9
and 13 improved a little (assigned → started p99 3.09 → 2.65 s, 4.35 → 3.43 s, 3.75 → 2.96 s;
one run each). Claude stopped and asked (the two modes); the user chose to keep M5.1b, its
target rewritten to what it does, and to move the 10 s target to M5.1c, where the round lets go
of the lock (decision 469).

### <a id="D476"></a><a id="D477"></a>2026-11-13: a first start, tried from nothing (decisions 476, 477; B248)

The user asked for a minimal setup in the documentation that starts schedr. Claude ran it first
against the published 1.0.0 wheel, from a clean directory: a `.venv` with `pip install schedr`
and `examples/sync.py` in `dags/`. Three things came up. With no DAG path, standalone watched no
root and said nothing, an empty UI being the only sign (`dags test`'s default of the current
directory is that command's alone). A stopped standalone, Ctrl-C included, kept the master's
lease, so every restart waited it out as a standby (20-30 s): `serve_master` ended on the gRPC
server's clean stop and dropped the master's loop, whose release then ran in a detached task the
process didn't wait for (B248, fixed by awaiting the loop; two start-stop cycles then led at
once). And a side agent asked whether unpausing the example would replay every day since its
`start`: it doesn't, `catchup` is off by default, one scheduled run appeared.

The user accepted the warning (decision 476) and the fix, put the pages in a section of their
own ahead of Writing DAGs (`docs/01-start/`), and asked for PyPI to show that, not the README,
which is about working on the repository (decision 477): `PYPI.md`, its DAG checked against the
example by a unit test.

### <a id="N4"></a>2026-11-13: The liveness redesign drafts retired: settled by D442-D460 (note 4; corrects D442)

The drafts `liveness-redesign.md` and `liveness-one-clock.md` (2026-11-11, never committed; in
the worktree `schedr-grok-liveness`) were reviewed against what was built, and dropped with the
worktree (the user: B). Their decisions became D442-D453 and D460: master time (D442, D447, D453),
the incarnation hold (D443), epochs and the new leader's wait (D444), the cutoff of
`cleanup_anywhere` tasks only (D445, D252), the simulator (D446), and the numbered messages that
replaced a start deadline and the draft's Revoke (D460); Grok's review of them became B230 and
B231, fixed. Left open: user-set deadlines on the wall clock (the draft's decision 6), now Q27; a
formal model of the protocol (the draft's section 9, optional), not planned. Known limits stay as
D442 and the guarantees page state them.

### <a id="D478"></a>2026-11-13: user-set deadlines count on the wall clock (decision 478, Q27)

- **Question (Q27, from the retired liveness drafts' decision 6, N4):** liveness counts on master
  time (D442), but the deadlines a person sets and the master enforces compare a stored wall-clock
  time with the wall clock now: a wait step's `max_wait` (`dispatch.rs`, `deadline`) and a
  resource's `unschedulable_timeout` (`fail_waiting_users`), both checked in the code.
  `execution_timeout` is the worker's own, on its clock, and not in question.
- **Options:** (A) keep them on the wall clock and say so in the docs; (B) count them on master
  time while one master leads, from the stored wall time after a restart or takeover.
- **Chosen: A** (the user). They are set in wall time, and a restarted master can only count them
  from the stored times; a step of the wall clock moving them (forward ends a wait early, back
  stretches it) is shown on the guide's master-clock page, not fixed.
- **Not chosen:** B: two ways of counting one deadline (master time while leading, the wall clock
  across a restart), for a step of the master's own clock that the times people see share anyway.
