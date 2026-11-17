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

### <a id="D479"></a>2026-11-13: the Python integration tests on several processes (decision 479)

- **Question (the user):** can the integration suites run in parallel? `make check` took 17 min
  alone, 24-26 min beside a colleague's build; the Python integration suite 7 min (409 s), run
  on one process, the Rust one already parallel under nextest (4.3 min).
- **Found on the way:** two tests (`test_concurrency_limits_hold_with_slots_to_spare`,
  `test_a_resource_is_cleaned_once_after_its_last_user`, 40 s each) wrote their logs into
  their own DAG root; every file there counts for the code hash (decision 305), so each write
  was new code and the next tasks were rejected `CODE_MISMATCH` and waited `CODE_RETRY_DELAY`
  (35 s). Shown with `dags test`: 8.7 s with nothing written into the root, about 40 s with the
  log there. The DAGs moved to a folder of their own (79 s to 13 s), and the pitfall documented
  (the guide's parsing page, the tasks page), at the user's word.
- **Options:** (A) `pytest-xdist`, 6 processes, `--dist loadgroup`, the container tests one
  group; (B) A plus the Python suite beside the Rust one; (C) the test fix alone.
- **Chosen: A** (the user). `run_dag` takes free ports and a private directory per run, so the
  tests can run side by side; the connection tests share fixed names in the containers (the
  role and user `odd`, the table `async_rows`, the Mongo database `schedr_test`).
- **Not chosen:** B, for about 1.5 min more against the 12 cores both suites would share and
  the flaky timeouts already seen under load (B245, B247); C, which leaves about 5 min.

### <a id="D480"></a>2026-11-13: the core lock released at the end of each transaction (decision 480; replaces 468)

- **Measured first (M5.1c, round parts and a probe of one assignment, stress 23, SQLite):**
  a round holding the lock for up to 3.3 s spends it in the assign part: of 17.6 s of
  assignment transactions over the run, 10.8 s are the wait-step checks (`ended_before_its_call`:
  `wait_step_strikes` and `first_start`, about 5.9 ms each, 1,840 calls: 500 assigned and 1,340
  visits to tasks past the round's cap of 50), 4.8 s the claim (`assign_attempt`, 9.6 ms each),
  1.5 s the deadline (`keep_deadline`, 3.1 ms each); the rest is negligible. An assignment costs
  about 18 ms (Claude first said 33 ms, read off a histogram bucket). Commands reach 1.6-3.3 s at
  p99 in scenarios 13 and 23.
- **Question:** how the lock is shared between workers' turns and the round. The user asked what
  M4B built (savepoints were planned, M4B.2 and M4B.6, both dropped: a turn's groups are one
  transaction each, a failed group redone one by one) and how a turn orders its messages
  (segments cut at barriers: the `Started`s, then the `Finished`s, then heartbeats and warnings,
  then the barrier alone).
- **Options:** (A) release the lock at the end of each transaction, the turns' groups as they
  are; (B) one turn for all workers: all messages in one inbox in arrival order, grouped as one
  worker's turn is, a barrier from any worker cutting the pile (the user's form of 468); (C)
  decision 468 as written, barriers per worker.
- **Chosen: A first** (the user). Small and safe: no transaction is added, only lock hand-offs
  (FIFO, microseconds); the longest wait falls to one transaction. B is kept for later, built
  if commits per second limit under several workers (scenario 13 measured): it cuts commits from
  N per group kind to one, but a barrier from any worker cuts everyone's group, a failed group
  reaches every worker, and one worker gains nothing.
- **Not chosen:** C, its per-worker barriers more complex than B for the same commits.

### <a id="D481"></a>2026-11-13: the round's assignments set-based; tasks past the cap pending (decisions 481, 482)

- **From the probe (D480's entry):** of 17.6 s of assignment transactions in stress 23, the
  wait-step checks take 10.8 s (1,840 visits, 1,340 of them past the cap), the claim 4.8 s,
  the deadline 1.5 s.
- **Chosen (Claude's proposal, the user agreeing):** the wait-step checks as two queries a round
  for all ready wait steps (482, Claude's: nothing changes for users, the same wait steps end in
  the same round); the claim and the deadline in statements per batch, the one-by-one fallback
  kept (482); the round stops visiting tasks at its cap (481, the user's, as it changes what
  people see): they count as `next_round` and show a pending reason, written in one statement a
  round ("show in the UI something like (Pending state...)").
- **Not chosen:** keeping the visits past the cap (cheap once the checks are set-based, but
  still 1,340 visits); writing each task's real reason every round.
- **Plan (the user):** M5.1c1 builds D480 and these; M5.1c2 measures the commands (1.6-3.3 s at
  p99 in scenarios 13 and 23); then the user decides whether more M5.1c steps are needed before
  M5.1d.

### <a id="N5"></a>2026-11-13: M5.1c2: what a round's commands spend (stress 13 and 23) (note 5)

Probed after M5.1c1 (temporary timers, SQLite, stress 13 and 23; every command a `TriggerRun`),
per command in scenario 23 (50 commands; scenario 13 about twice as slow, its machine busy with
tasks): marked applying 10.0 ms, `active_dags` 10.4 ms (all DAGs read to find one), the nearest
earlier run 4.5 ms, the run inserted 10.7 ms, the running count 4.0 ms, the queued runs 4.3 ms,
the queued runs started 10.7 ms, the decision stored 9.6 ms: about 64 ms a command, no step
dominating. The four writes are each their own commit (a command isn't one transaction); the
reads are 4-10 ms each. Up to 50 commands a round: 3.2 s at most. What to do with it is the
user's decision after M5.1c1 and M5.1c2's review.

### <a id="D483"></a>2026-11-13: a command is one transaction, the lock released between commands (decision 483)

- **Question:** what to do with M5.1c2's numbers (N5): a `TriggerRun` costs about 64 ms in
  stress 23 over eight steps, four of them writes committed on their own (41 ms), and
  `active_dags` reads every DAG to find one (10 ms); 50 commands a round hold the core lock up
  to 3.2 s. Commands are the API's actions (the CLI, the UI, `schedr.api`, tasks calling it);
  scheduled and event runs and worker messages don't go through them, so the cost shows in
  bursts of API actions.
- **Options:** (A) one transaction per command, the lock released between commands (about 35
  ms, a worker waiting at most one command); (B) read only the command's DAG (about 10 ms less);
  (C) a round's commands in one transaction (one commit, about 15 ms a command, but the lock held
  for the whole batch and one bad command rolling back all), with savepoints per command to keep
  a bad one from undoing the others (savepoints not built, M4B.2 dropped; memory snapshots per
  command needed too); (D) leave them.
- **Chosen: A + B** (the user), as M5.1c3: about 25 ms a command, a round of 50 about 1.25 s, no
  worker waiting behind more than one command. Savepoints help only when several commands share a
  transaction, so not here; batches of about 10 commands with a savepoint each are the follow-up if
  commits still show after M5.1c3.
- **Not chosen:** C (the lock held for the batch, against decision 480), D.

### <a id="N6"></a>2026-11-14: M5.1d judged on the recording of a wait step's end, not the pokes' figure (note 6)


M5.1d's baseline after M5.1c3 (stress 23, Postgres): pokes p99 7.75 s (11.1 s after M4B), short
of the step's "under 7 s". A poke checks every 5 s, and the stress run opens its file just after
the pokes checked, so about 5 s of it is the wait for the next check; schedr's own share, the end
recorded, is about 1.8 s, as the `await` steps (checking every second) show. The user's decision:
M5.1d is judged on that recording (`await` p99 less its 1 s check, 2.9 s now) and on no attempt
ended twice, not on the pokes' figure, which a run opening the file mid-cycle could meet with no
change at all. The same baseline counted the statements (`make stress STRESS_STATEMENTS=1`): about
8,000 in 35 s, 2.3 s of the server's time, so the database isn't busy; the work is round trips,
and about 360 of the 500 wait steps' results were sent twice (the stress setup's 1 s heartbeat
resends a result unacknowledged at two heartbeats), each repeat read again and ended to no effect.

### <a id="D484"></a>2026-11-14: a worker's turn takes up to 80 messages (decision 484)

- **Question:** M5.1e: how many of a worker's messages the master takes in one turn (40 since
  M4B.3, `SCHEDR_HIDDEN__MASTER__MESSAGES_PER_TURN`), from measurements. The user left the
  laptop idle and the choice to the numbers ("we'll not argue about that"), rounded to fives,
  raising the size until it got worse, then a value between the best neighbours.
- **Measured** (one run each, stress 1, 13 spawned and forked, 23; `tasks_per_s`, assigned to
  started p99, 23's `await` p99):

  | Size | 1: tasks/s | 1: start p99 | 13 forked: tasks/s | 13 forked: start p99 | 23: `await` p99 |
  |---|---|---|---|---|---|
  | 10 | 28.2 | 0.80 s | 18.9 | 0.53 s | 3.86 s |
  | 20 | 28.1 | 0.71 s | 19.6 | 0.45 s | 2.42 s |
  | 40 | 27.6 | 0.98 s | 19.5 | 0.49 s | 2.03 s |
  | 60 | 27.1 | 0.92 s | 19.1 | 0.48 s | 1.64 s |
  | 80 | 28.1 | 0.77 s | 19.6 | 0.50 s | 1.64 s |
  | 120 | 27.0 | 1.09 s | 18.6 | 1.24 s | 1.54 s |
  | 160 | 27.1 | 0.88 s | 19.0 | 0.50 s | 1.74 s |

  Steady throughput moves within about 4 % at every size (noise); the size shows in bursts:
  500 wait steps ending at once are recorded in 3.9 s at 10 and 1.6 s from 60 up. From 120 the
  starts get slower again (scenario 13 forked 1.24 s at p99), and 160 is no better than 80.
- **Chosen: 80** (Claude's, as the user asked): the burst recorded as fast as at any size, no
  figure worse than the noise. Scenario 13 spawned (8.6 to 9.2 tasks a second) showed no trend.
- **Not chosen:** 40 (a burst recorded 0.4 s later), 120 or 160 (slower starts, no gain).

### <a id="D485"></a>2026-11-14: each tick one probe statement; every check on a stored row or column; triggers and events in a round of their own (decision 485)

- **Question:** M5.1f, the idle cost. An idle standalone (release build, the examples' DAGs,
  SQLite, measured 2026-11-14 over 10 minutes) makes 22.5 statements a second, 2.0 % of a core,
  about 200 context switches a second over its threads, 4.4 MiB written in 10 minutes: every 1 s
  tick, each round part reads its inputs and finds nothing (the trigger rows three times a
  round, open attempts and ready instances twice, active DAGs and current parses, then about ten
  more once each).
- **Options:** (A) rounds gated by the change signals the database triggers write, a slow safety
  pass, time-based parts sleeping to their next due time; (B) the idle tick stretched from 1 s to
  5 s after a few empty rounds; (C) B with the trigger rows read once and the DAGs only on a
  change; (D) an event-driven master keeping the state in memory, each write path marking what it
  touched, a timer queue for what comes due; (E) the 1 s tick kept, opening with one probe
  statement whose checks (an `EXISTS` or `MIN` on an index each) say which parts have work.
- **Chosen: E** (the user), with two rules of theirs: everything goes through the one probe
  statement a tick, and a check always reads a stored row or column, so a moment worked out from
  code each tick (a scheduled DAG's next fire time, an event trigger's window, a generator's
  `restart_if_idle`, a wait step's `max_wait` deadline) gets a column, kept by whoever sets it.
  And triggers and events are to be faster than the rest: they get a round of their own (the
  user's idea; Claude had proposed wakes into the main round), woken at once by a named event
  posted, a run's end recorded, a `TriggerEvent`, a trigger stopping or failing, and by the
  probe for their moments, so a busy main round (4.1 s at p99 in stress 1) no longer holds them.
  The columns stay with the round of its own: wakes cover what happens, not what comes due, and
  stored moments survive a restart or a takeover and can be shown. Built as M5.1f1 (the probe and
  the columns) and M5.1f2 (the trigger round).
- **Risks named:** a column a writer forgets goes stale with no polling behind it (a test per
  writer, and one rebuilding every column from scratch against what is stored); a check drifting
  from its part's query (each check kept next to its part's, a test per check); the trigger round
  being new concurrency against the dispatcher (a paranoid review).
- **Not chosen:** A (relies on the database triggers: a write they don't cover waits for the
  safety pass), B and C (up to 5 s more latency at idle), D (an in-memory model of the state the
  database already holds).

### <a id="D486"></a>2026-11-14: the trigger loop runs after any write, at a stored moment, or for a posted event (decision 486)

- **Question:** M5.1f2 moves the trigger pass into a loop of its own (decision 485). Some of
  its inputs live only in the master's memory: which workers are connected, drained, the roots
  each offers, the triggers being stopped. A stored moment covers what comes due, not those:
  what wakes the loop for them?
- **Options:** (A) after any write: a round or a worker's turn that wrote something (the
  database's write counter moved), plus a stream ending, a posted named event and a stored
  moment come, at most once every 50 ms, the pass gated by its own probe; (B) an explicit wake
  at each of about eleven places an input changes (a registration, a stream ending, a drain, a
  worker's roots, the three trigger messages, commands, new versions, a run ending, queued runs
  started).
- **Chosen: A** (the user, Claude's recommendation): every change of those inputs comes with a
  write, or is a stream ending, woken explicitly, so there is nothing to forget; an idle master
  writes nothing and runs no pass; a busy one runs about a pass a round, as before, outside the
  round. Event triggers keep their next moment in `dag_triggers.next_fire_at` (for them: when the
  master looks at the trigger next), so the tick's one probe wakes the loop; a named event
  posted through the API also clears it for the event triggers, found within a tick on SQLite,
  and notified at once on Postgres. A run the loop queues wakes the round as a `Finished` does
  (option A of 2026-11-14: the queued run is the stored record, the wake in memory).
- **Not chosen:** B (one place forgotten leaves a trigger unstarted or unstopped until a
  takeover, with nothing to show it).
- **Then, the same day:** how the triggers' moments are stored. (1) One column, "when the
  trigger loop looks next": `dag_triggers.next_fire_at`, the earliest of the window opening or
  closing, a restart's backoff and a generator's `restart_if_idle`, written only by the loop
  after its pass; (2) a column each (`live_from` and `live_until` copied from the code,
  `idle_restart_at` written at every event, the existing `not_before`), four probe conditions.
  **Chosen: 1** (the user, Claude's recommendation): no copy of the code's `start`/`end`
  (decision 459 refused one), no write per event, one writer; why a trigger is looked at can be
  shown by working it out from the trigger and its definition. Not chosen: 2 (four writers to
  keep in step, a hot row under a stream of events).

### <a id="D487"></a>2026-11-14: a waiting wait_for step stores its deadline and what it waits for (decision 487)

- **Question:** M5.1f1's plan said a wait step's `max_wait` needs no column, read only once the
  step is ready. That was wrong (the user asked where the fourth column was): a `wait_for` step
  the master holds (decision 380: no worker) stays a queued instance, so the Ready check finds
  it and the assign part reads and checks it every tick while it waits.
- **Options:** (A) while it waits, its `ready_at` is its deadline, and index columns on the
  instance name what it waits for (a DAG, an event's name): that DAG's run or task ending, or
  that event posted, sets `ready_at` back for exactly those steps; (B) the deadline alone, the
  step looked at again on any write, as the trigger loop is (decision 486).
- **Chosen: A** (the user, Claude's recommendation). Not chosen: B (a busy master would look at
  every waiting step on every write).
- **Then, the same day:** how a parked step's look is cleared when what it waits for ends.
  Claude had built database triggers without asking (migration 0003); the user asked how they
  work and which way is more robust. (A) database triggers: every writer covered, future ones,
  set-based statements and other processes included; (B) the code that ends a run or a task
  clears the looks, with a test scanning the statements (as good as its matching). **Chosen: A**
  (the user), as **row** triggers on both backends (`AFTER UPDATE OF state ... FOR EACH ROW WHEN`
  the state really changed, and `AFTER INSERT` for a row stored ended): Claude's first Postgres
  version used statement triggers with transition tables, which can't be limited to `state`, so
  they ran on every update of `runs` and `task_instances`, and a `pg_trigger_depth()` guard that
  would skip a run ended by another trigger. Their cost is to be confirmed by stress 1 and 23 on
  Postgres before the commit. Not chosen: B (a writer worded differently slips past the test).
- **Then, the same day:** measured on Postgres, the row triggers cost nothing visible: stress 23's
  await p99 1.82 s (f1 on the same day 1.79-1.83 s), stress 1 26.9 tasks/s. Before the commit the
  user asked for the unreleased migrations to be one file: f1's `0002_next_fire.sql` and this
  step's `0003_waits.sql` became `0002_next_looks.sql` on both backends. A migration is edited in
  place until a release ships it, and left as it is afterwards; `0001_initial.sql` shipped in
  1.0.0.

### <a id="D488"></a>2026-11-14: standalone takes its DAG and script folders on the command line (decision 488)

For a "Getting started" with the DAG files in the working folder (M5.1n), asked before the
remaining M5.1 steps were run unattended. Claude found that a root given as `.` fails today
(`normalize(".")` is empty, so the root has no name).
- **Names:** (A) `--dag-path` / `--script-path`, repeatable, `.` allowed; (B) also
  `--cwd-dag-path` / `--cwd-script-path`. **Chosen: A** (the user, Claude's recommendation).
  Not chosen: B (two spellings of `.`).
- **With the environment and the file:** (A) the option replaces the list; (B) adds to it.
  **Chosen: A**, as `--ops-port` and the rest override. Not chosen: B (a stale
  `SCHEDR_DAG_PATH` would join silently).
- **The name of `.`:** (A) the folder's own name, as any path; (B) a fixed `dags` / `scripts`.
  **Chosen: A**. Not chosen: B (`.` for both is still one folder in two lists).
- **Roles:** (A) standalone only; (B) the worker too. **Chosen: A**, the guide's case.

### <a id="N7"></a>2026-11-14: The UI isn't built or tested for phones (note 7; corrects D466)


While building M5.1g's Status block, Claude wrapped its table to scroll on a narrow screen; the
user: "No one is going to use this on the phone, you can drop those tests altogether." D466's
promise that charts stack and tables scroll on a phone is dropped: the Metrics page and its
charts (M5.1j-l) get no phone layout and no tests for one. The existing `max-width: 900px`
rules stay as they are.

### <a id="D489"></a>2026-11-14: a poke may be a plain def (decision 489)

The user asked whether `@sensor.poke` and `@trigger.poke` take both a plain `def` and an
`async def`. Claude read the code: both take `async def` only (the decorators' `Protocol`s, and
the checks in `sensors.py` and `event_triggers.py`). The user: allow a plain `def` for the pokes,
which "just have to end in the timeout window"; the pure `@sensor` and `@trigger` stay async.
Claude listed what it takes, from the code: a thread can be asked to stop but not cancelled (one
stuck in C ends its host after the grace, as a stuck `to_thread` call does now); the hosts' pool
(`_WatcherThreads`) has Python's default size, min(32, CPUs + 4), too small for stress 23's 100
pokes; a plain wrapper returning a coroutine must be awaited (as tasks do); sync pokes run in
parallel, so user state must be thread-safe; a CPU-bound poke holds the GIL; the two pokes' call
windows differ (a sensor's up to three times `every`, a trigger's `every`), and a plain one keeps
its form's. Its view: worth it, safer than the usual mistake of an `async def` that blocks the
loop. The user: add it as the last step of M5.1 (M5.1o). Claude took the `wait_for=` forms as
"not pokes", so async only, as the user's line drew it; to be confirmed. Not chosen: async only.

### <a id="N8"></a>2026-11-14: samples by process, the webserver's its own (note 8; on D464)

Building M5.1i, a side note pointed out that a standalone's master, worker and webserver are one
process reporting the same figures (M5.1h): samples by node would chart it three times and a
deployment's total three times too high. D464 said "per process"; the samples are keyed by
process ID, the leader folding a process's roles into one row. The leading master can't read a
webserver (it has no row), so a webserver in a process of its own writes its own sample a minute;
a standalone's is the master's. The deletes past 24 h and 30 d run with the rollup, in the
sampler, not in the `[retention]` pass: their ages are hidden timings, not `[retention]` keys.

### <a id="D490"></a>2026-11-14: started_at is the worker's start (decision 490)

M5.1m, the user's "nice to have" of 2026-11-13, kept for 1.1.0 (the user, 2026-11-14: it fixes
time measures, so it goes into the metrics too). `started_at` was the master's time when it
handled `Started`, so it counted the wait for the core's lock (stress showed seconds). Claude
built it from what the worker already has: its best master-time sample bounds master time now
between `master_ms + (now - arrived)` and `master_ms + (now - sent)`; the worker sends the middle
(off by at most half the round trip, with the drift allowance), the master converts it to its
wall clock and clamps it to the assignment and the arrival; set-based as before (pairs of id and
time). The protocol field goes into 2.1 with M5.1h's (moved once per release, the user). Not
chosen: the master converting a worker's wall clock (skewed), the lower bound (always early).
Grok's review found two early biases, both fixed: the estimate took the tightest lower bound's
sample, which can be a reply stamped at once and delivered late (now the sample with the
narrowest round trip, its error growing with the drift allowance as it ages); and the master
compared a wall time read before the lock and transaction with master time read later (now
both read together in `start_time`, as `HeartbeatAcks::steady_now` does).

### <a id="D491"></a>2026-11-14: a process's memory includes its tasks, split by kind (decision 491)

Building M5.1j, Claude drew each process's resident memory on the Metrics page, the breakdown
only in the row's panel. A side note pointed out what that hides: a worker's fork servers, wait
step hosts and tasks are processes of their own, so a worker running a 5 GiB task showed about
50 MB, and outside a container nothing on the chart showed its tasks (the accepted layout had
"worker-a 5.1 GiB"). The user: "I want to see tasks memory as well", then "can you also show the
split of memory? how much for worker/master how much for tasks?"

Built: each sample stores the process's whole memory (`memory_avg`, `memory_peak`: resident plus
a worker's parts, in the open `0002` migration), rolled up like the rest, and the lines draw it;
the rollups keep the parts' averages (largest tasks left out), so 7 and 30 days split too. Each
deployment gets a stacked "Memory by kind" chart (masters and webservers, workers, fork servers,
wait step hosts, tasks), its legend giving each kind's memory now; the table has Now (the whole),
Itself and Tasks; a row's panel stacks its parts. Not chosen: adding the parts at read time from
the minute rows only (7 and 30 days would have lost the tasks); the container's memory as the line
(absent outside a cgroup, and on a desktop a cgroup is the whole session). CPU stays each
process's own: a task's CPU isn't sampled.

### <a id="N9"></a>2026-11-15: Codex's review of M5.1j: totals, parts and limits made exact; buckets in SQL (note 9; corrects D464)

Codex's high review of M5.1j's storage and API (session `01a1dc3c`) found six problems, all
reproduced on both databases and fixed with tests that failed before:

1. A deployment's total over 7 or 30 days counted a process replaced within a 15-minute window
   twice: each rollup averaged only its own minutes, and the totals summed those averages. Each
   row now stores the `minutes` it covers; a bucket's total is its processes' memory weighted by
   their minutes over the most minutes any of them has (the "Memory by kind" chart likewise).
2. A rollup averaged a worker's parts only over the minutes that had them, but its whole memory
   over all, so they didn't add up: the parts are now averaged over every minute, 0 where it ran
   no worker.
3. A container that became unlimited kept its old limit: a rollup and a process now take the
   latest sample's limit, none meaning unlimited.
4. One process's request read every process's rows, and 30 days with 100 processes took about
   10 s: the buckets are now computed in the database (`sample_buckets`, three statements a
   request, the process filter in SQL), from numeric part columns (`forks_avg`, `hosts_avg`,
   `tasks_avg`) beside the JSON.
5. A process sampled since the last rollup was missing from 7 and 30 days: those ranges read the
   rollups before the window that last closed and the minutes after it.
6. A range returned 241 points: it now starts so that it holds 240 at most, `now`'s bucket the
   last.

Codex's check of those fixes (a fresh session, `01a1dc5f`) confirmed the zero-filled parts, the
filter in SQL, the point caps and the rounding, and found five more problems, all reproduced on
both databases and fixed with tests that failed before:

1. Dividing a bucket's weighted memory by the most minutes any process had still overcounted when
   no process spanned the bucket (a standalone's restart: up to 1.9 times). Each row now stores
   `covered`, the minutes of its window in which any process was sampled (1 for a minute), and a
   total divides by the bucket's coverage; the API gives it (`coverage`) for memory by kind.
2. A rollup's CPU and container averages are over the minutes that have them, but were weighted
   by all its minutes (a process's first minute has no CPU): rows store `cpu_minutes` and
   `container_minutes`, those averages' weights.
3. The latest limit fell back to a bucket's highest when no minute sample was recent: it now
   comes from each process's latest row, with its roles (`latest_of`).
4. Where the rollups end was taken from the clock, so a window the leader hadn't rolled up (no
   leader) went missing from 7 and 30 days: it is now the end of the newest rollup stored.
5. Absurd byte counts overflowed the weighted sums: the sums are taken in floating point and
   byte counts are capped at 4 PiB when stored. Postgres rounds through `NUMERIC`, half away
   from zero as SQLite does.

A third check (`01a1dc90`) found three more, each reproduced on both databases and fixed with a
test that failed before:

1. A bucket's memory and its coverage were read in two statements, so a minute stored between
   them halved the total: coverage is now joined into the buckets' statement.
2. A webserver's minute stored late (captured before a window closed, inserted after its rollup)
   stayed out of that rollup forever: a window is rolled up 2 minutes after it closes
   (`ROLL_GRACE`).
3. Past 500 rows an insert was one transaction a chunk, so a chunk failing after another
   committed left a window partly rolled up and moved where the rollups end past the processes
   missing: all chunks are now one transaction.

### <a id="D492"></a>2026-11-15: the Metrics endpoints keep their final buckets (decision 492, Claude's)

Codex's second check timed 30 days of 100 processes: 4.5 s on SQLite. The user asked that
metrics not weigh on schedr and to profile before committing. Measured on a file database
(100 processes, 30 days of rollups, 24 h of minutes, 432,100 rows), release build:

| | SQLite before | SQLite after, first | SQLite after, again | Postgres before | Postgres after, again |
|---|---:|---:|---:|---:|---:|
| samples 24 h, all | 2.10 s | 1.47 s | 44 ms | 1.37 s | 51 ms |
| samples 30 d, all | 2.59 s | 3.1-4.4 s | 50-61 ms | 2.10 s | 57 ms |
| samples 24 h, one | 1.18 s | 0.59 s | 5 ms | 0.47 s | 14 ms |
| memory 24 h (the sparkline) | 1.31 s | 1.27 s | 8 ms | 0.38 s | 8 ms |

(First reads vary by the run on a file growing between runs.) The sampler's writes were already
light: a minute's insert of 100 rows 13-29 ms, a window's read 21-27 ms, its rollup's insert
13-29 ms, the deletes 2-44 ms, on either database; nothing to change there.

The reads cost by volume (24 h: 143,000 minute rows, each with 15 aggregates), and the
Deployments page asked for its 24 h sparkline every 15 seconds and at each live change, in each
tab. Three changes:

- **Final buckets kept** (decision 492): a bucket's rows are all stored a grace after it ends
  (minutes within the sampler's 2-minute grace, rollups once), and buckets are aligned to the
  epoch, so a later request's are the same; each request reads the buckets since the last final
  one. `FINAL_AFTER` is 5 minutes, a constant derived from `ROLL_GRACE` with a margin for the
  clocks of the processes writing (not a setting: nothing to tune). A test asks one state each
  minute over 70 minutes of samples (a webserver's stored a minute late, windows rolled up) and
  compares each answer with a fresh state's; with no margin it fails.
- **`latest_of`** read every row of each process (the join on `at` without the step couldn't use
  the primary key): 150 ms for a few minutes. The steps named and a `CROSS JOIN` (SQLite's
  documented join order): 9 ms (Postgres 118 -> 5 ms).
- **The range's two sides bounded each**: with the cut a day before the start, SQLite read a day
  of minutes for a few (73 ms for 200 rows).

Not chosen: a coarser rollup tier for 30 days (first reads stay 1-4 s on SQLite, once per range
per process until evicted); a cache by time only (each refresh past it would read everything
again).

### <a id="N10"></a>2026-11-15: M5.1k built: a task's attempts' peaks, the DAG's p95s (decision 467) (note 10)

Built as decision 467 says, with two choices of Claude's:

- **Bounded reads.** The DAG page reloads on each live change, so its table must be cheap:
  over 30 days of a DAG running 5 tasks a minute (216,000 attempts, SQLite), ranking every
  attempt took 850 ms; the DAG's newest 1000 runs of the 30 days (by `runs_by_dag`) take 31 ms.
  A task's last 30 attempts walk the DAG's runs newest first: 1 ms. Each task's p95 is over its
  newest 1000 attempts at most (a mapped task's instances).
- **The limit is the current version's** `memory=`; an older attempt may have run under another
  (the bars show what it used, the line what holds now). A task without one shows "the worker's
  share", which differs by worker. The table and the chart need no new column or index.

### <a id="N11"></a>2026-11-15: M5.1l split; the DAGs page by deployment (note 11)

M5.1l had two parts. The DAGs page's tabs (the user, 2026-11-14) are built as M5.1l1. Throughput
and start latency are M5.1l2, left for a decision: M5.1a's stage timers are Prometheus histograms
in each process, so charting them per minute needs the workers to report them (a heartbeat
field) and a choice of what is stored.

Small choices of Claude's for the tabs: the choice is in the address (`?view=<deployment>` or
`all`), so it can be shared and the live reload keeps it; it is remembered in a cookie
(`schedr_dags_view`, a year, as the zone's), read by the server, so `/` opens on it with no
script and no flash of the other list. A name no deployment has shows all. Not chosen:
`localStorage` (the server couldn't render the remembered tab; the page would switch after
loading).

### <a id="N12"></a>2026-11-15: M5.1o built: a poke may be a plain def (note 12; on D489)

Built as decision 489 says. A plain `def` poke is wrapped, where the hosts and `schedr.testing`
call pokes (`awaited_poke`), into an awaited `asyncio.to_thread` call: the host's default
threads, so it is its step's (context, log, secrets) and stopped as a `to_thread` call is when
its window ends (decision 337); a coroutine it returns is awaited. A plain sensor poke's `.fn`
runs it the same way, so a test awaits either form alike. A poke that yields (a generator) is
refused; `@sensor`, `@trigger` and the `wait_for=` forms stay `async def`.

The host's pool (`_WatcherThreads`) lets a thread run per wait step on top of Python's default
(`min(32, CPUs + 4)`), its limit set as steps come and go; threads start only when a call needs
one. Stress 23b (100 plain pokes on one worker, each call blocking 1 s, every 5 s): all succeed,
p99 5.3 s from the file to their end; with the limit left at the default, 20 of them failed on
overruns.

Open for the user: decision 489's "to be confirmed", that the `wait_for=` forms are not pokes
and stay `async def` (built so).

Codex's review of M5.1o (`01a1dd76`) found five; four fixed with tests, one a known limit:

1. A trigger's poke call stopped past `every` could still hold its thread (blocked in C) while
   the next call ran beside it, and nothing ended the host: after a failed call, the next now
   waits for the step's calls on the host's threads to stop, within the cancel grace; one that
   doesn't ends the trigger, its host with it, as a stuck wait step does. (Async pokes awaiting
   `to_thread` had this too.)
2. A plain `def` wrapping an `async def` with `functools.wraps` looked async and ran on the event
   loop: a poke is called on a thread unless the function itself is `async def`.
3. A plain `def` annotated `-> Coroutine[..., X]` was refused at definition: a returned
   coroutine is awaited, so its value is `X` wherever annotations are read (`type_hints`).
4. Not fixed: the types accept a generator as a sensor poke (an unconstrained `R`); the
   definition refuses it with "yields". Typing can't exclude it here.
5. A test's 3 ms call window flaked under load: 50 ms.

### <a id="N13"></a>2026-11-15: Codex's review of the UI steps M5.1g-l1: seven fixed (note 13; corrects D492)


Codex's joint review (`01a1dd5d`) of M5.1g, j, k and l1 found seven, all confirmed by tests that
fail on the old code, all fixed:

1. A backfill of old moments ending now was left out of the DAG's memory p95s (its runs were
   picked by `planned_for`): the DAG's newest runs are now the last to end, with the ones still
   going.
2. A task's last attempts were picked and ordered by run and map index: now by their end.
3. A sample stored behind the cache's mark (a webserver's clock behind, a paused insert) stayed
   missing: points are final after 15 minutes, not 5, and the guide says a clock further behind
   shows late.
4. A clock going back across the range's start lost its first buckets: a request starting before
   what was kept clears the entry.
5. Evicting a key a request held gave that key a second lock: eviction skips held keys.
6. A deployment named `all` took the All tab's address: All is `/?view=` (an empty value), and
   such a deployment gets its own tab.
7. The 7- and 30-day browser cases passed with no chart: each range now needs its three.

### <a id="N14"></a>2026-11-15: M5.1n built: standalone's --dag-path and --script-path (note 14; on D488)


Built as decision 488 says. The options are command-line overrides of `dags.path` and
`scripts.path` (a list each, a `:` kept inside its path), so the loader names and checks the
roots as it does the variables', its messages naming the flag (`--dag-path: ...`). A path is
kept as given, as the variable's entries are (so a relative rename in `[dags.roots]` still
names it); only one with no name of its own is resolved: `.` is the working folder, so it is
named after it, and one ending in `..` its real path (`canonicalize`). A path that isn't UTF-8
is refused. Checked, as the step asked: a module beside the DAG
file imports with no script root (the DAG root is on the import path). Standalone's warning for
no DAG folders names `--dag-path .`.

The Getting started page using it stays local until the user has read it.

Codex's review (`01a1de10`) found three, all confirmed and fixed with tests: `..` resolved
lexically picked the wrong folder past a symbolic link (`live/../dags`); a path not in UTF-8
was copied lossily, so another folder could be picked; and paths made absolute no longer
matched a relative rename the variable's accept. Hence the rule above.


### <a id="D493"></a>2026-11-15: tunable constants are hidden settings (decision 493)

The user, reading the Metrics cache's 15-minute `FINAL_AFTER`: "any other hidden constants not
'HIDDEN' yet?" Claude listed the ones added since 1.0.0; the user: "sweep over all constants and
evalute, provide a descriptive name and add in "p" step as HIDDEN as you recommend." Built as
M5.1p, the last step of M5.1. The rule it applies was the user's already (a new value is a key or
hidden; a binary constant needs a really good reason); this extends it to the constants from
before it.

Sweep of every numeric and duration constant (2026-11-15): 251 in the crates' sources, 30 in
`python/schedr`. Each is (A) already a key or a `SCHEDR_HIDDEN__*` variable, (B) fixed for a
reason, or (C) a tunable with neither: these become hidden, their defaults unchanged.

(C) Timings (SCHEDR_HIDDEN__...):
- WEBSERVER__METRICS_FINAL_AFTER (15 min, series cache FINAL_AFTER)
- MASTER__SAMPLE_ROLLUP_GRACE (2 min, samples ROLL_GRACE; must stay below METRICS_FINAL_AFTER, checked at load)
- WEBSERVER__MEMORY_PEAKS_WINDOW (30 days, attempt_memory PEAKS_OVER)
- WEBSERVER__EVENT_FUTURE_TOLERANCE (1 min, named_events AT_TOLERANCE)
- MASTER__FOR_FUTURE_TOLERANCE (1 min, planning FOR_TOLERANCE)
- MASTER__CODE_RETRY_DELAY (35 s, dispatch CODE_RETRY_DELAY: the git-sync interval differs)
- MASTER__NODE_ROW_KEPT (1 h, nodes MASTER_ROW_KEPT)
- CLIENT__CONNECT_TIMEOUT (5 s), CLIENT__REQUEST_TIMEOUT (30 s), CLIENT__COMMAND_WAIT (30 s): schedr-client, the CLI's and `schedr ui`'s
- DAGS__TEST_RUN_TIMEOUT (1 h, local TEST_RUN_TIMEOUT)

(C) Limits (whole numbers above zero):
- WEBSERVER__METRICS_CACHE_KEYS (64), WEBSERVER__METRICS_CACHE_BUCKETS (100,000)
- WEBSERVER__MEMORY_PEAKS_RUNS (1000, PEAKS_PER_TASK)
- MASTER__ASSIGNMENTS_PER_ROUND (50), MASTER__EXPIRIES_PER_ROUND (50),
  MASTER__WAITING_REASONS_PER_ROUND (100), MASTER__PENDING_REASONS_PER_ROUND (500),
  MASTER__COMMANDS_PER_ROUND (50), MASTER__SCHEDULED_RUNS_PER_ROUND (10),
  MASTER__STARTED_INSTANCES_PER_ROUND (2000), MASTER__RETRIES_PER_ROUND (100)
- MASTER__RUNS_PER_PLANNING_PASS (100, MAX_RUNS_PER_PASS), MASTER__RUN_OF_RUNS_PER_PASS (50)
- MASTER__FAILED_GROUP_MESSAGES_PER_TURN (10, ALONE_PER_TURN)
- MASTER__WORKER_STREAM_BUFFER (1024, STREAM_BUFFER)
- MASTER__NOTIFICATIONS_PER_PASS (50, sender BATCH)
- WORKER__RESULTS_PER_JOURNAL_GROUP (32), WORKER__STARTS_PER_JOURNAL_GROUP (32)
- WORKER__RESULT_RESENDS (8, MAX_RESENDS), WORKER__RESULT_RESEND_GAP_MAX (32, MAX_RESEND_GAP)
- WORKER__UNACKED_MESSAGES_KEPT (100,000, SENT_KEPT)
- WORKER__UNSENT_CONNECTION_ANSWERS (16)
- WORKER__TASK_CONNECTIONS_OPEN (64, ctx MAX_CONNECTIONS)
- WORKER__WATCHER_LOG_BACKLOG (16, ROUTE_MESSAGES)
- WEBSERVER__EVENTS_BACKLOG (1024), UI__EVENTS_BACKLOG (1024)
- WEBSERVER__AUTH_REMEMBERED_MAX (1000)
- DATABASE__CHANGES_POLL_ROWS (1000)
- DATABASE__DAG_VERSIONS_CACHED (4096, DagCache::MAX)
- TLS__HANDSHAKES_AT_ONCE (256, MAX_HANDSHAKES)

A bug, fixed (no new value): MASTERS_HEARD (samples) and MASTERS_HEARD_WITHIN (routes), 30 s,
assume the 10 s node heartbeat that SCHEDR_HIDDEN__MASTER__NODE_HEARTBEAT changes: three
heartbeats instead.

(B) Fixed, and why:
- Data format and protocol: MINUTE, ROLLUP (stored `step_s`), USAGE_EVERY (the samples' step),
  PROTOCOL_VERSION, CTX_PROTOCOL, CLOCK_FIELD, INPUT_CHUNK, GRPC_DEFAULT_LIMIT,
  MESSAGE_OVERHEAD, RESULT_OVERHEAD, INLINE_INPUTS_LIMIT, MAX_CTX_MESSAGE_SIZE,
  MAX_MESSAGE_SIZE (runner), MAX_VALUE_SIZE, MAX_HANDLES_SIZE, EVENT_BODY_ROOM, NONCE_LEN.
- Parameter limits of the databases: the *_PER_STATEMENT, *_PER_QUERY, ROWS_PER_INSERT.
- Operating system and physics: MAX_FDS_PER_DATAGRAM, exit codes and signals, DRIFT_PER_MILLE,
  PAUSE_SLACK, PAUSE_PER_MILLE, fire_times MARGIN (daylight saving), HTTPS_PORT.
- Contracts users see (decisions, the API, the DSL's defaults): MAX_ID_LEN, MAX_EVENT_KEY,
  MAX_PARAMS_BYTES, MAX_INPUTS, MAX_LAG, MAX_CONNECTION_SIZE, the list sizes and maxima
  (DEFAULT_RUNS, MAX_RUNS, DEFAULT_EVENTS, MAX_EVENTS, DAG_BACKFILLS, MAX_REQUEST_KEY), the
  log chunk sizes, MAX_NAME, MIN_UI_TOKEN, the DSL's default retries and sizes, MIN_BUDGET and
  MIN_TASK_MEMORY (no Python starts below), MAX_CONNECTION_TEST_MS.
- Derived from another value: WORKERS_HEARD and PARTS_WITHIN (USAGE_EVERY), refresh_limit
  (the loop stall), WRITE_LIMITS and BATCH_LIMITS (the query timeout), PROBE_ROUNDS and
  PROBE_LIMITS (the pooler check), RENEW_AT (Vault's two thirds).
- Display: the UI's geometry, page sizes, tails, SERIES, CHARTED_ATTEMPTS, LARGEST_TASKS, NEAR,
  EVENTS_SHOWN, MAX_FAILED_TASKS, MAX_ERROR_CHARS, RUNNER_OUTPUT_LINES, POLL_MS.
- Small loop and buffer bounds with no deployment reason: COPY_BUFFER, GONE_RUNS_OF_A_LOOK,
  BRANCH_READS, LEADER_LOOKUP, COMMAND_TRIES, the client's POLL_INTERVAL and STREAM_TIMEOUT.
- Shared by two processes, so one side's setting would break the other: API_CALL_LIMIT (the
  in-task CLI waits it and its answer time), CONNECTION_CHECK_TIMEOUT_MS (the worker's limit is
  the master's deadline).
- In a task's process, before any configuration is read: the in-task CLI's and `schedr`
  Python's timeouts (ANSWER_TIMEOUT, API_ANSWER_TIMEOUT, _TIMEOUT) and Python's other numbers.

(A) Already a key or hidden: the rest (every DEFAULT_* behind a key, and the 70 hidden timings).
Three first taken for (C) are a key's default though their doc comments don't say so: fork
PRELOAD_TIMEOUT (`[worker] fork_server_preload_timeout`), PARSE_JOB_TIMEOUT (`[master]
parse_job_timeout`), the logs' REQUEST_TIMEOUT (`[webserver] worker_request_timeout`); their
comments say it in M5.1p.

Not chosen: public keys for these (each is a tuning knob for an odd deployment, unsupported as
the other hidden ones are); one variable for a group (the caps are measured one by one, B18).

### <a id="D494"></a>2026-11-15: throughput and start latency come from the attempts table (decision 494)

M5.1l2 waited for where its numbers come from. Claude found the attempts table holds them:
`assigned_at`, `started_at` (the worker's start in master time since decision 490) and
`ended_at`. Options: (A) read them from attempts; (B) workers report M5.1a's stage timers in
their heartbeat, stored per minute in `samples`; (C) move the step out of M5.1. **Chosen: A**
(the user, Claude's recommendation). Not chosen: B (a protocol field, 2.1 to 2.2, new sample
columns and rollups, for the stages inside a worker that the stress page already measures); C.

The same round of questions, before the user left Claude to finish M5.1 and the stress runs:
- **B250** (a root holding a `.venv` scans it): the user chose to leave the scan as it is.
  Getting started keeps its DAGs in their own folder (`--dag-path dags`), and the guide says to
  add a `.schedrignore` where a root holds a virtualenv. Not chosen: skipping folders holding
  `pyvenv.cfg`; skipping hidden folders (a task's data in one would no longer count). B250 is
  closed as documented.
- **Decision 489's open point** confirmed: the `wait_for=` forms aren't pokes and stay
  `async def`.
- **Getting started** (`--dag-path dags`) is committed with M5.1, not kept for the user to read.

### <a id="N15"></a>2026-11-15: M5.1p built: 41 hidden settings and the masters' window (note 15; on D493)


Built as decision 493 says: 11 timings and 30 limits (the two event backlogs are two), each read
by the loader into `Timings` or `HiddenLimits` and carried to its use; their defaults are the
constants they replaced (a test compares them), and a test sets each and reads it back where
the roles are built. `SAMPLE_ROLLUP_GRACE` not shorter than `METRICS_FINAL_AFTER` stops the
start. The masters' "heard within" window, in the samples and on the Deployments page, is three
node heartbeats; a fixed 30 s dropped a master whose `NODE_HEARTBEAT` was a minute (tests for
both).

Small choices of Claude's: the event tolerance is `EVENTS__FUTURE_TOLERANCE`, not under
`WEBSERVER`, since tasks post events through their master too; the notifications' cap is the
sender's, carried with the outbox; the worker's limits ride in `WorkerTimeouts`, which already
held non-durations (the watcher budget, the overruns), so no test changed its setup; the
client's timeouts ride in `ClientArgs` as a field clap skips, filled once the configuration is
read. The timings guide lists each with what depends on it and its restrictions, and a section
says what stays in the binary and why.

Codex's review found five, all fixed with a test each: a rollup grace past the minute samples'
retention lost a window's minutes before their rollup (the grace with its 15-minute window must
now be shorter than `MASTER__SAMPLES_KEPT`); a limit past tokio's bounds panicked the TLS
accept loop or an events subscription (limits are 1 to 16,777,216); a code retry delay that
overflowed a timestamp parked its task for good (timings are at most 3650 days, and the
deadline saturates); `dags test` kept its own client timeouts; the pending reason named the
constant instead of the configured cap.

### <a id="N16"></a>2026-11-15: M5.1l2 built: the attempt charts on the Metrics page (note 16; on D494)

Built as decision 494 says, read from the attempts: per bucket, the attempts ended by how
(`success`; `failed`, `timed_out`, `oom_killed` as failed; the rest as stopped), and the
exact nearest-rank p50 and p95 of `started_at - assigned_at` for those started, below zero
read as zero. `GET /api/v1/metrics/tasks` keeps a range's final buckets between requests, the
samples' way (decision 492, the same `METRICS_FINAL_AFTER`), so a refresh reads the last few
buckets; on SQLite a first read of 30 days of a million attempts took 2 to 3 s for the ends and
5 to 8 s for the latency (a throwaway bench), an hour 3 to 10 ms.

Found while building: the two indexes on `ended_at` and `started_at`, plain at first, took the
plan of the rounds' read of unstarted attempts (`started_at IS NULL`) from `attempts_open`;
they are partial (`IS NOT NULL`), which the range reads imply, and a test checks each read's
plan. The attempts ended draw as a stacked bar per bucket (`Spec::bars`): as an area, buckets
with nothing between them drew spikes. And a stacked area's point alone between gaps drew
nothing (the memory by kind of samples three minutes apart); it is a bar now, as a line's lone
point is a dot.

Small choices of Claude's: the charts sit below the processes' table and show without samples;
an attempt read failing leaves the processes' charts with a warning of its own. Not built: a
"ready → started" line (the wait before a worker takes a task): `ready_at` is overwritten by a
retry, so it would be exact only for an instance's last attempt; the guide says what the
latency leaves out, and the line is the user's call.

### <a id="D495"></a>2026-11-15: a stopping worker waits for the master to take its Deregister (decision 495)

B242: a worker test saw a stopped worker `disconnected`, not `offline`. Grok found the path: a
stopping worker sent `Deregister`, waited `DEREGISTER_WAIT` (5 s; 1 s in that test) for the
master to end the stream, and returned; dropping the connection sent `RST_STREAM CANCEL`
over a frame not written yet, and a master frozen past the wait never got it. The user chose
between fixing it before 1.1.0 (the worker stays until the master takes it, or its grace
ends) and leaving it open past the release: fix it.

Built: `DEREGISTER_WAIT` defaults to an hour, so the drain's `deregister_by` (B237: a quarter of
the report period before the grace period ends) alone bounds the wait; the setting stays, to
shorten it, with the caveat in the timings guide. A master that is gone ends the connection at
once, so only a stalled one is waited for. The test freezes the master 3 s and expects the
worker to wait through it and end `offline`.

Not chosen: leaving it open; retiring `DEREGISTER_WAIT` (a deployment setting it would still
start, but it's a knob to shorten the wait); flushing the frame before returning at the short
wait (h2 gives no sign that a frame left the socket).

### <a id="D496"></a>2026-11-15: a role starting waits for a database it can't reach yet (decision 496)

B247's test failed because a master started during a 2.4 s I/O stall gave up connecting at
its 1 s query timeout (the pool's acquire timeout) and exited. The test was fixed; the product
still exited on a database not reachable at its start, and the webserver too (its schema wait
came after an eager connect). The user, after a side agent's note that only the test was fixed:
retry at start, indefinitely, then (a second note: a mistyped host or port is refused like a
database that's down, so it waits too) a clear warning naming the host only, never the
credentials.

Built: `DbError::is_transient` (a time limit, a pool timeout or closed, an I/O error, Postgres's
08 class, 57P01-57P03 and 53300); `roles::once_reached` tries a database step again every
`SCHEDR_HIDDEN__WEBSERVER__SCHEMA_RETRY` while it fails transiently, until a stop is asked for,
warning "can't reach the database at <host:port> (<why>)" at once and every minute while it
waits, and once it answers; the master, webserver and standalone connect through it, and the
master's `lead` reads the schema through it. `database_place` names a Postgres server's host
and port (the URL's or `host=`), or a SQLite file, never its user or password. A wrong
password, TLS setting or URL still exits at once.

Changed after Codex's review, the same day: a stop cuts a try in progress (a schema read can
hang on a quiet connection); `is_transient` takes the classes a statement's retry already took
(SQLite busy or locked, a statement timeout; Postgres's 08P01 kept, since PgBouncer refuses with
it while its pool is full); the reason in the warning is codes only
(`DbError::waiting_reason`), since the server's text may name the user ("too many connections
for role ..."); `database_place` reads the URL through sqlx, whose `host=` and `port=` win; the
webserver's schema wait names the place and repeats an error every minute.

Not chosen: exiting and leaving restarts to a supervisor (Kubernetes' CrashLoopBackOff can
keep a master down minutes after its database is back; a standalone has none); retrying for a
bounded time, then exiting (a refusal that won't pass already exits).

### <a id="D497"></a>2026-11-15: an unset cleanup timeout fits in the grace period (decision 497)

B252: run with no configuration (Getting started's examples), every worker and standalone start
warned that `[worker] cleanup_timeout` (5 min) doesn't fit in `termination_grace_period` (30 s).
The user: "the config is either wrong or the warning is wrong". The warning is right (a cleanup
past what a drain leaves of the grace is lost when the pod is killed), so the defaults were
wrong. Chosen by the user from four: left unset, `cleanup_timeout` is the shorter of 5 minutes
and what fits (the grace less the drain's terminate and report periods), at least 1 s (a grace
the drain takes whole, which the loader allows, leaves none); a value set by hand is kept and
still warned about. A side agent's note, which the user had added to the release notes: it
limits every cleanup's try, also outside a drain, so a setup outside the chart goes from 5
minutes to 25 s.

Not chosen: a 25 s default everywhere (the chart's pods lose their 5 minutes); a 330 s default
grace (a Ctrl-C waits 5.5 minutes for a hung task, and pods outside the chart must match it);
warning only in Kubernetes (the defaults would still disagree).

### <a id="D498"></a>2026-11-15: a release with a new migration goes through an rc first (decision 498)

Preparing 1.1.0, the user asked whether it needs a release candidate. Claude said no, as the user
had decided on 2026-11-14 (R1.1's scope: the release workflow and the builds are 1.0.0's,
unchanged since `v1.0.0`, and R1.1's local checks cover the new migration and protocol 2.1).
The user then had the migrations put in CI (`scripts/upgrade_check.py`: a database the previous
release filled, migrated by the tree's binary) and decided: "after that we need an rc for every
new migrations file, that includes 1.1.0 rc as well". A release whose code adds a migration
file is published as `vX.Y.Zrc1` first (TestPyPI, a GitHub pre-release, `X.Y.Zrc1` images) and
checked from there before `vX.Y.Z`.

Not chosen: 1.1.0 without an rc (the 2026-11-14 call): the workflow being unchanged says nothing
about a migration on the databases users have.

### <a id="D499"></a>2026-11-16: the Metrics page folds the processes that ended into one line (decision 499)

Watching `examples/metrics_load.py` on `make standalone-separate-ui`, the user saw the Metrics
page's list of processes grow. It grew with restarts, not the load: each start gives every
process a new ID, and the page drew one line and one row per process over the range (12 for
three starts of four processes, the rest folded into "4 other processes"). Chosen by the user
from three: running processes as lines, each by name as before, and those that ended (no sample
within three of the master's sample intervals, decided by the API as `ended`) one line
together, their values added up, left out of the table with a note counting them; a crashed or
OOM-killed process's memory stays on the chart.

Not chosen: masters and webservers keyed by a stable name, as workers are (they have none: a new
`name` key, or host and port, with two masters on one host merging); keeping a line per process
start.

### <a id="D500"></a>2026-11-16: the DAGs page filters by name, trigger switch, kind and tag (decision 500)

Watching the metrics load, the user asked for "a toggle in the UI to show paused/unpaused dags
or all", then "a name-filter field", then "the same goes for checkbox for tags and trigger
type". Built as: the switch as All, Active and Paused (named as the list's badges are; a DAG
without a trigger is neither, so only under All, as `dags list` calls it `manual`), a name
field outside the part that updates itself (so typing keeps its focus), and boxes for each
trigger kind and tag the deployment's DAGs have, any within a group, the groups together, each
count among what the other filters show. All in the address, so a filtered list can be linked
and its live updates keep it; only the switch is remembered per viewer, as the deployment is.

Not chosen (Claude's, small): manual DAGs under Active (they are never switched); remembering
the name and the boxes (a filter one forgets about hides DAGs on the next visit).

Then, the same day, the user: the filters "should be on the title bar with DAG Trigger Tags
Defined in", and "those checkboxes should be on the dropdown". So they are a second header row,
each under its column, and kinds and tags are dropdowns applied when they close (applying each
tick would swap the header and close the dropdown after every box). Codex's review of the first
version found five problems, fixed in this one: a live read could overwrite a filter's answer
(now synced on `#live`), Back showed a stale name (the page keeps no htmx history snapshot), the
two forms didn't combine without JavaScript (one form, its fields joined by `form=`), focus was
lost after a swap (stable ids), and percent-encoded keys were ignored.

Then: "those additional switches would be better if they were next to the title: DAG
|Filter by name| ... so that this bar is not so big", the switch next to the Kind dropdown, and
a dropdown "a list of checkboxes which has a scroll bar ... and it should stretch down". So one
header row, each column's filters beside its title, the dropdowns a column of boxes opening
downwards and scrolling past about 12; the DAG table no longer clips its content, so a dropdown
can run past a short list.

Then: the switch "could be a little smaller", the header "changes its height when switching
... because of changes to column width", "Tags dropdown switches from beneath to the side",
"maybe all/active/paused should also be a dropdown ... as a radiobutton". So the switch is a
dropdown of radio buttons (applied on choosing), the table's columns have fixed widths, and a
title cell never wraps.

Then: "looks good, apply similar field styles to Runs". Its filters (a DAG id, a state, a
deployment) moved from the form above the list into its header, the DAG field beside DAG and
State and Deployment as dropdowns of radio buttons; no counts there (the list is a page of 50,
filtered by the API), and the DAG field applies on Enter or leaving it (the API matches an
exact id, so applying as one types would show nothing until the id is whole).

Then: "can DAG filter behave in the same way as on DAGs page?" The Runs page lists 50 runs a
page, filtered by the API, so a part of a name needs the database: `GET /api/v1/runs` gains
`dag_contains` (additive; an older webserver refuses the unknown parameter, 400, rather than
listing everything). A side note warned that `LIKE '%part%'` over the runs reads every run.
Measured on SQLite with 500,000 runs over 200 DAGs: over the runs, 1 ms for a common part, 44 ms
for a rare one, 444 ms when nothing matches; matching in `dags` first and then `dag_id IN (...)`
by `runs_by_dag`, 114 ms, 3.7 ms and 0.2 ms: the cost of the exact filter per matched DAG,
never the whole history. In one statement with `$6 IS NULL OR ...` the planner kept the trigger-
time scan (306 ms for no match), so a part has a statement of its own.

Not chosen: matching only the start of the name (an index range, but `sync` wouldn't find
`daily_sync`, unlike the DAGs page); a DAG picker (a list per deployment to load first).

Then, a side note: a page filtered by name follows every run change. Pages already re-read at
most once a second on a poke, so the cost is per read: 0.3 ms unfiltered, up to 114 ms for a
name matching busy DAGs (500,000 runs). Options: A keep; B slower re-reads when filtered; C
follow only the matched DAGs (an API field, a set per deployment, a gap for DAGs deployed
later; it saves only the cheap reads). The user: "B - but when the dag/run number exceeds some
limit". Restated: what makes a read slow is the matched DAGs' runs in total, and counting them
costs as much as the read; the user chose the read's own time. A Runs page whose deployments
took over 50 ms re-reads at most every 5 s (hidden `UI__SLOW_READ`, `UI__SLOW_READ_WAIT`).
Not chosen: the matched DAGs' count (an API field; a poor stand-in), the matched runs' count
(as costly as the read).

Then: "We could also add ordering to each DAG name field on both pages". Restated as a sort by
the DAG title (A to Z, Z to A). The DAGs page sorts in the UI: small. The Runs page pages newest
first by its cursor; sorting by DAG would need an API parameter, another cursor, and an index
on `(dag_id, triggered_at, id)` in both migrations. The user: "DAGs page only". `order=desc` in
the address, kept by the other filters, not remembered. Also in this round: run lists' times
always with seconds (`show_seconds`; whole minutes were shorter), and a chained run's "By" as
`run …<last 8>` linking to it (the whole ID made the row tall).

Then Codex's review of the whole of D500 (session 01a1e3e3, Sol high): eight findings, all
confirmed, all fixed. (1) The backfill counts ran before the `LIMIT` on a DAG part, quadratic on
a large backfill: the page's runs are now chosen first, their places counted after. (2) Postgres
planned the part's subquery as a join and walked every run by trigger time, 96 ms over 250,000
runs with no DAG matched; the exact `dag` filter had the same cost (54 ms for a DAG of five
runs). Measured on Postgres: with a new index `runs_by_dag_trigger (dag_id, triggered_at, id)`
and the matched DAG IDs read first (none: nothing asked), bound as an array Postgres can count
(`string_to_array`, not a JSON list: 0.16 ms against 144 ms with no match). The user: "no, change
now", then "don't treat RC as real release": the index goes into migration 0002, which rc1
shipped (an rc1 database isn't taken; the changelog says so), and "because of the schema change
we have to go with another RC release": 1.1.0rc2. (3) A webserver older than 1.1 refuses
`dag_contains` (unknown field): the UI asks it for the whole DAG ID and says so. (4) Choosing a
deployment in the Runs header swapped only `#live`, the page still following the first one:
`#live` names what it follows, read again after a swap. (5) Back left the newer name in the kept
field over the older list: kept fields take the address's value on a history restore. (6) A
change held while a radio dropdown was open was dropped on closing it: read then. (7) A read
owed after a slow one didn't count for the pacing: it does. (8) The hidden timings' count in a
test (121).

### <a id="D501"></a>2026-11-17: the master's core lock is released during database calls (decision 501)

After 1.1.0 the user asked whether 26 tasks a second is the master's own limit. Scenario 1
(quick x2, 4 workers) with 8 slots a worker gave 25.4 tasks a second, with 16 gave 30.4; the
master used about a third of a core (15 % on the release build, 30.7 tasks a second). Its core
lock was held about the whole task phase (81.9 s on release): waiting for Postgres's answers,
not computing. No synchronous commit gained 6 %; Postgres executed about 11 s of statements; one
round trip through the stress suite's published port costs 0.98 ms (0.25 ms inside the
container). Assign's per-task upstream reads (7,200 statements) were the first cut (M5.2a: 40.5
tasks a second).

The user asked why the master needs a lock: the core's memory (each worker's free slots, the
attempts it holds, connections, jobs given out) must agree with what was committed, and a
rollback restores it from a copy; one task at a time makes that so. The user: "so master lock
is an easy fix to a very complex problem, although quite costly, something like GIL in Python".
Python's GIL is let go during I/O; this lock isn't. Options laid out: A, the assign part's
reads before the lock (about 50 tasks a second); B, the turns' reads too (about 80 to 120,
the writes still serial); C, the lock off the database calls altogether (the target reachable).
The user set the target, "50% of theoretical throughput": 160 tasks a second in scenario 1
(32 slots, tasks of 0.1 s), and chose C, built A, then B, then C, measured after each
(M5.2cA, M5.2cB, M5.2cC), the worker's start path after it (M5.2d).

Not chosen: B alone (short of the target by the estimate); C at once (no measurement between
the parts, and A and B shape each operation as C needs it).

### <a id="D502"></a>2026-11-17: one probe a round; a read skipped on it after a write wakes the next round (decision 502)

M5.2b. Tracing scenario 1's round parts on the master (each part's window and the statements
in it) showed the round's idle parts spending their lock time on one statement, the probe:
taken about four times a round, because decision 485 took it again after any write and a
worker's turn writes between every two parts. Each probe planned anew in 5 to 9 ms and ran in
1 to 2 (statement cache off). Options laid out: after a write, each check's own read instead of
the probe; a smaller re-probe of the checks a write can affect; one probe a round, accepting
that work a write made possible waits for the next round. The user asked what would wait:
a new run's first task, up to two ticks (2 s) on a quiet cluster. The user: "can we make those
also retrigger the run with 50ms backoff?". Compared, per busy round: the reads instead of a
re-probe saved little (about 15 small reads), the re-wake about 30 ms; its cost, up to about
100 ms for a new run's first task on a quiet cluster. The user chose the re-wake. `Hub::tick`,
which tests turn by hand, runs the follow-up rounds at once (at most 4) and is for tests only.

Measured (scenario 1, quick x2, release, statement cache off): 40.5 -> 43.8 tasks a second;
probes 620 -> 273; the commands, scheduler and prepare parts 35/25/25 -> 21.5/13.1/12.4 ms a
round.

### <a id="D503"></a>2026-11-17: queries stay prepared on their connections by default (decision 503)

M5.2b found the probe planned anew on every call: the Postgres pool had sqlx's statement cache
off (`statement_cache_capacity(0)`, M2.10: "unnamed statements, which every pooler and version
passes"). The user: "go on with M5.2b if we can turn the cache on that's an additional gain, an
extra, not a default": a key, off by default. Writing its test showed off wasn't unnamed: sqlx
0.9 prepares a persistent query (its default) under a new name and keeps it only in an enabled
cache, so each query was prepared under a new name and left on the connection until it retired
(B256). Off was no friendlier to poolers than on, and slower. Options: off made unnamed
(`persistent(false)` on every query); the cache on by default; both. The user chose both.

Measured (scenario 1, quick x2, release): 43.8 -> 51.2 tasks a second; a `Finished` group's
lock time 41 -> 27.5 ms, a `Started` group's 24 -> 18 ms.

Then Codex's review of M5.2a, M5.2b and B256 (session 01a1e9e1, Sol high): four findings, each
shown by a failing test. (1) A URL setting `statement-cache-capacity=0` turned sqlx's cache off
under the setting on, bringing B256 back: the URL now wins, and the queries are unnamed. (2) A
scheduled DAG visited for its queued run (held by `max_active_runs`) had its unchanged next
fire time written at every visit, and every write wakes another round (decision 502): rounds
every round gap for as long as the run was held. Its stored time is read first now. (3) The
assign round's batch read filtered runs and tasks apart, reading the outputs of every run and
task named (50 pairs read 2,500 outputs): it filters the pairs too. (4) The lease's and the
migrations' statements are built outside `on_pool!` and `in_tx!`, so unnamed with the setting
on: kept so, a few statements a minute.

### <a id="D504"></a>2026-11-17: a movable cleanup's try waits at a barrier for the master's word (decision 504)

B255 (nightly Stress, scenario 20): a `cleanup_anywhere` cleanup ran on the worker it moved to
and again on the frozen one, thawed. Grok found the cause (session c543e145): the cleanup gate is
read once, and the process starts after several awaits; a worker frozen between them starts it
after its deadline. Claude proposed a recheck just before the spawn, the deadline checked in
the new process, or both. The user: "none of that prevents it then": a freeze can fall between
any two instructions. The user's design: "the worker sends the request to master and starts the
cleanup, ... the cleanup holds on the barrier waiting for the response from the master with a
timeout". Claude's first version asked as the process started; the user: "the worker starts the
cleanup and waits for the cleanup to reach the barrier and THEN sends the request to master ...
so it's typical critical section": the process's wait must start before the question leaves.
The user also: "just letting through should be considered as cleanup started and could be
reported back to the master"; asked whether that changes the guarantee: not alone, the report
can be lost; what changes it is whether the master moves a cleanup it said `go` to, after
losing its worker. Kept for later (the user: "if we don't do that today then probably we
shouldn't leave this idea"): step M5.3b. Built in the M5.2a+b commit (the user: "put it with
M5.2ab").

Codex's review of B255's barrier (session 01a1ea41, Sol high): four findings, each shown by a
failing test, all fixed. (1) A worker told `Kill` after its cleanup moved was let through, the
master's answer taking any rest owed to it for the whole cleanup; (2) a rest owed after the
attempt ended was refused. The question now says whether it is a rest, answered as registration
answers it. (3) A try asked again after no word took the gate's word but not its new `CLEAN_REST`
boundary. (4) Without `schedr task-exec` the cleanup started unasked; it doesn't start now.
Then the user, told a forked Python cleanup was spawned to reach the barrier: "we could do fork
barrier aswell": the fork server's child waits at the same barrier.

Codex's review of the fork barrier and those fixes (session 01a1ea72, Sol high; the user: "do a
fork review aswell"): three findings, each shown by a failing test, all fixed. (1) Each try asked
again left out the `CLEAN_REST` boundary's handles once more, dropping handles never cleaned:
`AttemptHandles::skip` counts from the first handle now, idempotent. (2) A retry after a failed
try didn't see a `CLEAN_REST` given meanwhile, asked as the whole cleanup and was refused: every
try reads the gate first. (3) A cleanup restored from the journal had no gate, so a later
`CLEAN_REST` couldn't reach it: it has one, and a waiting cleanup's boundary is left to where its
handles are skipped, not drained twice. The user, on stress: "I don't think we have to run stress
again": the cut scenarios passed on the first barrier; not run again.

### 2026-11-17: M5.2cA, the assign part's reads before the core lock (decision 501, its first part)

The assign part reads the ready tasks, the DAGs held, the wait steps' reads, each DAG's current
version and the upstream of a round's worth of ready tasks before taking the lock
(`AssignReads`); under it the slots and limits in memory, the claim (`WHERE state = 'queued'`,
refusing a task anything changed since: its slot given back), the messages and the commit.
Measured (scenario 1, quick x2, release): 51.2 -> 55.7 tasks a second; the assign part's lock
time 35.7 -> 13.7 ms a round; the lock held 49.2 -> 41.2 s. A `Finished` group still holds it
25.5 ms, its run read 0.14 ms of it on the server: a turn's time is its round trips and writes.
The user, on the review: one at the end of M5.2c ("I mean at the end of M5.2c").
