# AI_INSTRUCTIONS: how to work with me

For coding agents. Copy this file into a project: it says how I like to work, what I expect
at each step and how to talk to me. How a project is set up is in `AI_SETUP.md`.

## At the start of a session

- Read `NEXT_SESSION.md`, then `PROJECT.md`, `PLAN.md` and `BUGS.md` (whichever exist).
  Don't re-ask what is decided there.
- If they disagree with each other or with the code, say so briefly and propose the fix.

## How a step goes

1. **One step at a time,** as `PLAN.md` orders them. Before starting, read the code the
   step touches.
2. **Ask when there is a real choice** (see Decisions); otherwise pick the sensible default,
   say which, and go on.
3. **Do the whole step:** code, unit tests, integration tests, docs and docstrings, generated
   files. **Update the guide** (the hand-written pages on how the system works, see
   `AI_SETUP.md`, Documentation) wherever the step changes what they describe: a page that
   says something the code no longer does is a bug. Reference pages generated from the code
   need no edits, but a new setting, command, endpoint or metric needs its doc comment.
4. **Run `make check` and `make docs`** (or the project's equivalents) and make sure both
   pass. Never claim something passes without running it; report failures with their output.
   **Run the stress suite** (`make stress`, if the project has one) at the end of each
   milestone and after a step that changes the core's scheduling, database or worker code, and
   report its numbers.
5. **Run a code review once over the whole step** (the code-review skill, `/code-review`, on
   everything the step changed, after the checks pass). It also checks that the guide matches
   the change. Fix what it confirms, re-run the checks, and list in the summary what it found
   and what was fixed or left (with why). A finding outside the step's scope goes to
   `BUGS.md`.
6. **Record the decisions** in `PROJECT.md`, in the section they belong to, with what wasn't
   chosen and why.
7. **Present a short summary and wait:** what was built, the decisions made (so I can
   overrule them), anything surprising (new bugs, inconsistencies, coverage close to the
   floor), and the proposed commit.
8. **Commit only when I say so** ("commit", "accepted, commit and go on"). Never push: I push
   myself, often without saying so, so don't claim CI hasn't run. To know CI's state, ask me
   (I may not have `gh` installed).

## Working with a worker agent (Grok)

A step can be handed to a second agent, Grok Build, through the `/grok` skill (set up as
`AI_SETUP.md`, Agent tooling, says). I still talk only to you. Grok writes the code; you
relay, decide, and check.

- **Grok works in its own git worktree** (`<repo>-grok-<step>`, branch `grok/<step>`; one per step, so two can run side by side), approving
  its own commands there. It never commits, pushes, or touches the main checkout. It
  follows this file, except the code review, the summary, and committing, which are yours.
- **Who decides:**

  | Decision | Decided by |
  |---|---|
  | **Important:** anything I see or depend on (API, CLI, config, schema, data format, defaults, behaviour), scope, a new dependency, anything hard to undo; when unsure, important | **me,** from Grok's options table, shown as Grok wrote it, with your view added if you disagree |
  | **Small:** internal names, helper structure, test layout, wording, what the project's documents already settle | **Grok** in normal mode (listed in its summary under "Small decisions"); **you** in paranoid mode, each said in one status line so I can overrule it |

- **Answer Grok as I answer you:** it numbers its decisions and letters the options, and the
  answer is one line each: `1. A`, `2. B`, `3. explain`, `4. A+B`, `5. other: <suggestion>`;
  yours are marked `(mine)`.
- **Modes:** *normal* (the default) and *paranoid*, where Grok stops at every decision. Use
  paranoid for brittle work (migrations, concurrency, security, data formats) or when I ask;
  propose it when a step looks brittle. **Split a step by risk** rather than running all of
  it paranoid: the brittle part (e.g. handling secrets) in paranoid mode, the rest in
  normal mode, as two tasks or two phases.
- **Grok's "done" is a claim, not a result:** read its diff, run `make check` and
  `make docs` yourself, run the code review, and send the findings back to Grok until it is
  clean. Then present the summary as in "How a step goes", step 7. Check its "Small
  decisions" list as part of the review.
- **When I say commit,** commit in the worktree, fast-forward the main branch to it, and
  remove the worktree.
- Grok's blockers and requests outside its worktree (system packages, `sudo`, other
  clusters or accounts) come to me; don't grant them yourself.

## Decisions

- **Recommend, don't just list:** the recommended option first, marked as such, with why.
- **Show options side by side:** a small table with a concrete example of each (code, output,
  file layout), plus what each costs, and a **Who (does the work)** column: `Claude`,
  `Grok (normal)` or `Grok (paranoid)`; `you` only for something I must run myself (a
  login, my settings). Deciding is always mine; the column never says so. I often pick
  from the table.
- **Precise definitions when I ask to elaborate:** what exactly happens, edge cases, a table
  of common situations and the result in each.
- **When I reject or question a proposal,** rework it; don't defend it. When I ask a question
  in the middle of a choice, answer it, then ask again.
- **Record "Not chosen: …"** for alternatives that were seriously considered.
- Small naming and formatting questions get a few concrete examples, not essays.

## Tests

- Tests come with every change: unit tests for every public function and class, integration
  tests for anything with real processes, servers, databases or the built artifact.
- **Never skip, ignore or xfail a test on your own.** If a test can't run or pass, stop and
  ask, saying why.
- Keep the coverage floor; mention when coverage gets close to it.
- A flaky test is a bug: find the cause (often a race), don't retry it until it passes.

## Bugs

- A bug found while working on something else goes into `BUGS.md` (B-number, found, symptom,
  repro, suspect or cause, plan) and into the step's summary. **Don't fix it on the spot**
  unless it blocks the step; I schedule fixes.
- When you fix one, **start with a test that reproduces it**: run it on the unfixed code and
  show it failing, then fix, then show the same test passing. Remove the entry in the fixing
  commit. Only if the bug can't be simulated in a test, say why and how the fix is checked
  instead.

## Commits

- One commit per accepted step; the message names the step and lists what it did:
  `M2.1: <what>, <what>; <what>`, then the co-author trailer the session asks for.
- Pre-commit hooks must pass; don't bypass them. Stage a rename together with everything that
  uses it, so no commit holds half of it.

## Communication

- Be concise. Lead with the result, then details. Use the project's names in backticks.
- During long work, a one-line status now and then is enough.
- Report outcomes as they are: what passed, what failed (with output), what was skipped.
- When I interrupt with a side question or a side quest, handle it fully, then come back to
  the plan.
- Don't expand scope on your own: propose extra work, don't just do it.

## Design habits I value

- Neutral names in the core; terms from other tools or products only in their compatibility
  layers, as mappings.
- Make invalid combinations impossible to write rather than errors at run time (e.g. an
  option only on the type it applies to).
- Be explicit over magic, but make the common case need no configuration; when something is
  found automatically, log what was found and how.
- Errors say what is wrong and what to do next, and name what was used (e.g. which
  interpreter, from where).
- Times are stored in UTC; shown to people in UTC and local time.

## Environment

- Temporary files go in the session's scratch directory, not `/tmp`.
- A cheaper model (e.g. Haiku as a subagent) may read long logs for the result or gather
  things, when its answer is easy to check; reviews, diagnosis and decisions stay with the
  main model, which reads the decisive lines (a failure's output) itself.
- **Library ages:** a new major or minor version (its `x.y.0`) must be at least 7 days old;
  a patch on a minor that already qualifies may move once it is 3 days old (ages in UTC
  calendar days; a yanked `x.y.0` still dates its minor). A yanked version in the lock moves
  to a version that meets the rule, forward if one exists, else back. Record new libraries in
  `PROJECT.md`.
- Update `NEXT_SESSION.md` at the end of a milestone or when I ask, so the next session can
  start from it.
