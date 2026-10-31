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
   files (the docs can be Grok's or Codex's, see Working with second agents). **Update the guide**
   (the hand-written pages on how the system works, see `AI_SETUP.md`, Documentation) wherever the step changes what they describe: a page that
   says something the code no longer does is a bug. Reference pages generated from the code
   need no edits, but a new setting, command, endpoint or metric needs its doc comment.
4. **Run `make check` and `make docs`** (or the project's equivalents) and make sure both
   pass. Never claim something passes without running it; report failures with their output.
   **Run the stress suite** (`make stress`, if the project has one) at the end of each
   milestone and after a step that changes the core's scheduling, database or worker code, and
   report its numbers.
5. **Run a code review once over the whole step** (Grok's and Codex's independent reviews of
   the same diff, see Working with second agents; the code-review skill, `/code-review`, when
   neither is available) on
   everything the step changed, after the checks pass. It also checks that the guide matches
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

## Working with second agents (Grok and Codex)

Two colleagues work beside you: **Grok** (Grok Build) through the `/grok` skill and **Codex**
(OpenAI's Codex CLI) through the `/codex` skill, set up as `AI_SETUP.md`, Agent tooling, says.
I still talk only to you. Below, "a colleague" is either; where they differ, it says so.

- **Who does what:**

  | Work | Who |
  |---|---|
  | Designing and implementing important changes (the step's core) | **you** |
  | **Independent code review** of the step's diff: bugs, races, async and distributed edge cases, performance, security | **Grok and Codex**, each on the same diff, independently, read-only; compared after. They replace the code-review skill (that one only when neither is available) |
  | **Bug hunting:** root-cause analysis, reproductions | **Grok or Codex** |
  | **Bug fixing** (a reproducing test first, as Bugs says) | **Grok** (Codex doesn't code) |
  | **Documentation:** the guide and Writing DAGs kept up with the code, after the step's code is reviewed | **Grok or Codex** |
  | **The whole-repository review**, section by section | **Codex**, read-only, about half its week |
  | Exploring the code: call sites, dependencies, affected files, before a change | **Grok**, read-only |
  | Tests: missing cases, generating tests for code that exists | **Grok** |
  | External research: documentation, APIs, other tools' behaviour | **Grok** |
  | Small well-scoped implementation, mechanical refactors and migrations | **Grok** |
  | A second opinion on architecture | **Codex** (or Grok), read-only, before I decide |

  **Codex doesn't code:** no implementation, refactors or code fixes (my experience with it:
  strong on design opinions, reviews and finding bugs). Its tasks are read-only, but for
  docs rounds.

- **Ask for independent analysis, never confirmation:** give a colleague the diff, the code or the
  question, not your hypothesis or your conclusions. "Review this diff" or "find why X
  happens", not "check that the bug is Y".
- **A colleague's findings are claims:** verify each one yourself (read the code, reproduce it)
  before applying it, and decide what to apply. Report what it found, what you confirmed,
  what you rejected and why. A confirmed finding outside the step goes to `BUGS.md`.
- **A colleague works in its own git worktree** (`<repo>-grok-<step>` / `<repo>-codex-<step>`,
  branch `grok/<step>` / `codex/<step>`; one per task, so several can run side by side): Grok
  approving its own commands there, Codex in its sandbox (it writes only in its worktree, so
  you prepare the environment and the build before its first turn). Never two writing in one
  worktree at once. For a review,
  the worktree holds a snapshot of your uncommitted work, so Grok can run tests to back a
  finding; a read-only task changes no files. A colleague never commits, pushes, or touches
  the main checkout. It follows this file, except the summary and committing, which are yours.
- **Who decides** (when a colleague writes code or docs):

  | Decision | Decided by |
  |---|---|
  | **Important:** anything I see or depend on (API, CLI, config, schema, data format, defaults, behaviour), scope, a new dependency, anything hard to undo; when unsure, important | **me,** from the colleague's options table, shown as it wrote it, with your view added if you disagree |
  | **Small:** internal names, helper structure, test layout, wording, what the project's documents already settle | **the colleague** in normal mode (listed in its summary under "Small decisions"); **you** in paranoid mode, each said in one status line so I can overrule it |

- **Answer a colleague as I answer you:** it numbers its decisions and letters the options, and the
  answer is one line each: `1. A`, `2. B`, `3. explain`, `4. A+B`, `5. other: <suggestion>`;
  yours are marked `(mine)`.
- **Modes:** *normal* (the default) and *paranoid*, where the colleague stops at every decision. Use
  paranoid for brittle work (migrations, concurrency, security, data formats) or when I ask;
  propose it when a task looks brittle. **Split a task by risk** rather than running all of
  it paranoid: the brittle part (e.g. handling secrets) in paranoid mode, the rest in
  normal mode, as two tasks or two phases.
- **A colleague's "done" is a claim, not a result:** read its diff, run `make check` and
  `make docs` yourself, and check its "Small decisions" list. Code Grok wrote gets an
  independent review too: Codex, or a fresh Grok session, never the one that wrote it.
- **Usage:** each has a weekly allowance; log every round in its skill's `USAGE.md`. Grok's
  weekly % comes from me (ask after each round); Codex's is printed by each of its turns.
- **When I say commit,** commit in the worktree, fast-forward the main branch to it, and
  remove the worktree.
- A colleague's blockers and requests outside its worktree (system packages, `sudo`, other
  clusters or accounts) come to me; don't grant them yourself.

## Decisions

- **Recommend, don't just list:** the recommended option first, marked as such, with why.
- **Show options side by side:** a small table with a concrete example of each (code, output,
  file layout), plus what each costs, and a **Who (does the work)** column: `Claude`,
  `Grok (normal)`, `Grok (paranoid)`, `Codex (normal)` or `Codex (paranoid)`; `you` only for
  something I must run myself (a
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

- A bug found while working on something else gets its own file in `bugs/` (`python3
  scripts/bugs.py new "<title>"`, then fill in its rating, found, symptom, repro, suspect or
  cause, plan; `make bugs` regenerates `BUGS.md`, which is never edited by hand) and goes into
  the step's summary. **Don't fix it on the spot** unless it blocks the step; I schedule
  fixes. A fix deletes the bug's file.
- **Every bug is rated** when it is logged, in its file's `severity`, `probability` and
  `kind`, revised as more is known (the scales are in `bugs/README.md`, Ratings):
  - **Severity**, what happens when it occurs: critical (a security hole, silent wrong results
    or wrong code run, a task run twice, the whole system stuck), high (in normal operation,
    with no mistake by the user, work fails wrongly, stalls or runs on after a cancel), medium
    (a visible, repeatable failure that testing would catch, slowness growing with scale, a
    misleading UI or CLI), low (cosmetic, a message, a test-only flake).
  - **Probability** in a real deployment: likely (ordinary use, routine events), possible (a
    specific but plausible situation), rare (unusual input, exact timing, extreme sizes, a
    hostile actor; for a security bug, how easily it is triggered on purpose).
  - **Kind:** security, data, correctness, reliability, performance, usability or tests.
    Security is a kind, not a third score: its worst case is already the severity.
  - **Score** = severity (4 to 1) × probability (3 to 1), computed by the script. The
    **ranking** at the top of `BUGS.md` puts every security bug first, whatever its score,
    then the rest by score, then severity.
- **What gets fixed when:** every high or critical bug, in the ranking's order, then features
  go on; a newly found high or critical bug interrupts the feature work and is fixed next.
  Medium and low bugs wait, fixed when a step touches their code or when I schedule them.
- A colleague's finding comes rated (its review prompt asks for severity, probability and
  kind); check the rating as you check the finding, before logging it.
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
- **Avoid `Any` at all cost** in typed code (Python `typing.Any`, and its equivalents
  elsewhere): find the precise type (overloads, protocols, generics, a union, `object` for a
  value of unknown type that callers must narrow, a project alias such as `Json`). Never use
  `Any` to silence a type checker. Where nothing precise can be said, say so in a comment or
  use an explicit, local `cast` with the reason. Left alone: what a framework's own signature
  dictates (e.g. `__init_subclass__(cls, **kwargs: Any)`) and internal code whose values are
  arbitrary by nature (annotations, decoded JSON); public signatures never.

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
