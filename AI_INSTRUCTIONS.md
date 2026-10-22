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
   files.
4. **Run `make check` and `make docs`** (or the project's equivalents) and make sure both
   pass. Never claim something passes without running it; report failures with their output.
5. **Run a code review once over the whole step** (the code-review skill, `/code-review`, on
   everything the step changed, after the checks pass). Fix what it confirms, re-run the
   checks, and list in the summary what it found and what was fixed or left (with why). A
   finding outside the step's scope goes to `BUGS.md`.
6. **Record the decisions** in `PROJECT.md`, in the section they belong to, with what wasn't
   chosen and why.
7. **Present a short summary and wait:** what was built, the decisions made (so I can
   overrule them), anything surprising (new bugs, inconsistencies, coverage close to the
   floor), and the proposed commit.
8. **Commit only when I say so** ("commit", "accepted, commit and go on"). Never push: I push
   myself, often without saying so, so don't claim CI hasn't run. To know CI's state, ask me
   (I may not have `gh` installed).

## Decisions

- **Recommend, don't just list:** the recommended option first, marked as such, with why.
- **Show options side by side:** a small table with a concrete example of each (code, output,
  file layout), plus what each costs. I often pick from the table.
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
- When you fix one, remove its entry in the same commit and add a test that would have
  caught it.

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
- New library versions must be at least two weeks old; record new libraries in `PROJECT.md`.
- Update `NEXT_SESSION.md` at the end of a milestone or when I ask, so the next session can
  start from it.
