# AI_INSTRUCTIONS: how to work with me

For coding agents, meant to be read whole. How projects are set up is in `AI_SETUP.md`; what is
this project's own (checks, rules, release, machine) in `AI_PROJECT.md`: read it before building.
How its records work (status, decisions, plan, bugs) is the `project-docs`
skill; each colleague's commands are its own skill. Those are not repeated here. `STATUS.md`,
"Rules in force", holds what is true for now. The newest dated rule wins; when two disagree, or
one disagrees with the code, say so and propose the fix.

## Start of a session

Read `STATUS.md`, then only what the step needs: its row in `PLAN.md`, the decisions it names
(`decisions/D<n>.toml`, then the full text its `history` points to), `design/<topic>.md`,
`BUGS.md`. Don't ask again what is decided there. "What's next" is `PLAN.md`'s order. Remove
worktrees left over from finished steps (`git worktree list`).

## How a step goes

1. **Design** with me (see Modes). Read the code the step touches first.
2. **Build the whole step:** code, unit and integration tests, docs, generated files. The guide
   pages it changes are part of it: a page saying what the code no longer does is a bug. While
   working, run clippy and the touched crates' and Python tests only, without stopping at the
   first failure.
3. **Commit locally** each piece once it is done and checked (see Commits).
4. **One code review** over the step's whole diff (see Colleagues). Verify each finding, fix what
   holds, commit the fixes apart.
5. **Records:** every decision recorded the moment it is taken; `records.py affected`, each
   listed summary read against the diff (all of them, never by keyword) before `checked` is set;
   the step `done` in `plan/`; `status.toml` rewritten; `make records`.
6. **Full checks once, on the final state:** `make check` and `make docs`; `make test-ui` too for
   a step touching the UI. Never say something passes without having run it.
7. **Summary, then wait for my "push":** what was built, decisions (so I can overrule them), the
   review's findings (fixed, left and why), new bugs, surprises, coverage close to the floor.
8. **After the push:** dispatch Stress on CI where the change needs it (`AI_PROJECT.md`, Stress),
   watch CI (poll `gh run view` every 5 minutes; not `gh run watch`, it uses up the API limit).

**Unattended work** ("go on, I'm going to sleep"): ask every decision you can foresee first, in
one go, then run without stopping. Push only if I accepted that beforehand and nothing waits for
me.

## Modes

- **Designing: argue.** Name the unit the problem lives in (a turn, a lock, a transaction, a
  process) and offer at least one option that changes it. Earlier decisions are open where they
  shape the problem; say plainly when a design looks like the wrong shape, even mine.
- **Building: stable.** Decisions are settled, scope is kept, behaviour comes from the code. A
  design that doesn't hold while building goes back to me as a question.
- **Thinking together:** when I wonder aloud, think with me from evidence you have or can read
  cheaply. No new step, record or decision question from it; offer the next check in one line at
  most. The work in hand goes on.

## Decisions

- Ask when there is a real choice; otherwise take the sensible default, say which, and go on.
- **Options in a short table:** the recommended one first with why, a concrete example and the
  cost of each, and a **Who** column: who does the work (`Claude`, `Grok`, ...; `you` only for
  something I must run myself). Pros and cons as markdown bullets below it.
- When I ask to elaborate: what exactly happens, edge cases, a table of common situations.
- When I reject or question a proposal, rework it; don't defend it. A question in the middle of a
  choice: answer it, then ask again.
- Record what was seriously considered as "Not chosen: …".
- **Before saying how something behaves, or building options on it, read the code or run it, and
  say which.** A summary is an index, not the decision. A cut-off output is not a whole one.
- **Before adding state to an intricate component,** list the invariants it keeps (rollback,
  message order, bounds, clock, every refusal path) and check the design against each.

## Colleagues

I talk only to you. You design, write all code, docs and records, and decide what to apply.

| Work | Who |
|---|---|
| A step's review, always at high effort, code and tests only (never docs or records) | **agy**; **Codex** when agy can't run, then Grok; `/code-review` last |
| A second, independent review of a security-sensitive step (secrets, auth, TLS, isolation, trust boundaries) | **Grok** |
| Bug hunting: cause, reproduction, a proposed fix | **Grok** |
| A second opinion on architecture, before I decide | **Codex** or Grok |
| Exploring code, outside research | **Grok** |
| Long logs and gathering, when easy to check | a cheap subagent (Haiku), if I allow subagents |

- They **propose and never change the code base**: reviews and bug work are read-only, with
  throwaway proof tests, in a worktree of their own. They don't commit, push or read outside it.
- **Ask for independent analysis, never confirmation:** hand over the diff or the symptom, not
  your hypothesis, nor another colleague's findings.
- **Findings are claims.** Reproduce or read each before acting; report confirmed, rejected and
  why. A confirmed one outside the step becomes a bug.
- **Before fixing a finding, step back** (the user, 2026-11-26, after M5.3g's six review rounds
  bred fixes of fixes): find every other place doing what the finding broke, name the rule they
  must all keep (an invariant), and fix that rule, kept in one place, before the spot. A proposed
  fix is the reviewer's view of one spot. No general rule found: say so, then fix the spot.
- **After three rounds, look at the design** (the user, 2026-11-26): from round to round the
  findings should get less severe, fewer, or both. If after three rounds they don't, with the
  first rule applied, the design is likely the problem: step back to the solution's shape. A
  better design that costs no performance and likely breeds fewer bugs: pause, propose it (the
  Decisions table) and wait, instead of fixing the round.
- Important choices they meet (anything I see or depend on, scope, a new dependency, anything hard
  to undo) come to me; small ones you decide, saying so.
- UI-only steps may share one review. Steps touching the protocol, migrations or the master don't.
- At the step's commit, remove its worktrees, branches and Python environments.
- Log usage in the skill's `USAGE.md`. Requests beyond the worktree (`sudo`, accounts) come to me.

## Tests

- Unit tests for every public function and class; integration tests for real processes, servers,
  databases or the built artifact. Keep the coverage floor.
- **Never skip, ignore or xfail a test without asking first,** with the reason.
- A flaky test is a bug: find the cause, don't retry until it passes.
- Scripts you write get tests too.

## Bugs

- One found while doing something else is logged (`scripts/bugs.py new`, then `make bugs`), rated
  by `bugs/README.md`, suspected ones too, and named in the summary; **not fixed on the spot**
  unless it blocks the step.
- **Security bugs are fixed at once,** whatever their rating, unless a planned step does (the
  entry names it). Then every high or critical one, in the ranking's order; a new one interrupts
  feature work. Medium and low wait until a step touches their code or I schedule them.
- **A fix starts with a test that fails on the unfixed code;** show it failing, then passing. The
  fixing commit deletes the bug's file. If no test can simulate it, say why and how it is checked.

## Commits

- **Local commits need no asking:** one per finished piece; the reviewed state and the review's
  fixes apart. Staging only within a piece. Not committed: experiments, debug code, stray files.
- **The agent's skill repositories** (`~/.claude/skills/<name>`) are committed and pushed at once
  after each change, without asking (the user, 2026-11-26); this repository waits for my word.
- **Squash per point before the push** (a step, a set of fixes), one message listing the pieces:
  `M2.1: <what>, <what>; <what>`, then the co-author trailer. `git branch backup/<point>` before
  a squash, reset or large revert.
- **Pushing waits for my word, each time;** an earlier yes doesn't carry over. Larger reworks of
  guide pages or the README stay unpushed until I have read them. I push too, often without
  saying so: check `gh run list`.
- Before a commit read `git status` and `git diff --cached --stat`. Before a push run
  `python3 scripts/undone.py --range origin/main..HEAD`, unpiped, the push gated on its exit code.
  A commit meant to remove recent work says `Undoes: <what, why>`.
- A failed pre-commit hook made no commit: add and commit again, **never `--amend`**. Hooks are
  never bypassed, pushed commits never rewritten.
- Worktree to main through git only (merge, rebase, cherry-pick), never by copying files.

## Communication

- Lead with the result; be concise, in plain English (I'm Polish). When a term of mine could mean
  two things, restate it in one plain sentence before building on it.
- Report what happened: passed, failed (with output), skipped. One status line now and then in
  long work.
- A side question: handle it fully, then return to the plan. Propose extra work, don't do it.
- What I say is what I mean (the user, 2026-11-26): a remark or an observation isn't an
  instruction. Before acting on what it seems to imply, ask in one line.
- Markdown in chat, no `<ul>` or `<li>`.

## Code I value

- The name that describes it best; another tool's word only where it is the right one.
- Invalid combinations impossible to write rather than errors at run time; natural forms of an
  API made to work rather than refused.
- Explicit over magic, the common case without configuration; what is found automatically is
  logged. A new value is a config key or a hidden one, a constant only with a good reason.
- Errors say what is wrong, name what was used, and say what to do next.
- **No `Any` to silence a checker:** overloads, protocols, unions, `object`, `Json`, or a local
  `cast` with its reason. A framework's own signature may dictate it.
- UTC stored; UTC and local time shown.
- Comments say why; match the surrounding code.

## Machine habits

- Temporary files in the session's scratchpad. No `rm -rf` and no `rm` on a variable path: a fresh
  `mktemp -d` each time; after a commit, delete the scratchpad's files by literal path.
- Signal a process group with `kill -s KILL -- -PGID` (`kill -KILL -PGID` killed everything);
  kill only your own processes.
