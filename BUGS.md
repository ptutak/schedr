# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that reproduces
it: shown failing before the fix and passing after; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

A bug found by reasoning but **not reproduced yet** is logged too, marked **Suspected**, with
how it would be confirmed; once a test shows it, the mark goes. If it turns out not to be a
bug, the entry is removed, saying why in the commit.

## B30: large parse uploads exceed the master's default gRPC receive limit

- **Found:** 2026-10-29, reproduced with `make stress STRESS=stress_01_throughput`,
  quick profile ×4. Its 201 DAGs (9,001 tasks) produce a 4,922,341-byte parse upload.
  The worker logs `parse result not uploaded`, with `OutOfRange`: the message exceeds
  tonic's default 4,194,304-byte receive limit. `Hub::service` does not override it.
- **Effect:** no DAGs are stored, so the first stress scenario appears stuck until its
  ten-minute parsing deadline. Production roots with similarly large parse results also
  fail. The limit applies to the whole root's parse upload, not each DAG separately.
  The quick profile scales with CPU and memory; the ×2 baseline's smaller upload did not
  expose this limit.
- **Proof:** paired gRPC tests in this session uploaded the same valid 5,429,488-byte
  parse: the default rejected it; changing only the server's receive limit to 8 MiB
  accepted it and stored all 201 DAGs. A second, process-level regression with two DAGs
  produced a 6,292,428-byte upload and failed against the original binary at the same
  4 MiB limit. These tests and the attempted fixes were reverted at the user's request.
- **Plan:** add `[master] grpc_max_message_size` with a 64 MiB default and its environment
  mirror, represented by a validated size type.
  **Deferred:** the user requested only this bug report; no fix is retained.

## B29: a `@branch` edge to a target it didn't choose isn't faint on the run graph (Suspected)

- **Found:** 2026-10-28, by Grok while writing the docs for M4.1a-M4.2a (by reading).
  `joined_edges` (`crates/schedr-ui/src/runs.rs`) marks an edge "not taken" only when it has
  an `on` list; a selection edge has none, so an unchosen target's line stays solid (its box
  shows `skipped`). The guide (12-ui) describes it as it is.
- **To confirm:** a UI test rendering a run whose branch chose one of two targets. **A fix:**
  mark a selection edge not taken once its branch succeeded without choosing the target
  (the branch's stored output), and say so in the guide.

## B28: the master doesn't check that selection edges come only from `@branch` tasks (Suspected)

- **Found:** 2026-10-28, while fixing Grok's review of M4.1a-M4.2a. `dag_problems`
  (`crates/schedr-master/src/validate.rs`) checks edges' ends and data edges for references,
  but not an edge's kind against its upstream: a hand-written or old DAG JSON can have a
  selection edge from a plain task, or a plain edge from a branch. The Python API never
  writes either (`DagBuilder.add_edge`).
- **Effect:** a selection edge from a plain task is taken only when that task's output
  happens to read as a list of IDs naming the target (`Execution::chosen_of`); otherwise its
  target is skipped as not chosen.
- **To confirm:** a master test settling a run whose plain task has a selection edge.
  **A fix:** refuse both shapes in `dag_problems`.

## B27: a reference inside a list or dict argument isn't checked against the element's type

- **Found:** 2026-10-28, while fixing Grok's review of M4.1a-M4.2a. `_argument`
  (`python/schedr/tasks.py`) checks only a top-level reference against the parameter's type;
  references in a container are checked only for their path. Shown: `total([name()])` with
  `total(values: list[int])` and `name() -> str` parses; the run then fails with
  `input_mismatch`, not retried.
- **A fix:** check each reference against the type `_build.typed_references` already gives
  it (the None check's walk), as the top-level one is.

## B24: a forked attempt started while the master link is down is recorded as `spawn` (Suspected)

- **Found:** 2026-10-27, by Grok's review of M2.5.4a (by reading; not executed). The agent
  sends `Started` before the process exists, with `spawn`, and a second `Started` with
  `fork` once a fork server has forked it (`Done::Spawned` in
  `crates/schedr-agent/src/agent.rs`). That second message is sent only while the master
  link is up (`if forked && let Some((to_master, _)) = &link`); when it is down, it is
  skipped, and the `Register` sent on reconnecting carries the attempt's journal state but no
  start method (`journal_entries`).
- **Effect:** the attempt shows `start_method: spawn` in the attempts API although it was
  forked; nothing else (the start-time metric is the agent's own and counts `fork`).
- **To confirm:** an agent test that forks an attempt while disconnected, then reconnects,
  and checks the methods the master gets. **A fix:** keep the method in the journal record
  (`JournalRecord::Running` with the process identity) and send it with the journal entries on
  `Register`, or send `Started{fork}` again on reconnecting.

## B22: a draining worker once missed its 8 s grace under load

- **Seen:** 2026-10-26, one `make check` while other role tests were running.
  `roles::a_draining_worker_stops_a_task_already_being_cancelled_within_the_grace_period`
  (`crates/schedr/tests/roles.rs`) panicked: `terminate_within` for the worker, grace 8 s,
  was not success ("the worker didn't stop within the grace period"). 37 role tests had
  passed; that one failed and `make check` exited 2.
- **Not reproduced** on the next full `make check` (1257 Rust unit tests, 662 Python unit
  tests, 99 Rust integration tests, 27 Python integration tests, then `make docs`).
- **Suspect:** the 8 s grace under load, the same kind of limit as B21 (a bound that is
  long enough until the machine is busy). Not fixed.
- **Seen again:** 2026-10-28, `make check` during M4.1b (the same message, `None`: the
  worker hadn't ended at all). Alone it passed three times in about 10.5 s, so it is the
  load again, not a hang. Two misses in three days.
- **Next:** look at the grace and what the worker was doing in those 8 s (its log in the
  test's output shows the drain's steps), then decide whether the limit or the drain is
  what's slow under load.

## B4: `runs trigger` in a roles integration test got no result once

- **Seen:** 2026-10-22, the first `make test-integration` of M2.3:
  `roles::a_worker_that_never_comes_back_is_lost_and_stays_listed` panicked in
  `Cluster::trigger` (`crates/schedr/tests/roles.rs`): `cli_json(["runs", "trigger", …])` was
  `None`, so the CLI exited non-zero, after 34.8 s; the whole run took 35 s instead of the
  usual 12 s.
- **Not reproduced** in the two runs right after (53/53 passed each time). Postgres had just
  been started by podman; `cli_json` doesn't keep the CLI's stderr, so the reason is unknown.
- **2026-10-23:** the role tests now keep every failed or slow (over 2 s) CLI call's stderr,
  and the system's I/O stall, in their failure messages. Most likely B6 (fixed the same day:
  a 7–8 s Postgres stall after a rebuild, and the master declaring live workers lost during
  it; the trigger's CLI waits 30 s for the master's decision, and the run took 35 s instead
  of 12 s, as in B6's stalls), but unproven: no test reproduces it.
- **Next:** if it happens again, read the CLI's stderr in the failure message; otherwise close
  it after a few weeks of clean `make check` runs.
