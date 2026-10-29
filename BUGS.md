# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that reproduces
it: shown failing before the fix and passing after; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

A bug found by reasoning but **not reproduced yet** is logged too, marked **Suspected**, with
how it would be confirmed; once a test shows it, the mark goes. If it turns out not to be a
bug, the entry is removed, saying why in the commit.

## B35: a parse upload refused for its size is retried forever, never shown as an import error (Suspected)

- **Found:** 2026-10-29, by the review of B30's fix (by reading). A refused upload
  (`OUT_OF_RANGE`) leaves the parse job assigned with nothing recorded for the root; after
  the 10-minute `parse_job_timeout` the job goes to the next worker, which fails the same way.
- **Effect:** a root past `[master] grpc_max_message_size` keeps its stale DAGs, the UI shows
  no import error, and the only sign is a worker log line (`parse result not uploaded`).
- **To confirm:** a master test with a hub limited to 4 MiB, an upload past it, then the
  job's timeout. **A fix:** the worker reports a too-large result as a parse failure the
  master records for the root, naming the setting.

## B34: a parse upload blocks the agent's session loop while it is sent (Suspected)

- **Found:** 2026-10-29, by the review of B30's fix (by reading). The agent awaits
  `upload_parse_result` inside its session loop (`crates/schedr-agent/src/agent.rs`,
  `on_done`), so no heartbeat is sent and no master message is read meanwhile. B30's fix allows
  uploads of 64 MiB and more (up to 1 GiB), where it was 4 MiB.
- **Effect:** an upload slower than the lease (60 s) gets the worker marked lost and its
  running attempts ended.
- **To confirm:** an agent test with a slow upload and a short lease. **A fix:** upload from a
  task of its own, its result reported back to the loop.

## B33: a task's output past `[master] grpc_max_message_size` ends its worker's stream (Suspected)

- **Found:** 2026-10-29, by the review of B30's fix (by reading). The limit applies to the
  worker's `Control` stream too: a `Finished` larger than it makes the master's decoding fail,
  which ends the whole stream, not just that message. Nothing checks a task's
  `max_output_size` against the limit (neither the Python API nor `validate.rs`).
- **Effect:** with `max_output_size` set above the limit, such an output drops the worker's
  link; the result is journalled and sent again in the next `Register`, which is too large as
  well, so the worker may never register again.
- **To confirm:** an agent and master test with a task returning more than a small limit.
  **A fix:** refuse a `max_output_size` above the limit when the DAG is validated, or fail the
  attempt with `output_too_large` on the worker before sending.

## B32: the UI briefly freezes on Trigger and other links, the request left pending (Suspected)

- **Found:** 2026-10-28, by the user (dev notes): clicking "Trigger", and sometimes other
  links, the page hangs for a moment with nothing happening; the browser's Inspect shows the
  request `Pending`; then it goes through and the DAG is triggered properly.
- **Suspect:** the browser's limit of six HTTP/1.1 connections per host: the run and log
  pages hold SSE streams (live changes, the log tail), and other tabs on the same UI hold
  theirs, so a new request waits for a free connection (B25 and B26 reduced this, not to
  zero). Not checked.
- **To confirm:** a browser test with several live pages open on one UI, timing a click
  until its request is sent. **A fix** depends on the cause (one shared stream per browser,
  HTTP/2, fewer streams per page).

## B31: a finished task stays `scheduled` on the UI long after it ended (Suspected)

- **Found:** 2026-10-28, by the user (dev notes): during a run a task stays `scheduled` on
  the run page for quite a long time before it shows `success`, while its times say it
  finished a second after its upstream task.
- **Suspect:** the run page isn't told of the change and waits for the webserver's fallback
  `refresh`, every 15 s (`REFRESH_INTERVAL`, `crates/schedr-api/src/events.rs`), e.g. when
  the database's changes can't be followed (SQLite, standalone) or a change of that kind isn't
  announced. Not checked.
- **To confirm:** a test running a two-task DAG in standalone with a run page's live stream
  open, timing each task's state change against when the page hears of it.

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
