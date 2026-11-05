# Next session: where things stand

Read this file, then `PLAN.md` (M4 and M4B), `BUGS.md`, and the parts of `PROJECT.md` a step
names. `PROJECT.md` is the decision log: every decision goes there, in its section, with
"Not chosen" and why.

## State (2026-11-05, morning): M4.8f committed; M4.8z (chaos) next

- **Done: M4.8f** (decisions 278, 279; from a side note): a resource's cleanup can be cancelled
  (`schedr tasks cancel`, the panel's Cancel; a run's cancel still keeps them), the resource
  then `cleanup_cancelled`, the run `cancelled` (`failed` if a task failed); a resource cleanup
  without `timeout=` gets `[worker] resource_cleanup_timeout` (30 min, the user's pick, per try;
  public). Codex's review: 1 low, fixed; docs by Codex.
- **Next: M4.8z (chaos scenarios),** the user's go (2026-11-05): run the new chaos scenarios
  while building them, and the whole quick stress suite once before committing.
- **Flaky under load?** `schedr-agent agent::tests::a_stuck_fetch_of_inputs_doesnt_hold_up_the_stop_or_the_heartbeats`
  failed once (12 s) while a worktree build ran beside it; passed 3/3 alone. Watch it.

## State (2026-11-05, early morning): M4.8e committed; M4.8 done, stopped as asked

- **Tonight's instruction:** "commit and push as needed, keep up with the review, if decisions
  wait for morning, stop after M4.8". Done: M4.8c2, M4.8d and M4.8e committed and pushed.
- **Done: M4.8e:** a run's cleanups and resources in the API (`AttemptView.cleanup`,
  `TaskView.cleanup`, `RunDetail.resources`), `schedr dags test --json`, `run_dag`
  (`Attempt.cleanup`, `TaskResult.cleanup`, `RunResult.resources`) and the UI (status
  `state · outcome · cleanup`, the Cleanup tab, the Resources table, caps and rail per 262, edges
  per 272). Codex's review found 8, all fixed (handles masked as task data in the API; see
  PROJECT.md's "Built (M4.8e" entry); docs by Codex.
- **My calls, for the user to overrule:**
  - M4.8d: ScriptResource and `cleanup_script` moved to M4.9; `.value` on an `@resource` setup and
    a `CommandResource` (typed as the value); `run_dag` gives a `CommandResource` setup's output as
    JSON; Codex's finding 7 (a late report by a child process during the output drain) not taken.
  - M4.8e: the UI draws the user -> cleanup lines itself (the DAG has none); handles stay as stored
    JSON in `run_dag`; `worker_lost` reads `cleanup lost`; a mapped task shows its instances' worst
    cleanup.
- **No new configuration values** in M4.8c2, M4.8d or M4.8e.
- **Not checked by eye:** the graph's caps and rail in a browser (the HTML is tested, the look
  isn't); worth a glance, or a Playwright case in `make test-ui`.
- **Next:** M4.8z (chaos scenarios), then M4.9. Grok's weekly reset is Friday 9:00.
- **Codex:** weekly 35%, 5 h 21%.

## State (2026-11-05, small hours): M4.8c1 committed; the user relogs

- **Done:** M4.8b, b2, b3 (decisions 242-258) and M4.8c1 (decisions 259-269): `@resource`,
  `Resource(name)`, `@cluster.cleanup`, `uses=`, the `requirement` edge, `Dag.resources`, the
  master's validation, `docs/02-dags/16-resources.md`. See PROJECT.md's "Built (M4.8c1" entry.
  Only parsing: the master doesn't run a resource as one yet, and the page says so; remove that
  note in M4.8c2.
- **Decisions the user added late:** 266 what follows a group waits for the cleanups in it, unless
  it or anything after it uses the resource, `blocking=False` to opt out; 267 one cleanup per
  resource wherever imported; 268 `@cluster.cleanup` as a method; 269 generators for connections,
  two functions for resources.
- **Next: M4.8c2** (master's state, cleanup, run result), then M4.8d, M4.8e (caps and rail UI),
  M4.8z (chaos scenarios, nightly `stress.yml`). Flow per step: finish, Codex review (Sol, high;
  also the security review until Grok's reset, Friday 9:00), docs by Codex (terra, medium),
  `make check`, `make docs`, commit, push, remove worktree, branch and env; stop on decisions.
- **For c2:** `Dag.resources[].users` is what the cleanup waits for: those edges aren't in
  `Dag.edges`. A requirement edge can only stop a task, never enable it
  (`crates/schedr-master/src/progress.rs` around 421 and 458 must learn this; `may_skip` and
  `check_nones` in `_build.py` already did). `RunIf` has no "all done", so the master gates the
  cleanup itself. An order edge from a cleanup (decision 266): a failed cleanup gives
  `upstream_failed`. Run cancel keeps resource cleanups. `run_dag` and `schedr dags test` run the
  example today only as ordinary tasks.
- **Decided after the commit (uncommitted in PROJECT.md):** 270 slot kept until cleanups end, 271
  `cluster.cleanup_command`/`cleanup_script`, 272 the graph draws what follows from the cleanup when
  blocking, from the attached tasks and dashed when not. PLAN.md's M4.8d row and the Resources
  section still say `CommandCleanup`: update them at M4.8d.
- **273 (also uncommitted):** a cleanup waits for an `unschedulable` user, up to
  `@cluster.cleanup(unschedulable_timeout=...)`; then the user is `failed`. Goes into M4.8c2. There
  is no "clear" in schedr: `tasks retry` only re-queues `unschedulable` tasks. Nothing is open for
  the rest of M4.8.
- **Codex:** weekly 24%, 5 h 36%.

## State (2026-11-04, end of day): M4.7e committed; stopped for the day

- **M4.7e** (decisions 207-211, the user's): connections in the web UI. A Connections page
  (every deployment's, by root) and a DAG page's section above its runs, Check all per root
  and Test per row behind a confirmation, results filled in live; `GET /api/v1/connections`.
  See PROJECT.md's "Built (M4.7e" entry. Reviewed by Codex (security included; Grok out until
  Friday 9:00): 9 findings, all fixed; the guide's Connections section by Codex (terra).
- **CI:** M4.7d's run green, the SSH test fixed (OpenSSH 10's `ssh-keyscan` comments).
- **Also in M4.7e (decision 212, the user's):** every connection type's clients in the base
  image, Python (psycopg, PyMySQL added) and command line (psql, MariaDB's client, Mongo's
  tools, ssh, curl; about 670 MB); `schedr[connections]`; the demo image only adds scripts.
  Later, if someone needs it: a slim image beside the full one (decision 212).
  Committed before `make image` and `make demo-check` finished on it (the user's call): check
  both, and CI's image job.
- **Next:** M4.8 (PLAN.md), or B160 (a task's timeout restarting after its secrets: a
  behaviour change for the user to decide). Codex was reset on 2026-11-04.

## State (2026-11-04, small hours): M4.7 done (M4.7d committed); paused

- **M4.7d built** (decisions 202-206): the connections demo, `examples/demo`; `make
  demo-check` passes (both deployments' runs, `connections check` and a `test` of all seven
  connections in both). See PROJECT.md's "Built (M4.7d" entry. Decision 206 (the user's):
  Mongo users log in where Mongo's clients do; documented.
- **CI's SSH test fixed:** Ubuntu 26.04's runners have OpenSSH 10, whose `ssh-keyscan`
  writes `# host:port SSH-2.0-...` comments on standard output; the test took `#` as the
  impostor's host. It now skips comment lines.
- **B161 logged** (suspected): `No` masked in the demo's MySQL client error, matching no
  secret; not reproduced with the mask alone.
- **Codex's review** (security included; Grok out until Friday 9:00): 9 findings, all
  fixed (PROJECT.md's M4.7d entry); no guide round (the demo's README and the DAG page
  cover it). Codex at 94% weekly: the user resets it.
- **Paused** (the user's instruction: pause after M4.7). Next: M4.8 (PLAN.md), or B160 (a
  task's timeout restarting after its secrets: a behaviour change for the user to decide).

## State (2026-11-03, late night): M4.7c committed; M4.7d next

- **M4.7c committed** (decisions 174, 193, 198-200): `schedr connections check` / `test`.
  Codex's review (11 findings) and Grok's security review (5, two shared) all fixed: see
  PROJECT.md's "Built (M4.7c" entry. B160 logged (a task's timeout restarting after its
  secrets are read: a behaviour change for tasks, for the user to decide).
- **CI:** Mongo pinned to 7.0.45 (8.x fails on Linux 6.19+); the SSH host-key test failed in
  CI only (`SSHException` where `BadHostKeyException` is expected, passing locally): it now
  records the exception's message and CI prints sshd's log on failure. Read them after this
  push.
- **Next: M4.7d** (decisions 202-205), the demo; its security review by Codex (Grok out
  until Friday). Then **pause** (the user's instruction).

## State (2026-11-03, night): M4.7b2 committed; M4.7c next

- **Grok is out until Friday 9:00** (97% weekly after M4.7b2; M4.7c's security review its
  last, the user): M4.7d's security review goes to Codex.
- **The user's standing instruction:** continue to the end of M4.7 (now M4.7b2, M4.7c, then
  the demo M4.7d), each sub-step reviewed (Codex, then Grok security-only), its guide by Codex
  on gpt-6.1-terra medium, `make check`, committed and pushed; stop only for the user's
  decisions; **pause after M4.7d**. No history rewrite.
- **M4.7b2 committed** (decisions 184, 190-197, 201): commands get connections as `PREFIX_*`
  variables and tools' files in `/dev/shm/schedr-<uid>/<hash>/<worker>/<attempt>/...`
  (`[worker] connection_dir`); MariaDB supported (`mariadb://`, an 11.8 container in the
  integration tests and CI). Codex's review (10 findings) and Grok's (5) all fixed: see
  PROJECT.md's "Built (M4.7b2" entry. The docs say `mysql --defaults-file` and `curl -q -K`.
- **Next: M4.7c** (decisions 174, 193, 198-200): `schedr connections check` (agent-side
  rendering) and `test` (a connection opened by a Python runner one-off on a worker, no slot,
  one at a time per worker, 60 s default with `--timeout`; without a check, open and close;
  a Generic renders its values). Planned: `Command::CheckConnections` /
  `TestConnection` in `crates/schedr-model/src/commands.rs`, stored in `commands` (`result`
  JSONB), the master sending a new MasterMessage to a chosen worker like `AssignParse`, the
  CLI polling `GET /api/v1/commands/{id}`.
- **Then M4.7d** (decisions 202-205): the podman-compose demo (`make demo`): Postgres,
  MySQL, MariaDB, Mongo, SSH, go-httpbin (pinned); dev + prod standalones (one DAG folder,
  separate secrets) without UI, one `schedr ui`; a demo image FROM the base with the client
  tools.

## State (2026-11-03, evening): M4.7b1 committed; M4.7b2 next

- **The user's standing instruction:** continue until the end of M4.7, each sub-step
  reviewed, checked, committed and pushed; stop only for the user's decisions. **No git
  history rewrite for now** (the user, 2026-11-03); `user.email` here is
  `p.tutak@outlook.com`.
- **M4.7a committed** (`c45d766`, decisions 174, 179-182).
- **M4.7b1 committed** (decisions 183, 185-189): the built-in types `HTTP`, `SSH`, `Mongo`,
  `SQL`, `Generic` (`python/schedr/connection_types.py`), each library an extra; tested
  against real servers (MySQL, Mongo, an OpenSSH server added to `make test-integration` and
  CI). Decision 187: SQL takes libpq's `sslmode` for both databases, `verify-full` by default;
  188: `user`/`password` apart from the URL, and t-string values in a URL percent-encoded
  (the reference's `encode: "url"`, the agent's renderer, the masker's encoded form); 189:
  Mongo verifies TLS by default. Reviewed by Codex (8 findings) and Grok's security review
  (4), all fixed. `WORKAROUND(pymysql==1.2.3)` in `_mysql`: MySQL 8's first login without TLS
  (upstream fix PyMySQL #1275, unreleased): remove once a release has it. B159 logged (a
  registration test past its 10 s under `make check`'s load).
- **Next: M4.7b2** (decisions 184, 186): what commands get from a typed connection: the dict
  form `CommandOperator(connections={"API": billing_api})`, `API_*` variables, the agent's
  files (0600, in the attempt's folder), `<PREFIX>_JSON`, `task-ctx connection --env` writing
  the files; `PGPASSWORD`/`MYSQL_PWD` from decision 188's `password`. Then **M4.7c**.
- **Usage:** Grok 94% weekly (reset Friday 9:00), Codex 87%.

## State (2026-11-03, day): M4.6f done; M4.7a in progress

- **The user's standing instruction:** continue until the end of M4.7, each sub-step
  reviewed, checked, committed and pushed; stop only for the user's decisions.
- **M4.6e committed** (decisions 176-178, B155 closed): secrets typed as their values; the
  guide warns a fallback is never secret.
- **M4.6f committed** (decision 175, B31/B32 closed): one stream per browser in a
  **SharedWorker** (`crates/schedr-ui/assets/live.js`; the user chose it over Web Locks, which
  need https or localhost), the server's `/events` unfiltered JSON, each page filtering; the
  log tail polled; the SSE extension removed. Reviewed by Grok (5 findings, all fixed: an
  error answer ended the log tail; the colour at the panel's cut was lost; a crashed page kept
  the worker's stream, now let go after 3 silent minutes; a `refresh` of another deployment
  read by every page; stale PROJECT.md text). B156 logged (the command poll's same shape).
- **M4.7a in progress** (decisions 174, 179, 180, the user's 2026-11-03): `@connection`
  functions recorded by the parser per root; the agent renders a connection for the task
  (Python `with name():` over `connection_get`, commands `schedr task-ctx connection NAME --env PREFIX | --json`);
  **a task opens only what it declares** (`@task(connections=[...])`, operators too), each
  rendered before it starts; values are secrets and plain values only (no `dag_ctx`); a
  declared name the folder lacks is a warning on the DAG's page (computed on read from the
  root's current parse), checked by `schedr dags check` over a folder, and fails the attempt
  before its code runs; doubles `task_context(connections=)` and `run_dag(connections=)`
  (`dags test --connection`). Built and tested end to end; reviewed by Codex (9 findings, all fixed) and Grok's
  security review (4: 3 fixed, the key beside the database kept for a local target); the
  user's decisions 181 (a connection over 1 MiB fails its import) and 182 (a file refused
  after it imported keeps its connections). `make standalone` and
  `standalone-separate-ui` keep a key per deployment and set the docs examples' secrets.
  B157 logged (inline inputs near 4 MiB). Left: Codex's operator guide round (terra),
  checked; `make check`; commit, push (pre-accepted through M4.7); drop the worktree
  `schedr-codex-4.7a`, its branch and pyenv env.
- **Sandboxes:** Grok's and Codex's `sandbox.sh` give `uv` a cache of its own
  (`~/.cache/grok-uv`, `~/.cache/codex-uv`; Codex's also in its `writable_roots`): `uv` failed
  with a read-only `~/.cache/uv` (the user saw it in a side agent's log).

## State (2026-11-03): M4.6 done

- **Committed:** M4.6a+b (`b702efc`), M4.6c (`f9589e2`), M4.6d (Vault: the client over
  `reqwest`, `type = "vault"`, the chart's `secretStorage`, the test Vault in
  `make test-integration` and CI; decision 173; reviewed by Codex and Grok, both for
  security). **M4.6 is done.**
- **The M4.6 guide** (written by Claude: the user said "do it yourself, save Grok"):
  `docs/02-dags/13-secrets.md` (DAG authors), `docs/03-guide/13-secrets.md` (operators,
  decision 173), `examples/docs/secrets.py` (tested with `--secret` values), the stale lines
  fixed. **B155** logged: a secret or a t-string passed to a typed Python task parameter
  fails mypy (and `task_ctx.secret` is `Any`): a typing decision for the user.
- **Overnight (2026-11-03):** B31 and B32 reproduced by Codex: one cause, the browser's six
  HTTP/1.1 connections held by live pages' streams (bug files updated). **Waiting for the
  user:** B31/B32's fix, B155's typing, and the M4.7 questions (in the last message of
  the session, `claude --resume` below).
- The user said: no dynamic secrets planned; most probably no release before 1.0.0.
- **Grok's weekly usage: 85%**, Codex's 80% (resets: Grok Friday 9:00).
- **Rust coverage** stays just over the 85% floor: new code brings its tests.
- **uv:** `.claude/settings.local.json` sets `UV_PROJECT_ENVIRONMENT` and `UV_NO_SYNC=1`;
  the Makefile unexports `UV_NO_SYNC` so `make` syncs as CI does.
- **Dev containers** may still run: `podman rm -f schedr-pg-dev schedr-vault-dev`.

## State (2026-11-02)

- **Committed:** M4.5a (`ab6ffb9`), M4.5b (`8515b29`), M4.5c, the UI's trigger form
  (`8722b5f`, decisions 149-152, 159), R0.1 (`2e4bb05`), the chart on ghcr (`0fbc8a1`),
  stress scenario 16's check (`d1dd8c7`: a mapped task's whole-task row has no attempt;
  tonight's scheduled Stress run confirms it on GitHub).
- **0.1.0 released (R0.3, 2026-11-02):** tag `v0.1.0` on `fd2da59`, run 41483839551, all
  jobs green. On PyPI (four wheels, Python >=3.14, `License-Expression: MIT`, author
  `p.tutak@outlook.com`), the GitHub Release (the wheels, `SHA256SUMS`, attested),
  `ghcr.io/ptutak/schedr` and `schedr-server` `:0.1.0` and `:0.1` (public), the docs on
  https://ptutak.github.io/schedr/. Checked as a user. The rehearsal `v0.1.0rc1` (R0.2)
  went to TestPyPI; its pre-release, TestPyPI's copy and the orphan `schedr-worker`
  package were deleted. The steps for the next release are in AI_SETUP.md; a new ghcr
  package's first push is denied until ghcr links it (re-run the failed jobs).
- **Next:** the user's choice: M4.6 (secrets) or the open bugs (`BUGS.md`).
- **The disk:** WSL's ext4.vhdx is on Windows C: and not sparse; stale worktrees' `target/`
  filled it (WSL died, 2026-11-02). Remove a step's worktrees, branches and pyenv envs once
  it is committed; check C: with `powershell.exe -NoProfile -Command "(Get-PSDrive C).Free/1GB"`.
- **Test Postgres:** `make` uses podman; Docker's published 127.0.0.1 ports hang in
  mirrored mode. After a crash, a corrupt incremental cache: `rm -rf target/debug/incremental`.
- **The sandboxes** name the worktree's own pyenv env to uv (`UV_PROJECT_ENVIRONMENT`), since
  the user's `.bashrc` no longer does.

## State (2026-11-01, morning)

- **Bugs B135-B139, B147 fixed (`9f4d89d`), the browser test fixed (`b5ba037`).** A worker
  registering with no queues is stored serving `default`. B150, B151 logged (flaky,
  not reproduced).
- **The Deployments redesign (the user's notes on M4.4b; decisions 128-130):** one graph
  on `/deployments` (`schedr-ui/src/deployments.rs`, pure `graph()` laid out by tiers):
  the UI (inside the standalone process when it shares the webserver's process ID;
  `Ui.process_id`/`host`), a frame per deployment, a rectangle per process, lines HTTP
  (UI to webserver), database (webserver to masters), gRPC (master to its workers, dashed
  when not connected); nodes yellow/red by their problems (masters report theirs in the
  heartbeat row: `masters.problems`, `schedr-master/src/problems.rs`; workers' computed in
  `routes.rs` `worker_problems`, offline none by decision 130); panels with facts, problems
  and the configuration as TOML (nulls left out). The per-deployment pages and `/config`
  removed (the UI's panel holds the Config page's content). The Import errors page has
  Master and Worker columns (`parse_files.master_id`). Checked in headless Chromium and
  by a Playwright test.
- **Next:** Codex reviews this batch together with `masked.rs` and `connect_error`
  (pushed in `41a0200` unreviewed); then back to bugs (B103, B98, B151, B150, B148, then
  by score).

## State (2026-11-01, small hours)

- **Codex reviewed the M4.4 batch** (weekly 53% → 56%): 4 findings, each with a failing
  proof; the critical one (a proxy's 502 dropped the browser's action key: a second run
  when sent again) and the high one (an expiry whose commit answer was lost left its
  attempt counted for good, the DAG held back until a restart) fixed in `7ccb426`, with the
  low one (the integration test's DAG cap didn't bind on its own); the medium one partly
  (the DAG cap checked first), the rest logged as B148.
- **M4.4b built** (the Deployments and Config pages): `masters` table and heartbeat
  (`schedr::node`), `workers.master_id/process_id/config` written at `Register`
  (`process_id` and `config_json` in the proto), `GET /api/v1/deployment`, the UI's
  `/deployments`, `/deployments/<name>` (an SVG rectangle per process, `:target` panels
  for each node's configuration), `/config`, the Workers page's Master column and dialog
  text, the navigation; every configuration masked at its node, again by the master (a
  worker's) and again by the UI. Small choices recorded in PROJECT.md (the Deployments
  page, "Claude's, as built").
- **Codex reviewed M4.4b** (weekly 56% → 59%): 6 findings, all fixed with tests: the
  masking missed DSNs (`password=…`), URLs without a scheme, encoded and quoted names, and
  secrets held as numbers, lists or tables (`masked.rs` rewritten; 3 critical); a webserver
  unreachable showed its URL's password in the error (`connect_error` in the client masks
  it); a live refresh closed the open panel (`:target` set on the new one, proved in
  headless Chromium); masters' changes didn't refresh the page (`every 15s`); short IDs
  collided for masters started together (now the last 8 characters).
- **M4.4b committed and pushed (`41a0200`, CI green); then bugs B141-B146 fixed** (the UI's
  live parts: a change during a read read again after it, a failed read keeps the part
  with a note, a page's stream sends a change it may have missed (`since`), a log tail
  resumes after what it showed and follows a log that grew before its attempt ended).
  The browser proofs ran in headless Chromium with the vendored htmx (harness kept out of
  the repo: a Python server and a DevTools script); the Rust tests check the hooks.
- **B149 found and fixed** (the graph's proptest: a free vertical strip left wide,
  `squeeze()`); **B150 logged** (a flaky test under the coverage run, not reproduced).

## State (2026-11-01, night)

- **M4.4 done (concurrency limits), M4.4b next, then bugs** (the user, 2026-11-01 night:
  "once you finish M4.4b in case there are no decisions to make commit and push, after that
  go back to bugs, start fixing if no decisions to make, fix until I come back").
  `limits.rs` in the master (open attempts counted by DAG, task and a run's task; read once
  from the database; rolled back with memory), the DAG page's limits with counts, the
  integration test, stress scenario 16 (written, not run), B119 fixed.
- **F10 and F11 scanned** (Codex, weekly ~53%): F10's B133/B134 fixed, B135-B139 logged;
  F11's B140 (critical, a lost answer shown "not sent" inviting a second run) fixed by
  **decision 126** (the `Idempotency-Key`: one command per key and person, resent by the UI
  and CLI up to 3 times, the browser keeping its key until an answer is definite),
  B141-B147 logged (medium), the docs finding fixed. **F12-F14 still to scan.**
- A test-only flake fixed: `service/tests.rs`'s shared moving clock (`own_clock`).

## State (2026-10-31, evening)

- **M1–M3 done; M4 in progress.** M4.1–M4.3 done. **Now: M4.3e, the bug pause before
  M4.4.** No bug of severity high or critical is open. The chosen mediums are fixed (B56,
  B52, B111, B112, B117, B118, B80, B81, B60, B61; commit `d0b955f`, CI green), with Codex's
  review of them (6 findings, all fixed in that commit) and B132 (decision 120, found while
  fixing B80). **Next: F10–F14** (Codex's section scans; Codex weekly ~49%), **then M4.4
  (B119 inside it), then M4.4b, the Deployments page** (decisions 121–124 with secrets masked
  at the source, the user's requirement), then M4.5.
- **Decisions 116–124 taken on 2026-10-31 evening** (PROJECT.md): 116 timeout from launch;
  117 a file tested alone; 118 runner-built mapped elements (`.expand_kwargs` too); 119 `.on`
  per element when the list is also used whole; 120 an `.on` order edge from a mapped task
  decided per instance by `run_if`; 121–124 the Deployments page.
- The session scratchpad of 2026-10-31 is gone; check logs now go to `target/*.log`.
- **Decisions 103–115 taken on 2026-10-31** (PROJECT.md has each, with "Not chosen").
- **2026-10-31, 12:37:** WSL died from a full disk; the user freed space and removed stopped
  containers (the test Postgres `schedr-pg-check` was recreated on 127.0.0.1:45243, with
  `-c max_connections=300`). Codex's follow-up review was lost with it.
- **Stress (scheduled) failed on 2026-10-31:** its generated DAGs had `{n}` in a shell
  t-string, refused since decision 99; fixed (`env={"PLANNED": n}` in `stress/load.rs`).
- **Decision 102 (the code copy, B58 + B59), committed 2026-10-31:** the agent copies each
  settled root into `<data_dir>/code/<worker ID>/<root>-<hash8>/sha256-…` (`scan::capture`:
  hashed first, a complete copy reused, written under a temporary name and renamed into
  place; removed by renaming away first; read-only; links copied as contents; only the scanned
  files), and parse jobs and tasks import from it (`RootWatcher::code_dir`, `attempt::plan_at`,
  `WorkerSetup::sys_path_with`); `Agent::collect_code` removes copies nothing running uses;
  `python/schedr/runner/sources.py` imports DAG roots from source (extension modules too),
  never `.pyc`. Other roots are still imported live: B90, decision 103.
- **Bugs live in `bugs/`** (the user, 2026-10-30): one `bugs/B<n>.toml` per bug (title,
  severity, probability, kind, summary, found, suspected, details), `bugs/README.md` the rules,
  `bugs/next-id` the next number; `BUGS.md` is generated (`make bugs`; `make bugs-check` in
  `make check` and CI), never edited by hand. A new bug: `python3 scripts/bugs.py new
  "<title>"`; a fix deletes its file. `scripts/bugs.py` has its tests (scripts Claude writes
  get tests: the user, 2026-10-30).
- **Commits of 2026-10-30:** `5b451e1` (the M4.3 review fix-up), `a8bf777` (B39 partly),
  `819dcbb` (the harness, B49, B51), `fbcfd34` (B42, B79, B55, decisions 99–100, B84–B87
  logged), then the evening's: B53 by decision 99, B55's review fixes (including a process whose
  main thread ended still counting as running), B88 (never `kill(-1)` from the journal), and
  **BUGS.md's ratings** (severity × probability + kind, the ranking, security first). CI:
  `gh run list`.
- **2026-10-30, 19:53:** a Grok throwaway test ran `kill -KILL -<pgid>`, which procps 4.0.4
  reads as `kill -1`: every process of the user died ("WSL died"; the VM stayed up). The
  sandboxes now have `--unshare-pid` and the prompts forbid that form; a later Grok turn
  killed itself inside the sandbox and nothing else (it worked). If `/run/user/1002` is missing
  after such an event, restart WSL (podman needs it).

## The night of 2026-10-31 (the user asleep)

The user: "I go to sleep you work with grok and codex on bugs, if anything to decide leave it
for morning, you can commit and push if everything passes". So: commits that pass `make
check` (and their review) are committed and pushed without asking; every decision waits for
the morning, listed below.

- **Committed and pushed (the night's commit):** B58 + B59 by decision 102 (the code copy),
  the reconnect batch B50, B43, B44, B41, and three review rounds' fixes (Grok on the copy;
  Codex on the reconnect batch; a fresh Codex on the fix-up, then a second look). The last two
  fixes (a refused registration keeps starts known before it; a worker 1024 messages behind is
  disconnected) were not reviewed by a colleague: each has a test shown failing without it.
  `make check` and `make docs` pass. Next: decision 103 (B90) below, then B86, B84, B78,
  B64, B54, B66, B45, B63, B57, B47 (the ≥ high list), then M4.4.
- **Second commit of the night:** B84 (every transaction the DB crate starts, the
  migrations' too, goes through `query::Starting`: a start cut short closes its connection;
  Postgres tests with a TCP proxy holding `BEGIN`'s answer), B78 (an expansion sees its own
  step's states and outcomes), B45 (a registration's stream holds its own messages), B54 (a
  zombie counts as nothing unless its threads run), B47 (a result sent again fits the current
  limit), B63 (files hashed and copied through a buffer; the hash unchanged), B57 (`Messages`,
  a cancel-safe frame reader, for a forked task's status). Three Codex rounds (findings 2, 4
  and 2; one stale, the rest confirmed and fixed). The third round's two fixes (a zombie
  leader's memory read through a live thread; the notification probe's connections guarded)
  were not reviewed by a colleague: each has a test shown failing without it.
- **Codex's F5 (scheduling and discovery):** 8 findings, all confirmed, logged B91-B98. Fixed
  the same night with a failing test each: B91 (discovery's memory put back when a
  `CodeVersionChanged` rolls back), B92 (the catalog built from every root's stored current
  parse, so a restart keeps a cross-root conflict), B93 (a switch never recorded before the
  root's latest: a clock stepped back doesn't reorder code), B94 (a parse job finished only
  after the catalog update), B96 (a failed resume keeps the drained mark), B97 (a root dropped
  on re-registration moves away from its job). B95 waits for a decision (above); B98 (frequent
  cron schedules enumerate six hours per lookup) is medium. Codex's review of those fixes: 5
  findings, all confirmed; a failed registration now restores discovery's memory too (fixed,
  its test failing first, not reviewed again); B99-B102 logged, each a decision below.
- **CI broke with decision 102's commit (`d7b5581`), fixed in the third commit of the night:**
  the wheel smoke test's cleanup couldn't `rm -rf` the read-only code copies (Linux and
  aarch64 wheels; the script now makes them writable first, and the guide says to), and on
  macOS the copy was never renamed into place (macOS renames a directory only with write
  permission on it; the top directory is now made read-only after the rename, and writable
  before a removal's rename). The macOS fix has no test that can fail on Linux: macOS CI is
  its check, and it passed (run 41178099349, every job green). `dags test` meanwhile retried for ever: B103.
  The two stuck CI runs: the older one cancelled by Claude (to read its log).
- Grok: 48% of its week after its review (session `b3ccfd8a`, 46 calls, 199k context; about
  4%). Codex: 27% → 31% over the night's first three rounds, 38% after the next five.

## Open when this session ended

- **Codex's whole-repository review** (the Codex skill's `reviews/schedr-full-2026-10-30.md`,
  Codex at full capacity, mainly a reviewer): done F1–F9. Next F10–F14, one section at a time;
  worktree `schedr-codex-f1` (move it to `main` before a section).
- **Usage:** Grok's week reset on Friday 2026-10-31 9:00 (ask the user for the %); Codex
  43% weekly (resets Thursdays 01:57).
- **Bugs:** see `BUGS.md`; the next number is in `bugs/next-id`.

## How we work now (2026-10-30)

- **Claude implements; two colleagues, each through a skill:** **Grok** (`/grok`, grok-4.7,
  high) reviews each step, hunts and fixes bugs, writes docs; **Codex** (`/codex`,
  gpt-6.1-sol, high) gives second opinions on architecture, reviews beside Grok, finds bugs,
  writes docs, runs the whole-repository review; **never codes**. AI_INSTRUCTIONS "Working
  with second agents" and AI_SETUP "Agent tooling" have the details.
- **Both run in `sandbox.sh`** (bubblewrap): only the worktree, the toolchains (read-only) and
  their own state; no other project, no `~/.ssh`, no `/mnt/c`; nothing the user runs later is
  writable; an empty environment but an allowlist. Prompts also say to read only inside the
  worktree. Prepare a worktree (its pyenv, `uv sync`, a build) before a colleague's turn.
- **Every finding is verified before acting;** rejected ones reported with why. Code Grok
  wrote is reviewed by Codex (or a fresh Grok session).
- **Decisions:** numbered (the next is **103**, open: B90), lettered options, the recommended first, a
  "Who (does the work)" column. Ask all coming decisions at once before unattended work.
- **Commit and push only after the user accepts** each one. Check CI with `gh run list`.
- **Stress tests:** never without the user asking. **Processes:** kill only your own.
- **Checks:** `SCHEDR_TEST_PYTHON=$(pyenv prefix)/bin/python
  SCHEDR_TEST_POSTGRES_URL=postgres://postgres:schedr@127.0.0.1:45243/postgres make check`
  (the podman container `schedr-pg-check` on 127.0.0.1:45243), then `make docs`. Running
  `cargo nextest` by hand needs the same two variables (forgetting the URL fails the Postgres
  tests).
- **The fault-injection harness** (decision 98): any master operation that writes gets a
  `Scenario` (crates/schedr-master/src/faults.rs, dispatch/tests.rs `mod faults`) run by
  `fail_each_write`; a multi-write bug is proven and fixed against it.
- Commit messages read like `M4.x: <what>, <what>; <what>` and end with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Rules (also in PROJECT.md, "Repository and tooling", and in memory)

- **Unit tests:**
  - Every public item (Rust `pub`, `pub(crate)`, `pub(super)`; Python public names) gets one,
    with at least 85 % coverage. Private items aren't tested directly.
  - Python unit tests go in `python/tests/unit/schedr/.../test_<module>.py`; Rust unit tests
    in `src/<module>/tests.rs`.
- **Integration tests come with every step.** Anything that spawns processes or uses a real
  server, the binary or Postgres counts as integration. They live in `crates/*/tests/`
  (`crates/schedr/tests/roles.rs` has the `Cluster` helper: roles as processes, `start_as`,
  `terminate`, `kill`, `cli`, `attempts`, `worker_state`; `Cluster::with_tls` and
  `start_tls` for mTLS, certificates from `schedr_tls::testing::TestCa`; the cluster's
  config is `[tls] mode = "mesh"` otherwise), `python/tests/integration/` and
  `examples/`. Multi-worker tests use Postgres (`postgres_database()`), not SQLite.
- **Never skip, ignore or xfail a test without asking first**, and say why.
- **Bugs found on the way go to `bugs/`** (`scripts/bugs.py new`; B-numbers never reused), each
  rated (severity, probability, kind: `bugs/README.md`, Ratings), `make bugs` after; fix them
  when the user schedules it, removing the entry in the fixing commit. A bug that a step's
  own design has to fix anyway (like B5 in 2.4) can be fixed in that step: say so in the
  summary and in PROJECT.md.
- **No Airflow terms in core** (XCom etc. only inside `schedr.airflow`).
- Secrets only come from env vars, never from the config file. A DB URL with a password in
  the file is a startup error; tests pass it via `SCHEDR_CONFIG__DATABASE__URL`.
- Rust MSRV is 1.99, `unsafe_code = forbid` (use rustix for fcntl, flock, signals and the
  like). Clippy limits functions to 100 lines: split by meaning when it complains.
- Library ages (PROJECT.md, Libraries): a new major or minor (its `x.y.0`) at least 7 UTC
  calendar days old, a patch at least 3 days; a yanked version moves forward, else back. Check with
  `curl https://crates.io/api/v1/crates/<name>/versions` (or PyPI), and add them to the
  "Libraries" table in PROJECT.md.
- `0001_initial.sql` (both `postgres/` and `sqlite/`) and the protocol (`PROTOCOL_VERSION`
  0.1) are edited in place until the first release: **1.0.0, after M4B** (the user,
  2026-10-29; PLAN.md, Release 1.0.0). From then on, migrations and additive protocol changes.
- Match the surrounding code: comment density, naming, and the error message style (backticks
  around names, lower case, what to do next).
- Regenerate `schema/` with `make schema` after API or DAG JSON changes (`make check` runs
  `schema-check`).

## Environment notes

- **ufw is on** (2026-10-30). This WSL runs in mirrored networking mode, where `127.0.0.1`
  goes over `loopback0`: ufw allows in and out on it (without that rule every local
  connection fails: the tests, `make standalone`, the test Postgres).

- Logs and scratch files go in the session scratchpad, not `/tmp`.
- **The kubectl current context is a real OpenShift cluster (`…bradford-lab`). Never apply
  anything to it.** Use a local kind cluster only, with an explicit `--context`, and ask
  first. The OpenShift ("bradford") manifest comes later, when the user asks.
- Python outside `make`:
  ```
  cd /home/ptutak/Git/schedr
  export UV_PROJECT_ENVIRONMENT=$(pyenv prefix) UV_PYTHON=$(pyenv prefix)/bin/python; unset VIRTUAL_ENV
  ```
  The Python with schedr installed is `$(pyenv prefix)/bin/python`.
- **Rust integration tests by hand:** `export SCHEDR_TEST_PYTHON=$(pyenv prefix)/bin/python`,
  then e.g. `cargo nextest run -p schedr --test local`. The Postgres tests also need
  `SCHEDR_TEST_POSTGRES_URL`; `make test-integration` starts Postgres
  (`postgres:17.6-alpine`, digest-pinned) with podman and sets both. The docker snap under WSL has broken
  port forwarding.
- **`run_dag` by hand** with a freshly built binary: `SCHEDR_BIN=$PWD/target/debug/schedr`.
- **Wheels:** `make smoke` and `make smoke-musl` both call `scripts/build-wheel.sh`
  (release builds, a few minutes each: manylinux x86_64 and musllinux x86_64 in
  digest-pinned images, podman locally, then `scripts/smoke-wheel.sh`). The script
  installs rustup-init 1.29.1 (minimal channel only, `maturin build --locked`). Linux
  aarch64 and macOS arm64 are CI only (one matrix, `wheel-native`, shown as
  `wheel-linux-aarch64` and `wheel-macos-arm64`; macOS installs the same rustup-init, not
  the runner's). CI does not use `maturin-action` (the sdist installs maturin from
  `scripts/maturin-requirements.txt`).
- **Every role opens the ops port** (`[ops] port`, 8090): tests set it to 0, like the other
  ports, and read `ops on http://…` from the log.
- **Roles need `[tls]`:** `app` mode (the default) needs `ca`, `cert`, `key`; set
  `SCHEDR_CONFIG__TLS__MODE=mesh` to run roles by hand without certificates. Standalone never
  uses TLS. Tests and `dags test` set `[worker] log_port = 0`.
- **Ports:** 8000 is taken on this machine. `make docs-serve` uses `DOCS_PORT` (default 8765).
  Tests bind port 0 and read the bound address from the logs.

## Code map, briefly

- `crates/schedr-model`: shared types (`logs.rs`: the log endpoint's headers and part sizes; `trigger.rs`, `commands.rs` with every command,
  `runner.rs` with `ErrorSummary` and the error kinds `TASK_NOT_FOUND` / `DAG_FILE_MISSING`,
  `version.rs` with `SchedrVersion`).
- `crates/schedr-db`: sqlx, SQLite and Postgres, with dialect placeholders `{uuid}`, `{ts}`,
  `{json}` and `{text}`; INTEGER columns are read as i32. `workers.rs` (registry, `drained`),
  `dags.rs` (parses, `root_hashes`), `attempts.rs`, `instances.rs` (`cancel_requested_at`),
  `reads.rs` (API reads, `run_code_hashes`).
- `crates/schedr-agent`: the worker agent.
  - `agent.rs`: the loop (sessions, offline waits, assignments, `Finished`, drain wiring).
  - `journal.rs`: the on-disk journal and orphan stopping; `drain.rs`: the drain's timing.
  - `logs.rs`: the log endpoint; `retention.rs`: the log sweep.
  - `launch.rs` / `process.rs` / `supervise.rs`: processes; `attempt.rs`: planning;
    `watch.rs` / `scan.rs`: root hashes.
- `crates/schedr-master`:
  - `core.rs`: the master (registration, messages, tick, `sync_code`).
  - `discovery.rs`: reported hashes, the current code per root, parse jobs (`parse_jobs.rs`).
  - `dispatch.rs`: assignment, rejections, cancels, code wait, wait reasons.
  - `workers.rs`: the live registry, `Needs`, the spread choice.
  - `execution.rs` / `progress.rs`: run state; `commands.rs`: applying commands;
    `service.rs`: the gRPC service (`Hub`); scheduling in `scheduler.rs` and friends.
- `crates/schedr-api`: the REST `/api/v1` (axum, utoipa → `schema/openapi.json` via
  `cargo xtask`); `logs.rs`: the log proxy (`LogSource`, `HttpLogSource`).
- `crates/schedr-tls`: `material.rs` (PEM → rustls configs, the fixed worker name),
  `reload.rs` (`Tls`, polling the files), `incoming.rs` (the listener doing handshakes, for
  tonic and axum), `connect.rs` (the worker's channel), `testing.rs` (feature `testing`).
- `crates/schedr`: the binary.
  - `roles.rs`: master / webserver / worker roles (`role_tls`, `WorkerLinks`, `log_endpoint`), `stop_on_signal` (a count of stop requests:
    the first drains a worker, a second interrupts), `agent_settings`, `lock_journal`.
  - `standalone.rs` (drains its worker before stopping master and API), `cli.rs`, `app.rs`,
    `user.rs` (server commands), `local.rs` (`dags check` / `dags test`), `client.rs`,
    `interpreter.rs`, `times.rs`.
- `python/schedr`: the native API, `_serialization`, `runner/` (started with an inherited
  socket in `SCHEDR_AGENT_FD`; modes `parse` and `task`), `testing.py`.
- `examples/`: example DAGs and their tests, `examples/kubernetes/` (Dockerfile, standalone
  StatefulSet); `scripts/smoke-wheel.sh`: the wheel's smoke test.

Resume this session with:
claude --resume 7a3fe1e5-0fb5-48ce-a52a-5f177878911e
