# Next session: where things stand

Read this file, then `PLAN.md` (M4 and M4.5), `BUGS.md`, and the parts of `PROJECT.md` a step
names. `PROJECT.md` is the decision log: every decision goes there, in its section, with
"Not chosen" and why.

## State (2026-10-30)

- **M1–M3 done; M4 in progress.** M4.1 (outcomes), M4.2 (`@branch`, groups, import errors) and
  **M4.3 (mapping) are done**, M4.3d (the CLI's `runs show`, `tasks attempts`, `tasks output`)
  included. **Now: the bugs of severity high or critical** (the user, 2026-10-30: fix down to
  "high", then features; a new ≥ high bug interrupts), in the ranking's order, security first:
  every one left needs a decision first: B90 (critical, decision 103), B102, B66, B64, B86,
  B95, B99, B101, and B100 a reviewed round (see
  "Decisions for the morning"); the night of 2026-10-31 fixed the reconnect batch (B50, B41,
  B43, B44) and B84, B78, B45, B54, B47, B63, B57. Then M4.4 (concurrency limits: decisions 87–92, 95–96 taken).
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
  its check; look at it first (`gh run list`). `dags test` meanwhile retried for ever: B103.
  The two stuck CI runs: the older one cancelled by Claude (to read its log).
- Grok's weekly % after its review (session `b3ccfd8a`, 46 calls, 199k context): ask the
  user. Codex: 27% → 31% over the night's first three rounds, 35% after the next three.

Decisions for the morning:

- **103 (B90, critical × rare):** a module a task or parse imports from **another** DAG root
  comes from that root's live checkout, not a copy (Grok's review of the code copy; not new:
  before decision 102 every root was imported live). Options: **A** pin every root at its
  current copy for each parse or attempt, kept while it runs (Claude; recommended: it is what
  decision 102 means); **B** document that roots don't import from each other; **C** only the
  root's own copy on `sys.path` (a cross-root import fails at once). Being critical, it is
  fixed before M4.4 whichever is chosen.
- **B66 (critical × rare, data):** a user dict whose only key is a serialization tag
  (`{"$date": "x"}`) decodes as the tagged value, silently. Options: **A** escape such a dict
  when encoding (e.g. wrap it as `{"$dict": {...}}`), decode unchanged otherwise (Claude;
  recommended: old outputs still read, only colliding dicts change form); **B** refuse a dict
  whose key is a tag when encoding (an error the user sees); **C** a tag key no JSON object
  from users can have (a format change for every tagged value).
- **B64 (high × possible, data):** numbers through the channel: a float may change in its last
  digit (serde_json without `float_roundtrip`), an integer beyond 64 bits becomes a float.
  Options: **A** `float_roundtrip` plus refusing integers outside i64/u64 when encoding, with a
  message (Claude; recommended); **B** `float_roundtrip` plus `arbitrary_precision` (big
  integers kept exact; a serde_json feature affecting every crate that uses it); **C** big
  integers tagged as strings (a new tag).
- **B86 (high × possible, correctness):** a string with NUL (e.g. `find -print0`'s output)
  is accepted by the Python codec but Postgres's jsonb refuses it, rolling back the attempt's
  completion. Options: **A** refuse NUL when the task's value is encoded (the attempt fails
  with a clear error; Claude; recommended); **B** escape NUL in a tagged form (a new tag);
  **C** store outputs as text, not jsonb (a migration; loses jsonb queries).
- **B95 (high × possible, correctness; F5):** a DAG ID in conflict between `a.py` and `b.py`;
  `a.py` is removed and `b.py` fails to import: the catalog keeps the stored version, which
  is `a.py`'s (the removed file's), and `b.py`'s error holds nothing (holds match the stored
  location). Options: **A** keep the conflict until the remaining file loads (no runs
  meanwhile; Claude; recommended: nothing runs from a file that no longer defines it); **B**
  remove the DAG until `b.py` loads (its history kept, as any removed DAG); **C** keep
  `a.py`'s version but hold it under `b.py`'s error.
- **B102 (critical × possible, correctness):** after a master restart, a lagging worker
  reconnecting first makes its old hash current, and its catch-up then counts as a switch,
  recorded as newest (above the real newest). Options: **A** a switch only away from the
  newest *recorded* hash, not the current one decided since the restart (Claude;
  recommended: no new state, matches the guide's promise); **B** record no switches until
  the master has heard from every worker it knew, or a settling period passed.
- **B101 and B99 (high), one question:** a worker message, or a command's follow-up, whose
  handling failed (a database error) is not retried: a `CodeVersionChanged` (the worker
  keeps its stream, the master its old hash) and a resume's parse jobs. Options: **A** end the
  worker's stream when its message can't be handled (it registers again, reporting all
  afresh; Claude; recommended for B101); for B99, keep the failed follow-up and retry it each
  round until it succeeds (Claude; recommended); **B** the master retries each message itself.
- **B100 (high):** a read failing right after a parse retry's files are stored loses the
  catalog update. A fix is likely mechanical (forget `current` on any failure after a store,
  as `refresh_or_forget` does), but it reaches into the retry path: for a reviewed round.
- Rejected from Grok's review (no decision needed, for the record): a task can `chmod` or
  rename its own read-only copy (it runs as the copy's owner): within decision 101 (workers and
  their DAG code are trusted); the guide now says the copy guards against mistakes only.

## Open when this session ended

- **Codex's whole-repository review** (the Codex skill's `reviews/schedr-full-2026-10-30.md`;
  ~50% of Codex's week): done F1–F8 (B41–B98, all logged). Next F9 (the Python DAG API),
  then F10–F14, one section at a time; its worktree is `schedr-codex-f1` (detached
  at `819dcbb`; move it to `main` before a section, copy the current BUGS.md in). A section's
  findings now come rated (severity, probability, kind).
- **Usage:** Grok 44% of its week before M4.3d's follow-up (resets Fridays 9:00; ask the user
  for the %); Codex 25% weekly (read from its turns; resets Thursdays 01:57). Grok for brittle
  work only, Codex for small fixes, follow-ups in the same session (the user, 2026-10-30).
- **Bugs:** B4, B22, B24, B31, B32 (older, suspected); B37 (M4.3d); B38, B39 (M6); B40–B90
  (fixed: B41–B45, B47, B49–B51, B53–B55, B57–B59, B63, B78, B79, B84; B88 found and fixed the
  same day); B91–B102 from F5 and its fixes' review (fixed: B91–B94, B96, B97); B103 (`dags
  test` never giving up). The next number is **B104**
  (`bugs/next-id`).

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
  0.1) are edited in place until the first release: **1.0.0, after M4.5** (the user,
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
claude --resume 8b1bd615-ec70-4343-ae4d-2552c481b4b4
