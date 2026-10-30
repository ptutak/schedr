# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that reproduces
it: shown failing before the fix and passing after; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

A bug found by reasoning but **not reproduced yet** is logged too, marked **Suspected**, with
how it would be confirmed; once a test shows it, the mark goes. If it turns out not to be a
bug, the entry is removed, saying why in the commit.

## B77: valid large timedeltas cannot render in agent templates

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), low severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/codec.py:159`. Python stores the full supported timedelta range as integer microseconds. Agent rendering accepts the tag only through `Value::as_i64` at `crates/schedr-agent/src/render.rs:261`. `timedelta(days=200000000)` encodes successfully and its microsecond count survives the channel exactly, but rendering treats it as an unsupported object.
- **A fix:** to decide when it is scheduled.

## B76: Python accepts output nesting that the agent refuses

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/codec.py:260`. Recursive encoding checks values and byte size without enforcing the receiving JSON parser’s nesting limit. A small `Any` output containing 140 nested lists passes encoding and `check_size()` and is emitted as `task_succeeded`, but the agent cannot read it.
- **A fix:** to decide when it is scheduled.

## B75: truncation detection applies to unrelated user dictionaries

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/runner/task.py:96`. `resolve()` checks `value.get(f"{step}_truncated") is True` for every dictionary field, rather than only process-result streams. Referencing `data` in a complete output `{"data": "complete", "data_truncated": True}` raises a truncation error.
- **A fix:** to decide when it is scheduled.

## B74: `Literal` compatibility conflates booleans with integers

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/compatibility.py:151`. The check uses `v in target.values`, so Python equality makes `True` fit `1`. The runtime Literal codec requires both equal value and equal type. A `Literal[True]` producer therefore connects successfully to a `Literal[1]` parameter, then fails at execution.
- **A fix:** to decide when it is scheduled.

## B73: optional TypedDict keys are falsely promised as present

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/models.py:105`. `_all_required()` sets `copied["required"] = list(node["properties"])` on every nested object schema. A model’s nested TypedDict can legitimately omit `NotRequired` keys; dumping the model does not add them. A model containing `row: {x: NotRequired[int]}` is consequently accepted for one requiring `row.x`.
- **A fix:** to decide when it is scheduled.

## B72: approximate divisibility admits incompatible `multipleOf` constraints

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/schemas.py:538`. The proof uses `math.isclose(ratio, round(ratio))`. Its relative tolerance accepts nonintegral ratios at sufficiently large magnitudes. A source with `multiple_of=10**12` is accepted for a target with `multiple_of=3`, although `10**12` itself fails that target.
- **A fix:** to decide when it is scheduled.

## B71: integer constraint comparisons lose precision

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/schemas.py:517`. `_limit()` converts integer boundaries with `float(schema[plain])`. This collapses distinct boundaries above the exact integer range of a float. A source allowing `x >= 9007199254740992` is accepted for a target requiring `x >= 9007199254740993`. The source’s minimum value then fails decoding.
- **A fix:** to decide when it is scheduled.

## B70: reference memoization skips comparisons against different inline target schemas

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/schemas.py:246`. The cache key contains only reference strings:
- **A fix:** to decide when it is scheduled.

## B69: embedding a model descriptor loses its `$ref` resolution context

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/schemas.py:132`. `from_desc()` nests a model’s schema and `$defs` under `items` or another enclosing schema. `_Compare` continues resolving references from the outer root. At `schemas.py:444`, a missing definition becomes `{}`, which `fits()` accepts as unrestricted. Thus `list[TextBox]`, containing a model field `x: str`, is accepted for `list[IntBox]`, whose corresponding field is `x: int`. The same comparison without the enclosing list correctly rejects it.
- **A fix:** to decide when it is scheduled.

## B68: a strict model can encode successfully but cannot read its own output

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/models.py:135`. `dump()` uses JSON mode, turning datetimes into strings. `validate()` then calls `cls.model_validate(data, ...)` in Python mode. A strict datetime field rejects that string, although the parse-time compatibility check accepts the model-to-itself connection.
- **A fix:** to decide when it is scheduled.

## B67: exceptions during result serialization escape without a structured failure

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/runner/task.py:423`. Both encoding and size checking catch only `CodecError`. Recursive values, failing Pydantic serializers, and exceptions from JSON/UTF-8 conversion escape those handlers. The task function completes, but the runner closes its channel without sending `task_failed`.
- **A fix:** to decide when it is scheduled.

## B66: ordinary dictionaries collide with serialization tags

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/codec.py:433`. The encoder preserves arbitrary string keys. The decoder interprets any singleton dictionary whose key is in `TAGS` as a tagged value: `if len(data) == 1 and next(iter(data)) in TAGS`. Consequently, even a value encoded and decoded against the same `dict[str, Any]` annotation changes:
- **A fix:** to decide when it is scheduled.

## B65: mixed structured arguments reach functions in the wrong Python shape

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/runner/task.py:181`. `build()` constructs mixed arguments with `return [build(...)]` or `return {k: build(...)}`, without constructing the annotated tuple, dataclass or model. A DAG declaring `consume((produce(), 3))` for `value: tuple[int, int]` passes parsing, but receives a list. Likewise, `consume({"x": produce()})` passes parsing for a dataclass or Pydantic parameter, then receives a dict.
- **A fix:** to decide when it is scheduled.

## B64: the channel does not preserve Python numbers

- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), high severity, with a failing check, confirmed by Claude (`serde_json`
  has neither `float_roundtrip` nor `arbitrary_precision`); at `python/schedr/_serialization/codec.py:129`; `Cargo.toml:46`. The encoder accepts unrestricted integers (`isinstance(value, int)`) and finite floats. The agent reads them using `serde_json::from_slice` at `crates/schedr-agent/src/channel.rs:90`, with the default `serde_json` configuration. A task’s exact integer `18446744073709551617` becomes an approximate float in the agent. A downstream `int` decoder rejects that float. Finite floats can also change value during the read.
- **A fix:** to decide when it is scheduled.

## B63: hashing a root reads every file whole into memory

- **Found:** 2026-10-30, by Codex's whole-repository review, section F7 (the agent's files
  and parsing), with a failing proof each; checked against the code by Claude.
  `content_hash` (`crates/schedr-agent/src/scan.rs:169`) reads each scanned file with `std::fs::read`, Python or not: a data file bigger than the worker can allocate fails the hash, and before registration `Agent::hashes` retries for ever (`agent.rs:659`); Codex's proof: a 256 MiB sparse file under a 192 MiB address-space limit, `OutOfMemory`.
- **A fix:** stream each file into the hasher (a buffered reader).

## B62: the log endpoint follows a symlink named like an attempt's log

- **Found:** 2026-10-30, by Codex's whole-repository review, section F7 (the agent's files
  and parsing), with a failing proof each; checked against the code by Claude.
  The endpoint validates the attempt ID as a UUID, then opens `log_dir/<uuid>.log` following links (`crates/schedr-agent/src/logs.rs:157`): a `<uuid>.log` symlink planted by a task or a local user serves the target's contents to an authorised log reader (Codex's proof: a test private key outside the log directory, served with 200). No HTTP-only way to plant one was shown. The writer (`process.rs:126`) follows links too.
- **A fix:** open logs with `O_NOFOLLOW` (and check the file is regular), reading and writing.

## B61: a DAG file reached through a followed symlink gets an unusable location

- **Found:** 2026-10-30, by Codex's whole-repository review, section F7 (the agent's files
  and parsing), with a failing proof each; checked against the code by Claude.
  Scanning follows symbolic links (documented), but `registry.py:71` resolves the file's path and keeps only its name (`DagLocation(LOCAL_ROOT, file.name)`): `root/linked -> ../shared` with `dag.py` scans as `linked/dag.py` and parses as `local:dag.py`, which the master refuses (`validate.rs:36`: located at `local:dag.py`, parsed from `etl:linked/dag.py`). Codex ran the real parser and validator.
- **A fix:** keep the scanned (root-relative) path as the location, unresolved.

## B60: a relative DAG root breaks parsing and task imports

- **Found:** 2026-10-30, by Codex's whole-repository review, section F7 (the agent's files
  and parsing), with a failing proof each; checked against the code by Claude.
  The executor uses the configured directory both as the child's working directory and as `root_dir` in the request (`crates/schedr-agent/src/agent.rs:295`; tasks the same, `attempt.rs:245`, `:252`); configuration keeps relative paths (`schedr-config/src/roots.rs:155`), so `dags/etl` becomes `dags/etl/dags/etl/dag.py` (Codex's proof: `FileNotFoundError` after a successful scan). `WorkerSetup::sys_path` keeps them relative too.
- **A fix:** make roots absolute when the configuration is read (or the request's `root_dir`).

## B59: a new content hash can import stale bytecode

- **Found:** 2026-10-30, by Codex's whole-repository review, section F7 (the agent's files
  and parsing), with a failing proof each; checked against the code by Claude.
  The runner imports DAG files with the standard source loader (`python/schedr/runner/parse.py:65`; tasks at `task.py:290`), whose `.pyc` check is the source's mtime in whole seconds and its size: an edit of the same length within the same second (git-sync switching revisions does that) reuses the old bytecode, while the agent's scan saw the change and hashed the new source (Codex's proof: hash changed, the source says `new`, a fresh runner parsed `old`).
- **A fix:** no bytecode cache for DAG roots (`sys.dont_write_bytecode` and not reading `__pycache__`), or hash-based `.pyc` checked against the source.

## B58: parsing can attribute a root's new code to its old hash

- **Found:** 2026-10-30, by Codex's whole-repository review, section F7 (the agent's files
  and parsing), with a failing proof each; checked against the code by Claude.
  A parse job takes the file list from the settled snapshot but the directory from the watcher's path (`crates/schedr-agent/src/agent.rs:1047`), and the result keeps the assignment's hash with no check after: when the root's symlink switches revision between hashing and parsing (git-sync), revision B's DAGs are uploaded under A's hash (Codex's proof: cached hash old, content new, parsed `new`). `code_mismatch` and `remember_code` use the cached hash too (`:1014`, `:965`).
- **A fix:** parse the snapshot's resolved directory (the revision the hash is of), or re-hash after parsing and drop a result whose hash changed.

## B57: an interrupted read of a forked process's exit status loses its framing

- **Found:** 2026-10-30, by Codex's whole-repository review, section F6 (the agent's task
  execution), with a failing proof each; checked against the code by Claude.
  `ForkedProcess::wait` awaits `read_message(status)` (`crates/schedr-agent/src/fork.rs:861`), whose header and partial body live in the future; the supervisor's `select!` drops it when a stop or deadline wins and calls `wait` again, which reads the rest as a new header, fails, kills the group and returns `Termination::Unknown` for a healthy shim (Codex's proof with the real fork server, the frame split at a barrier: `Unknown` instead of `Signaled(9)`). Frequency in practice not measured.
- **A fix:** a cancel-safe reader (the partial frame kept in the struct), or the read in its own task.

## B56: a cancel or timeout is ignored while a runner hasn't said hello

- **Found:** 2026-10-30, by Codex's whole-repository review, section F6 (the agent's task
  execution), with a failing proof each; checked against the code by Claude.
  `run_python` starts the runner and waits for its hello (up to 30 s) and sends the request before `supervise` starts (`crates/schedr-agent/src/launch.rs:274`); a cancel queued meanwhile, or the execution timeout, does nothing until then, and a failed start afterwards is reported `start_failed`, the cancel lost (Codex's proof: a runner stalled before its hello, still running 2 s after a cancel and a 100 ms timeout). Spawned and forked paths alike.
- **A fix:** the stop channel and the deadline raced against the handshake too; a cancel during it reported as a cancel.

## B55: a cancelled task can hang forever collecting its output

- **Found:** 2026-10-30, by Codex's whole-repository review, section F6 (the agent's task
  execution), with a failing proof each; checked against the code by Claude.
  When the leader exits on SIGTERM, `supervise` returns at once (`crates/schedr-agent/src/supervise.rs:158`) and never sends SIGKILL; `run_command` then awaits `process.output()` without a bound (`launch.rs:332`, the same for Python tasks at `:295`): a child in the group that ignores SIGTERM and holds stdout open keeps the attempt there, and later cancels or a drain find no supervisor (Codex's proof: 2 s after a 100 ms grace, still collecting; the child alive). Startup and parse paths already bound it (`OUTPUT_DRAIN_TIMEOUT`).
- **A fix:** after the leader exits, kill the group at the grace's end anyway, and bound the output wait as elsewhere.

## B54: an unreaped child switches off a forked task's memory budget

- **Found:** 2026-10-30, by Codex's whole-repository review, section F6 (the agent's task
  execution), with a failing proof each; checked against the code by Claude.
  For a forked group (`crates/schedr-agent/src/memory.rs:159`) any process whose PSS can't be read discards the whole sample (`None => return None`); a zombie in the group has none, so while a task leaves one child unreaped every sample is skipped and the budget never applies (Codex's proof: a live parent at 32 MiB with a 16 MiB budget, no kill). Skipping an unreadable live process is the documented choice; a zombie (state `Z`) has no memory to count. Forked parse runners use the same watchdog.
- **A fix:** count a zombie as 0 (its `stat` says `Z`) instead of discarding the sample.

## B53: shell injection when a command's author quotes a reference

- **Found:** 2026-10-30, by Codex's whole-repository review, section F6 (the agent's task
  execution), with a failing proof each; checked against the code by Claude.
  Values in a command t-string are shell-quoted for the author (PROJECT.md, Safe substitution into shell commands), but the renderer quotes without looking at the text around the placeholder (`crates/schedr-agent/src/render.rs:327`; plain values the same at parse time, `python/schedr/_templates.py:103`): `t'printf "%s" "{out}"'` renders `"'$(touch x)'"`, which runs `touch`; inside single quotes the value closes them (`''; touch x; #''`). Codex executed both. Authors quote by habit; nothing refuses it.
- **A fix:** a decision for the user (refuse a placeholder inside quotes at parse time, with the rule in the message; or quote by the context), with tests of both quote kinds, for references and plain values.

## B52: the per-round caps still read the whole backlog

- **Found:** 2026-10-30, by Codex's whole-repository review, section F2 (dispatch and
  attempts), with a failing proof each; checked against the code by Claude.
  `pending_commands` (`crates/schedr-db/src/commands.rs:299`) has no `LIMIT` (`fetch_all`); the master keeps `COMMANDS_PER_ROUND` (50) after reading and decoding every pending row (`schedr-master/src/commands.rs:129`): draining N commands reads N, N-50, ... rows (Codex: 2,000 commands, 32 MB of audit strings loaded for 50 applied). Expired attempts the same (`unstarted_attempts`, then `truncate`, `dispatch.rs:864`).
- **A fix:** `LIMIT` in the queries (the round's cap).

## B51: a rolled-back message can still cancel a healthy sibling instance

- **Found:** 2026-10-30, by Codex's whole-repository review, section F2 (dispatch and
  attempts), with a failing proof each; checked against the code by Claude.
  Fail-fast's cancellations go to `self.stops` (`crates/schedr-master/src/dispatch.rs:1074`), which the rollback snapshot (`Memory`: workers, rejections, code_missing, `dispatch.rs:218`) doesn't hold: a `Finished` whose commit fails is undone in the database and memory, but its queued `CancelAttempt` for a sibling is sent at the next step (Codex's Postgres proof, a deferred constraint failing the commit: the sibling cancelled while both stay `running`). An assignment's rollback restores workers and outgoing messages but not `stops` either.
- **A fix:** `stops` in the snapshot (restored with it), or queued only after the commit.

## B50: a running task can be expired and assigned again after a reconnect

- **Found:** 2026-10-30, by Codex's whole-repository review, section F2 (dispatch and
  attempts), with a failing proof each; checked against the code by Claude.
  When `Started` is lost with the stream and the worker reconnects within the start timeout (10 s) with the attempt `Running` in its journal, the master adopts it (`crates/schedr-master/src/dispatch.rs:381`: only the slot reserved) and answers `Continue`, but records no start; the expiry (`started_at IS NULL`) then ends it as `AssignmentExpired` and assigns the task again while the first process runs (Codex's proof: one new assignment). The agent resends `Finished` on `Continue` but no `Started`.
- **A fix:** record the start on adoption (the journal says it runs), or have the agent resend `Started`.

## B49: an expired assignment can leave its task running for good

- **Found:** 2026-10-30, by Codex's whole-repository review, section F2 (dispatch and
  attempts), with a failing proof each; checked against the code by Claude.
  `record` (`crates/schedr-master/src/dispatch.rs:1050`) ends the attempt, then moves the task, in separate writes; the expiry path calls it outside any transaction (`expire`, `:889`), so a failed second write leaves the attempt ended and the task `running`, never revisited (expiry looks at open attempts only; Codex's proof: four healthy rounds later, still `running`, no open attempt). The same split in `incarnation_replaced` and `worker_lost` (B42's path).
- **A fix:** `record`'s writes in one transaction (`Database::atomically`) on every path.

## B48: every attempt message copies the master's whole worker registry under its lock

- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  `Started`, `Rejected` and `Finished` each take `Dispatcher::memory` (the D1 snapshot: `workers`, `rejections`, `code_missing` cloned, `crates/schedr-master/src/core.rs:499`, `dispatch.rs:280`) under the core lock, so each message costs in proportion to the whole registry and its outstanding attempts; Codex measured 10 vs 1,000 workers (4 attempts each), 500 snapshots each, unrelated attempts included.
- **A fix:** a snapshot of what the message can change (its attempt and worker) instead of everything; measure first with M4.5.1's per-turn timings.

## B47: a replayed result ignores a lowered master message limit

- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  On `Continue`, the agent resends an unacknowledged `Finished` as stored (`crates/schedr-agent/src/agent.rs:843`); `fit_result` (which truncates a result to `Registered`'s `master_limit`) runs only on new completions. A 5 MiB result journalled under a larger limit, then a master announcing 4 MiB: every replay ends the stream (Codex's probe: 5,242,891 bytes sent against 4,194,304).
- **A fix:** fit the replayed result to the current limit, as a new one is.

## B46: a worker can report `Finished` for another worker's attempt

- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  `Dispatcher::finished` (`crates/schedr-master/src/dispatch.rs:828`) never checks `attempt.worker_id` against the sender; `Started`, `Rejected` and the journal reconciliation do. A registered `w2` knowing `w1`'s attempt ID stores its success and frees `w1`'s slot while the process still runs (Codex's proof: `Some(Success)`).
- **A fix:** refuse (and log) a `Finished` from another worker than the attempt's, as `Started` does.

## B45: a registration with many new roots deadlocks before its stream is returned

- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  `Control` delivers `Registered` and every `AssignParse` into the worker's channel (`STREAM_BUFFER` 1,024) before returning the receiver that drains it (`crates/schedr-master/src/service.rs:568`); 1,024 new roots fill it and `deliver` waits for ever, holding the streams mutex (other deliveries and stream changes wait too). Codex's proof: registration with 1,024 unseen roots never returns.
- **A fix:** return the stream first and deliver after, or never block delivery on one worker's full channel (and not under the shared mutex).

## B44: an old stream of a worker can change and disconnect its new incarnation

- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  Messages and the disconnect are keyed by worker ID only (`crates/schedr-master/src/service.rs:446`, `:467`); the incarnation check (`same_channel`) guards only the outgoing map. An incarnation `i1` whose lease was lost while its transport stays open: `i2` registers; `i1`'s messages still change the worker, and `i1` closing marks `i2` disconnected, which also lets another registration in despite `i2`'s open stream (Codex's gRPC proof: `Disconnected` instead of `Active`).
- **A fix:** tie each stream's messages and its disconnect to its incarnation; ignore those of a replaced one.

## B43: cancelling one task while its worker is away is lost on reconnect

- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  The journal reconciliation resends a cancel only when the run is cancelled (`crates/schedr-master/src/core.rs:830`: `run.cancel_requested`); a `CancelTask` sets the instance's flag, so after a reconnect the attempt gets `Continue` and runs on (Codex's proof: 0 cancels where 1 was due). Rejections and completions check both flags; the reconnect test covers `CancelRun` only.
- **A fix:** check the instance's `cancel_requested` too, with a test of `CancelTask` across a reconnect.

## B42: a database error while a worker is lost stops its cleanup for good

- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  `forget` (`crates/schedr-master/src/core.rs:857`) removes the connection, then writes the worker's state and ends its attempts; if a write fails, later lease checks (which walk `connections`) never retry it: the worker stays `lost` with its attempt open (Codex's proofs, a failure injected at each write: the healthy next tick never retries). `Dispatcher::worker_lost` removes the worker before its fallible writes too; deregistration takes the same path.
- **A fix:** remove from memory only after the writes succeed (or keep a retry list), as B7 did for parse jobs.

## B41: a result the master failed to store isn't sent again while the link stays up

- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  The agent sends `Finished` once and keeps it in `unacked` (`crates/schedr-agent/src/agent.rs:1114`); heartbeats don't resend it, only a reconnect's `decide` does. The master logs a handling error and keeps the stream (`crates/schedr-master/src/service.rs:448`). One failed write: the attempt stays running for as long as the link lives (Codex's proofs: 12 healthy heartbeats and ticks, still open). Contradicts the Worker lifecycle's "`Finished` until `FinishedAck`".
- **A fix:** resend unacknowledged results on a timer (or with heartbeats) until acknowledged.

## B40: a lease test times out its 2 s query limit under load

- **Found:** 2026-10-30, in `make check` while two other builds and test runs shared the
  machine: `lease::tests::a_master_taken_over_never_writes_itself_back_as_the_holder` failed
  with `QueryTimeout(2s)` (`crates/schedr-master/src/lease/tests.rs:101`, an `unwrap` on a
  query). Alone it passed 5 times out of 5 (6-9 s each).
- **The cause, likely:** the test runs with the production 2 s query limit; SQLite under a
  loaded CPU can exceed it. Not investigated: whether the test exercises a write that should
  be retried rather than a limit that should be longer in tests.
- **A fix:** the test's own query limit (or a retry of what the master retries itself), and
  the same check over the other tests using the default limit.

## B39: sibling groups on different layers make the run graph exponentially wide

- **Found:** 2026-10-30, by Grok's review of M4.2b's graph layout (decision 65).
  `shift_group` (`crates/schedr-ui/src/graph.rs`) moves a group only on the layers holding
  one of its tasks, while its box covers every layer from its top task to its bottom one;
  the layers in between keep their places, the box stretches over them, and the next pass
  shifts it again. Grok's shape: `k` sibling groups, each a top and a bottom task, the tops
  feeding one chain outside them and group `i`'s bottom hanging off the chain's `i`th task:
  8 groups (24 tasks) are 193,152 px wide, about 2.2 times wider per group.
- **Reproduced:** a test of that shape asserting the drawing is no wider than every task
  side by side in its own box fails from 5 groups (4,358 px over a 3,512 px bound). The
  property tests use 3 groups in one shape and check no width.
- **A fix:** shifting the box's whole span on every layer it covers was tried (2026-10-30)
  and isn't enough: the width stays and two property tests (nested boxes, overlaps) fail;
  the separation loop needs a real look. Kept for it: the failing test.

## B38: an edge between two tasks outside a group can be drawn through its box

- **Found:** 2026-10-30, by Grok's review of M4.2b's graph layout. On a layer a group spans
  without a task there, a task outside it is pushed right of the box on that layer only;
  the edge from it to a task on the other side is a curve between the two, through the box.
  Decision 65 keeps tasks out of boxes, not lines. Grok's case: `A0 -> U -> A1`, `U -> B1`,
  `A = {A0, A1}`, `B = {B0, B1}`: `U` lands right of `B`, its edges cross `B`'s box.
- **Cosmetic:** every task is in the right box. **A fix:** route such edges around boxes,
  or order layers so that it can't happen; with B39.

## B37: the CLI has no `tasks attempts`, `tasks output` nor `runs show`

- **Found:** 2026-10-30, by Grok's review of M4.3c: decision 86 (PROJECT.md, Mapping) puts
  `--map-index N` on `tasks logs / attempts / output / cancel / retry` and has `runs show`
  list a mapped task's instances, but `schedr tasks` has only `cancel`, `retry` and `logs`,
  and `schedr runs` no `show`. The API has every one of them (`/runs/{run}`,
  `/runs/{run}/tasks/{task}/attempts`, `.../output`, each with `?map_index`).
- **A fix:** the three commands over the client's existing methods, with `--map-index` and
  the `fetch[i]` rows after the task's own (as `dags test` prints them); or decision 86
  narrowed to what exists.

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
