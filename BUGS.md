# Known bugs

Open bugs, newest first. IDs (B1, B2, …) are never reused, so commits can say "fixes B1".
A fixed bug is removed in the commit that fixes it, together with a test that reproduces
it: shown failing before the fix and passing after; git history keeps the record. Finding a bug doesn't mean fixing it at once: it
is logged here and its fix scheduled with the user. Not part of the documentation site.

A bug found by reasoning but **not reproduced yet** is logged too, marked **Suspected**, with
how it would be confirmed; once a test shows it, the mark goes. If it turns out not to be a
bug, the entry is removed, saying why in the commit.


## Ratings

Every bug has three ratings, set when it is logged (by whoever finds it; Claude checks them)
and revised when more is known:

- **Severity**, what happens when it occurs: **critical** (a security hole, silent wrong
  results or wrong code run, a task run twice, the whole cluster stuck); **high** (in normal
  operation, with no mistake by the DAG's author, a task or run fails wrongly, stalls, runs on
  after a cancel, or a worker can't work); **medium** (a visible, repeatable failure that
  testing a DAG would catch, slowness growing with scale, a misleading UI or CLI); **low**
  (cosmetic, a message, a test-only flake, an author's type mistake caught at run time
  instead of at load).
- **Probability**, in a real deployment: **likely** (ordinary use or routine events),
  **possible** (a specific but plausible situation: a reconnect at the wrong moment, git-sync,
  a feature combination, scale), **rare** (unusual input, exact timing, extreme sizes, a
  hostile actor). For a security bug: how easily it is triggered, by accident or on purpose.
- **Kind**: security, data, correctness, reliability, performance, usability or tests.
  Security is a kind, not a third score: its worst case is already the severity.

**Score** = severity (critical 4, high 3, medium 2, low 1) × probability (likely 3,
possible 2, rare 1). The ranking below puts **every security bug first**, whatever its score
(the user, 2026-10-30), then the rest by score, then severity; fixes go in its order
unless the user schedules otherwise. Each entry's first line is its rating; the table is
regenerated whenever an entry is added, re-rated or removed.

**What gets fixed when** (the user, 2026-10-30): every bug of severity **high or critical** is
fixed (security first, then the ranking), then features go on; a new high or critical bug
interrupts the feature work and is fixed next. Medium and low bugs wait here, fixed when a
step touches their code or the user schedules them.

## Ranking

| # | Bug | Score | Severity | Probability | Kind |
|---|---|---|---|---|---|
| 1 | B62: the log endpoint follows a symlink named like an attempt's log | 4 | critical | rare | **security** |
| 2 | B46: a worker can report `Finished` for another worker's attempt | 4 | critical | rare | **security** |
| 3 | B58: parsing can attribute a root's new code to its old hash | 8 | critical | possible | data |
| 4 | B50: a running task can be expired and assigned again after a reconnect | 8 | critical | possible | correctness |
| 5 | B86: accepted strings containing NUL cannot be stored in production | 6 | high | possible | correctness |
| 6 | B84: cancelling transaction startup returns an open transaction to the pool | 6 | high | possible | reliability |
| 7 | B78: expansion reads instance states before the current result is stored | 6 | high | possible | correctness |
| 8 | B64: the channel does not preserve Python numbers | 6 | high | possible | data |
| 9 | B54: an unreaped child switches off a forked task's memory budget | 6 | high | possible | reliability |
| 10 | B44: an old stream of a worker can change and disconnect its new incarnation | 6 | high | possible | reliability |
| 11 | B43: cancelling one task while its worker is away is lost on reconnect | 6 | high | possible | correctness |
| 12 | B41: a result the master failed to store isn't sent again while the link stays up | 6 | high | possible | correctness |
| 13 | B87: per-tick queries lack indexes excluding completed history | 6 | medium | likely | performance |
| 14 | B83: mapping repeats the entire source list in every instance’s inputs | 6 | medium | likely | performance |
| 15 | B32: the UI briefly freezes on Trigger and other links, the request left pending (Suspected) | 6 | medium | likely | usability |
| 16 | B31: a finished task stays `scheduled` on the UI long after it ended (Suspected) | 6 | medium | likely | usability |
| 17 | B66: ordinary dictionaries collide with serialization tags | 4 | critical | rare | data |
| 18 | B59: a new content hash can import stale bytecode | 4 | critical | rare | data |
| 19 | B45: a registration with many new roots deadlocks before its stream is returned | 4 | critical | rare | reliability |
| 20 | B81: valid elements inside mapped lists are rejected as though they must themselves be lists | 4 | medium | possible | correctness |
| 21 | B80: using a mapped output elsewhere breaks per-element narrowing | 4 | medium | possible | correctness |
| 22 | B68: a strict model can encode successfully but cannot read its own output | 4 | medium | possible | correctness |
| 23 | B65: mixed structured arguments reach functions in the wrong Python shape | 4 | medium | possible | correctness |
| 24 | B61: a DAG file reached through a followed symlink gets an unusable location | 4 | medium | possible | correctness |
| 25 | B60: a relative DAG root breaks parsing and task imports | 4 | medium | possible | correctness |
| 26 | B56: a cancel or timeout is ignored while a runner hasn't said hello | 4 | medium | possible | correctness |
| 27 | B63: hashing a root reads every file whole into memory | 3 | high | rare | reliability |
| 28 | B57: an interrupted read of a forked process's exit status loses its framing | 3 | high | rare | correctness |
| 29 | B47: a replayed result ignores a lowered master message limit | 3 | high | rare | reliability |
| 30 | B40: a lease test times out its 2 s query limit under load | 3 | low | likely | tests |
| 31 | B85: disconnects during notification draining can silently lose changes | 2 | medium | rare | correctness |
| 32 | B82: settling can perform cubic work on a chain | 2 | medium | rare | performance |
| 33 | B76: Python accepts output nesting that the agent refuses | 2 | medium | rare | correctness |
| 34 | B75: truncation detection applies to unrelated user dictionaries | 2 | medium | rare | correctness |
| 35 | B67: exceptions during result serialization escape without a structured failure | 2 | medium | rare | correctness |
| 36 | B52: the per-round caps still read the whole backlog | 2 | medium | rare | performance |
| 37 | B48: every attempt message copies the master's whole worker registry under its lock | 2 | medium | rare | performance |
| 38 | B69: embedding a model descriptor loses its `$ref` resolution context | 2 | low | possible | correctness |
| 39 | B39: sibling groups on different layers make the run graph far too wide (partly fixed; the rest after 1.0.0, in M6) | 2 | low | possible | usability |
| 40 | B38: an edge between two tasks outside a group can be drawn through its box | 2 | low | possible | usability |
| 41 | B22: a draining worker once missed its 8 s grace under load | 2 | low | possible | tests |
| 42 | B77: valid large timedeltas cannot render in agent templates | 1 | low | rare | correctness |
| 43 | B74: `Literal` compatibility conflates booleans with integers | 1 | low | rare | correctness |
| 44 | B73: optional TypedDict keys are falsely promised as present | 1 | low | rare | correctness |
| 45 | B72: approximate divisibility admits incompatible `multipleOf` constraints | 1 | low | rare | correctness |
| 46 | B71: integer constraint comparisons lose precision | 1 | low | rare | correctness |
| 47 | B70: reference memoization skips comparisons against different inline target schemas | 1 | low | rare | correctness |
| 48 | B24: a forked attempt started while the master link is down is recorded as `spawn` (Suspected) | 1 | low | rare | usability |
| 49 | B4: `runs trigger` in a roles integration test got no result once | 1 | low | rare | tests |

## B87: per-tick queries lack indexes excluding completed history

- **Rating:** severity **medium**, probability **likely**, performance (score 6): every tick scans finished history, growing until retention exists (M4.14).
- **Found:** 2026-10-30, by Codex's whole-repository review, section F4 (the database layer),
  medium severity, confirmed by Codex's failing test, not yet re-checked by Claude; at both `migrations/{sqlite,postgres}/0001_initial.sql:145` and `:170`. The task-state index covers only queued tasks: It cannot serve `due_retries()`’s `state = 'up_for_retry'` predicate. Attempts have primary/unique indexes, but none covering `unstarted_attempts()`’s `status IS NULL` predicate. Both queries run in the dispatch step (`dispatch.rs:464`). **Scenario:** completed task and attempt history accumulates, with no retries or assignments awaiting expiry → every tick still scans that history. This occurs with an **empty backlog**, separately from known B52’s reads of pending work. **
- **A fix:** to decide when it is scheduled.

## B86: accepted strings containing NUL cannot be stored in production

- **Rating:** severity **high**, probability **possible**, correctness (score 6): on Postgres an attempt's completion is rolled back and the task stays running (with B41); an output string with NUL, such as `find -print0`'s stdout.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F4 (the database layer),
  medium severity, confirmed by Codex's failing test, not yet re-checked by Claude; at `crates/schedr-db/migrations/postgres/0001_initial.sql:176`. Outputs use: `store_output()` binds the serializer’s JSON through `$3{json}`, expanded to `::jsonb`. The Python codec accepts NUL-containing strings, but PostgreSQL rejects their escaped representation. **Scenario:** a task successfully returns `"before\0after"` → SQLite stores it → PostgreSQL rejects the output write. In worker-message handling, that error rolls back the transaction containing the attempt’s completion (`core.rs:526`, `dispatch.rs:1096`, `execution.rs:284`). **
- **A fix:** to decide when it is scheduled.

## B85: disconnects during notification draining can silently lose changes

- **Rating:** severity **medium**, probability **rare**, correctness (score 2): the UI's live feed misses changes until the next one; the database listener disconnecting with a notification queued.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F4 (the database layer),
  medium severity, confirmed by Codex's failing test, not yet re-checked by Claude; at `crates/schedr-db/src/changes.rs:220`. Only the first receive handles `None` as `Change::Missed`. The subsequent drain accepts only notifications: This discards a disconnect result or cancels reconnection after the listener has consumed its disconnect state. **Scenario:** one notification is queued → the listening connection dies → another change occurs while disconnected → draining consumes the disconnect → following resumes without reporting `Missed`. The API feed sends a refresh for `Missed` (`schedr-api/src/events.rs:122`), so clients can retain stale data. **
- **A fix:** to decide when it is scheduled.

## B84: cancelling transaction startup returns an open transaction to the pool

- **Rating:** severity **high**, probability **possible**, reliability (score 6): a pooled connection left inside a transaction holding the lease row's lock; a write cancelled while starting (query timeouts during a stall).
- **Found:** 2026-10-30, by Codex's whole-repository review, section F4 (the database layer),
  medium severity, confirmed by Codex's failing test, not yet re-checked by Claude; at `crates/schedr-db/src/query.rs:156`. The connection is closed only when `begin_with(...).await` returns an error: Cancelling that future bypasses this branch. In the pinned sqlx implementation, transaction depth advances only after startup completes, so its cancellation rollback does nothing at this point. **Scenario:** a fenced write waits for a lock during startup → its future is cancelled → the lock becomes available → PostgreSQL finishes the cancelled startup → another borrower receives that connection inside the unfinished transaction, including its transaction-local settings and lease-row lock. **
- **A fix:** to decide when it is scheduled.

## B83: mapping repeats the entire source list in every instance’s inputs

- **Rating:** severity **medium**, probability **likely**, performance (score 6): each mapped instance's inputs carry the whole source list: quadratic, and it can reach the message limit; any `.expand` over an upstream output.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F3 (runs moving forward),
  medium severity, confirmed by Codex's failing test, not yet re-checked by Claude; at `crates/schedr-master/src/mapping.rs:293`. Before consulting the resolved-list cache, every `inputs()` call reloads sources and clones their complete outputs: **Scenario:** a plain task returns N strings, used only by `.expand(x=source)`. Each instance receives its selected string under `$expand` **plus all N strings under `source`**. Fetching, cloning and serializing inputs therefore grows quadratically. The runner uses `$expand` for these mapped arguments. Batch measurement `f3_mapped_inputs_repeat_the_whole_list`, with 256-byte strings and one shared cache: **Confidence: high.** I searched the other call sites and matching implementations for these problems. I did not run the full suite, stress tests, distributed process tests, PostgreSQL fault injection, or release-build benchmarks. The reproductions used SQLite; restart checks recreated the master core against the same database. All throwaway files were removed. `git status --short` was empty before and after; `git diff --exit-code` passed. No tracked files changed. DONE: --- session 01a18d2e-d496-74a2-be56-b991ff6fee40 | 34 commands | tokens in 3111663 (cached 2947456), out 17085 | weekly 21.0%, 5 h 63.0% | /home/ptutak/.claude/skills/codex/logs/01a18d2e-d496-74a2-be56-b991ff6fee40.1.jsonl [exited with code 0]
- **A fix:** to decide when it is scheduled.

## B82: settling can perform cubic work on a chain

- **Rating:** severity **medium**, probability **rare**, performance (score 2): the master's lock held for seconds; a long chain listed in reverse order.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F3 (runs moving forward),
  medium severity, confirmed by Codex's failing test, not yet re-checked by Claude; at `crates/schedr-master/src/progress.rs:454`. Each pass visits all tasks and calls `edges_into()` for scheduled tasks. That helper scans the entire edge vector (`crates/schedr-model/src/dag.rs:116`): The outer loop repeats until nothing changes. A chain listed in reverse dependency order advances one task per pass, yielding Θ(T²E) work—Θ(T³) for a chain. **Scenario:** preparing an 800-task empty chain occupies the master’s core lock for seconds; the service holds that lock while ticking. Batch measurement `f3_settle_scaling`, unoptimized test build: Both task orders produced the same successful result. The same edge-scanning helper is used in mapped settling at `progress.rs:554` and `:600`. **Confidence: high for the complexity; measured timings are debug-build figures.**
- **A fix:** to decide when it is scheduled.

## B81: valid elements inside mapped lists are rejected as though they must themselves be lists

- **Rating:** severity **medium**, probability **possible**, correctness (score 4): a valid mapped DAG fails its run at preparation, visibly.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F3 (runs moving forward),
  medium severity, confirmed by Codex's failing test, not yet re-checked by Claude; at `crates/schedr-master/src/mapping.rs:152` and `:158`. Recursive resolution contains unconditional errors: **Scenario:** both of these pass Python DAG construction, but fail the run during preparation: The outer value is already a list; its elements should resolve to strings.
- **A fix:** to decide when it is scheduled.

## B80: using a mapped output elsewhere breaks per-element narrowing

- **Rating:** severity **medium**, probability **possible**, correctness (score 4): per-element `.on(...)` fails when the mapped output is also used elsewhere.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F3 (runs moving forward),
  medium severity, confirmed by Codex's failing test, not yet re-checked by Claude; at `crates/schedr-master/src/mapping.rs:84`. Lane detection requires exactly one reference: With another reference, narrowing falls through to whole-output resolution. At `mapping.rs:170`, `.on(Rows)` is then compared against the whole mapped task’s synthetic `SUCCESS` outcome, producing `null`. **Scenario:** the Python API accepts: After both fetches succeed, finishing `gate` should allow the matching element to proceed. Instead, expansion fails. This also contradicts the mapping guide’s statements that `.on(...)` works per element and an additional use makes the task wait for the whole upstream.
- **A fix:** to decide when it is scheduled.

## B78: expansion reads instance states before the current result is stored

- **Rating:** severity **high**, probability **possible**, correctness (score 6): a run fails although its producers succeeded; expanding over one element of a mapped task, depending on which instance finishes last.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F3 (runs moving forward),
  high severity, confirmed by Codex's failing test; read and judged sound by Claude; at `crates/schedr-master/src/execution.rs:597`. `advance()` applies the result to memory, then calls: Only afterwards does it call `store()`. Meanwhile, `expansion()` reads `mapping::sources()` from the database. At `mapping.rs:241`, that reader substitutes `null` for any instance whose stored state is not yet `Success`. **Scenario:** `producer.expand(x=[0,1])` returns `[10,20]` from both instances; `consumer.expand(x=produced[1])` waits for the whole producer. When producer instance 1 finishes last, expansion sees its old `Running` state and resolves `produced[1]` as `null`. The consumer and run fail despite successful producers.
- **A fix:** to decide when it is scheduled.

## B77: valid large timedeltas cannot render in agent templates

- **Rating:** severity **low**, probability **rare**, correctness (score 1): a timedelta beyond ~292,000 years can't be rendered into a command.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), low severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/codec.py:159`. Python stores the full supported timedelta range as integer microseconds. Agent rendering accepts the tag only through `Value::as_i64` at `crates/schedr-agent/src/render.rs:261`. `timedelta(days=200000000)` encodes successfully and its microsecond count survives the channel exactly, but rendering treats it as an unsupported object.
- **A fix:** to decide when it is scheduled.

## B76: Python accepts output nesting that the agent refuses

- **Rating:** severity **medium**, probability **rare**, correctness (score 2): an output nested over 128 levels fails after the task succeeded.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/codec.py:260`. Recursive encoding checks values and byte size without enforcing the receiving JSON parser’s nesting limit. A small `Any` output containing 140 nested lists passes encoding and `check_size()` and is emitted as `task_succeeded`, but the agent cannot read it.
- **A fix:** to decide when it is scheduled.

## B75: truncation detection applies to unrelated user dictionaries

- **Rating:** severity **medium**, probability **rare**, correctness (score 2): an output dict with a `<field>_truncated: True` key refused as truncated.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/runner/task.py:96`. `resolve()` checks `value.get(f"{step}_truncated") is True` for every dictionary field, rather than only process-result streams. Referencing `data` in a complete output `{"data": "complete", "data_truncated": True}` raises a truncation error.
- **A fix:** to decide when it is scheduled.

## B74: `Literal` compatibility conflates booleans with integers

- **Rating:** severity **low**, probability **rare**, correctness (score 1): `Literal[True]` accepted for `Literal[1]` at load, failing at run time.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/compatibility.py:151`. The check uses `v in target.values`, so Python equality makes `True` fit `1`. The runtime Literal codec requires both equal value and equal type. A `Literal[True]` producer therefore connects successfully to a `Literal[1]` parameter, then fails at execution.
- **A fix:** to decide when it is scheduled.

## B73: optional TypedDict keys are falsely promised as present

- **Rating:** severity **low**, probability **rare**, correctness (score 1): an optional nested TypedDict key accepted as required at load.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/models.py:105`. `_all_required()` sets `copied["required"] = list(node["properties"])` on every nested object schema. A model’s nested TypedDict can legitimately omit `NotRequired` keys; dumping the model does not add them. A model containing `row: {x: NotRequired[int]}` is consequently accepted for one requiring `row.x`.
- **A fix:** to decide when it is scheduled.

## B72: approximate divisibility admits incompatible `multipleOf` constraints

- **Rating:** severity **low**, probability **rare**, correctness (score 1): huge `multipleOf` constraints compared approximately at load.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/schemas.py:538`. The proof uses `math.isclose(ratio, round(ratio))`. Its relative tolerance accepts nonintegral ratios at sufficiently large magnitudes. A source with `multiple_of=10**12` is accepted for a target with `multiple_of=3`, although `10**12` itself fails that target.
- **A fix:** to decide when it is scheduled.

## B71: integer constraint comparisons lose precision

- **Rating:** severity **low**, probability **rare**, correctness (score 1): integer bounds above 2^53 compared as floats at load.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/schemas.py:517`. `_limit()` converts integer boundaries with `float(schema[plain])`. This collapses distinct boundaries above the exact integer range of a float. A source allowing `x >= 9007199254740992` is accepted for a target requiring `x >= 9007199254740993`. The source’s minimum value then fails decoding.
- **A fix:** to decide when it is scheduled.

## B70: reference memoization skips comparisons against different inline target schemas

- **Rating:** severity **low**, probability **rare**, correctness (score 1): one reference compared against two different inline targets, cached as one.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/schemas.py:246`. The cache key contains only reference strings:
- **A fix:** to decide when it is scheduled.

## B69: embedding a model descriptor loses its `$ref` resolution context

- **Rating:** severity **low**, probability **possible**, correctness (score 2): a wrong `list[Model]` connection accepted at load, failing at run time.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/schemas.py:132`. `from_desc()` nests a model’s schema and `$defs` under `items` or another enclosing schema. `_Compare` continues resolving references from the outer root. At `schemas.py:444`, a missing definition becomes `{}`, which `fits()` accepts as unrestricted. Thus `list[TextBox]`, containing a model field `x: str`, is accepted for `list[IntBox]`, whose corresponding field is `x: int`. The same comparison without the enclosing list correctly rejects it.
- **A fix:** to decide when it is scheduled.

## B68: a strict model can encode successfully but cannot read its own output

- **Rating:** severity **medium**, probability **possible**, correctness (score 4): a strict model with a datetime field can't read its own output.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/models.py:135`. `dump()` uses JSON mode, turning datetimes into strings. `validate()` then calls `cls.model_validate(data, ...)` in Python mode. A strict datetime field rejects that string, although the parse-time compatibility check accepts the model-to-itself connection.
- **A fix:** to decide when it is scheduled.

## B67: exceptions during result serialization escape without a structured failure

- **Rating:** severity **medium**, probability **rare**, correctness (score 2): a task whose result can't be serialised ends without a structured failure.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/runner/task.py:423`. Both encoding and size checking catch only `CodecError`. Recursive values, failing Pydantic serializers, and exceptions from JSON/UTF-8 conversion escape those handlers. The task function completes, but the runner closes its channel without sending `task_failed`.
- **A fix:** to decide when it is scheduled.

## B66: ordinary dictionaries collide with serialization tags

- **Rating:** severity **critical**, probability **rare**, data (score 4): a dict such as `{"$date": "x"}` comes back as a different value, silently; a user key equal to a tag.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/_serialization/codec.py:433`. The encoder preserves arbitrary string keys. The decoder interprets any singleton dictionary whose key is in `TAGS` as a tagged value: `if len(data) == 1 and next(iter(data)) in TAGS`. Consequently, even a value encoded and decoded against the same `dict[str, Any]` annotation changes:
- **A fix:** to decide when it is scheduled.

## B65: mixed structured arguments reach functions in the wrong Python shape

- **Rating:** severity **medium**, probability **possible**, correctness (score 4): a tuple, dataclass or model argument built from references arrives as a list or dict.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), medium severity, with a failing check; Codex's check, not yet re-checked by Claude; at `python/schedr/runner/task.py:181`. `build()` constructs mixed arguments with `return [build(...)]` or `return {k: build(...)}`, without constructing the annotated tuple, dataclass or model. A DAG declaring `consume((produce(), 3))` for `value: tuple[int, int]` passes parsing, but receives a list. Likewise, `consume({"x": produce()})` passes parsing for a dataclass or Pydantic parameter, then receives a dict.
- **A fix:** to decide when it is scheduled.

## B64: the channel does not preserve Python numbers

- **Rating:** severity **high**, probability **possible**, data (score 6): a float can change in its last digit silently, an integer beyond 64 bits fails downstream; any numeric output.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F8 (the Python runner
  and serialization), high severity, with a failing check, confirmed by Claude (`serde_json`
  has neither `float_roundtrip` nor `arbitrary_precision`); at `python/schedr/_serialization/codec.py:129`; `Cargo.toml:46`. The encoder accepts unrestricted integers (`isinstance(value, int)`) and finite floats. The agent reads them using `serde_json::from_slice` at `crates/schedr-agent/src/channel.rs:90`, with the default `serde_json` configuration. A task’s exact integer `18446744073709551617` becomes an approximate float in the agent. A downstream `int` decoder rejects that float. Finite floats can also change value during the read.
- **A fix:** to decide when it is scheduled.

## B63: hashing a root reads every file whole into memory

- **Rating:** severity **high**, probability **rare**, reliability (score 3): a worker can't register (it retries for ever); a file in a root bigger than the worker can allocate.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F7 (the agent's files
  and parsing), with a failing proof each; checked against the code by Claude.
  `content_hash` (`crates/schedr-agent/src/scan.rs:169`) reads each scanned file with `std::fs::read`, Python or not: a data file bigger than the worker can allocate fails the hash, and before registration `Agent::hashes` retries for ever (`agent.rs:659`); Codex's proof: a 256 MiB sparse file under a 192 MiB address-space limit, `OutOfMemory`.
- **A fix:** stream each file into the hasher (a buffered reader).

## B62: the log endpoint follows a symlink named like an attempt's log

- **Rating:** severity **critical**, probability **rare**, security (score 4): the log endpoint serves any file the worker can read; needs a `<uuid>.log` symlink planted by a task or a local user.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F7 (the agent's files
  and parsing), with a failing proof each; checked against the code by Claude.
  The endpoint validates the attempt ID as a UUID, then opens `log_dir/<uuid>.log` following links (`crates/schedr-agent/src/logs.rs:157`): a `<uuid>.log` symlink planted by a task or a local user serves the target's contents to an authorised log reader (Codex's proof: a test private key outside the log directory, served with 200). No HTTP-only way to plant one was shown. The writer (`process.rs:126`) follows links too.
- **A fix:** open logs with `O_NOFOLLOW` (and check the file is regular), reading and writing.

## B61: a DAG file reached through a followed symlink gets an unusable location

- **Rating:** severity **medium**, probability **possible**, correctness (score 4): a DAG under a symlinked directory is refused by the master.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F7 (the agent's files
  and parsing), with a failing proof each; checked against the code by Claude.
  Scanning follows symbolic links (documented), but `registry.py:71` resolves the file's path and keeps only its name (`DagLocation(LOCAL_ROOT, file.name)`): `root/linked -> ../shared` with `dag.py` scans as `linked/dag.py` and parses as `local:dag.py`, which the master refuses (`validate.rs:36`: located at `local:dag.py`, parsed from `etl:linked/dag.py`). Codex ran the real parser and validator.
- **A fix:** keep the scanned (root-relative) path as the location, unresolved.

## B60: a relative DAG root breaks parsing and task imports

- **Rating:** severity **medium**, probability **possible**, correctness (score 4): a relative root in the configuration breaks parsing and task imports.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F7 (the agent's files
  and parsing), with a failing proof each; checked against the code by Claude.
  The executor uses the configured directory both as the child's working directory and as `root_dir` in the request (`crates/schedr-agent/src/agent.rs:295`; tasks the same, `attempt.rs:245`, `:252`); configuration keeps relative paths (`schedr-config/src/roots.rs:155`), so `dags/etl` becomes `dags/etl/dags/etl/dag.py` (Codex's proof: `FileNotFoundError` after a successful scan). `WorkerSetup::sys_path` keeps them relative too.
- **A fix:** make roots absolute when the configuration is read (or the request's `root_dir`).

## B59: a new content hash can import stale bytecode

- **Rating:** severity **critical**, probability **rare**, data (score 4): old code runs under the new hash; an edit of the same length within the same second (git-sync).
- **Found:** 2026-10-30, by Codex's whole-repository review, section F7 (the agent's files
  and parsing), with a failing proof each; checked against the code by Claude.
  The runner imports DAG files with the standard source loader (`python/schedr/runner/parse.py:65`; tasks at `task.py:290`), whose `.pyc` check is the source's mtime in whole seconds and its size: an edit of the same length within the same second (git-sync switching revisions does that) reuses the old bytecode, while the agent's scan saw the change and hashed the new source (Codex's proof: hash changed, the source says `new`, a fresh runner parsed `old`).
- **A fix:** no bytecode cache for DAG roots (`sys.dont_write_bytecode` and not reading `__pycache__`), or hash-based `.pyc` checked against the source.

## B58: parsing can attribute a root's new code to its old hash

- **Rating:** severity **critical**, probability **possible**, data (score 8): one revision's DAGs stored under another revision's hash; git-sync switching the root between hashing and parsing.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F7 (the agent's files
  and parsing), with a failing proof each; checked against the code by Claude.
  A parse job takes the file list from the settled snapshot but the directory from the watcher's path (`crates/schedr-agent/src/agent.rs:1047`), and the result keeps the assignment's hash with no check after: when the root's symlink switches revision between hashing and parsing (git-sync), revision B's DAGs are uploaded under A's hash (Codex's proof: cached hash old, content new, parsed `new`). `code_mismatch` and `remember_code` use the cached hash too (`:1014`, `:965`).
- **A fix:** parse the snapshot's resolved directory (the revision the hash is of), or re-hash after parsing and drop a result whose hash changed.

## B57: an interrupted read of a forked process's exit status loses its framing

- **Rating:** severity **high**, probability **rare**, correctness (score 3): a healthy forked attempt killed and ended `unknown`; a stop arriving while its exit status is being read.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F6 (the agent's task
  execution), with a failing proof each; checked against the code by Claude.
  `ForkedProcess::wait` awaits `read_message(status)` (`crates/schedr-agent/src/fork.rs:861`), whose header and partial body live in the future; the supervisor's `select!` drops it when a stop or deadline wins and calls `wait` again, which reads the rest as a new header, fails, kills the group and returns `Termination::Unknown` for a healthy shim (Codex's proof with the real fork server, the frame split at a barrier: `Unknown` instead of `Signaled(9)`). Frequency in practice not measured.
- **A fix:** a cancel-safe reader (the partial frame kept in the struct), or the read in its own task.

## B56: a cancel or timeout is ignored while a runner hasn't said hello

- **Rating:** severity **medium**, probability **possible**, correctness (score 4): a cancel or timeout waits up to 30 s for a slow runner start, and is lost if the start fails.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F6 (the agent's task
  execution), with a failing proof each; checked against the code by Claude.
  `run_python` starts the runner and waits for its hello (up to 30 s) and sends the request before `supervise` starts (`crates/schedr-agent/src/launch.rs:274`); a cancel queued meanwhile, or the execution timeout, does nothing until then, and a failed start afterwards is reported `start_failed`, the cancel lost (Codex's proof: a runner stalled before its hello, still running 2 s after a cancel and a 100 ms timeout). Spawned and forked paths alike.
- **A fix:** the stop channel and the deadline raced against the handshake too; a cancel during it reported as a cancel.

## B54: an unreaped child switches off a forked task's memory budget

- **Rating:** severity **high**, probability **possible**, reliability (score 6): a forked task's memory budget off, so the worker can run out of memory; a task leaving a child unreaped.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F6 (the agent's task
  execution), with a failing proof each; checked against the code by Claude.
  For a forked group (`crates/schedr-agent/src/memory.rs:159`) any process whose PSS can't be read discards the whole sample (`None => return None`); a zombie in the group has none, so while a task leaves one child unreaped every sample is skipped and the budget never applies (Codex's proof: a live parent at 32 MiB with a 16 MiB budget, no kill). Skipping an unreadable live process is the documented choice; a zombie (state `Z`) has no memory to count. Forked parse runners use the same watchdog.
- **A fix:** count a zombie as 0 (its `stat` says `Z`) instead of discarding the sample.

## B52: the per-round caps still read the whole backlog

- **Rating:** severity **medium**, probability **rare**, performance (score 2): thousands of pending commands read whole on each round.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F2 (dispatch and
  attempts), with a failing proof each; checked against the code by Claude.
  `pending_commands` (`crates/schedr-db/src/commands.rs:299`) has no `LIMIT` (`fetch_all`); the master keeps `COMMANDS_PER_ROUND` (50) after reading and decoding every pending row (`schedr-master/src/commands.rs:129`): draining N commands reads N, N-50, ... rows (Codex: 2,000 commands, 32 MB of audit strings loaded for 50 applied). Expired attempts the same (`unstarted_attempts`, then `truncate`, `dispatch.rs:864`).
- **A fix:** `LIMIT` in the queries (the round's cap).

## B50: a running task can be expired and assigned again after a reconnect

- **Rating:** severity **critical**, probability **possible**, correctness (score 8): the same task runs twice at once; needs a lost `Started` and a reconnect within the 10 s start timeout.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F2 (dispatch and
  attempts), with a failing proof each; checked against the code by Claude.
  When `Started` is lost with the stream and the worker reconnects within the start timeout (10 s) with the attempt `Running` in its journal, the master adopts it (`crates/schedr-master/src/dispatch.rs:381`: only the slot reserved) and answers `Continue`, but records no start; the expiry (`started_at IS NULL`) then ends it as `AssignmentExpired` and assigns the task again while the first process runs (Codex's proof: one new assignment). The agent resends `Finished` on `Continue` but no `Started`.
- **A fix:** record the start on adoption (the journal says it runs), or have the agent resend `Started`.

## B48: every attempt message copies the master's whole worker registry under its lock

- **Rating:** severity **medium**, probability **rare**, performance (score 2): each worker message costs in proportion to the whole registry; hundreds of workers.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  `Started`, `Rejected` and `Finished` each take `Dispatcher::memory` (the D1 snapshot: `workers`, `rejections`, `code_missing` cloned, `crates/schedr-master/src/core.rs:499`, `dispatch.rs:280`) under the core lock, so each message costs in proportion to the whole registry and its outstanding attempts; Codex measured 10 vs 1,000 workers (4 attempts each), 500 snapshots each, unrelated attempts included.
- **A fix:** a snapshot of what the message can change (its attempt and worker) instead of everything; measure first with M4.5.1's per-turn timings.

## B47: a replayed result ignores a lowered master message limit

- **Rating:** severity **high**, probability **rare**, reliability (score 3): every replay ends the stream, so the worker can't reconnect; a master message limit lowered under a journalled large result.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  On `Continue`, the agent resends an unacknowledged `Finished` as stored (`crates/schedr-agent/src/agent.rs:843`); `fit_result` (which truncates a result to `Registered`'s `master_limit`) runs only on new completions. A 5 MiB result journalled under a larger limit, then a master announcing 4 MiB: every replay ends the stream (Codex's probe: 5,242,891 bytes sent against 4,194,304).
- **A fix:** fit the replayed result to the current limit, as a new one is.

## B46: a worker can report `Finished` for another worker's attempt

- **Rating:** severity **critical**, probability **rare**, security (score 4): one worker can end another's running attempt as a success; needs a registered, misbehaving worker.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  `Dispatcher::finished` (`crates/schedr-master/src/dispatch.rs:828`) never checks `attempt.worker_id` against the sender; `Started`, `Rejected` and the journal reconciliation do. A registered `w2` knowing `w1`'s attempt ID stores its success and frees `w1`'s slot while the process still runs (Codex's proof: `Some(Success)`).
- **A fix:** refuse (and log) a `Finished` from another worker than the attempt's, as `Started` does.

## B45: a registration with many new roots deadlocks before its stream is returned

- **Rating:** severity **critical**, probability **rare**, reliability (score 4): the master's stream mutex blocked for ever, every worker stalls; a registration with 1,024 or more new roots.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  `Control` delivers `Registered` and every `AssignParse` into the worker's channel (`STREAM_BUFFER` 1,024) before returning the receiver that drains it (`crates/schedr-master/src/service.rs:568`); 1,024 new roots fill it and `deliver` waits for ever, holding the streams mutex (other deliveries and stream changes wait too). Codex's proof: registration with 1,024 unseen roots never returns.
- **A fix:** return the stream first and deliver after, or never block delivery on one worker's full channel (and not under the shared mutex).

## B44: an old stream of a worker can change and disconnect its new incarnation

- **Rating:** severity **high**, probability **possible**, reliability (score 6): the live worker marked disconnected and a second registration let in; a lease lost while the old transport stays open.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  Messages and the disconnect are keyed by worker ID only (`crates/schedr-master/src/service.rs:446`, `:467`); the incarnation check (`same_channel`) guards only the outgoing map. An incarnation `i1` whose lease was lost while its transport stays open: `i2` registers; `i1`'s messages still change the worker, and `i1` closing marks `i2` disconnected, which also lets another registration in despite `i2`'s open stream (Codex's gRPC proof: `Disconnected` instead of `Active`).
- **A fix:** tie each stream's messages and its disconnect to its incarnation; ignore those of a replaced one.

## B43: cancelling one task while its worker is away is lost on reconnect

- **Rating:** severity **high**, probability **possible**, correctness (score 6): a cancelled task runs on; a cancel while its worker is away.
- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  The journal reconciliation resends a cancel only when the run is cancelled (`crates/schedr-master/src/core.rs:830`: `run.cancel_requested`); a `CancelTask` sets the instance's flag, so after a reconnect the attempt gets `Continue` and runs on (Codex's proof: 0 cancels where 1 was due). Rejections and completions check both flags; the reconnect test covers `CancelRun` only.
- **A fix:** check the instance's `cancel_requested` too, with a test of `CancelTask` across a reconnect.

## B41: a result the master failed to store isn't sent again while the link stays up

- **Rating:** severity **high**, probability **possible**, correctness (score 6): a finished task stays running for as long as the link lives; one failed result write (a database stall).
- **Found:** 2026-10-30, by Codex's whole-repository review, section F1 (the master's core
  loop and the worker link), with a failing proof each; checked against the code by Claude.
  The agent sends `Finished` once and keeps it in `unacked` (`crates/schedr-agent/src/agent.rs:1114`); heartbeats don't resend it, only a reconnect's `decide` does. The master logs a handling error and keeps the stream (`crates/schedr-master/src/service.rs:448`). One failed write: the attempt stays running for as long as the link lives (Codex's proofs: 12 healthy heartbeats and ticks, still open). Contradicts the Worker lifecycle's "`Finished` until `FinishedAck`".
- **A fix:** resend unacknowledged results on a timer (or with heartbeats) until acknowledged.

## B40: a lease test times out its 2 s query limit under load

- **Rating:** severity **low**, probability **likely**, tests (score 3): a lease test fails under load.
- **Found:** 2026-10-30, in `make check` while two other builds and test runs shared the
  machine: `lease::tests::a_master_taken_over_never_writes_itself_back_as_the_holder` failed
  with `QueryTimeout(2s)` (`crates/schedr-master/src/lease/tests.rs:101`, an `unwrap` on a
  query). Alone it passed 5 times out of 5 (6-9 s each).
- **The cause, likely:** the test runs with the production 2 s query limit; SQLite under a
  loaded CPU can exceed it. Not investigated: whether the test exercises a write that should
  be retried rather than a limit that should be longer in tests.
- **A fix:** the test's own query limit (or a retry of what the master retries itself), and
  the same check over the other tests using the default limit.

## B39: sibling groups on different layers make the run graph far too wide (partly fixed; the rest after 1.0.0, in M6)

- **Rating:** severity **low**, probability **possible**, usability (score 2): DAGs with many groups drawn far too wide (M6).
- **Found:** 2026-10-30, by Grok's review of M4.2b's graph layout (decision 65). A group was
  moved only on the layers holding its tasks while its box covers every layer between, so
  boxes stretched and pushed the next group further each pass: 8 sibling groups of 3 tasks
  reached tens of thousands of px.
- **Partly fixed, merged 2026-10-30** (Grok sessions `e9ee40d3`, `3b1e9c47`, reviewed by Codex
  `01a18cef`; the user merged it after a comparison with the earlier layout on 3,000 random
  grouped DAGs: 1,669 narrower by over 10%, 1,192 about the same, **139 wider** by over 10%
  (up to 2.7x, 6,000-14,000 px); the widest from 51,250 to 13,963 px; 8 / 16 flat staggered
  groups from 29,987 / 7,145,763 px to 2,992 / 5,825; layout time about the same).
  Decisions with the user,
  1 A (a group is a rigid column: its layers share the rightmost left edge, moving only right;
  a pushed item takes its outermost group along, on every layer, each item once), 5 A
  (outermost first), 7 A (a nested group whose layers don't line up is its own column; its
  parent doesn't drag a layer holding it); tests 2 A, 3 A (random trees of 1-6 groups, a
  4-deep nest, a group skipping layers), 8 B (one extra row in the random width bound). The
  original shape is fixed (8 groups: 29,987 px before, now within the side-by-side bound).
- **Open (Codex's review), for M6:** the 139 random shapes the merged layout draws wider; R1
  (high) nested staggered siblings still grow about 4x per doubling (16 parents 27,169 px, 32:
  100,629, against 7.4 million and 4.6e11 px before); R2 quadratic scans per round (`take_group`,
  `align_group`, `shift_closure`, `clear`); R3 a transitive push moves only the innermost
  group; R4 the random properties pass when no box is drawn. **Chosen, not implemented:**
  9 A (pull a nested group left of its parent's column onto it, indented; don't chase one to
  the right; push by the largest single intrusion, always the outermost group), 10 A (one
  work counter over the four scans, bound 8n), 11 A (the seven-task R3 graph), 12 B (every
  populated group has a box, plus a populated-and-empty fixed test).
- **Parked** (the user, 2026-10-30): back in M6, as a layout correct by construction (each
  group a block, placed recursively) plus a safety net (check, then fall back to strips),
  PLAN.md M6. It affects only how DAGs with many groups are drawn, nothing that runs; each
  review round of the repair approach found the next shape.
## B38: an edge between two tasks outside a group can be drawn through its box

- **Rating:** severity **low**, probability **possible**, usability (score 2): an edge drawn through a group's box (M6).
- **Found:** 2026-10-30, by Grok's review of M4.2b's graph layout. On a layer a group spans
  without a task there, a task outside it is pushed right of the box on that layer only;
  the edge from it to a task on the other side is a curve between the two, through the box.
  Decision 65 keeps tasks out of boxes, not lines. Grok's case: `A0 -> U -> A1`, `U -> B1`,
  `A = {A0, A1}`, `B = {B0, B1}`: `U` lands right of `B`, its edges cross `B`'s box.
- **Cosmetic:** every task is in the right box. **A fix:** route such edges around boxes,
  or order layers so that it can't happen; with B39.

## B32: the UI briefly freezes on Trigger and other links, the request left pending (Suspected)

- **Rating:** severity **medium**, probability **likely**, usability (score 6): UI clicks hang for a moment; seen by the user (the cause suspected).
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

- **Rating:** severity **medium**, probability **likely**, usability (score 6): a finished task shown `scheduled` for up to ~15 s; seen by the user (the cause suspected).
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

- **Rating:** severity **low**, probability **rare**, usability (score 1): a forked attempt shown as `spawn` after a reconnect (suspected).
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

- **Rating:** severity **low**, probability **possible**, tests (score 2): a drain test misses its grace under load.
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

- **Rating:** severity **low**, probability **rare**, tests (score 1): one test run got no result once (likely B6, fixed).
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
