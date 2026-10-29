# Routing by outcome

Every task reports **which kind of success** it had: its **outcome**. Downstream tasks
route on it with `.on(...)`. An expected problem is a success path (a caught connection
error returned as `Unavailable`; a command's declared exit code). An uncaught exception,
or an exit code the task did not declare, is a failure: schedr retries, notifies, and
fails the run.

This DAG fetches from two sources, loads whichever returned `Rows`, reports a mirror
outage, routes a command's exit code, and a `@branch` chooses a subset of its downstream
tasks.

```{literalinclude} ../../examples/docs/outcomes.py
:language: python
:caption: examples/docs/outcomes.py
```

`load` runs because `fetch` returned `Rows` (`fetch_mirror` returned `Unavailable`, so
that argument is `None`). `report_outage` runs on the mirror. `probe` exits 0, which
`Check` declares as `CHANGED`, so `changed` runs and `unchanged` is skipped. `dest`
returns `s3`, so `to_s3` runs and `to_gcs` is skipped; `done` still runs
(`RunIf.NONE_FAILED`), `all_ok` is skipped (`RunIf.ALL_SUCCEEDED`).

## Declaring outcomes

Every task has an outcome enum. The backend stores a **name**; the classes and enums
exist only in Python.

| How it is declared | Outcome names | Stored `(output, outcome)` |
|---|---|---|
| Ordinary return (`-> int`, `-> Rows`, `-> int \| None`) | `SUCCESS` | The value, and `SUCCESS` |
| A union of two or more dataclasses, Pydantic models and `None` (`-> Rows \| Unavailable`) | `Rows`, `Unavailable` | The value, and the class name (`"None"` for `None`) |
| `-> Check` (`Outcome` enum) | The members' names | `(null, "CHANGED")` |
| `CommandOperator(..., outcomes=Check)` | The members' names | The `ProcessResult`, and the member whose value is the exit code |
| `CommandOperator` without `outcomes=` | `SUCCESS` | The `ProcessResult`, and `SUCCESS` on exit code 0 |
| `EmptyOperator` | `SUCCESS` | `(null, "SUCCESS")` |
| `@branch` | `SUCCESS` | The chosen task IDs, and `SUCCESS` |

`SUCCESS` is `DefaultOutcome.SUCCESS`. Users never write the pair: they write return
types, `outcomes=` and `.on(...)`; the runner (Python) or the agent (commands) builds it.

Outcome classes derive from `schedr.Data` (`class Rows(Data, frozen=True)`, a dataclass),
so type checkers accept `.on(Rows)`; a plain `@dataclass` works too, with a `TypingHint`
warning and `on(output, Rows)` for type checkers (see [Type checkers](09-type-checkers.md)).
Pydantic models are recognised as outcome members, but outputs of them come later: a task
returning one is refused for now.

A union is an outcome union only at the **top level**, and only of dataclasses, Pydantic
models and `None`. `int | None`, `int | str`, `Rows | int` and `list[Rows | Unavailable]`
are ordinary data with outcome `SUCCESS`. Members that are subclasses of one another, the
same class with different type parameters, or two classes of one name, are a `TypeError`
at import. An `Outcome` enum mixed into a union is too: outcomes are either a union of
classes or one enum. For data or nothing, return an empty dataclass or `None`.

```python
class Check(Outcome):
    CHANGED = 0
    UNCHANGED = 1
```

For a command, each member's value is the exit code meaning it. A declared code is a
success with that outcome; any other code, or a signal, fails the attempt. A custom enum
is the whole set of success codes: include `0` as a member if that code is a success.
Without `outcomes=`, exit code 0 is `SUCCESS`. The same enum works for a Python task
(`-> Check`): the member values are ignored, the output is `None`, and the member is the
outcome.

## `.on(...)`

`.on(...)` is an edge and data. It makes the downstream task depend on those outcomes
and passes the output **narrowed** to them. An edge whose outcome did not happen is not
taken. A plain reference or plain `>>` is taken on any outcome.

```python
load(primary.on(Rows), mirror.on(Rows))
probe.on(Check.CHANGED) >> changed
```

| Expression | Edge | Downstream receives when taken |
|---|---|---|
| `x(f)` | any outcome | the value, decoded into the member the outcome names |
| `f >> x` | any outcome | nothing (order only) |
| `x(f.on(Rows))` | `Rows` | the `Rows` value |
| `f.on(Rows) >> x` | `Rows` | nothing (order only) |
| `x(f.on(Rows, Unavailable))` | `Rows` or `Unavailable` | the value |
| `x(f.on(Rows).rows)` | `Rows` | the field of the `Rows` value |
| `x(c.on(Check.CHANGED))` | `CHANGED` | `None` for a Python enum task; a `ProcessResult` for a command |
| `x(g.on(None))` | `None` | `None` (useful for order) |

`.on()` takes only members of the task's own outcomes: a class of its union, or a member
of its enum (`DefaultOutcome.SUCCESS` on a task whose enum is `DefaultOutcome`). A kind
the task does not have is a `TypeError` at import, because that edge could never be
taken. Narrow the whole output first (`f.on(Rows).rows`); `f.rows` on a union is a
`TypeError`, and `f.on(Rows).on(Rows)` is too.

## `.outcome`

`.outcome` passes the outcome itself as data: the member of the task's `Outcome` enum,
for an enum task or a command. The stored value is the member's name; a parameter
annotated with that enum is decoded into the member (`react` prints `CHANGED`). A
parameter annotated `str` receives the name.

`.outcome` is a `TypeError` on a union of classes (the value's class already says which)
and on a part of the output. On a task whose output may have a field or key `outcome`
(one declaring it, a dict, `Any`), `.outcome` is refused: write `result["outcome"]` for
the field.

## `run_if`

Trigger rules count **edges**. An edge not taken (an `.on(...)` whose outcome did not
happen, a `@branch` that did not choose that target, or a skipped upstream task) counts
as skipped.

| | `RunIf.NONE_FAILED` (default) | `RunIf.ALL_SUCCEEDED` |
|---|---|---|
| Every edge succeeded | runs | runs |
| Some skipped, the rest succeeded | **runs** | **skipped** |
| Every edge skipped | skipped | skipped |
| Any failed | `upstream_failed` | `upstream_failed` |
| Data from an edge that wasn't taken | `None` (the parameter must allow `None`) | never happens |

Both decide **as soon as the result is certain**: the first failure makes the task
`upstream_failed` without waiting for the others. For `ALL_SUCCEEDED`, a skip decides
once no other edge can still fail (a failure wins).

Set it on `@task`, `.override(...)`, `CommandOperator`, `EmptyOperator` and
`TaskDefaults`. `load` in the example uses the default, so it runs from one `Rows`
source. `all_ok` sets `RunIf.ALL_SUCCEEDED` and is skipped because `to_gcs` was.

## When a parameter must allow `None`

Under `NONE_FAILED`, a task whose **every** edge isn't taken is skipped, not run with
`None`. A parameter must allow `None` only when its reference may stand for `None`
(narrowed with `.on(...)`, or to a task that may be skipped) **and** the task has
another edge that can run it anyway: `load(primary.on(Rows), mirror.on(Rows))` takes
`Rows | None` twice; `report_outage(mirror.on(Unavailable))` takes plain `Unavailable`.

The check runs when the `with Dag` block ends, so an order edge added after the call
counts. It reaches references at any depth (a list or dict that holds one) and outcome
references (`check.outcome` when `check` may be skipped). `ALL_SUCCEEDED` never receives
`None` from upstream. Order-only edges need nothing.

A `None` that reaches a t-string or a command line cannot be rendered: that attempt
fails.

## `@branch`

Outcomes cover "route on what happened" (exactly one outcome per result). `@branch`
chooses **any subset** of its **direct** downstream tasks in the function's code. Prefer
outcomes when they fit.

The function returns task handles (one, or a list). `[]` chooses none. Returning `None`,
a string, or a task that is not right after the branch, fails that attempt. The stored
output is the chosen task IDs; the outcome is `SUCCESS`. Each outgoing edge is a
**selection edge**, taken when the branch chose that target. A target not chosen counts
that edge as skipped, like any edge not taken: under `NONE_FAILED` it still runs if
another of its edges succeeded (`done`); under `ALL_SUCCEEDED` it is skipped (`all_ok`).

`.on(...)` on a `@branch` task is a `TypeError`: those edges are taken by choice.

```python
@branch
def dest() -> list[Task]:
    return [s3]

chosen = dest()
chosen >> [s3, gcs]
```

## What `schedr dags test` shows

The report's `OUTCOME` column is the outcome name on success (`Rows`, `CHANGED`,
`SUCCESS`) and `-` when the task did not succeed (skipped, failed, `upstream_failed`).
`--json` stores the same name on a successful task and `null` otherwise.
[Testing a DAG](07-testing.md) is the rest of the command.
