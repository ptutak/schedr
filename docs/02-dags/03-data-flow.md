# Data flow

A task receives another task's output by being called with it. `>>` only decides the
order. This DAG does both: `load` and `show` use `fetch`'s data, and `left` and `right`
run after `fetch` and before `finish` without reading it.

```{literalinclude} ../../examples/docs/data_flow.py
:language: python
:caption: examples/docs/data_flow.py
```

`fetch()` returns an `Output`. `batch.rows` and `batch["source"]` and `batch[0]` are
outputs too, each naming a part. The master extracts that part when it builds the
downstream task's inputs. A key is a `str` or an `int`; a negative index is a
`ValueError`.

| Where a reference is written | What it does |
|---|---|
| An argument of a `@task` call | A data edge. The downstream task waits for that output. Lists, tuples and dict values are searched, at any depth. |
| A command string, a list element or an `env` value | A data edge, the same way. |
| `x.on(Kind)` | A data or order edge taken only on that outcome. [Routing by outcome](08-outcomes.md). |
| `a >> b`, `b << a`, or `chain(a, b)` | An order-only edge. `b` waits for `a` and receives nothing from it. |
| `a >> [b, c]`, `[a, b] >> c` | An edge from every task on the left to every task on the right. |
| `chain(a, [b, c], d)` | `a >> [b, c] >> d`. |

`chain` and `>>` take tasks and outputs. A bare function is a `TypeError`. Both kinds of
edge may exist between the same pair. A task depending on itself, or on a task that is not
in this DAG, is a `ValueError`. When the block ends, a cycle is a `ValueError`:
`` DAG `data_flow` has a dependency cycle: a -> b -> a ``.

The downstream call has to come after the upstream call, so the `Output` exists to pass.
Build a dataclass inside the task that returns it. A reference placed inside a dataclass
or a set in the DAG body is a `TypeError` at import. The message names the task and the
argument, then `Output is not supported` or `set is not supported`. Lists, tuples and
dict values are the containers a reference may sit in.

In the DAG body an `Output` is a reference. Using it as a value is a `TypeError`: `if
batch`, `for x in batch`, `str(batch)` and an f-string. Pass it to a task, or write it in
a t-string.

## What the path must match

The part is checked against the upstream return type at import.

| The upstream type | A valid next step |
|---|---|
| A dataclass or a `TypedDict` | A field that exists. `batch.rows` is `int` because `Batch.rows` is. |
| `dict[str, T]` | Any string key, of type `T`. |
| `list[T]` | Any index, of type `T`. |
| `tuple[A, B]` | An index that exists. |
| `Any` | Anything; the part stays `Any`. |
| `ProcessResult` | `stdout`, `stderr`, `exit_code`, `stdout_truncated`, `stderr_truncated`. |
| An outcome union (`Rows \| Unavailable`) | A field of a member, after `.on(Rows)`: `batch.on(Rows).rows`. |

A mismatch is a `TypeError` naming the argument and both types. There is no coercion
except `int` for `float`. When the upstream return type is an outcome union, name the
member first: `batch.on(Rows).rows`.

## t-strings

A t-string becomes a template. References in it are filled in when the task runs. A plain
value is formatted while the file is imported: a string as itself, a number or a bool with
`str`, a `datetime` or `date` in ISO 8601, a `timedelta` as seconds (`3600`, `1.5`).

| Format | Where |
|---|---|
| none | The text above. |
| a spec containing `%`, such as `:%Y-%m-%d` | A `datetime`, a `date`, or `Any`. |
| `:raw` | A shell command, where it turns quoting off. Elsewhere it is a `ValueError`. |

`!r`, `!s` and `!a` apply to a plain value. On a reference they are a `TypeError`. A
t-string nested in another, or used as a dict key, is a `TypeError`.

## When a task does not run

A task runs when its incoming edges and `run_if` allow it
([Routing by outcome](08-outcomes.md)). If an upstream task fails, this task ends
`upstream_failed` and does not run. An edge that wasn't taken (`.on(...)` whose outcome
didn't happen, or a `@branch` that didn't choose this task) counts as skipped. Tasks with
no dependency on the failure keep running.
[Defaults and retries](05-defaults.md) is what a failure does to the task that failed.
Task groups and mapped tasks are
[Native API](../_generated/05-design/07-python-api-structure.md#native-api).
