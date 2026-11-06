# Type checkers

schedr types its API so that mypy (`--strict`), pyright and basedpyright check a DAG body as
you write it: a wrong argument is reported in your editor, before the file is loaded. They
check less than schedr does when it loads the file (below), so a clean editor is no proof
that the file loads.

```{literalinclude} ../../examples/docs/type_checkers.py
:language: python
:caption: examples/docs/type_checkers.py
```

## How a DAG body reads

Inside `with Dag(...)`, calling a task records it and gives a **reference** to its output.
Type checkers read that call as the **value** it stands for: `find(...)` above is a
`Found | Empty`, `load(rows)` an `int`. So the value passes to a task taking that type, and
its fields are typed (`rows.rows` is a `list[str]`). Passing the wrong type is reported, as
it would be for an ordinary call.

| What you write | How type checkers read it |
|---|---|
| A call of a task returning `T` | `T` |
| A call of a task returning `None` | `Done`: nothing to pass, `>>` orders it |
| A wait step returning `T` | `Waited[T]`: `.on(T)` and `.on(TimedOut)` narrow it; `.on(T, TimedOut)` is typed as `T \| TimedOut` |
| A call of a `@branch` task | `Chosen`: `>>` orders it (`chosen >> [a, b]`) |
| `.on(Found)` on an outcome union | `Found`; `.on(A, B)` is `A \| B`, `.on(None)` is `Done` |
| A call of a task returning an `Outcome` enum | The enum; `.outcome` is the enum, `.on(member)` is `Done` |
| `CommandOperator(..., outcomes=Disk)` | `CommandOperator[Disk]`: `.on` takes `Disk`'s members, `.outcome` is a `Disk`, `.on(...)`, `.output` are a `ProcessResult`, `.stdout` a `str` |
| `EmptyOperator(...)` | Itself; `.on(...)` and `.output` are `Done` |
| `dag_ctx.run.planned_for`, `.since` | `datetime` |
| `dag_ctx.run.id`, `.kind`, `dag_ctx.dag_id` | `str` |
| `dag_ctx.params.region` | `Any`. The path is checked when the file loads ([Run parameters](12-params.md)). |
| `dag_ctx.params(ReportParams)` | `ReportParams`, the class passed |

`>>` works where one side is a task call returning `None`, a branch call, an operator, an
outcome class or enum, or a command's output. Between two values of builtin types
(`rows >> counts`, a `list` and an `int`), it type-checks on neither side: write
`chain(rows, counts)`.

For wait-step values without `>>`, such as `str`, use `chain` to order after a narrowed
value. See [Wait steps](18-sensors.md) for routing after a value or timeout.

For an async task in a test, use `await task.fn(...)`: it is typed as a coroutine and
retains the task's declarations. `await task()` runs outside a DAG but type checkers flag it,
because the call reads as the task's value in a DAG. For `-> None`, awaiting `.fn` gives
`None`. Without an async test plugin, use `asyncio.run(task.fn(...))`.

## Outcome classes

An outcome class (a member of a union a task returns) derives from **`schedr.Data`**,
which makes it a dataclass and gives it `.on` and `>>` for type checkers:

```python
class Found(Data, frozen=True):    # instead of @dataclass(frozen=True)
    rows: list[str]
```

The dataclass options are class keywords: `frozen`, `eq`, `order`, `kw_only`,
`unsafe_hash`, `repr` (not `slots`). Ordinary data, a class a task returns on its own or
inside a list, stays a plain `@dataclass`. A Pydantic outcome model derives from
`schedr.pydantic.Model` instead of `BaseModel` (see
[Data flow](03-data-flow.md#pydantic-models)).

A plain `@dataclass` still works as an outcome class: the DAG runs as written. Type
checkers then reject `.on(Plain)`, because the class doesn't declare it. Write
`on(output, Plain)` instead, which runs the same and is typed `Plain`:

```python
from schedr import on

plain_load(on(plain_fetch(), Plain))
```

Loading such a file raises a `schedr.TypingHint` warning naming the class. `schedr dags
check` prints it as a `warn` line, and pytest lists it in its warnings summary. The check
still passes.

## What only loading the file checks

Type checkers check the types of what you pass. Loading the file checks the DAG itself,
and finds what they can't:

- **Using an output as a value in the DAG body.** Type checkers read `rows` as a list, so
  they accept `if rows:`, `len(rows)`, `for r in rows:` or `rows[0].name.upper()`. When the
  file is loaded, `rows` is a reference that exists only at run time, and each of these
  fails: `if`, `==`, `!=` and `str()` with "an output can't be used as a value in the
  DAG file" (the same for `.outcome`), and `for` with that message for an output.
  `dag_ctx` fields fail `if`, `==`, `!=` and `str()` with "a reference can't be used as a
  value in the DAG file", and `dag_ctx.params` fails `for` with it too. The others fail
  with Python's own `TypeError`. In a DAG body, pass outputs, take their fields
  (`rows[0].name`), and order tasks; decide in a task.
- **A t-string inside a list or dict** is a `str` in its place: `[t"{n}"]` for a `list[int]`
  parameter is refused when the file loads, as a t-string passed whole is.
- **An output or task of another DAG.** Type checkers accept `consume(first_out)` in a
  second DAG; when the file loads it is refused ("task `x` belongs to DAG `a`, not `b`"),
  even if this DAG has a task of the same ID. Call the task again in this DAG.
- **`None` from a skipped edge.** Under `NONE_FAILED`, an argument is `None` when its edge
  isn't taken and the task runs through another edge. That is an `.on(...)` that leaves an
  outcome out, or an upstream task that may be skipped. An `.on(...)` that names every
  outcome of the task is an edge always taken, so it gives no `None`. The parameter must
  allow `None`, or the task takes `run_if=RunIf.ALL_SUCCEEDED` (as `report` above). Type
  checkers don't know which edges may be skipped.
- **Outcomes and graphs:** `.on(...)` with a class that isn't one of the task's outcomes,
  a branch returning a task that isn't right after it, cycles, duplicate task IDs.

## Settings

mypy needs nothing beyond `--strict`. For basedpyright (and pyright), DAG code makes
statements of expressions on purpose: `a >> b` records a dependency, and
`CommandOperator(...)` alone on a line records a task. Turn off the two rules that report
them, in `pyproject.toml`:

```toml
[tool.basedpyright]
reportUnusedExpression = false
reportUnusedCallResult = false
```

schedr's own `make lint` checks the example DAGs this way, with mypy `--strict` and
basedpyright, so these pages stay type-clean.
