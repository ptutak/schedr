# Mapping

A mapped task runs **once per element** of a list: one instance per file, per region, per
customer. The list can be written in the DAG file or come from an upstream task's output, so
the number of instances is known only when the run gets there.

```{literalinclude} ../../examples/docs/mapping.py
:language: python
:caption: examples/docs/mapping.py
```

## Mapping a task

`.partial(...)` fixes the arguments every instance shares; `.expand(...)` names the ones
that vary, each a list. `fetch.partial(bucket="raw").expand(path=list_files())` runs one
`fetch` per file, each with `bucket="raw"`.

- **Several mapped lists** give every combination: `.expand(region=["eu", "us"],
  day=[1, 2])` makes four instances, the first argument written varying slowest (`eu`/1,
  `eu`/2, `us`/1, `us`/2).
- **`.expand_kwargs(...)`** maps over a list of dicts, each one instance's arguments.
  A list written in the file is checked when the file loads: every element is a dict
  (a reference element is left to its type), each key is a parameter, a key also passed to
  `.partial()` is refused, the fixed arguments and the dict together supply every required
  parameter, and each value matches that parameter's type. The list itself must be a
  `list[dict[str, Any]]`, written out or taken from an upstream task. A reference is not
  checked element by element at load. When the task waits for that output as a whole, an
  element that is not a dict fails the whole task when it expands, before any instance
  runs. When the list is another mapped task's instances one by one (a
  [lane](#lanes-mapping-over-a-mapped-output)), it fails that one instance when the
  instance is assigned, as `input_mismatch`, and that failure is not retried.
- **What `.expand()` maps over is checked when the file loads:** each list against the
  parameter's type (`path: str` needs a `list[str]`), names that exist, nothing both fixed
  and mapped.
- **The instances** are numbered from 0 in that order (their `map_index`). They run in
  parallel as worker slots allow, and each retries on its own.
- **An empty list** runs no instance: the task succeeds, and its output is `[]`.
- **At most `[mapping] max_instances`** instances (1024 by default, a server setting); past
  it, the task fails without running any and says why.

A mapped task's output is the **list of its instances' outputs**, in their order. For type
checkers `.expand(...)` returns `schedr.Mapped[R]`, a `list[R]` that also has `.on(...)` for
mapping over it element by element: index it (`results[0]`) or pass it whole to a task
taking a list.

## Lanes: mapping over a mapped output

Mapping over another mapped task's whole output pairs the instances element by element:
`count.expand(rows=results.on(Rows))` gives `count[i]` the output of `fetch[i]`. Each pair is a
**lane**, and lanes move on their own: `count[0]` starts as soon as `fetch[0]` is done,
without waiting for the slowest file.

- **`.on(...)` works per element** inside `.expand()`: an instance whose upstream instance
  ended with another outcome is **skipped**, and its place in the list is `None`.
- **To wait for all of them first,** add an order edge from the whole task:
  `results >> counts` (the two mapped outputs) next to the expansion. Then nothing of `count`
  starts before every `fetch` has finished, and nothing of it runs if one failed.
- Using the upstream task anywhere else in the same task (`.partial(all=results)`,
  `results[0]`) also makes it wait for the whole task.

## Gathering

A task that takes the whole list (`total(counts)`) is a **gather**: it waits for every
instance, then receives their outputs in order.

| The instances | The mapped task | The gather |
|---|---|---|
| all succeeded | success | runs, with every output |
| some skipped | success | runs, with `None` in their places: its parameter must allow it (`Sequence[int \| None]`, checked when the file loads) |
| all skipped | skipped | skipped, as after any skipped task |
| none (an empty list) | success | runs, with `[]` |
| one **failed** after its retries | failed at once: the other instances are cancelled, running ones stopped | `upstream_failed`: doesn't run |

A gather that may receive `None` takes a `Sequence[int | None]` rather than a
`list[int | None]`: type checkers read the mapped output as a `list[int]` (they can't know
which instances will be skipped), and a `list` parameter accepts only its exact element
type, a `Sequence` any narrower one.

**All or nothing:** with `run_if=RunIf.ALL_SUCCEEDED` on the mapped task, one skipped instance
skips the whole task, and so its gather: the gather gets every result or doesn't run. A
mapped task applies its `run_if` to its instances the way any task applies it to its
upstream edges.

## Gathering everything

A gather sees every instance only if none of them is skipped, and none of them fails:

- **Don't narrow, decide in the task.** `.on(Rows)` skips the instances that ended
  otherwise. Mapping over the whole union (`count_any.expand(result=results)`, taking
  `Rows | Missing`) runs every instance, and the task decides what each one means.
  `report(results)` takes the whole union of every instance the same way.
- **Return expected problems as outcomes, don't raise them.** `fetch` returns `Missing` for
  a file that isn't there: the instance succeeds, and the gather receives the `Missing`. An
  uncaught exception is a real failure: it fails the step, cancels the other instances, and
  the gather doesn't run. That is on purpose: a failure means the step failed.

## Following the instances

- **Inside a task,** `task_ctx.map_index` is the instance's index (`None` for a task that
  isn't mapped), and `task_ctx.try_number` its attempt
  ([The DAG context](06-context.md#the-running-tasks-own-context-task_ctx)). A command gets
  `SCHEDR_MAP_INDEX`.
- **In the UI,** a mapped task is one node, labelled with its instance count (`fetch ×3`)
  and how many are done and how many failed. Its panel opens on the Instances tab; each
  instance's own panel has that instance's attempts, log and output. The whole task's
  output is the gathered list.
  [The web UI](../03-guide/12-ui.md#a-mapped-task) is those panels.
- **In the API,** a run's tasks list each mapped task as a whole (`map_index: -1`,
  `instances: 3`) and then its instances. `?map_index=-1` means the same as leaving the
  index off: the task as a whole. A task's `output` without `?map_index` (or with `-1`) is
  the gathered list, with `?map_index=1` that instance's; `attempts`, `log` and `retry` need
  `?map_index` for a mapped task; `cancel` without it, or with `-1`, cancels the whole task.
  An index outside `0` to `N-1` is a 400, on every one of those endpoints, cancel included.
- **In the CLI,** `schedr tasks logs / cancel / retry` take `--map-index N`; `schedr dags
  test` lists the instances as `fetch[0]`, `fetch[1]`, ... after the task.
- **In tests,** `run_dag(...).tasks["fetch"]` is the whole task (its `output` the list) and
  `tasks["fetch"][1]` an instance ([Testing a DAG](07-testing.md#run_dag)).

`.partial()` and `.expand()` work on `@task` functions; operators can't be mapped yet.
