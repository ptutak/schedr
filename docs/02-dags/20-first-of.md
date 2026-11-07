# Acting on the first of several

A task starts after all its upstream tasks end. To go on as soon as one of several has
succeeded, use `first_of`, a node of its own that the master ends, like `EmptyOperator`: no
process, no slot and no polling.

```{literalinclude} ../../examples/docs/first_of.py
:language: python
:caption: examples/docs/first_of.py
```

## `first_of`

`publish(first_of(a, b))` passes on the output of the task that succeeded first.
`value, winner = first_of(a, b, with_winner=True)` also gives the winner's task ID. Type checkers read
the first as the tasks' value type and the second as `(value, str)`. To order a task after the node
without its value, use `chain(first_of(a, b), x)`.

Narrow a task with `.on(...)` to say which outcomes count: with `first_of(a.on(str), b.on(str))`, a
wait step that timed out doesn't win. A skipped task doesn't win either, so `first_of` also joins the
paths of a `@branch`, and the task after it gets the one value there is.

| The tasks given | The node |
|---|---|
| one has succeeded, on an outcome that counts | `success` at once, with its value |
| none has yet, and some still can | waits |
| none can, and one failed | `upstream_failed` |
| none can, and one was cancelled | `cancelled` |
| none can, and all were skipped or ended on another outcome | `skipped` |

Nothing is cancelled. The others run on as any task. One that fails later fails the run when the run
ends, but the node and what follows it stay as they ended.

## `at_least`

`accepted(at_least(2, a, b, c))` waits for two of them and passes their values on as a list, in the
order the tasks ended; `with_winner=True` also gives the list of their IDs. The rest is as for
`first_of`, with "can" meaning enough of them still can.

## Limits

- At least two different tasks, each given whole: `a.rows` is a part of an output and is refused. Another
  node's value counts as whole; its winner doesn't.
- No mapped task: it has no one value.
- What it keeps is at most what its options may give together, the sum of their `max_output_size`, so values
  each allowed always fit. Its own `max_output_size` doesn't apply.
- Nothing can be ordered before the node: `x >> first`, or `x >> g` for a group `g` that starts with it, is
  refused when the file loads. Order the tasks it was given instead.
- `at_least` reads a list as it is stored: two classes with the same fields, as outcomes of a task, can't be
  told apart in its list. `first_of` tells them by its winner's outcome.
- The node's ID is `first_of` or `at_least`, numbered from the second on; `task_id=` names it.

## In the graph

On a run's page the tasks a node chooses from sit in one box, with one dashed line to the node,
since they are treated as a group and aren't each a dependency. The winner is outlined and named on
the node. A task given to two nodes, or tasks at different depths or in different groups, can't share
a box, so each gets its own dashed line.
