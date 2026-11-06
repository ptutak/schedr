# Wait steps

A wait step waits for one thing belonging to a run, then ends with its value or the
`TimedOut` outcome. Declare an `async def` with `@sensor` when you write the waiting
yourself, or with `@sensor.poke(every=...)` when schedr should call it repeatedly.

```{literalinclude} ../../examples/docs/sensors.py
:language: python
:caption: examples/docs/sensors.py
```

A poke returns its value when ready, or `NOT_YET` while waiting: annotate it
`-> str | NotYet`. `None` is a value, not a signal to wait. Each call is limited to
`every`, and calls never overlap. A call that raises or runs out of that time is a
failure with the usual retries.

`max_wait=` limits the wait. If omitted, `[sensors] default_max_wait` applies (24 hours by
default); `max_wait` in `StepDefaults` sets it for a DAG or group, while `timeout` there is for tasks. When the wait expires, schedr
produces `TimedOut`: do not put it in the function's return type.

## What downstream tasks see

Suppose `file` is a call of a wait step returning `str`, and `load` takes `path: str`.
The call reads as `Waited[str]`: it can produce a value or `TimedOut`.

| Expression | What happens |
|---|---|
| `load(file.on(str))` | Runs with the value; the edge is not taken on timeout. |
| `file.on(TimedOut) >> alert()` | Runs `alert` on timeout. |
| `handle(file)` with a parameter `str \| TimedOut` | Receives either; write `handle(file.on(str, TimedOut))` for type checkers. |
| `file >> x` | Orders `x` only when the wait has its value. |
| `chain(file.on(str, TimedOut), x)` | Orders `x` after either outcome. For a value type without `>>`, such as `str`, type checkers need `chain`. |
| `load(file)` with `path: str` | Refused at parse and by type checkers: the timeout is also possible. |

A failed wait step is not a timeout: downstream tasks end `upstream_failed`. When a
downstream task has another upstream task, `run_if` decides what happens when an edge is
not taken, as described in [Routing by outcome](08-outcomes.md).

## Limits for now

`cleanup=` is refused; use `finally` in the function. `.expand()` on a wait step is
refused. `preload` and `fresh_process` are not options.

For now a wait step runs as a task's process does, on a task slot, with its clock per
attempt. M4.11a2–a4 will move it to a shared host with watcher slots, without changing
the DAG. A function that ignores cancellation at `max_wait` is stopped past the grace period
and fails `timed_out`.

## Testing

`await wait.fn(...)`, or `asyncio.run(wait.fn(...))`, calls the function itself. For a
poke this is one call, which may return `NOT_YET`. `run_dag` runs the wait step for real;
see [Testing a DAG](07-testing.md).
