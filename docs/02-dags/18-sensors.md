# Wait steps

A wait step waits for one thing belonging to a run, then ends with its value or the
`TimedOut` outcome. Declare an `async def` with `@sensor` when you write the waiting
yourself, or with `@sensor.poke(every=...)` when schedr should call it repeatedly. To wait
for another DAG's run or task, use [`wait_for(RunOf(...))`](21-wait-for.md), which can
also produce `Failed`.

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
not taken, as described in [Routing by outcome](08-outcomes.md). A timeout leaves the run
`success` whether or not a task takes it. Use `.on(TimedOut)` to do something about it.

A task normally waits for its upstream edges to be decided before it runs. To act on the first of several, as
`fast_report` does above, use [`first_of`](20-first-of.md).

## Running and deploying wait steps

This section describes sensors. The master ends `wait_for(RunOf(...))` without a watcher.

Wait steps using the same interpreter and code copies run as coroutines in one long-lived
process, the watcher host. It stops when its last wait step ends. A DAG file's top level is
imported once per host, so module globals are shared between its wait steps. The working
directory and environment belong to the host, not a run: use absolute paths and read run
values from `dag_ctx` and `task_ctx`, never `SCHEDR_*` environment variables.

Use asynchronous clients and yield while waiting: a blocking call holds up the other wait
steps on the host. If one holds the loop past `[watchers] block_limit` (30 seconds by
default), its host is killed and it fails `blocked_event_loop` without a retry. The other
wait steps start again on a new host in the same attempt, with their `max_wait` and `timeout`
still running and no retry used. Put heavy work in a task. Read declared secrets with
`await task_ctx.asecret(...)` or
`await task_ctx.asecret_value(...)`; the plain forms hold the loop when a secret under a
`SecretPrefix` must be fetched. See [Secrets](13-secrets.md#reading-a-secret-inside-a-task).

A wait step can't start a process or a thread of its own: nothing could stop what it left running
while other wait steps keep the host alive, and a retry could overlap it. Use an async client or
`asyncio.to_thread`, or run a command in a task after the wait step.

- **When its DAG is defined**, so `schedr dags check` says so before you deploy: a wait step's
  code using `subprocess`, `os.system`, `os.fork`, `os.spawn*`, `os.exec*`, `multiprocessing`,
  `ProcessPoolExecutor`, `asyncio.create_subprocess_*`, `threading.Thread`, `threading.Timer` or
  `ThreadPoolExecutor`, or a function, decorator or class of your own that does, is an import error
  naming the call and its line. A class named in `isinstance`, `issubclass` or an annotation is fine.
  This reads what the code names, so it can miss a value it can't follow.
- **When it runs**, for what can't be read that way (a library's insides, `getattr`, a function
  passed in): starting a process raises `ProcessInWaitStepError`, from a thread of the step too. The
  standard library's `platform` and `ctypes.util` helpers still work, as client libraries call them
  and they wait for their command. A library may start threads; nothing refuses those.

This isn't a sandbox: C code gets past both, and what it leaves running lives until the host stops.

`asyncio.to_thread` belongs to its wait step: its prints go to the step's log, `task_ctx` works in
it, and it runs out of time with the step. When the call is cancelled (`max_wait`, `every`, a cancel,
your own `asyncio.timeout`) or the step ends with the call still running, `StepEndedError` is raised
in its thread at its next Python line. It isn't an `Exception`, so `except Exception` doesn't stop
it, and `finally` runs. Python can't reach a thread blocked in C (`time.sleep(600)`, a read with no
timeout): if it hasn't ended after the cancel grace, the step's result is reported and the host is
killed; the other wait steps start again as described above. Give blocking calls a timeout of
their own, and put heavy work, such as a large query in a library with its own threads, in a task.

That error can land on any Python line, also halfway through a library updating its own state, such
as a connection pool or a cache. `with` and `finally` run, but code with no cleanup for it stays
half done, and the other wait steps of the same code share that library in their process. So a call
that has a timeout of its own, and ends by itself, is always the safer way to stop.

When a step ends, asyncio tasks it left running are cancelled and awaited. `print` and Python logging from the step go to its
attempt log, as does a thread it started; output from C code, or from a thread still running after
its step ended, goes to the host's log, masked for secrets. Nothing can stop a running thread from
outside: end the ones you start, or they live until the host stops. The step's log warns if a
library leaves a thread running.

When new code arrives, a `@sensor.poke` moves between calls if it can start on that code.
It keeps its deadline and uses no retry; its log says it moved and the attempt reports the
code it moved to. If the task was renamed or removed, the file cannot be imported, or the
new signature does not fit the run's arguments, it stays on its old code and its log says
why. A full-control `@sensor` stays on its code until it ends, keeping its host and code copy:
frequent deploys with very long waits can therefore cost memory. A retry starts on the newest
code.

A wait step takes a watcher slot (`[watchers] slots`, 500 per worker by default), not a task slot.
Its `max_wait` runs from its first start, over retries and moves. A draining worker hands its pokes
to others at once, using no retry; a full-control `@sensor` gets the drain's grace time, then runs
again elsewhere, also using no retry. Cancellation gives it the usual grace time; if it does not end, its host is killed and the other wait steps start
again as above. A host crash is handled differently; see [The watcher host](../03-guide/05-workers.md#the-watcher-host).

After `[watchers] switch_off_after` host kills in a row (default 3), a wait step is switched
off: new attempts fail `switched_off` without reaching a host. A success resets the count.
New code gives it one chance: another strike switches it off again until a success. List these
steps with `schedr sensors switched-off` and switch one on with
`schedr sensors switch-on <dag> <sensor>`. The DAG page lists its switched-off wait steps
and has a **Switch on** button; the DAGs list shows a banner with their count and links to
their DAGs.

## Limits for now

`cleanup=` is refused; use `finally` in the function. `.expand()` on a wait step is
refused. `preload` and `fresh_process` are not options.

A function that ignores cancellation at `max_wait` is stopped past the grace period and
fails `timed_out`.

## Testing

`await wait.fn(...)`, or `asyncio.run(wait.fn(...))`, calls the function itself. For a
poke this is one call, which may return `NOT_YET`. `run_dag` runs the wait step for real;
see [Testing a DAG](07-testing.md).
