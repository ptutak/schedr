# Event triggers

An event trigger watches for things outside a run and starts a DAG run for each new event it sends.
Unlike a [wait step](18-sensors.md), it belongs to the DAG, not to a run. Here file names
carry their time, so sorting them lets the trigger resume after its last file. An outside
system can also [post named events](22-events.md) to start a DAG directly:

```{literalinclude} ../../examples/docs/event_triggers.py
:language: python
:caption: examples/docs/event_triggers.py
:end-before: ANCHOR: batch
```

`@trigger` makes an async generator that yields events. Its first argument is
`after: str | None`, supplied by schedr; the other arguments must be literals because no
run exists yet. Pass the call to `Dag(trigger=...)`. Declare `secrets=` and `connections=`
on the decorator as on a wait step.

For polling, use `@trigger.poke(every=...)` on an async function returning
`list[Event[T]]`, oldest first. Return an empty list when there is nothing new. Calls never
overlap and each must finish within `every`:

```python
from datetime import timedelta
from schedr import Event, trigger

@trigger.poke(every=timedelta(minutes=1))
async def new_numbers(after: str | None) -> list[Event[int]]:
    number = int(after) + 1 if after is not None else 0
    return [Event(str(number), number)]
```

## Wait steps and triggers side by side

In the plain forms you write the loop yourself; in the `.poke` forms schedr owns it; with
`wait_for=` the master waits and calls your function for a run of another DAG or a
[named event](22-events.md).

| Form | Your function is | It returns | Called again |
|---|---|---|---|
| `@sensor` | `async def`, its own waiting loop | Its value, once | Never: it is the whole wait, limited to `max_wait` |
| `@sensor.poke(every=...)` | `async def`, one check | Its value, or `NOT_YET` | After `NOT_YET`, `every` after the last call started |
| [`@sensor(wait_for=...)`](21-wait-for.md#reading-the-run-sensorwait_for) | `async def`, one look at a run or event | Its value, or `NOT_YET` (not this item) | With a later match, until `max_wait`; each call limited by `timeout=` if given |
| `@trigger` | An async generator, its own loop | Never returns: it yields an `Event` each time it finds one | Never: it runs while its DAG is live; returning or raising is an error on its status |
| `@trigger.poke(every=...)` | `async def`, one check | `list[Event[T]]`, oldest first; `[]` for nothing | Every `every`, counted from when each call started |
| [`@trigger(wait_for=...)`](21-wait-for.md#deciding-per-run-triggerwait_for) | `async def`, one look at a run or event | An `Event`, or `None` for no run | For each run that ends or named event posted while live; limited to `timeout=` |

A wait step ends with one value, or `TimedOut`, and its run goes on. A trigger never ends:
each new event it sends starts a run; a batched poke sends one event for a group of items.
`NOT_YET` is a wait step's only: a plain `@sensor` waits by
awaiting, and a trigger with nothing to report yields nothing, returns `[]` or `None`. The
generator and the poke take `after: str | None` first, the last key acted on.
Every form's function must reach an `await` within 30 seconds: one that blocks the event loop
stops every wait step and trigger on its worker's [watcher host](../03-guide/05-workers.md#the-watcher-host),
which is restarted without it.

## Batching pokes

Add `batch=timedelta(...)` to `@trigger.poke` to gather items from successive pokes into
one event. The window starts when the first item is returned and lasts `batch`; it is sent
when it ends, even if a poke is still running. An empty window starts no run. `batch` must be
positive and in whole milliseconds.

```{literalinclude} ../../examples/docs/event_triggers.py
:language: python
:start-after: ANCHOR: batch
:end-before: ANCHOR_END: batch
```

Use it in a DAG, passing the event data to the task:

```python
with Dag("batched_files", trigger=batch_files(folder="/tmp/schedr-incoming")):
    load_batch(dag_ctx.event.data)
```

Each item is kept as an event of its own, so keys are deduplicated item by item: the run
gets the window's new items, as a `list[Event[T]]`, and a window of only keys seen before
starts none. A run reads that list as `dag_ctx.event.data`; a task receiving it takes
`list[Event[T]]`. The run's event is keyed by its last new item and has no `at` of its own;
the bookmark is the window's last key. Running an item again by its key (`schedr dags
trigger --event KEY`, or "Run again") gives a list of that item alone.

A window goes early when adding an item would put its data over `max_output_size`; that
item starts the next window. One item too big on its own fails as it would unbatched. On
restart, the trigger finds unsent items again from the bookmark. A poke that consumes what it reads cannot find them
again, so a restart loses that window's items.

## Events and the bookmark

`Event(key, data, at)` carries a key, typed data (default `None`), and an optional
time-zone-aware `at` for when it happened. The key identifies an event within its DAG: a
key already seen starts no new run. It must be 1 to 1024 UTF-8 bytes with no control
characters. Keep secrets out of keys: a key holding a secret the trigger read is stored and
shown masked, so two keys differing only in secrets count as one key (the trigger's log says
so, once). Use a stable, unique identifier for the same external event across retries and
restarts; do not generate a fresh random key on each observation. Data is limited by the
trigger's `max_output_size` (1 MiB by default); secrets are masked as in task output.

The bookmark is the last key schedr acted on. On each start it passes that key as `after`,
or `None` initially. Resume after it when the source supports that. This requires an order
in which later events follow the bookmark, as in the example; if the source cannot provide
one, inspect all events and let schedr deduplicate their keys. Delivery is **at least once**:
an event may be sent again after a reconnect. Make downstream effects safe to repeat too,
including when someone runs an event again by hand.

## The run

A new event creates a run of kind `event`. Tasks read `dag_ctx.event.key`, `.data` and
`.at`; the example passes the key and data to `load`. `planned_for` is the event's `at`,
or the time schedr received it if absent; `since` is the previous event's `planned_for`.
Event runs queue like manual runs when the DAG is full. The trigger itself is not in the
run graph and has no attempt or row in the run.

## Running the trigger

A trigger runs while its DAG is unpaused, not held and within its activity window. Pausing
or holding the DAG stops it; it starts from its bookmark when eligible again. A new DAG
version or code, a worker drain or loss also stops and restarts it from the bookmark.
Failures restart after 5 seconds, doubling up to 5 minutes. A generator that returns stops
with `trigger_returned` until its DAG version or code changes. A poke that raises or exceeds
`every` keeps its bookmark and gets its next call; a successful call clears its consecutive
failure count.

Use `new_files(folder="/tmp/schedr-incoming").override(...)` to set:

- `queue=`: the worker queue for the trigger.
- `restart_if_idle=`: a positive `timedelta` without events after which a generator restarts
  from its bookmark; off by default and applies to generators only.
- `start=` and `end=`: its activity window; naive times are read as UTC.

Triggers use watcher slots and share a watcher host's event loop and memory. The same
[wait-step rules](18-sensors.md#running-and-deploying-wait-steps) for blocking calls, secrets,
connections and stopping apply. An invalid or oversized event can fail the trigger with
`event_refused`; it restarts from the bookmark. See [Workers](../03-guide/05-workers.md#event-triggers)
for queue limits and switched-off triggers.

## Testing

`run_trigger` exercises the trigger in this process, without creating runs. For example,
`run_trigger(new_files(folder="/tmp/schedr-incoming"), after=None, take=3)` returns up to
three events; it waits up to 30 seconds by default, or the `timeout=` timedelta. It returns
fewer only if a generator returns, and raises on timeout or a failed poke. Import it from
`schedr.testing`; use `task_context` around it to supply secrets and connections. For a
batched poke it returns one event per poke that found items; it does not wait for a window.

For a task that reads the event directly, use `with task_context(event=Event("file-1", data)):`
from `schedr.testing`, then call the task function. To test a whole DAG, pass an `Event` as
`run_dag(path, dag_id, event=Event("file-1", data))`. The command-line equivalent supplies
JSON data and an optional event time:

```sh
schedr dags test event_triggers --path examples/docs/event_triggers.py --event-key file-1 --event-data '{"path":"/tmp/file-1","size":42}' --event-at 2026-11-07T10:00:00Z
```

`--event-data` also accepts `@file.json`; without `--event-at`, the run's moment is used.
See [Testing a DAG](07-testing.md).

## Manual runs and operations

A manual run of an event-triggered DAG needs an event. Reuse a stored event by key, or give
one explicitly:

```sh
schedr runs trigger event_triggers --event file-1
schedr runs trigger event_triggers --event-key file-2 --event-data '{"path":"/tmp/file-2","size":42}' --event-at 2026-11-07T11:00:00Z
```

The latter accepts JSON or `@file.json` for data; without `--event-at` it uses the run's
moment. Either form creates a `manual` run and writes no event, so an actual event with a
new key can still start its own run.

`schedr triggers list` lists triggers, `schedr triggers show <dag>` shows one, and
`schedr triggers events <dag>` lists its events. `schedr triggers restart <dag>` starts it
again from its bookmark; `schedr triggers switch-on <dag>` clears its strikes and switches
it back on. `schedr triggers skip <dag>` passes the run an
[`@trigger(wait_for=...)`](21-wait-for.md#deciding-per-run-triggerwait_for) keeps failing on. The [DAG page](../03-guide/12-ui.md#a-dag) also shows trigger status and events.
