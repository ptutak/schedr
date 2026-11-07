# Event triggers

An event trigger watches for things outside a run and starts a DAG run for each new event.
Unlike a [wait step](18-sensors.md), it belongs to the DAG, not to a run. Here file names
carry their time, so sorting them lets the trigger resume after its last file:

```{literalinclude} ../../examples/docs/event_triggers.py
:language: python
:caption: examples/docs/event_triggers.py
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

## Events and the bookmark

`Event(key, data, at)` carries a key, typed data (default `None`), and an optional
time-zone-aware `at` for when it happened. The key identifies an event within its DAG: a
key already seen starts no new run. It must be 1 to 1024 UTF-8 bytes with no control
characters. Use a stable, unique identifier for the same external event across retries and
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
  from its bookmark; off by default and for generators only.
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
`schedr.testing`; use `task_context` around it to supply secrets and connections.

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
it back on. The [DAG page](../03-guide/12-ui.md#a-dag) also shows trigger status and events.
