# Events posted by outside systems

An outside system can post a named event to start a DAG or wake a step already in a run.
In this example a payment starts a run, which waits for the same order to be packed:

```{literalinclude} ../../examples/docs/named_events.py
:language: python
:caption: examples/docs/named_events.py
```

## Posting

Post as a signed-in user or with a token, as for triggering a run. For example:

```sh
curl -X POST "$SCHEDR_URL/api/v1/events" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: payment-request-123' \
  -d '{"name":"paid","key":"order-123","data":{"amount":42},"at":"2026-11-08T10:00:00Z"}'
schedr events post packed --key order-123 --data '{"box":"A"}'
```

The name is written as a DAG ID. The key is 1 to 1024 UTF-8 bytes, with no control
characters, line separators or bidirectional marks. Data is JSON, `null` if omitted, and is
limited by `[events] max_size` (1 MiB by default). `at` is when the event happened; it
defaults to the posting time, and may be at most a minute ahead of it. An event for a moment
before the previous run's gets an empty window (`since` is its own `at`). The CLI's
`--at` supplies it and `--data` also accepts `@file.json`.

Each post creates an event, even if its name and key have been posted before. To retry a
request without creating another event, send the same `Idempotency-Key`: the API returns the
first stored event. Reusing that request key for a different event is rejected. This request
key is separate from the event's key.

## Starting a run

`Dag("ship_paid", trigger=EventNamed("paid"))` starts one run per event key posted while
the DAG is live. A key already seen by that DAG starts no second run. The run is planned for
the event's `at`, and its tasks can read `dag_ctx.event.key`, `.data` and `.at`. Events posted
while the DAG is paused or absent are not caught up; a held DAG waits until the hold clears.
Queued event runs count against `[triggers] max_queued_events`.

## Waiting in a run

`wait_for(EventNamed("packed", key=...))` is a step the master ends without a worker or
slot. It takes the first matching event posted since the run was created, including one
posted while the run was queued or before the step began waiting. Every run waiting for a
matching event can receive it. Without `key=`, any key matches.

The key may be a literal string, a run parameter, `dag_ctx.run.id`, `dag_ctx.dag_id`,
`dag_ctx.event.key`, or an upstream task's output. A reference must resolve to a valid event
key; a missing or invalid value fails the step. The event is an `Event[Json]`, with `key`,
`data` and `at`, read through `.on(Event[Json])` as above. If no matching event is posted
within `max_wait`, the step ends with `TimedOut`; without `max_wait`, the master's
`[sensors] default_max_wait` applies.

## Reading an event in a sensor

A sensor can inspect a matching event before deciding whether to finish. Define it inside
`with Dag(...)` when its key depends on the run: `dag_ctx` and the run's parameters are
references there, as are upstream outputs.

```{literalinclude} ../../examples/docs/named_event_sensor.py
:language: python
:caption: examples/docs/named_event_sensor.py
```

The master gives the first matching event posted since the run was created and no later than
its `max_wait` to the function's first parameter. `NOT_YET` means “not this event”: it waits
for a later match until `max_wait`. A retry gets the same event. Without `key=`, any key
matches. As with the node, every waiting run can receive the same event.

## Deciding whether an event starts a run

A trigger can inspect each posted event before starting a run:

```{literalinclude} ../../examples/docs/named_event_trigger.py
:language: python
:caption: examples/docs/named_event_trigger.py
```

`@trigger(wait_for=EventNamed("paid"))` takes no `key=`: it is called for each event of
that name posted while the DAG is live, with the `Event` as its first parameter. Each event
is passed once. The returned `Event` starts a run, with the input event's key and `at` unless
the return gives its own; `None` starts none. A repeated key still starts no second run.
A failed call is retried with the same event, subject to `run_retries=` and the trigger's
[other call rules](21-wait-for.md#deciding-per-run-triggerwait_for).
