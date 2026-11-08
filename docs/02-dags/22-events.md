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

The `@sensor` and `@trigger` forms for named events come in part 2.
