# Notifications

A DAG can tell channels about runs, tasks and triggers. A string names a server channel;
the operator configures it: a webhook's URL and body, or an email channel's recipients. A list
uses the server's default events (`RUN_FAILED` unless changed):

```python
with Dag("orders", notify=["ops-slack"]):
    ...
```

A dict chooses events for each channel. This example also defines webhooks in the DAG:

```{literalinclude} ../../examples/docs/notifications.py
:language: python
:caption: examples/docs/notifications.py
```

| `Notify` event | When it fires |
|---|---|
| `RUN_FAILED` | A run ends failed, including a cancellation with a failure. |
| `RUN_CANCELLED` | A run is cancelled without a failure. |
| `RUN_SUCCEEDED` | A run ends successfully. |
| `RUN_RECOVERED` | A run succeeds after the DAG's previous run failed. |
| `TASK_FAILED` | A task fails after its last retry. |
| `TASK_RETRY` | An attempt fails and will be retried. |
| `TRIGGER_FAILING` | A trigger has `[triggers] failing_after` errors in a row, or its generator returns. |
| `LATE_CLEANUP_FAILED` | Cleanup after the run ended fails. |

A `WebhookChannel` posts JSON to its URL, which must be within the operator's
`[notifications] dag_urls`. Keep secrets out of the URL: it is in the DAG file and its
JSON. Slack and Teams webhook URLs are secrets; ask the operator for a server channel.
A string names a server webhook or email channel. For example,
`notify={"oncall": [Notify.RUN_FAILED]}` sends that event to the operator's `oncall` channel.
There is no `EmailChannel` class: the operator chooses the recipients.

An email is plain text, with the summary as its subject and details in its body. For example:

```text
Subject: DAG `orders`: run run-123 failed (failed: prepare)

DAG `orders`: run run-123 failed (failed: prepare)

Event:        RUN_FAILED
DAG:          orders
Run:          run-123
Planned for:  2026-11-09T02:00:00Z (2026-11-09 03:00 Europe/Warsaw)
At:           2026-11-09T03:00:12Z (2026-11-09 04:00 Europe/Warsaw)
Link:         https://schedr.example.com/runs/run-123

Failed tasks:
  prepare: connection failed
```

## Bodies

A DAG webhook's `body` can contain JSON values, placeholders and Python t-strings. Import
`from schedr import notify` and write `notify.dag`: importing `notify.task` or `notify.text`
directly would shadow `schedr.task` or `schedr.text`. A bare placeholder keeps its JSON type;
in a t-string it becomes text. Plain values in a t-string are baked in when the DAG loads.
Paths such as `notify.data['rows']` and `notify.data['items'][0]` select fields and indexes.
A task send whose data lacks a path used by the body is refused, naming the field. Event
notifications have null data; a path into it becomes an empty string in text.

Bodies refuse non-JSON values, non-string object keys, f-strings over placeholders, calls on
placeholders, and conversions or format specs on them at load. Task outputs, secrets and
`dag_ctx` references cannot be interpolated: the master fills the body without running Python
or seeing task secrets.

| Placeholder | Value |
|---|---|
| `notify.event` | Event name; `MESSAGE` for a task send. |
| `notify.dag` | DAG ID. |
| `notify.run` | Run ID, or null (for a trigger). |
| `notify.planned_for` | Run's planned time in UTC (RFC 3339), or null. |
| `notify.state` | Run or task state, or null. |
| `notify.text` | One-line summary, or the task's text. |
| `notify.link` | UI link to the run or DAG, or null without `ui_url`. |
| `notify.failed_tasks` | List of objects with `task` and `error`. |
| `notify.task` | Task ID for task events and sends, or null. |
| `notify.attempt` | Attempt number for task events and sends, or null. |
| `notify.time` | Event time in UTC (RFC 3339). |
| `notify.zone` | DAG's time zone. |
| `notify.data` | Task send's JSON data, or null for events. |

Without a body, schedr posts an object with all these fields by name, except `data` is
omitted when null. Its top-level `text` works with Slack and Mattermost incoming webhooks.
A Microsoft Teams workflow webhook accepting a simple text body can use
`body={"text": notify.text}`. Configure its secret URL on the server. Check the workflow's
own schema if it expects a different body.

The example's email service bodies show a plain send and a stored-template send with a
template ID and variables. Adapt their field names and URLs to your service; these are
webhooks, not built-in email channels. Choose this recipe when an email service accepts HTTP
requests or uses stored templates instead of SMTP. The operator can instead configure these
bodies on server channels, including authentication headers from environment variables.

## Sending from a task

A task can send its own message. The name is looked up among the DAG's channels first,
then server channels. The event is `MESSAGE`, with the task's DAG, run and task filled in.

```python
from schedr import api

relay.send("Loaded rows", data={"rows": 42})
api.notify("ops-slack", text="Loaded rows", data={"rows": 42})
api.notify("oncall", text="Loaded rows", data={"rows": 42})
```

A command task can send JSON directly or read it from a file:

```sh
schedr task-ctx notify ops-slack --text 'Loaded rows' --data '{"rows":42}'
schedr task-ctx notify ops-slack --text 'Loaded rows' --data @file.json
schedr task-ctx notify oncall --text 'Loaded rows' --data '{"rows":42}'
```

For an email channel, the task's `text` becomes the subject: its first line is trimmed and
shortened after 200 characters. The full text and `data` appear in the body.

A return means queued, not delivered. Each attempt may send at most
`[notifications] max_per_attempt` messages (100 by default); beyond it Python raises and the
CLI exits non-zero. See [Notifications for operators](../03-guide/16-notifications.md) for
delivery and retries.

For a unit test, `task_context` records sends without checking the channel against a master:

```python
from schedr import api
from schedr.testing import task_context

with task_context() as values:
    api.notify("ops-slack", text="Loaded rows", data={"rows": 42})
assert values.notifications == [
    {"channel": "ops-slack", "text": "Loaded rows", "data": {"rows": 42}}
]
```
