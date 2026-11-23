# The plugin protocol

This page is for writing an [extension](21-extensions.md)'s plugin in a language schedr has no
helper for. It says everything a plugin must do, in the order it happens. Python
(`schedr_extension`) and Rust (the `schedr-extension` crate) have helpers that do all of it.

The protocol's version is **1**. Every message is described by
[`schema/extension.schema.json`](https://github.com/ptutak/schedr/blob/main/schema/extension.schema.json),
a JSON Schema generated from the Rust crate's types: `PluginMessage` for what a plugin sends,
`WorkerMessage` for what the worker sends. The Python helper's tests check each message both
ways against it.

## Being asked what it is

With discovery on (`[extensions] discover = true`), the worker runs each `schedr-ext-*`
executable on its `PATH` with the single argument `--describe`, before it registers. The plugin
then:

- prints one line of JSON to standard output and exits 0:
  `{"name": "spark", "versions": ["1.0", "1.1"], "protocol": 1}`;
- does so within **5 seconds**, in at most **64 KiB** of output;
- reads nothing: its standard input is empty, and its environment holds only `PATH`.

`name` follows a task's extension name: 1 to 64 letters, digits, `_` and `-`, starting with a
letter, without `__`. Each version has 1 to 64 characters, with no whitespace or control
characters. A plugin that fails any of this is skipped with a warning. The worker kills its
process group afterwards, so nothing it started outlives the question.

A plugin configured by name (`[extensions.<name>]`) is never asked: its config says what it
serves.

## Starting

For each attempt, the worker starts the plugin's `command` in a process group of its own:

| | |
|---|---|
| Working directory | the DAG's root |
| Environment | only the task's: `SCHEDR_*` variables and the allow-listed ones; `SCHEDR_AGENT_FD` among them |
| Standard input | empty |
| Standard output, error | the attempt's log, as written, with secrets masked |

`SCHEDR_AGENT_FD` is the number of an inherited descriptor: a **Unix stream socket** connected to
the worker. Before anything else, the plugin should:

1. check that it is a Unix stream socket;
2. set **close-on-exec** on it, so a program the plugin starts doesn't get the worker's channel.

Without `SCHEDR_AGENT_FD`, the plugin wasn't started by a worker; the helpers exit with 2.

## Messages

Each message is a 4-byte **big-endian length**, then that many bytes of **UTF-8 JSON**, an
object whose `type` names the message. A message is at most **64 MiB**. A message the worker
can't read (too large, not JSON, of a type it doesn't know) closes the channel; one it reads
but doesn't expect at that point ends the attempt as one without a result (see Ending).

### The handshake

The plugin speaks first:

```json
{"type": "hello", "schedr_version": "2.4.0", "mode": "extension", "protocol": 1}
```

`schedr_version` is the plugin's own version, not checked. The worker answers one of:

```json
{"type": "welcome"}
{"type": "refused", "reason": "the plugin speaks runner protocol 2, this worker 1: ..."}
```

After `refused`, the plugin exits.

### The request

After `welcome`, the worker sends one `extension_task`:

```json
{
  "type": "extension_task",
  "dag_id": "nightly_spark",
  "task": {
    "id": "aggregate", "kind": "extension", "name": "spark", "version": "1.0",
    "payload": {"job": "aggregate", "days": 7},
    "settings": {"max_output_size": 1048576, "...": "..."},
    "...": "..."
  },
  "run": {"run_id": "...", "try_number": 1, "planned_for": "...", "...": "..."},
  "secrets": [{"name": {"path": "spark/api", "key": "token"}, "value": "..."}],
  "...": "..."
}
```

| Field | What |
|---|---|
| `task.name`, `task.version` | the extension and the exact version the task is written for |
| `task.payload` | the task's JSON payload, as the DAG gave it |
| `task.settings.max_output_size` | the most the output may take, in bytes of JSON |
| `run` | the run: `run_id`, `try_number`, `planned_for` and more |
| `secrets` | the secrets the task declared, already read by the worker |
| `cleanup` | only for a cleanup run: the handles the attempt registered, in order (maybe none) |

A request with `cleanup` set is the task's **cleanup**: undo what the attempt started, using its
handles. Without it, the request is the attempt.

### Asking the worker

While it works, the plugin may send requests. Each is answered before the next is sent: **one
at a time**, never two in flight.

| The plugin sends | The worker answers |
|---|---|
| `{"type": "secret_get", "secret": {"path": "etl/api", "key": "token"}}` (`storage` optional; no `key` for the whole secret) | `{"type": "secret", "value": ...}` or `{"type": "secret_failed", "kind": ..., "message": ...}`; `kind` is `secret_denied` for a secret the task didn't declare, `secret_not_found` or `secret_unavailable` otherwise |
| `{"type": "connection_get", "name": "warehouse"}` | `{"type": "connection", "values": {...}}` or `{"type": "connection_failed", "kind": ..., "message": ...}` |
| `{"type": "handle_add", "handle": {"handle_type": "job", "value": {"id": 7}}}` | `{"type": "handle_added"}`, or `{"type": "handle_refused", "message": ...}` for a task without `cleanup=True` |
| `{"type": "api_call", "method": "POST", "path": "/api/v1/...", "body": {...}, "idempotency_key": "..."}` (`body`, `idempotency_key` optional) | `{"type": "api_answer", "status": 200, "body": ..., "note": ...}` (`note` optional) |

A handle is kept by the worker as soon as it answers `handle_added`. Register a handle **before**
the thing it undoes can be lost: a job's ID as soon as it is submitted.

### Ending

The plugin's last message is one of:

```json
{"type": "task_succeeded", "output": {"rows": 3}, "outcome": "SUCCESS"}
{"type": "task_failed", "error": {"exception": "SparkError", "message": "...", "traceback": ""}, "kind": null}
```

Then it exits. How the worker judges the end:

| Last message | Exit | The attempt |
|---|---|---|
| `task_succeeded` | 0 | succeeded, with `output` |
| `task_succeeded` | not 0 | failed, by its exit |
| `task_failed` | any | failed, with `error` |
| none | 0 | failed: `runner_failed`, "the runner exited without a result" |
| none | not 0, or a signal | failed, by its exit |

`output` must be JSON of at most `max_output_size` bytes; the task reading it checks it against
its declared type. `kind` is `null` for the plugin's own failures.

Once the plugin's process exits, the worker reads what is left of its output for at most 5
seconds, then kills whatever is left of its process group: nothing a plugin starts outlives
its attempt.

## Being stopped

On a cancel or a timeout, the worker sends **SIGTERM** to the plugin's process group, then
**SIGKILL** once the grace is over (`[master] cancel_grace`, 30 s, as for any task; see
[Workers](05-workers.md)). A plugin that
started something elsewhere, such as a remote job, should stop it on SIGTERM. What it can't
stop in time, its cleanup run stops, using the handles.

## A service

An extension's service (see [Extensions](21-extensions.md#building-a-service-in-python)) has two
more channels, in the same framing, each a Unix stream socket the worker hands out:

```
   worker <---- (1) SCHEDR_AGENT_FD, per attempt ----> plugin
     |                                                    |
     |                                    (3) SCHEDR_SERVICE_FD, per attempt
     |                                                    |
     +------- (2) SCHEDR_SERVICE_FD, the service's -----> service
```

Channel (1) is the plugin's, as above. Channel (3)'s end reaches the plugin as
`SCHEDR_SERVICE_FD` beside `SCHEDR_AGENT_FD`, set only when its extension has a service; the
plugin makes it close-on-exec as the other.

### The control socket (2)

The service speaks first:

```json
{"type": "hello", "schedr_version": "2.4.0", "mode": "service", "protocol": 1,
 "name": "spark", "versions": ["1.0", "1.1"]}
```

The worker answers `{"type": "welcome", "secrets": [{"name": {"path": "spark/api", "key":
"token"}, "value": "..."}]}` (its `service_secrets`, read for this start), or `{"type":
"refused", "reason": ...}` (another protocol, another extension, no configured version served),
after which it exits. Then:

| From | Message | |
|---|---|---|
| service | `{"type": "ready"}` | Within `service_ready_timeout` of its start: attempts may connect from now on |
| worker | `{"type": "attempt_connected", "attempt_id": ..., "task": {"name": ..., "version": ...}}` | With one descriptor (`SCM_RIGHTS`, on the message's first byte): the service's end of the attempt's channel (3) |
| service | `{"type": "attempt_accepted", "attempt_id": ...}` | Within `service_accept_timeout`; only then does the plugin start. Unanswered, the attempt fails |
| service | `{"type": "secret_get", "secret": {"path": ..., "key": ...}}` | One of its declared secrets again; answered `secret` or `secret_failed` as a plugin's |
| service | `{"type": "status", "phase": ..., "message": ...}` | What it does, an event on the extension's page; no answer |
| service | `{"type": "metrics", "points": [...]}` | Points of the extension's declared metrics, as a plugin's, and of `service_call_seconds` (a histogram by `method`: how long it took to answer each call, failed ones too), the one built-in it may send ([its built-in metrics](21-extensions.md#its-built-in-metrics)); no answer |
| worker | `{"type": "drained"}`, `{"type": "resumed"}` | The worker drained for maintenance, and resumed: no attempts meanwhile; it keeps running. No answer |

The worker asks no liveness: a service is watched by its process, its
memory, and the attempts it serves. Its standard output and error go to the worker's log.
`SIGTERM` stops it, `SIGKILL` after the grace.

### An attempt's calls (3)

The plugin calls, the service answers, each by the call's `id` (unique on the connection);
several calls may be in flight, their answers in any order:

```json
{"type": "call", "id": 7, "method": "submit", "args": {"job": "daily"}}
{"type": "answer", "id": 7, "result": {"job_id": "j-42"}}
{"type": "call_failed", "id": 8, "message": "the cluster said no"}
```

The connection's end is the attempt's: the plugin closing it (its process ended) tells the
service the attempt is gone; the service closing it (it crashed) fails the plugin's calls in
flight. The worker reads neither side of it.

## Compatibility

Within protocol 1 (and the service's protocol 1):

- the worker may add **fields** to its messages: a plugin ignores the fields it doesn't know;
- **new message types** come only with a new protocol version, which a plugin's `hello` and
  `--describe` name; a worker refuses a plugin speaking another protocol than its own.

The Rust crate's version follows the protocol: `schedr-extension` 1.x speaks protocol 1.

## Every message

Each message is a JSON object whose `type` names it, framed by its 4-byte big-endian length. The
schemas are `schema/runner.schema.json` (the plugin's channel) and `schema/extension.schema.json`.
**In Python** is what `schedr_extension` does for you: the call that sends the message, or what
its answer becomes; a plugin or service in another language sends and reads **On the wire**
itself.

### Plugin to worker (1)

| `type` | When | Answered with | In Python | On the wire |
|---|---|---|---|---|
| `hello` | First, always | `welcome` or `refused` | `main(run, name="spark", versions=["1.0"], metrics=[...])` sends it | `{"type": "hello", "schedr_version": "2.4.0", "mode": "extension", "protocol": 1, "metrics": [{"name": "jobs", "kind": "counter", "help": "Jobs"}]}` |
| `secret_get` | A secret the task declared | `secret` or `secret_failed` | `await channel.secret("spark/api", "token")` | `{"type": "secret_get", "secret": {"path": "spark/api", "key": "token"}}` |
| `connection_get` | A connection the task declared | `connection` or `connection_failed` | `await channel.connection("warehouse")` | `{"type": "connection_get", "name": "warehouse"}` |
| `handle_add` | A cleanup handle, before what it cleans up | `handle_added` or `handle_refused` | `await channel.add_handle("job", {"id": "j-42"})` | `{"type": "handle_add", "handle": {"handle_type": "job", "value": {"id": "j-42"}}}` |
| `api_call` | A call to schedr's REST API, made by the master as the task | `api_answer` | `await channel.api("POST", "/api/v1/events", {"name": "paid"})` | `{"type": "api_call", "method": "POST", "path": "/api/v1/events", "body": {"name": "paid"}}` |
| `stop_reason_get` | After `SIGTERM`: why it is being stopped | `stop_reason` | `await channel.stop_reason()` | `{"type": "stop_reason_get"}` |
| `status` | Where the task is | none | `await channel.status("running", "step 2 of 4", progress=0.5)` | `{"type": "status", "phase": "running", "message": "step 2 of 4", "progress": 0.5}` |
| `link` | A link to what it started elsewhere (`http`, `https`); the same label replaces it | none | `await channel.link("Job", "https://spark.example/jobs/j-42")` | `{"type": "link", "label": "Job", "url": "https://spark.example/jobs/j-42"}` |
| `metrics` | Points of declared metrics; an undeclared one is dropped, warned of once; one named `service_*` (schedr's) dropped without a word | none | `await channel.metric("jobs", 1, outcome="ok")` | `{"type": "metrics", "points": [{"name": "jobs", "labels": {"outcome": "ok"}, "value": 1}]}` |
| `notify` | A declared notification event, to the DAG's channels subscribed to `EXTENSION_EVENT` | none | `await channel.notify("job_done", "the job is done", {"rows": 3})` | `{"type": "notify", "event": "job_done", "text": "the job is done", "data": {"rows": 3}}` |
| `task_succeeded` | Last: the task (or cleanup) succeeded | none; it exits | `return {"rows": 3}` from `run` | `{"type": "task_succeeded", "output": {"rows": 3}, "outcome": "SUCCESS"}` |
| `task_failed` | Last: the task (or cleanup) failed | none; it exits | `raise RuntimeError("the cluster said no")` in `run` | `{"type": "task_failed", "error": {"exception": "RuntimeError", "message": "the cluster said no", "traceback": "..."}, "kind": null}` |

### Worker to plugin (1)

| `type` | Means | In Python | On the wire |
|---|---|---|---|
| `welcome` | The hello is accepted | `main` goes on | `{"type": "welcome"}` |
| `refused` | The hello isn't (another protocol): the plugin exits | `main` returns 2 | `{"type": "refused", "reason": "the plugin speaks runner protocol 2, this worker 1: use a plugin built for it"}` |
| `extension_task` | The attempt; a cleanup has `cleanup` set to its handles | `run(request, channel)`: `request.payload`, `request.name`, `request.version`, `request.handles` | `{"type": "extension_task", "dag_id": "etl", "task": {"id": "submit", "kind": "extension", "name": "spark", "version": "1.0", "payload": {...}, "settings": {...}}, "run": {"run_id": "01", "try_number": 1, ...}, "secrets": [{"name": {"path": "spark/api", "key": "token"}, "value": "s3cret"}], "inputs": {}, "cleanup": null}` |
| `secret` | A secret's value | `channel.secret(...)` returns it | `{"type": "secret", "value": "s3cret"}` |
| `secret_failed` | Why a secret can't be read | `channel.secret(...)` raises `ExtensionError` | `{"type": "secret_failed", "kind": "secret_not_found", "message": "`spark/api#token`: ..."}` |
| `connection` | A connection's values | `channel.connection(...)` returns them | `{"type": "connection", "values": {"host": "db.example", "port": 5432}}` |
| `connection_failed` | Why a connection can't be opened | `channel.connection(...)` raises `ExtensionError` | `{"type": "connection_failed", "kind": "connection_not_found", "message": "connection `warehouse`: ..."}` |
| `handle_added` | The handle is registered | `channel.add_handle(...)` returns | `{"type": "handle_added"}` |
| `handle_refused` | Why not | `channel.add_handle(...)` raises `ExtensionError` | `{"type": "handle_refused", "message": "the task has no cleanup"}` |
| `api_answer` | The API's status and body | `channel.api(...)` returns `(status, body)` | `{"type": "api_answer", "status": 201, "body": {"id": 7}}` |
| `stop_reason` | Why it is being stopped (`cancel`, `timeout`, `drain`, `master_lost`; null while it isn't), and the time left before `SIGKILL` | `channel.stop_reason()` returns `("timeout", 27.5)`, in seconds | `{"type": "stop_reason", "reason": "timeout", "grace_ms": 27500}` |

The worker sends nothing unasked after `extension_task`: a stop comes as `SIGTERM`.

### Service to worker (2)

| `type` | When | Answered with | In Python | On the wire |
|---|---|---|---|---|
| `hello` | First, always | `welcome` or `refused` | `serve(on_call, name="spark", versions=["1.0"])` sends it | `{"type": "hello", "schedr_version": "2.4.0", "mode": "service", "protocol": 1, "name": "spark", "versions": ["1.0"]}` |
| `ready` | After the welcome, within `service_ready_timeout`: attempts may connect | none | sent when `start(context)` returns | `{"type": "ready"}` |
| `attempt_accepted` | For each `attempt_connected`, within `service_accept_timeout`: the attempt's plugin may start | none | sent by `serve` as it takes the connection | `{"type": "attempt_accepted", "attempt_id": "01a2..."}` |
| `secret_get` | One of its `service_secrets`, read again | `secret` or `secret_failed` | `await context.secret("spark/api", "token")` | `{"type": "secret_get", "secret": {"path": "spark/api", "key": "token"}}` |
| `status` | What it is doing: an event on the extension's page | none | `await context.status("connected", "logged in")` | `{"type": "status", "phase": "connected", "message": "logged in"}` |
| `metrics` | Points of the extension's declared metrics | none | `await context.metric("jobs_submitted", 1, queue="etl")` | `{"type": "metrics", "points": [{"name": "jobs_submitted", "labels": {"queue": "etl"}, "value": 1}]}` |

### Worker to service (2)

| `type` | Means | In Python | On the wire |
|---|---|---|---|
| `welcome` | The hello is accepted, with its `service_secrets` read for this start | `start(context)`: `context.secret_value("spark/api", "token")` | `{"type": "welcome", "secrets": [{"name": {"path": "spark/api", "key": "token"}, "value": "s3cret"}]}` |
| `refused` | The hello isn't (another protocol, another extension, no configured version served): it exits | `serve` returns 2 | `{"type": "refused", "reason": "it serves none of the configured versions (1.0)"}` |
| `attempt_connected` | An attempt's connection; its descriptor comes with the message (`SCM_RIGHTS`, on its first byte) | its calls reach `on_call(attempt, ...)`, `attempt.attempt_id`; its end reaches `on_closed(attempt)` | `{"type": "attempt_connected", "attempt_id": "01a2...", "task": {"name": "spark", "version": "1.0"}}` + one descriptor |
| `drained` | The worker drained for maintenance: no attempts meanwhile; it keeps running | `on_drained()` | `{"type": "drained"}` |
| `resumed` | The worker resumed | `on_resumed()` | `{"type": "resumed"}` |
| `secret` | A secret's value | `context.secret(...)` returns it | `{"type": "secret", "value": "s3cret"}` |
| `secret_failed` | Why it can't be given | `context.secret(...)` raises `ServiceError` | `{"type": "secret_failed", "kind": "secret_denied", "message": "`x` isn't one of [extensions.spark] service_secrets"}` |

A stop comes as `SIGTERM` (`stop()` in Python), then `SIGKILL` after the grace.

### Plugin and service (3)

| `type` | From | Means | In Python | On the wire |
|---|---|---|---|---|
| `call` | plugin | A call; `id` is unique on the connection, and its answer carries it | `await channel.service.call("submit", {"job": "daily"})` | `{"type": "call", "id": 7, "method": "submit", "args": {"job": "daily"}}` |
| `answer` | service | The call's result | `on_call` returns it; `call(...)` returns it | `{"type": "answer", "id": 7, "result": {"job_id": "j-42"}}` |
| `call_failed` | service | The call failed | `on_call` raises; `call(...)` raises `ServiceError` | `{"type": "call_failed", "id": 7, "message": "ValueError: unknown method submitt"}` |

Closing the connection ends it: the plugin's close tells the service its attempt is gone
(`on_closed`); the service's fails the plugin's calls in flight (`ServiceError`).
