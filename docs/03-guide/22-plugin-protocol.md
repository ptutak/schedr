# The plugin protocol

This page is for writing an [extension](21-extensions.md)'s plugin in a language schedr has no
helper for. It says everything a plugin must do, in the order it happens. Python
(`schedr.extension`) and Rust (the `schedr-extension` crate) have helpers that do all of it.

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

## Compatibility

Within protocol 1:

- the worker may add **fields** to its messages: a plugin ignores the fields it doesn't know;
- **new message types** come only with a new protocol version, which a plugin's `hello` and
  `--describe` name; a worker refuses a plugin speaking another protocol than its own.

The Rust crate's version follows the protocol: `schedr-extension` 1.x speaks protocol 1.
