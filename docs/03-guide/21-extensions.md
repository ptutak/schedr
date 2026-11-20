# Extensions

An extension adds a kind of task that schedr's own worker doesn't know how to run: one that
submits a Spark job, starts a pod in another cluster, or calls a vendor's service through its own
client. Its tasks are ordinary tasks to the master, with retries, timeouts, cleanups and the UI's
pages. What runs them is a **plugin**, a program the worker starts for each attempt (decision
510). The plugin may be closed source and in any language; the master and the worker stay as
they are.

An extension whose tasks are Python functions needs none of this: a package with its own
decorators around `@task` runs on any worker where it is installed. An extension is for what the
Python runner can't run.

## A DAG's side

A task of an extension is an `ExtensionTask`. An extension's package usually wraps it in
decorators or operators of its own:

```python
from schedr import Dag, ExtensionTask, task

with Dag("nightly_spark"):
    job = ExtensionTask(
        "aggregate",
        name="spark",                 # the extension
        version="1.0",                # the version its payload is written for, matched exactly
        payload={"job": "aggregate", "days": 7},
        returns=dict[str, int],       # the type of what the plugin returns
        queue="spark",                # where workers with the plugin take tasks
        secrets=["spark/api#token"],  # what the plugin may read
        cleanup=True,                 # the plugin runs again after each attempt
    )

    @task
    def report(rows: dict[str, int]) -> str:
        return f"{rows['total']} rows"

    report(job.output)
```

- **The payload** is plain JSON, at most 1 MiB, stored with the DAG and sent to the plugin as it
  is. It takes no references: what the plugin needs from upstream it reads itself, or a Python
  task passes it on.
- **The output** is what the plugin returns, checked against `returns=` by the task that reads it
  (`Json` by default), as a `@task`'s return value is.
- **The usual options** apply: `retries`, `timeout`, `queue`, `memory`, `secrets`,
  `connections`, `max_active`, `run_if`.
- **The name** is 1 to 64 letters, digits, `_` and `-`, starting with a letter, without `__`;
  `discover` is reserved.

When the master reads a DAG asking for an extension and version that no connected worker
offers, it logs a warning; the DAG is still stored, as such a worker may connect later.

## A worker's side

A worker runs an extension's tasks only with a plugin for it, configured by name:

```toml
[extensions.spark]
command = ["/opt/acme/schedr-ext-spark"]   # the program, then its arguments
versions = ["1.0", "1.1"]                  # the versions it serves
```

As environment variables: `SCHEDR_CONFIG__EXTENSIONS__SPARK__COMMAND='["/opt/acme/schedr-ext-spark"]'`
and `SCHEDR_CONFIG__EXTENSIONS__SPARK__VERSIONS='["1.0"]'`.

With `[extensions] discover = true` (off by default), the worker also asks each `schedr-ext-*`
executable on its `PATH` what it is, running it with `--describe`. A plugin answers with one JSON
line, `{"name": "spark", "versions": ["1.0"], "protocol": 1}`. One that fails, prints something
else, speaks another protocol or takes over 5 seconds is skipped with a warning, and the worker
starts without it. A plugin configured by name wins over one discovered with the same name.

The worker announces what it has when it registers (`Register.extensions`). Routing stays by
queue: give the plugin's workers a queue of their own (`[worker] queues`) and the package's tasks
that queue.

A worker given a task it has no plugin for, or whose plugin doesn't serve the task's exact
version, gives it back with `EXTENSION_UNDEFINED`, and no try is counted. The master then tries
another worker of the queue. Once every eligible worker has given it back, the task is
`unschedulable`, with each worker's reason ("every eligible worker rejected it (w1: extension
`spark` 2.0: this worker's plugin serves 1.0, 1.1)").

## How a plugin runs

Each attempt is the plugin's own process in a **task slot**, supervised as a Python task's runner
is:

| | |
|---|---|
| Start | its `command`, with the task's environment (`SCHEDR_*` variables, the allow-listed ones) and its working directory in the DAG root; never forked from the Python fork server |
| Output | its standard output and error go to the attempt's log, masked |
| Timeout, cancel | `SIGTERM`, then `SIGKILL` after the grace, as for any task |
| Memory | the task's `memory=` budget, watched by the memory watchdog |
| Lost worker | the attempt is `lost`, as any task's |

A plugin never shares a process with another task (decision 512): one that crashes ends only its
own attempt.

It speaks the [runner protocol](15-protocol.md) over the socket the worker hands it, its number in
`SCHEDR_AGENT_FD`:

1. it sends `hello` with `"mode": "extension"` and `"protocol": 1`. The worker refuses another
   protocol; the plugin's own version is any;
2. the worker sends `extension_task`: the task (its `name`, `version`, `payload`), its run, the
   secrets it declared and the worker fetched;
3. while it works, the plugin may ask what a Python task may: `secret_get`, `connection_get`,
   `handle_add`, `api_call`, each answered before the next;
4. it ends with `task_succeeded` (its output) or `task_failed` (an error).

## Cleanup

With `cleanup=True`, the plugin registers **handles** while it runs: what its cleanup needs that
only the attempt knows, such as a job's ID or a pod's name (`handle_add`). The worker keeps each
one as it is registered. After the attempt ends, whatever the end, the worker runs the plugin
again with `extension_task` carrying `cleanup`, the handles in order. Its retries, timeout and
`cleanup_anywhere` follow the task's `cleanup_*` options, as for a Python task's cleanup.

A plugin that only waits for something has another way: a `@sensor` in a Python package, run in
a watcher slot. A sensor has no cleanup and no handles, which is why an extension that starts
something to undo is a plugin.

## Writing a plugin

**In Python**, `schedr.extension` is the loop: a plugin gives it a coroutine.

```python
from schedr.extension import ExtensionChannel, ExtensionRequest, main

async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
    token = await channel.secret("spark/api", "token")
    job_id = submit(request.payload, token)
    await channel.add_handle("job", {"id": job_id})
    return {"total": wait_for(job_id)}

async def cleanup(request: ExtensionRequest, channel: ExtensionChannel) -> object:
    for handle in request.handles:
        cancel(handle.value["id"])
    return None

if __name__ == "__main__":
    raise SystemExit(main(run, name="spark", versions=["1.0"], cleanup=cleanup))
```

`main` also answers `--describe`. An exception fails the attempt; an output that isn't JSON, or
is over the task's `max_output_size`, fails it too.

**In Rust**, the `schedr-extension` crate holds the messages a plugin speaks and the same loop
(`schedr_extension::main`, a `Plugin` trait with `run` and `cleanup`); its example,
`hello-plugin`, greets the payload's `name`. The crate stands on its own: it depends on no other
schedr crate, and its version follows the protocol (1.x speaks protocol 1), not schedr's
releases. A test in schedr's workspace reads each message both ways between it and the worker's
own types (decision note 17).

**The dummy extension** in `examples/extensions/dummy/` is a plugin whose payload says what to do
(sleep, log, read a secret, register a handle, return, fail, hang). `make standalone`,
`make standalone-separate-ui` and `make distributed` find it by discovery, and
`examples/extensions/dummy_extension.py` has a DAG for each case: a run with output, a secret, a
cleanup after a failure, a timeout, a version no worker serves, and plugins in two languages side
by side (the dummy and the Rust `hello-plugin`, which those targets build and configure by path).
