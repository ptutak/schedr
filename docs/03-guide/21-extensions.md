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
What discovery found and skipped is on the Extensions page too: a plugin skipped there, never
run, is listed by the name its file gives (`schedr-ext-broken` as `broken`), its page showing
why.

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
   `handle_add`, `api_call`, and `stop_reason_get`, each answered before the next; and it may
   report, with no answer, its `status`, its `link`s and its `metrics`;
4. it ends with `task_succeeded` (its output) or `task_failed` (an error).

[The plugin protocol](22-plugin-protocol.md) says all of it precisely, for a plugin in any
language, with the JSON Schema of its messages.

## Reporting

A plugin says more of its work than a task's log can (decision 513), with messages the worker
doesn't answer:

| Message | Shown |
|---|---|
| `status`: a phase (`pulling`, `running`), a message, a progress from 0 to 1 | on the task's attempts, the latest beside it and every one with its time (the first and the latest 100 kept) |
| `link`: a label and an `http` or `https` URL | on the task's attempts, beside its phase: a pod's page, a console (at most 10, one per label) |
| `metrics`: points of its declared metrics | on the worker's `/metrics` and the extension's page |

Each is masked as the attempt's output is: a secret it read doesn't show. An attempt sends at
most 10 reports a second, 50 at once after a quiet spell: past that its newest status is held
and sent when it ends, a link is dropped with a warning, and a metric point is kept on the
worker as usual (only its figures travel, once a minute).

**Metrics** are declared in the plugin's `hello` (as NETCONF's hello announces its models), or in
the worker's `[extensions.<name>]` config, which wins: each a `name`, a `kind` (`counter`, `gauge`
or `histogram`, with its `buckets`), a `help`, a `unit` and its `labels`, a label perhaps with the
`values` it may take. A counter's point adds to it, a gauge's sets it, a histogram's is an
observation. A point matching no declaration is dropped, with one warning.

```python
METRICS = [
    {"name": "pods_started", "kind": "counter", "help": "Pods started",
     "labels": [{"name": "namespace"}]},
    {"name": "queue_seconds", "kind": "histogram", "help": "Time to a running pod",
     "unit": "seconds", "buckets": [1, 5, 30, 120]},
]

async def run(request, channel):
    await channel.status("pulling", progress=0.1)
    await channel.link("Pod", f"https://dashboard/pods/{pod}")
    await channel.metric("pods_started", 1, namespace="batch")
    ...

main(run, name="kubernetes", versions=["1.0"], metrics=METRICS)
```

The worker serves them as `schedr_extension_<extension>_<metric>`, labelled with the
`extension_version` too, and sends them to the master once a minute: a counter's growth, a
gauge's value and a histogram's new observations are kept a minute each for a day, and per 15
minutes, summed over the workers, for 30 days, as the processes' samples are.
`GET /api/v1/extensions/<name>/metrics?range=1h` reads them, summed over the workers;
`&worker=<id>` gives one worker's own, for `1h` or `24h` only (they are kept a day). A metric keeps at most
`[extensions.<name>] max_series` series (100) an hour, counted over all the workers and kept
across a master's takeover: a new label combination past it, or a value outside its declared
`values`, counts as `other`, and the page says the series was folded. A heartbeat carries at most
a quarter of what the master accepts in a message: a version's figures past that are left out
for the minute, with an event saying so.

**Its page** is declared with them (`panels=`): each panel a `title`, the `metric` it draws, a
`chart` (`line`, `rate`, `stacked`, `bars`, `histogram`, `quantiles`, `number`, `table`, or `events`,
which draws no metric but the extension's own events (discovered, skipped, refused, a metric
point or figures dropped), newest first, and its attempts' latest phases, each linking to its
attempt), its `row`
(from 1 at the top, rows of one height) and its `cell` in the row: `full`, or `left` and `right`,
or `left`, `center` and `right`. A metric no panel draws gets a `full` row of its own below them.
Nothing of style is declared: the page draws in schedr's.

A chart's lines (`line`, `rate`, `quantiles`, and `stacked` areas) run from one reading to the
next across up to five empty buckets, so a job every few minutes reads as a line; past that the
line breaks, nothing having been sent. A counter's line so reads at its busy minutes' level. To
see each bucket as it was, empty ones left empty, a panel takes `bars`: a bar per bucket, the
series stacked in it (a counter's growth, a histogram's observations, a gauge's value).

The UI's **Extensions** shows each extension in a tab of its own, beside the deployment's others
(with several deployments, choose one first). Inside a tab, **All workers** draws the page summed
over its workers, for 1 hour up to 30 days; beside it, a tab for each worker heard from in the
last day draws that worker's own figures and events, for 1 hour or a day.

## Being stopped

A plugin is stopped as any task is: `SIGTERM` to its process group, then `SIGKILL` once the
grace is over. The grace is the master's `cancel_grace` (30 s) for a cancel or a timeout, and the
worker's `drain_terminate_period` when a draining worker interrupts it. The worker sends no
message of its own: a plugin that wants to know why it is being stopped asks, after the signal,
with `stop_reason_get`. The answer is one of

| Reason | When |
|---|---|
| `cancel` | someone cancelled the task or its run |
| `timeout` | the task ran past its `timeout` |
| `drain` | its worker is shutting down; the task will be retried elsewhere |
| `master_lost` | its worker has heard nothing from the master for its lease: the task is `lost`, and its cleanup may run on another worker |

with the milliseconds left before `SIGKILL`. Asked before any signal, the reason is `null`. A
memory kill has no grace and no question: `SIGKILL` at once.

What a plugin does with it is its own: a Job deleted on `cancel` and `drain` (it will run again
elsewhere), left to finish on `timeout` if that is cheaper, and nothing on `master_lost` (its
cleanup runs from the handles it registered). It has the grace left, and should use less: what it
hasn't finished by then is killed with it. Everything it started should be in its process group,
or registered as a handle for the cleanup.

**In Python**, `schedr.extension` turns `SIGTERM` into a cancellation of the plugin's coroutine:
`asyncio.CancelledError` is raised at the `await` it is waiting on.

```python
import asyncio

from schedr.extension import ExtensionChannel, ExtensionRequest, main

async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
    job = submit(request.payload)
    try:
        return await wait_for(job)
    except asyncio.CancelledError:
        reason, seconds_left = await channel.stop_reason()
        if reason in ("cancel", "drain"):
            await asyncio.wait_for(delete(job), timeout=max(seconds_left - 1, 0))
        raise
```

Re-raised, the attempt ends with the worker's reason (`cancelled`, `timed_out`, ...).

**In Rust**, a plugin installs its own handler (the `signal-hook` crate, for instance) setting a
flag its loop looks at, then asks:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn run(&mut self, request: &Request, channel: &mut Channel) -> Result<Value, String> {
    let stopping = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&stopping))
        .map_err(|e| e.to_string())?;
    let job = submit(&request.payload)?;
    while !finished(&job)? {
        if stopping.load(Ordering::Relaxed) {
            let (reason, left) = channel.stop_reason().map_err(|e| e.to_string())?;
            if matches!(reason, Some(StopKind::Cancel | StopKind::Drain)) {
                delete(&job, left)?;
            }
            return Err(format!("stopped: {reason:?}"));
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    Ok(result(&job)?)
}
```

**In Go**, with no helper: `signal.Notify` on `SIGTERM`, then the question on the socket in
`SCHEDR_AGENT_FD`, framed as every message is (a 4-byte big-endian length, then JSON):

```go
stop := make(chan os.Signal, 1)
signal.Notify(stop, syscall.SIGTERM)
select {
case result := <-done:
    send(conn, map[string]any{"type": "task_succeeded", "output": result, "outcome": "SUCCESS"})
case <-stop:
    send(conn, map[string]any{"type": "stop_reason_get"})
    var answer struct {
        Reason  *string `json:"reason"`
        GraceMs int64   `json:"grace_ms"`
    }
    receive(conn, &answer)
    if answer.Reason != nil && (*answer.Reason == "cancel" || *answer.Reason == "drain") {
        deleteJob(job, time.Duration(answer.GraceMs)*time.Millisecond)
    }
    os.Exit(143)
}
```

A shell plugin's way comes with `schedr extension shell` (M5.2i6).

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
(sleep, log, read a secret, register a handle, return, fail, hang). `make standalone` and
`make standalone-separate-ui` find it by discovery (`make distributed` runs a released schedr,
1.1.0, without extensions), and
`examples/extensions/dummy_extension.py` has a DAG for each case: a run with output, a secret, a
cleanup after a failure, a timeout, a version no worker serves, and plugins in two languages side
by side (the dummy and the Rust `hello-plugin`, which those targets build and configure by path).

## A shell script as a plugin

`schedr extension shell -- <script> [args]` makes any script an extension's plugin: it speaks the
protocol with the worker and runs the script for each attempt. Configure it as the extension's
command, its metrics and panels beside it (a script sends no `hello`, so they are declared here):

```toml
[extensions.backup]
command  = ["schedr", "extension", "shell", "--", "/opt/ops/backup.sh"]
versions = ["1.0"]
metrics  = [{name = "files_copied", kind = "counter", help = "Files copied", labels = [{name = "kind"}]}]
panels   = [{title = "Files copied", metric = "files_copied", chart = "bars", row = 1, cell = "full"}]
```

The script gets:

- **the payload** as JSON on its stdin;
- **the run** as `SCHEDR_*` variables, as a command task does (`SCHEDR_DAG_ID`, `SCHEDR_RUN_ID`,
  `SCHEDR_TASK_ID`, `SCHEDR_TRY_NUMBER`, ...), with `SCHEDR_EXTENSION` and
  `SCHEDR_EXTENSION_VERSION`;
- **its log**: its stdout and stderr; `schedr task-ctx log` writes a structured line;
- **its end**: exit 0 succeeds, anything else fails the attempt ("the script exited with 3").

It talks to the worker through `schedr task-ctx`, the folder of the `schedr` running it first
on its `PATH`, so the commands are that build's:

| Command | What it does |
|---|---|
| `extension output set JSON` | The task's output (JSON, or `@file.json`); set again, the last counts; none set, it is null |
| `secret get PATH#KEY`, `connection NAME --json` | A declared secret, a connection's values (`--env` isn't served: it writes files in a command task's folder) |
| `cleanup add TYPE FIELD...`, `cleanup list` | A handle, before what it cleans up; the handles, in the cleanup |
| `status PHASE [--message TEXT] [--progress FRACTION]`, `link LABEL URL` | Its phase and links, on the task |
| `metric NAME VALUE [--label NAME=VALUE]...` | A point of a metric declared in the config |
| `stop-reason [--field reason]` | After `SIGTERM`: `cancel`, `timeout`, `drain` or `master_lost`, and the seconds left |
| `api METHOD PATH`, `events post` | schedr's API and events, as the task |

A cleanup (`ExtensionTask(..., cleanup=True)`) runs the same script with `SCHEDR_CLEANUP=1`. A
stop reaches the script itself: the worker signals the plugin's whole process group, so a
`trap ... TERM` runs, and may ask why. A configured extension only: discovery asks `--describe`
of `schedr-ext-*` programs, which a script isn't.

`examples/extensions/backup/backup.sh` is one, configured by `make standalone`;
`examples/extensions/backup_extension.py` has its DAGs, one that succeeds and one that fails.

## The Kubernetes plugin

`examples/extensions/kubernetes/schedr-ext-kubernetes` runs each attempt as a Kubernetes Job,
driving `kubectl`. It is short enough to read whole, and shows what a plugin should do:

| | |
|---|---|
| The attempt | a Job named after the task, its run and its try, one pod, never restarted (`backoffLimit: 0`: schedr retries), with no API token (`automountServiceAccountToken: false`) |
| The payload | each field's type checked before anything is created; an unknown field or a mistyped one fails the attempt |
| Before creating it | registers the Job's name as a cleanup handle, so a worker lost right after still leaves it findable |
| While it runs | the pod's log streamed into the attempt's log (`kubectl logs -f`); its own lines through `logging`, structured; its phases (`creating`, `pending`, `running`, `succeeded` or `failed`) and, with `--dashboard <address>`, links to its Job and pod in the Kubernetes Dashboard |
| Its metrics | `jobs_started` by namespace, `jobs_ended` by outcome, `pods_running` (as the cluster counts schedr's), `start_seconds` (to a running pod) and `job_seconds`, with a page of six panels declared in its hello |
| The output | the ended pod's last log line `SCHEDR_OUTPUT: <json>`, read once the Job succeeded, never from the stream; without it, `null`; a log that can't be read fails the attempt |
| A failure | the pod's reason and exit code; a pod stuck on its image (`ErrImagePull`) fails at once |
| Being stopped | SIGTERM: it asks why; on a cancel, a timeout or a drain it deletes its Job, on a lost master it leaves it to the cleanup |
| The cleanup | deletes each handle's Job in the namespace the handle names, whatever the attempt's end, a lost worker's too |

The payload:

```python
ExtensionTask(
    "count",
    name="kubernetes",
    version="1.0",
    payload={
        "image": "busybox:1.36",
        "command": ["sh", "-c", "echo 'SCHEDR_OUTPUT: {\"rows\": 3}'"],
        "env": {"MODE": "fast"},
        "secret_env": {"TOKEN": "etl/api#token"},   # a declared secret, as a variable
        "resources": {"limits": {"memory": "64Mi"}},
        "node_selector": {"pool": "batch"},
    },
    returns=dict[str, int],
    secrets=[secrets["etl/api"]["token"]],
    cleanup=True,
)
```

A secret given as a variable is visible to anyone who can read the Job: that is for the
example; a real plugin would put it in a Kubernetes Secret.

On a worker, `kubectl` must work for the worker's user (its kubeconfig in `HOME`, or the pod's
service account), allowed to create, get and delete Jobs and to get pods and their logs in the
namespace:

```toml
[extensions.kubernetes]
command = ["/opt/schedr/schedr-ext-kubernetes", "--namespace", "batch"]  # also --context, --kubeconfig
versions = ["1.0"]
```

`scripts/kind-extension.sh` runs `examples/extensions/kubernetes_extension.py` on a local kind
cluster: a pod's log while it runs, a success, a failure, a timeout, and a worker lost with its
plugin, whose Job the cleanup finds by its handle.
