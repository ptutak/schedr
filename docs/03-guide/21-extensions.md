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
| `notify`: one of its declared notification events, a text and data | to the channels of the attempt's DAG subscribed to `EXTENSION_EVENT`, and on the extension's events panel |

Each is masked as the attempt's output is: a secret it read doesn't show. An attempt sends at
most 10 reports a second, 50 at once after a quiet spell: past that its newest status is held
and sent when it ends, a link is dropped with a warning, and a metric point is kept on the
worker as usual (only its figures travel, once a minute).

Extensions never slow the master (decision 521). It stores their reports off its round: a
worker's message hands them to a queue and goes on, and a writer of their own stores them at most
once a second, each write in one transaction within 2 s. Of an attempt's statuses in one such
write, a run of the same phase keeps its newest (a progress ticking within `pulling` is one row);
each change of phase is kept. A full queue, a failed write or a slow one drops what it held:
counted in the master's `schedr_master_extension_reports_dropped_total{reason}` (`full`, `failed`,
`slow`, and `many`: past 100 events of one extension in one write) and warned of in its log at
most once a minute. So a status may show on its attempt a second after the attempt ended, and on
a database too slow for them, an extension's page has gaps while its tasks run on.

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

**Notifications** (decision 515) are declared the same way, under `notifications` in its
`hello` or `[extensions.<name>]` (config wins), each a `name` (a lowercase identifier) and a
`help`; at most 50. `channel.notify(event, text, data)` sends one: the worker masks it, cuts its
text to 512 characters, and sends it to the master as the task's own `send` goes, counted against
`[notifications] max_per_attempt`. The master tells each channel of the attempt's DAG
subscribed to `Notify.EXTENSION_EVENT` (one event for every extension: a receiver filters on
`notify.details`, which holds the `extension`, `version` and `event`). An event it didn't declare
is dropped, with one warning per name an attempt. Both are listed on its events panel
(`notified`, `dropped`). A plugin's own API call can't send one: only `notify`, which the worker
checks.

```python
NOTIFICATIONS = [{"name": "image_pull_failed", "help": "A Job's image can't be pulled"}]

async def run(request, channel):
    ...
    await channel.notify("image_pull_failed", f"job {job}: {why}", {"job": job})

main(run, name="kubernetes", versions=["1.0"], notifications=NOTIFICATIONS)
```

```python
with Dag("pods", notify={"ops": [Notify.EXTENSION_EVENT, Notify.RUN_FAILED]}):
    ExtensionTask("job", name="kubernetes", version="1.0", payload={"image": "busybox:1.36"})
```

**Its page** is declared with them (`panels=`): each panel a `title`, the `metric` it draws, a
`chart` (`line`, `rate`, `stacked`, `bars`, `histogram`, `quantiles`, `number`, `table`, or `events`,
which draws no metric but the extension's own events (discovered, skipped, refused, a metric
point, figures or a notification dropped, a notification sent), newest first, and its attempts' latest phases, each linking to its
attempt), its `row`
(from 1 at the top, rows of one height) and its `cell` in the row: `full`, or `left` and `right`,
or `left`, `center` and `right`. A metric no panel draws gets a `full` row of its own below them.
Nothing of style is declared: the page draws in schedr's.

**How a panel draws its metric** (decision 516) is four optional keys; most panels name none:

| Key | Says | Default |
|---|---|---|
| `point` | how a bucket's point is worked out: a counter's `increase` or `rate` (per second); a gauge's `mean`, `last`, `max` or `min`; a histogram's `count`, `mean` or `quantiles` | a counter's `increase` (`rate` on a `rate` chart), a gauge's `mean` (DN26), a histogram's `count` |
| `combine` | how values are combined: the workers' on **All workers**, and the series `group_by` leaves together: `sum`, `max`, `mean` or `min` (a histogram's only `sum`) | `sum` |
| `group_by` | the labels that get a line each: a list, or `[]` for one line | every label |
| `limit` | how many lines are shown, the largest by their peak over the range; the rest are drawn as one `other` | all, up to `max_series` |

Values are combined at each moment first (a minute's readings over the workers), then the
bucket's point is taken over its moments: with `sum` and `max` a panel shows the total's peak.
A value every worker reads from one place, a cluster's pending pods, takes `combine: max`, so
three workers reading 10 show 10, not 30:

```python
{"title": "Pending pods", "metric": "pods_pending", "chart": "line", "row": 1, "cell": "full",
 "point": "max", "combine": "max", "group_by": ["namespace"], "limit": 5}
```

A function the metric's kind doesn't have, a `group_by` label it doesn't declare, a `limit` of 0,
any function on an `events` panel, or `point: quantiles` on another chart than `quantiles` is
refused when the declarations are loaded. Lines past `limit`, or past `max_series`, are drawn as
one `other` with any series a worker folded already. Past a day the master keeps 15-minute windows
across workers; each window keeps its points under every `combine`, so the 7- and 30-day views
draw the same functions (a `mean` over the series of a group is then their means' average).
`GET /api/v1/extensions/<name>/metrics` gives each panel's lines, worked out so, under `panels`.

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

**In Python**, `schedr_extension` turns `SIGTERM` into a cancellation of the plugin's coroutine:
`asyncio.CancelledError` is raised at the `await` it is waiting on.

```python
import asyncio

from schedr_extension import ExtensionChannel, ExtensionRequest, main

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

**Less strict than a task that exits on `SIGTERM`, by design (DN38).** A plugin may take its
grace to stop (deleting a Job), and one that hangs takes all of it. When its worker is frozen (a
paused VM, a stopped process) while its lease without the master runs out, the worker stops its
tasks only once thawed, and kills nothing meanwhile. With `cleanup_anywhere`, the master moves the
cleanup 40 s after the lease (decision 256), not knowing a frozen worker from one cut off, which
would have stopped the task itself (decision 252). So a plugin may still run when its own cleanup
starts on another worker: until its worker is thawed and its grace is over. The same holds for any
task that doesn't exit on `SIGTERM`; it is plugins that do so. Hence: on `master_lost`, stop at
once (the cleanup does the rest from the handles); and write the cleanup to be safe while the
attempt's plugin may still be finishing.

## Cleanup

With `cleanup=True`, the plugin registers **handles** while it runs: what its cleanup needs that
only the attempt knows, such as a job's ID or a pod's name (`handle_add`). The worker keeps each
one as it is registered. After the attempt ends, whatever the end, the worker runs the plugin
again with `extension_task` carrying `cleanup`, the handles in order. Its retries, timeout and
`cleanup_anywhere` follow the task's `cleanup_*` options, as for a Python task's cleanup.

A plugin that only waits for something has another way: a `@sensor` in a Python package, run in
a watcher slot. A sensor has no cleanup and no handles, which is why an extension that starts
something to undo is a plugin.

## Building a plugin in Python

A plugin is a program the worker runs for each attempt. In Python, `schedr_extension` is its whole
loop: you write a coroutine that does the task, and `main` speaks the protocol around it.

1. Write the program (`schedr-ext-spark`, executable, `schedr` installed beside it):

   ```python
   from schedr_extension import ExtensionChannel, ExtensionRequest, main

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

2. Configure it on the workers that should run it (see [A worker's side](#a-workers-side)):

   ```toml
   [extensions.spark]
   command  = ["/opt/acme/schedr-ext-spark"]
   versions = ["1.0"]
   ```

3. Use it in a DAG: `ExtensionTask("submit", name="spark", version="1.0", payload={...},
   secrets=[secrets["spark/api"]["token"]], cleanup=True)`.

What `run` returns is the task's output; an exception fails the attempt, and so does an output
that isn't JSON or is over the task's `max_output_size`. `channel` has what a Python task may do
(secrets, connections, handles, the API, the stop reason) and the reports of
[Reporting](#reporting): `status`, `link`, `metric`, `notify`. `main` also answers `--describe`
for discovery.

**In Rust**, or any language: a plugin speaks [the plugin protocol](22-plugin-protocol.md). The `schedr-extension` crate holds the messages a plugin speaks and the same loop
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

## Building a shell script plugin

`schedr extension shell -- <script> [args]` makes any script an extension's plugin: it speaks the
protocol with the worker and runs the script for each attempt; the script talks back through
`schedr task-ctx`.

1. Write the script:

   ```sh
   #!/bin/sh
   set -eu
   payload=$(cat)                                   # the payload, JSON
   schedr task-ctx status copying --progress 0.1
   token=$(schedr task-ctx secret get "backup/target#token")
   count=$(copy_files "$payload" "$token")          # the work
   schedr task-ctx extension metric files_copied "$count" --label kind=data
   schedr task-ctx extension output set "{\"files\": $count}"
   ```

2. Configure it as the extension's command, its metrics, panels and notifications beside it (a
   script sends no `hello`, so they are declared here):

   ```toml
   [extensions.backup]
   command  = ["schedr", "extension", "shell", "--", "/opt/ops/backup.sh"]
   versions = ["1.0"]
   metrics  = [{name = "files_copied", kind = "counter", help = "Files copied", labels = [{name = "kind"}]}]
   panels   = [{title = "Files copied", metric = "files_copied", chart = "bars", row = 1, cell = "full"}]
   notifications = [{name = "backup_done", help = "A backup's snapshot was taken"}]
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
| `extension metric NAME VALUE [--label NAME=VALUE]...` | A point of a metric declared in the config |
| `extension notify EVENT [--text TEXT] [--data JSON]` | One of the notification events declared in the config (`notifications = [{name = ..., help = ...}]`), to the DAG's channels subscribed to `EXTENSION_EVENT` |
| `extension stop-reason [--field reason]` | After `SIGTERM`: `cancel`, `timeout`, `drain` or `master_lost`, and the seconds left |
| `extension call METHOD [--args JSON]` | A call to the extension's service; prints its result as JSON, exits 1 with its error |
| `api METHOD PATH`, `events post` | schedr's API and events, as the task |

A cleanup (`ExtensionTask(..., cleanup=True)`) runs the same script with `SCHEDR_CLEANUP=1`. A
stop reaches the script itself: the worker signals the plugin's whole process group, so a
`trap ... TERM` runs, and may ask why. A configured extension only: discovery asks `--describe`
of `schedr-ext-*` programs, which a script isn't.

`examples/extensions/backup/backup.sh` is one, configured by `make standalone`;
`examples/extensions/backup_extension.py` has its DAGs, one that succeeds and one that fails.

## Building a service in Python

A plugin lives for one attempt. What its attempts share (a client logged in once, a watch on the
system it controls, a warm session) goes in the extension's **service**: one long-running
process per worker (decision 518). Configure it beside `command`:

```toml
[extensions.spark]
command  = ["/opt/acme/schedr-ext-spark"]
versions = ["1.0"]
service  = ["/opt/acme/schedr-ext-spark", "--service"]
service_secrets = [{path = "spark/api", key = "token"}]
```

| Key | Default | What |
|---|---|---|
| `service` | none | Its program and arguments, as `command`'s. Without it, nothing changes |
| `service_ready_timeout` | `60s` | How long it has to say `ready` after it starts |
| `service_memory` | `256MiB` | What its processes may use together; past it, it is killed. Taken off the memory the worker's task budgets are split from, but not off a `[worker] memory_budget` you set: leave room for it there ([Workers](05-workers.md#memory)) |
| `service_accept_timeout` | `10s` | How long it has to accept an attempt's connection |
| `service_timeouts` | `3` | Attempts timing out, or connections unanswered, in a row that take it for hung; `0` never |
| `service_restarts` | `5` | Restarts within `service_settle` before the extension is turned off on the worker |
| `service_settle` | `24h` | The window restarts count in, and how long it stays off |
| `service_secrets` | none | The secrets it is given at each start, never in its environment |
| `builtin_service_metrics` | `true` | Which [built-in metrics](#its-built-in-metrics) are collected: `true` all three, `false` none, or a list of their names |

**Its life on a worker:**

1. The worker starts it when it starts. It listens on nothing: its one link to the worker is an
   inherited socket, `SCHEDR_SERVICE_FD`.
2. It says `hello` (the extension, the versions it serves), gets its secrets in the answer, and
   says `ready`. Only then does the worker take the extension's tasks, and only at the versions
   it serves of the configured ones.
3. Each attempt gets a private connection to it: a socket pair, one end the plugin's (its
   `SCHEDR_SERVICE_FD`), the other passed to the service. The service accepts it, and the plugin
   starts. The worker isn't in the middle: the plugin calls the service directly
   (`await channel.service.call("submit", {...})`), several calls at once if it likes.
4. When it crashes (it exits, breaks the protocol, isn't ready in time) or goes past its memory,
   its attempts' connections close with it and their plugins fail them, retries applying. It is
   started again after 1 s, then 2, up to 1 min.
5. Nothing pings it. A service that hangs shows as attempts timing out, or connections it doesn't
   accept in time (each such attempt fails at once); `service_timeouts` of either in a row count
   as a crash.
6. Past `service_restarts` within `service_settle`, the extension is turned off on that worker
   until the settle has passed since the first of them, then tried again. Meanwhile, and whenever
   it isn't ready, the worker gives the extension's tasks back with `EXTENSION_NOT_READY`: no try
   is used, the queue's other workers get them, and the task waits as for code on its way,
   `unschedulable` only after `[master] code_wait` (10 min) with every worker saying so. Each
   refusal carries how many times the service had become ready, and each heartbeat the count
   now; once a worker's count is past its refusal's (the service ready again since), the refusal
   is forgotten and the task queued again in the next round, its code wait starting over.
7. When the worker stops, its drain done (no attempt connected any more), the service gets
   `SIGTERM`, and `SIGKILL` after the grace. A worker drained for maintenance keeps it running.

Its output goes to the worker's log, a line at a time, tagged with the extension, its secrets
masked. Its starts, readiness, crashes, memory kills, turn-offs, statuses and stops are events on
the extension's page. It may send points of the extension's declared metrics, kept under the
newest version it serves.

**Building one in Python**: a handler and `serve`, in the same program as the plugin or its own.

1. Write it:

   ```python
   from schedr_extension import ServiceAttempt, ServiceContext, serve

   # What the attempts share: one client, logged in once, and a watch it keeps.
   state = {"client": None, "context": None, "paused": False, "jobs": {}}


   async def start(context: ServiceContext) -> None:
       """Once, before `ready`: its secrets are given here, never in its environment."""
       state["context"] = context
       state["client"] = SparkClient(token=context.secret_value("spark/api", "token"))
       await context.status("connected", "logged in to the cluster")


   async def on_call(attempt: ServiceAttempt, method: str, args: object) -> object:
       """Each call of an attempt's plugin; what it returns is the answer, an exception the error."""
       client = state["client"]
       if method == "submit":
           job = await client.submit(args, tag=attempt.attempt_id)
           state["jobs"][attempt.attempt_id] = job.id
           await state["context"].metric("jobs_submitted", 1, queue=job.queue)
           return {"job_id": job.id}
       if method == "status":
           return await client.status(state["jobs"][attempt.attempt_id])
       raise ValueError(f"unknown method {method}")


   async def on_closed(attempt: ServiceAttempt) -> None:
       """An attempt's connection closed: its attempt ended, however it ended."""
       state["jobs"].pop(attempt.attempt_id, None)


   async def on_drained() -> None:
       """The worker drained for maintenance: no attempts come; stop acting on your own."""
       state["paused"] = True


   async def on_resumed() -> None:
       """The worker resumed: go on."""
       state["paused"] = False


   async def stop() -> None:
       """SIGTERM, when the worker stops: close what it holds, within the grace."""
       await state["client"].close()


   if __name__ == "__main__":
       raise SystemExit(
           serve(
               on_call,
               name="spark",
               versions=["1.0"],
               start=start,
               on_closed=on_closed,
               on_drained=on_drained,
               on_resumed=on_resumed,
               stop=stop,
           )
       )
   ```

2. Configure it beside the plugin (`service = [...]`, above).

3. Call it from the plugin's `run`:

   ```python
   job = await channel.service.call("submit", request.payload)
   ```

   An exception in the handler fails that call with `ServiceError`, and so does a service gone;
   several calls may be in flight at once. A Rust plugin calls with
   `channel.service_call(method, args)`, a shell plugin's script with `schedr task-ctx extension
   call METHOD --args JSON` (it prints the result).

Its hooks, all but `on_call` optional:

| Hook | Called |
|---|---|
| `start(context)` | Once, before it says `ready`; `context.secret_value(...)` reads a secret given at the start |
| `on_call(attempt, method, args)` | For each call of an attempt's plugin, several at once; returns the answer (JSON), raises to fail the call |
| `on_closed(attempt)` | An attempt's connection closed: its attempt ended |
| `on_drained()`, `on_resumed()` | The worker drained for maintenance, and resumed: no attempts meanwhile; it keeps running |
| `stop()` | On `SIGTERM`, when the worker stops, before it exits |

Its context also reads a secret again (`await context.secret(...)`) and reports as a plugin does
(`await context.status(...)`, `await context.metric(...)`, a metric declared for the
extension). The dummy extension's
service is a counter its attempts share (`examples/extensions/dummy/schedr-ext-dummy
--service`, configured by `make standalone`; `extension_service` in
`examples/extensions/dummy_extension.py` runs three attempts against it).

### Its built-in metrics

schedr measures three things of every service itself, with nothing for its author to declare or
send:

| Metric | Kind | What | Measured by |
|---|---|---|---|
| `service_memory_bytes` | gauge, bytes | Its processes' memory together, as its `service_memory` limit counts it | the worker, at each memory check |
| `service_accept_seconds` | histogram, seconds | From an attempt's connection handed to it to its `attempt_accepted` | the worker |
| `service_call_seconds` | histogram, seconds, by `method` | How long it took to answer each call, failed ones too | Python's `serve`, sent once a second |

They are the extension's metrics like its own, kept under the newest version it serves: on the
worker's `/metrics`, and on the extension's page. They are collected once that version is declared
on the worker, by its `[extensions.<name>] metrics` or its plugin's first run there: a worker
that hasn't run one of its tasks yet shows none (declared alone, they would hide the plugin's own
metrics from the page). Only the worker measures memory and accept times; a service sends its
calls' times and no other `service_*` point, and a plugin none. A panel draws one by its name, as any metric,
in the plugin's `PANELS` or the worker's `[extensions.<name>] panels`:

```toml
panels = [{title = "Service memory", metric = "service_memory_bytes", chart = "number",
           row = 1, cell = "left", combine = "max"}]
```

Those no panel names share one **Service** row at the bottom of the page: memory a line (the
largest worker's on **All workers**), the two times as quantiles. Names starting with `service_`
are schedr's: a plugin or a config declaring one is refused. A `method` takes as many series as
`max_series` allows, the rest folded into `other`.

`builtin_service_metrics` turns them off: `false` for none, or the names of those to keep
(`["service_memory_bytes", "service_call_seconds"]`); one left out isn't collected at all, and an
unknown name is refused when the configuration is read.

## Building a service in another language

A service in any language speaks the service part of [the plugin
protocol](22-plugin-protocol.md#a-service): length-prefixed JSON over the inherited socket
`SCHEDR_SERVICE_FD`. What it must do:

1. Send `hello` (`"mode": "service"`, `"protocol": 1`, its extension's `name` and `versions`),
   read `welcome` (its secrets), then send `ready` within `service_ready_timeout`.
2. Read messages from the control socket. On `attempt_connected`, take the descriptor that comes
   with it (`SCM_RIGHTS`, `recvmsg`), answer `attempt_accepted` within
   `service_accept_timeout`, and serve that connection: `call` in, `answer` or `call_failed`
   out, by the call's `id`, several at once. A connection closing is its attempt ending.
3. Keep serving the control socket while attempts run: a loop blocked on one attempt leaves the
   next unaccepted. Answer `drained` and `resumed` as it needs (no answer is expected), and exit
   on `SIGTERM`.

Its [built-in](#its-built-in-metrics) `service_call_seconds` is Python's `serve`'s to send: a
service in another language sends it as a plain `metrics` message of points named so, a `method`
label each, if it wants it.

The protocol page's [messages](22-plugin-protocol.md#every-message) list every message both
ways. `python/schedr_extension/_service.py` is a complete service in about 300 lines, to read beside it.


## The Kubernetes plugin

`examples/extensions/kubernetes/schedr-ext-kubernetes` runs each attempt as a Kubernetes Job,
with [a service](#building-a-service-in-python) beside its plugin (M5.2i11): one program, run as
the service with `--service`. The service holds the one `kubernetes_asyncio` client and watches
the namespace's Jobs and pods; the plugin calls it for everything in the cluster. It shows what
a plugin and a service should do:

| | |
|---|---|
| The attempt | a Job named after the task, its run and its try, one pod, never restarted (`backoffLimit: 0`: schedr retries), with no API token (`automountServiceAccountToken: false`) |
| The payload | each field's type checked before anything is created; an unknown field or a mistyped one fails the attempt |
| Before creating it | registers the Job's name as a cleanup handle, so a worker lost right after still leaves it findable |
| Its secrets | the task's secret variables (`secret_env`) put in a Kubernetes Secret of the Job's own (owned by it, deleted with it), the Job reading them from it: never written in the Job |
| Its pod | its start and end answered by the service's watch on the namespace, never polled |
| While it runs | the pod's log followed by the service and written into the attempt's log; its own lines through `logging`, structured; its phases (`creating`, `pending`, `running`, `succeeded` or `failed`) and links to its Job and pod in Headlamp (`--headlamp <address>`) or the Kubernetes Dashboard (`--dashboard <address>`) |
| Its metrics | the plugin's: `jobs_started` by namespace, `jobs_by_image`, `jobs_ended` by outcome, `start_seconds` (to a running pod) and `job_seconds`; the service's, from its watch: `pods_running`, `pods_pending` and `pods` by phase, the same on every worker, so their panels combine them with `max`. A page of nine panels declared in its hello, one of each chart kind |
| The output | the ended pod's last log line `SCHEDR_OUTPUT: <json>`, read once the Job succeeded, never from the stream; without it, `null`; a log that can't be read fails the attempt |
| A failure | the pod's reason and exit code; a pod stuck on its image (`ErrImagePull`) fails at once, told first as its declared event `image_pull_failed` (`pod_stuck` for another reason it never starts) to channels subscribed to `EXTENSION_EVENT` |
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

On a worker, the service's user needs a kubeconfig (in `HOME`, or `--kubeconfig`; in a
cluster, the pod's service account) allowed to create, get, watch, patch and delete Jobs, pods,
pods' logs and Secrets in the namespace; `kubernetes_asyncio` installed beside schedr (the
example's own dependency, never schedr's). The same options for both:

```toml
[extensions.kubernetes]
command = ["/opt/schedr/schedr-ext-kubernetes", "--namespace", "batch"]  # also --context, --kubeconfig
service = ["/opt/schedr/schedr-ext-kubernetes", "--service", "--namespace", "batch"]
versions = ["1.0"]
```

`scripts/kind-extension.sh` runs `examples/extensions/kubernetes_extension.py` on a local kind
cluster: a pod's log while it runs, a success, a failure, a timeout, and a worker lost with its
plugin, whose Job the cleanup finds by its handle.

### The demo on kind

`make demo-kind` presents the plugin on a local kind cluster, `schedr-demo`, kept between runs.
It installs Headlamp, the Kubernetes SIG's web UI, in it and prints its address with an admin
login token. Then it runs a standalone here, in the foreground, whose worker has the plugin
(`--headlamp`, for its links). Its two DAGs (`examples/extensions/kubernetes_demo.py`) run a
Job every 2 minutes, one quick and one slower. Within a few minutes the UI shows:

| Where | What |
|---|---|
| A run's task | the pod's rows in the attempt's log while it runs, its phases, and links to the Job and pod in Headlamp |
| The Extensions page | the jobs started and ended (one run in six fails on purpose), the pods running, the time to a running pod and a Job's duration |
| Headlamp, `http://127.0.0.1:4466` | the Jobs and pods in the `schedr-demo` namespace, deleted once each attempt ends |

Ctrl-C stops the standalone and the port-forward. `make demo-kind-down` removes Headlamp and the
demo's Jobs and keeps the cluster; `DELETE_CLUSTER=1` deletes the cluster too. It needs `kind`,
`kubectl`, `helm` and podman (`CONTAINER=docker` for Docker), and port 8080 free.

The plugin links to the Kubernetes Dashboard's pages instead with `--dashboard <address>`; that
project was archived in January 2026.
