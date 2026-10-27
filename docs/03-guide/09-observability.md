# Observability

## The ops port

Every role serves a separate plain-HTTP port, `[ops] port` (8090), with three endpoints
only, and never data or actions. That's why it needs no TLS or credentials: kubelet probes and
Prometheus can't present a client certificate. The ops port starts **first**, before the
database, so probes get an answer while a role starts. `schedr standalone` serves one ops port
for all its parts.

| Endpoint | Answers 200 when | Otherwise |
|---|---|---|
| `/healthz` (liveness) | every loop the role watches has turned within `[ops] loop_stall_timeout` (2 min) | 503, "the scheduling loop hasn't turned for 150 s (limit 120 s)" |
| `/readyz` (readiness) | every part of the role has started | 503, "starting: master" |
| `/metrics` | always | |

**Liveness never asks another component.** If it failed whenever the database or the master
was down, Kubernetes would restart the master or every worker in a loop, killing running tasks
for nothing. So it checks only the role's own loops: the master's scheduling loop and the
worker agent's event loop. Each **beats** on a monotonic clock, at every turn and at least
every 5 s while it waits. A wall clock that steps can't make a loop look stuck. The webserver
has no loop, so it is alive while it answers.

**Readiness** waits for each part to start. The master is ready after its schema check,
registry restore and gRPC listener; the worker after recovering its journal; the webserver
once it listens. It doesn't check the database either: with one master, taking it out of
its Service helps nobody.

## Metrics

`/metrics` serves Prometheus metrics in the OpenMetrics text format. The
[metrics reference](../_generated/04-reference/04-metrics.md) lists every metric, generated from
the code. Roughly:

- **master:** round durations (`schedr_master_tick_duration_seconds`), busy and quiet DAGs
  (`schedr_master_dags{state}`), quiet DAGs woken (`schedr_master_dag_wakes_total{reason}`),
  runs, tasks and workers by state, tasks waiting by reason, assignments, rejections,
  attempts ended, parse jobs, commands and how long they waited for a decision;
- **worker:** slots and slots used, attempts by status, how long a Python task took to start
  (`schedr_worker_task_start_seconds{method,phase}`), fork servers
  (`schedr_worker_fork_servers{interpreter,kind,state}`), tasks a fork server didn't fork
  (`schedr_worker_fork_fallbacks_total{interpreter,reason}`), watchdog kills, journal
  records, log disk usage;
- **webserver:** requests by route and status, log proxy errors by kind;
- **every role:** process memory, CPU, open files, start time (absent without `/proc`, so on
  macOS), and the build version.

`schedr_master_tick_duration_seconds` has 29 buckets, each √2 times the last, from 1 ms to
about 16 s. A round's p99 one bucket worse is about 1.4 times the previous bucket, so
[scenario 14](11-stress.md) can tell that from a doubling. `schedr_master_dags` counts DAGs
that can run by `state`: `busy` (visited every round) or `quiet` (visited when woken).
`schedr_master_dag_wakes_total` counts quiet DAGs woken, by `reason`: `timer`, `command`,
`run_queued`, `run_ended`, `new_version`, `sweep`. `sweep` increments only when the sweep or
the ID check found the DAG with something to do (a run running or queued, or one that round
created). That is a missed wake, and the counter staying at zero is the healthy case. See
[Busy and quiet DAGs](04-master.md#busy-and-quiet-dags).

`schedr_worker_task_start_seconds` is one histogram (10 ms to about 40 s). It is how M2.5
judges the fork server and which libraries to preload: the same series, split by how the
process was started, before and after. `method` is `spawn` (a new process) or `fork` (a
process from the fork server). A Linux worker with `fork_server = true` on its DAG
interpreter records `fork` when a fork server forked the task and the runner said hello (see
[The fork server](05-workers.md#the-fork-server)). `phase` is `runner`
(from starting the process to the runner's hello: the interpreter and `schedr`) or `import`
(from sending the task to the runner's `task_imported`: the DAG file imported and the
arguments built). The import phase is left out when that fails. The agent times both on its
own clock; the runner only sends the message. A command task is not in the histogram.

`schedr_worker_fork_servers` is a gauge: one series per interpreter, kind and state. `kind`
is `preloaded` or `bare`. `state` is `up`, `down` or `disabled`. The current state is 1 and
the other two are 0. The three series exist from the moment the worker starts that server,
so a server that is still starting is `down`. `interpreter` is the name in
`[dags] interpreter`. With `fork_server` off, and on macOS, there are no series.

`schedr_worker_fork_fallbacks_total` counts each time a fork server did not fork a Python
task. `reason` is `disabled` (the server is disabled), `down` (it isn't taking requests),
`fork_failed` (it refused the fork) or `shim_died` (the shim ended before
`process_started`). The next server, or a spawned process, runs the task. A task the first
server forks does not add to the counter.

Counters and histograms are updated where things happen. The gauges that need the database
(runs, tasks and workers by state) are refreshed by the master's loop every 15 s, with one
read-only query. **A scrape reads only memory**: never the database, never the master's lock.
Labels are bounded (states, kinds, reasons, `dag_id`, the interpreter's name, and `method`
/ `phase` on the start histogram), never a run, task or attempt ID.

## schedr's own logs

Each role logs to stdout. `[log] format` is `auto` (JSON when stdout isn't a terminal, as in a
pod; readable text in a terminal), `json` or `text`. `[log] level` takes `RUST_LOG`-style
filters. These are separate from **task logs**, which are files on the worker (see
[Workers](05-workers.md)).
