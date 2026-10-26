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

- **master:** round durations (`schedr_master_tick_duration_seconds`), runs, tasks and workers
  by state, tasks waiting by reason, assignments, rejections, attempts ended, parse jobs,
  commands and how long they waited for a decision;
- **worker:** slots and slots used, attempts by status, watchdog kills, journal records, log
  disk usage;
- **webserver:** requests by route and status, log proxy errors by kind;
- **every role:** process memory, CPU, open files, start time (absent without `/proc`, so on
  macOS), and the build version.

Counters and histograms are updated where things happen. The gauges that need the database
(runs, tasks and workers by state) are refreshed by the master's loop every 15 s, with one
read-only query. **A scrape reads only memory**: never the database, never the master's lock.
Labels are bounded (states, kinds, reasons, `dag_id`), never a run, task or attempt ID.

## schedr's own logs

Each role logs to stdout. `[log] format` is `auto` (JSON when stdout isn't a terminal, as in a
pod; readable text in a terminal), `json` or `text`. `[log] level` takes `RUST_LOG`-style
filters. These are separate from **task logs**, which are files on the worker (see
[Workers](05-workers.md)).
