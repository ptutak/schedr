# How schedr works

This guide explains which process does what, how a run moves through the system, what the
web UI shows, and what happens when a part fails.

[Writing DAGs](../02-dags/index.md) explains how to define a DAG. The **Design** section
explains the choices behind the system. The **Reference** section lists configuration keys,
commands, endpoints and metrics, generated from the code.

Read [Overview](01-overview.md) first; the other pages each go into one part.

```{toctree}
:maxdepth: 2

01-overview
02-glossary
03-life-of-a-run
04-master
05-workers
06-parsing
07-failures
08-network
09-observability
10-deployment
11-stress
12-ui
13-secrets
14-timings
15-protocol
16-notifications
17-master-clock
18-guarantees
19-masters-round
20-simulator
```
