# How schedr works

How the running system behaves, as built so far: which process does what, how a run moves
through them, what the web UI shows, and what happens when one of them fails. [Writing
DAGs](../02-dags/index.md) is how to define a DAG. The **Design** section has the reasons
behind each choice and the alternatives that weren't chosen; the **Reference** section has
every configuration key, command, endpoint and metric, generated from the code.

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
```
