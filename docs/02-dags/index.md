# Writing DAGs

How to define a DAG in Python with the `schedr` package: the tasks, the data between them,
how tasks are grouped, how they route by outcome, how a task runs once per element, when
runs start, the parameters a run is given, the secrets a task reads, how it cleans up, how to test the file, and how type checkers
read it. These pages describe the API as it runs.
[How schedr works](../03-guide/index.md) is what the system does with a DAG once it is stored.
[Python API structure](../_generated/05-design/07-python-api-structure.md) is why the API is
shaped this way, including features that are not built yet. Signatures are on the
[Python API](../_generated/07-api/01-python.md) page. Commands and configuration keys are in
the [Reference](../_generated/04-reference/02-cli.md).

A DAG file is ordinary Python. Importing it records the DAGs; it runs no task. The examples
are files under `examples/` and `examples/docs/`, quoted below, and each one is tested.

```{toctree}
:maxdepth: 1

01-first-dag
02-tasks
03-data-flow
04-schedules
05-defaults
06-context
07-testing
08-outcomes
09-type-checkers
10-mapping
11-groups
12-params
13-secrets
14-connections
15-cleanup
16-resources
17-plain-scripts
18-sensors
19-triggers
20-first-of
21-wait-for
22-events
```
