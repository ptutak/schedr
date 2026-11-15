# Writing DAGs

Write DAGs with the `schedr` Python API: define tasks, pass data between them, set schedules
and run parameters, and test the result. Later pages cover outcomes, mapping, groups,
secrets, connections, cleanup, resources and events. Start with [A first DAG](01-first-dag.md),
or use the list below to find a topic.

These pages describe the API as it runs. [How schedr works](../03-guide/index.md) explains
what happens after a DAG is stored. The [Python API](../_generated/07-api/01-python.md)
lists signatures; the [CLI reference](../_generated/04-reference/02-cli.md) lists commands,
and the [configuration reference](../_generated/04-reference/01-configuration.md) lists settings.
[Python API structure](../_generated/08-history/project/07-python-api-structure.md) records
the design, including features that are not built yet.

A DAG file is ordinary Python. Importing it records the DAGs; tasks called inside a
`with Dag(...)` block are recorded for later execution. The examples are tested files under
`examples/`, `examples/docs/` and `examples/showcase/`, included in the pages below.

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
23-chained-runs
24-notifications
25-showcase
```
