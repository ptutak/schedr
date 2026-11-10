# A greenhouse, end to end

This example puts four DAGs together. Devices in two greenhouse zones write readings to
files. The first DAG stores them and raises alarms for hot devices. A second diagnoses each
alarm and waits for an operator. An acknowledgement starts maintenance; an unanswered alarm
pages the operator. A fourth DAG rolls up the readings by zone.

The shared models, SQLite connection and pager are in `examples/showcase/dags/greenhouse.py`.
The files and database live under `/tmp/schedr-greenhouse`.

## Run it

In one terminal, start the standalone with the showcase's DAG root:

```sh
make standalone DAGS=examples/showcase/dags
```

`make standalone` alone loads `examples/`, not the showcase: `examples/.schedrignore` keeps
`showcase/` out of that root. In another terminal, run:

```sh
make showcase
```

This unpauses the four DAGs and starts the simulator and a pager receiver. Open the UI at
http://localhost:8080 and watch the runs and their graphs. The simulator writes three rounds
of readings, including a corrupt file and two hot devices. It acknowledges `dev-a1` and
leaves `dev-b2` unanswered. Watch the maintenance run for the first and the timeout for the
second. The receiver prints pager requests in the second terminal. The rollup runs every five
minutes. Ctrl-C stops the simulator; stop the standalone separately with Ctrl-C.

## Ingest readings

```{literalinclude} ../../examples/showcase/dags/ingest.py
:language: python
:caption: examples/showcase/dags/ingest.py
```

A [poke trigger](19-triggers.md#batching-pokes) batches new files. A
[resource](16-resources.md) supplies a scratch folder, and [mapping](10-mapping.md) validates
each file. [Outcomes](08-outcomes.md) send good readings to the SQLite
[connection](14-connections.md) and corrupt files to quarantine. `detect` gathers the parsed
results, applies the run's [threshold parameter](12-params.md), and posts an
[`anomaly` event](22-events.md#posting-from-a-task) for each hot device. Its key combines the
device and reading time, so a later alarm can start another run. The task also has a hard
[memory limit](02-tasks.md#memory).

## Respond to an alarm

```{literalinclude} ../../examples/showcase/dags/response.py
:language: python
:caption: examples/showcase/dags/response.py
```

Each new `anomaly` key [starts a run](22-events.md#starting-a-run). Two probes race, and
[`first_of`](20-first-of.md) supplies the first answer for the diagnosis. The run then
[waits for an `ack` event](22-events.md#waiting-in-a-run), keyed by device. An acknowledgement
supplies the action for a [chained maintenance run](23-chained-runs.md), which the response
waits for. A timeout or failed maintenance sends a [pager notification](24-notifications.md).

## Maintain a device

```{literalinclude} ../../examples/showcase/dags/maintenance.py
:language: python
:caption: examples/showcase/dags/maintenance.py
```

The [chained trigger](23-chained-runs.md) accepts runs from `anomaly_response`. A
[command task](02-tasks.md#commands) receives the device and action as parameters and a token
from [secrets](13-secrets.md). It registers a [cleanup handle](15-cleanup.md#a-command-or-script)
and uses its exit code to route to a remote-fix report or a visit ticket. Both paths join at
`closed`.

## Roll up the readings

```{literalinclude} ../../examples/showcase/dags/rollup.py
:language: python
:caption: examples/showcase/dags/rollup.py
```

Every five minutes, a [scheduled run](04-schedules.md) covers its run window. A
[sensor](18-sensors.md) waits for readings; if it times out, `quiet` records the empty window.
Otherwise, a [group](11-groups.md) summarizes each zone, and a
[branch](08-outcomes.md#branch) chooses whether to open vents for warm zones or report that
all are normal. Past windows can be [backfilled](04-schedules.md#backfill).

## Test the DAGs

`examples/showcase/test_showcase.py` uses [`run_dag`](07-testing.md#run_dag) to run each DAG
on its own. It checks ingestion and quarantine, an unanswered alarm, both maintenance outcomes,
and rollups with and without readings. The test supplies its own events, connection and secret
where needed. It sets `GREENHOUSE_FAST=1` for shorter waits and passes that name through
`[worker] pass_env`, because a worker's processes get a clean environment.
