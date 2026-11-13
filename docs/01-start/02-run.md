# Run schedr

`schedr standalone` runs everything in one process: the master (the scheduler), the
webserver with the web UI, and one worker that runs the tasks. On a laptop it keeps its state in
SQLite, so there is no database to set up.

## A DAG to run

In the project directory from [Install](01-install.md), make a `dags` directory with one DAG
file in it, `dags/sync.py`:

```{literalinclude} ../../examples/sync.py
:language: python
:caption: dags/sync.py
```

[A first DAG](../02-dags/01-first-dag.md) explains what each line does.

## Start it

```sh
SCHEDR_DAG_PATH=dags .venv/bin/schedr standalone --migrate
```

- **`SCHEDR_DAG_PATH`** names the directories schedr watches for DAG files (several are
  separated by `:`). Without it, standalone watches none: it starts with the warning "no DAG
  directories", and has nothing to run.
- **`--migrate`** creates the database on the first start, and updates it after an upgrade.
- **Python:** tasks and DAG files run with the project's `.venv`, found by itself (an active
  virtualenv, then `.venv` or `venv` in this directory or a parent, then `python3`). The first
  lines of the log say which one it took.
- **Where things go:** the database, the task logs and the journal are in
  `~/.local/share/schedr` (`$XDG_DATA_HOME/schedr` if that is set).
- **Addresses:** the API and the web UI on `http://127.0.0.1:8080`, from this machine only.

Leave it running and use a second terminal for the rest. A new DAG file is picked up within
30 s, the default scan interval.

```{note}
After a crash (not a Ctrl-C), schedr waits up to about 30 s before it schedules anything: the
crashed process's lease on the database has to run out first. The log says
"another master holds the lease; waiting as a standby", then "leading" when it takes over.
```

## See it

Open `http://127.0.0.1:8080/` for the web UI: the DAG, its runs and their graphs, task logs.
Or ask from the command line, in the same directory:

```sh
.venv/bin/schedr dags list
```

```text
DAG             STATE   HELD  TRIGGER                                   DEFINED IN
sync_customers  paused  no    0 3 * * * (Europe/Warsaw), 2025-12-31 ..  dags:sync.py
```

A new DAG starts **paused**: the scheduler creates no runs of it until you unpause it.

## Run it

Trigger a manual run (a paused DAG can still be run by hand), then look at it:

```sh
.venv/bin/schedr runs trigger sync_customers
.venv/bin/schedr runs list sync_customers
.venv/bin/schedr runs show <run-id>
.venv/bin/schedr tasks logs <run-id> notify
```

```text
TASK     STATE    OUTCOME  TRIES  INSTANCES  REASON
extract  success  SUCCESS  1
load     success  SUCCESS  1
notify   success  SUCCESS  1
```

To have the scheduler run it at 03:00 Warsaw time every day:

```sh
.venv/bin/schedr dags unpause sync_customers
```

Unpausing creates the latest fire time's run only, not every day since `start`: catch-up is
off unless the trigger asks for it ([Schedules](../02-dags/04-schedules.md)).

## Stop it

Ctrl-C stops it: the worker drains (no new tasks; running ones get up to 30 s), then the
process gives up its lease on the database and exits, so the next start takes over at once.
Starting it again keeps everything: the DAGs, the runs, the logs.

The web UI signs no one in here. That is fine on `127.0.0.1`; before you open it to a network,
read [Network and security: Sign-in](../03-guide/08-network.md#sign-in).
