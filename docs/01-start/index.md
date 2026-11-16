# Getting started

Install `schedr`, start it with one DAG, run that DAG, and see it in the web UI.

## Install

`schedr` needs **Python 3.14** or newer. Install it into a virtual environment in a project
directory of its own; schedr finds that environment by itself later.

```sh
mkdir my-schedr && cd my-schedr
python3.14 -m venv .venv
.venv/bin/pip install schedr          # add [pydantic] for Pydantic models
```

With [uv](https://docs.astral.sh/uv/) the same is `uv venv --python 3.14` and
`uv pip install schedr`.

The package carries the `schedr` binary, so there is nothing to compile. Wheels exist for
Linux x86_64 (glibc and musl), Linux aarch64 and macOS arm64. Check it:

```sh
.venv/bin/schedr --version
```

### Images

For Kubernetes or Docker, the images are on `ghcr.io`, x86_64:

| Image | What it has | For |
|---|---|---|
| `ghcr.io/ptutak/schedr` | Python, schedr, the task libraries and connection clients | standalone, workers, the base for your own image |
| `ghcr.io/ptutak/schedr-slim` | the same without the task libraries and connection clients | a smaller base |
| `ghcr.io/ptutak/schedr-server` | the binary alone | the master, the webserver and the UI |

Each is built on Debian (glibc), tagged with the version (`1.1.0`) and its series (`1.1`);
the same on Alpine adds `-alpine` (`1.1.0-alpine`). There is no `latest`: a deployment names the
version it runs. [Deployment](../03-guide/10-deployment.md) covers the Helm chart.

## Run schedr

`schedr standalone` runs everything in one process: the master (the scheduler), the
webserver with the web UI, and one worker that runs the tasks. On a laptop it keeps its state in
SQLite, so there is no database to set up.

### A DAG to run

In the project directory from [Install](#install), make a `dags` directory with one DAG file
in it, `dags/sync.py`:

```{literalinclude} ../../examples/sync.py
:language: python
:caption: dags/sync.py
```

[A first DAG](../02-dags/01-first-dag.md) explains what each line does.

### Start it

```sh
.venv/bin/schedr standalone --migrate --dag-path dags
```

- **`--dag-path dags`** is the folder schedr watches for DAG files, named after it (`dags`);
  repeat it for more. Without it (or `SCHEDR_DAG_PATH`, the same list from the environment),
  standalone watches none: it starts with the warning "no DAG directories", and has nothing to
  run.
- **`--migrate`** creates the database on the first start, and updates it after an upgrade.
- **Python:** tasks and DAG files run with the project's `.venv`, found by itself (an active
  virtualenv, then `.venv` or `venv` in this directory or a parent, then `python3` beside
  the `schedr` binary or on `PATH`). The first lines of the log say which one it took.
- **Where things go:** the database, the task logs and the journal are in
  `~/.local/share/schedr` (`$XDG_DATA_HOME/schedr` if that is set).
- **Addresses:** the API and the web UI on `http://127.0.0.1:8080`, from this machine only.

Leave it running and use a second terminal for the rest. A new DAG file shows up within about
35 s: schedr looks for changes every 30 s, and waits for a file to stay unchanged for 5 s.

```{note}
After a crash (not a Ctrl-C), schedr waits up to about 30 s before it schedules anything: the
crashed process's lease on the database has to run out first. The log says
"another master holds the lease; waiting as a standby", then "leading" when it takes over.
```

### See it

Open `http://127.0.0.1:8080/` for the web UI: the DAG, its runs and their graphs, task logs.
Or ask from the command line, in the same directory:

```sh
.venv/bin/schedr dags list
```

```text
DAG             STATE   HELD  TRIGGER                                   DEFINED IN
sync_customers  paused  no    0 3 * * * (Europe/Warsaw), 2025-12-31 ..  dags:sync.py
```

A new DAG starts **paused**: its trigger creates no runs until you unpause it. Manual runs
are still allowed.

### Run it

Trigger a manual run (a paused DAG can still be run by hand), then look at it:

```sh
.venv/bin/schedr runs trigger sync_customers
.venv/bin/schedr runs list sync_customers
.venv/bin/schedr runs show <run-id>
.venv/bin/schedr tasks logs <run-id> notify
```

After the run finishes, the task table in `runs show` looks like this:

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

### Stop it

Ctrl-C stops it: the worker takes no new tasks and gives running ones about 25 s to finish
(the details: [Workers](../03-guide/05-workers.md#draining)), then the process gives up its
lease on the database and exits, so the next start takes over at once.
Starting it again keeps everything: the DAGs, the runs, the logs.

The web UI signs no one in here. That is fine on `127.0.0.1`; before you open it to a network,
read [Network and security: Sign-in](../03-guide/08-network.md#sign-in).

## Other layouts

### DAG files in the project folder

DAG files can live directly in the project folder. Use `--dag-path .` and exclude the
virtual environment with `.schedrignore` (gitignore syntax). Otherwise, `schedr` copies
`.venv` with the DAG root and imports Python files in it whose bytes contain `schedr`.

```text
my-schedr/
├── .schedrignore
├── .venv/
└── sync.py
```

```{code-block} text
:caption: .schedrignore
.venv/
```

```sh
.venv/bin/schedr standalone --migrate --dag-path .
```

The root takes the folder's name: `schedr dags list` shows the definition at
`my-schedr:sync.py`. Exclude notes, data and test outputs with `.schedrignore` too. Files
that remain in the scan count toward the code hash, so changing them creates a new code
version.

### A plain script

To run a script without writing a DAG, declare a job in `schedr.toml`. Put both files in
a script folder, such as `scripts`:

```text
my-schedr/
├── .venv/
└── scripts/
    ├── hello.py
    └── schedr.toml
```

```python
# scripts/hello.py
print("hello from a script")
```

```toml
# scripts/schedr.toml
[jobs.hello]
script = "hello.py"
interpreter = "python"
schedule = "0 6 * * *"
```

```sh
.venv/bin/schedr standalone --migrate --script-path scripts
```

`interpreter = "python"` selects the built-in Python interpreter, which finds the project
virtual environment as described above. A scheduled job becomes a one-task DAG and starts
paused. You can run it by hand while paused. Its ID combines the root, declaration file and
job name:

```sh
.venv/bin/schedr runs trigger 'scripts:schedr.toml#hello'
.venv/bin/schedr runs list 'scripts:schedr.toml#hello'
.venv/bin/schedr tasks logs <run-id> hello
```

Use `--dag-path` and `--script-path` together to load both kinds of root; repeat either
flag for more folders. [Plain scripts](../02-dags/17-plain-scripts.md) lists the job settings,
including arguments, environment variables, secrets, retries and parameters.

## Next steps

- **Write DAGs:** [Writing DAGs](../02-dags/index.md), starting with
  [A first DAG](../02-dags/01-first-dag.md): tasks, data between them, schedules, retries.
- **Test them without a server:** `schedr dags check` and `schedr dags test`, in
  [Testing a DAG](../02-dags/07-testing.md), also from pytest.
- **See how it works:** [How schedr works](../03-guide/index.md): the life of a run, the
  master and workers, failures.
- **Run it for real:** [Deployment](../03-guide/10-deployment.md): standalone in one pod on
  SQLite, then Postgres, then separate roles, with the Helm chart.
- **Every setting and command:** the [configuration](../_generated/04-reference/01-configuration.md)
  and [CLI](../_generated/04-reference/02-cli.md) references.
