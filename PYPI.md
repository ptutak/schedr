# schedr

A job scheduler written in Rust that runs Python DAGs, Python scripts and shell commands: a
native Python API for DAGs, one binary for the scheduler, its web UI and its workers, SQLite on
a laptop and Postgres in production.

**Documentation:** https://ptutak.github.io/schedr/ ·
**Source:** https://github.com/ptutak/schedr

## Install

Python 3.14 or newer, in a virtual environment of a project directory:

```sh
mkdir my-schedr && cd my-schedr
python3.14 -m venv .venv
.venv/bin/pip install schedr          # add [pydantic] for Pydantic models
```

The wheel carries the `schedr` binary (Linux x86_64 and aarch64, macOS arm64): nothing to
compile.

## Run it

Put a DAG in `dags/sync.py`:

```python
from datetime import timedelta

from schedr import CommandOperator, Cron, Dag, StepDefaults, dag_ctx, task

with Dag(
    "sync_customers",
    description="Syncs customers daily at 03:00 Warsaw time: extract, load, notify.",
    trigger=Cron("0 3 * * *", tz="Europe/Warsaw", start="2026-01-01"),
    defaults=StepDefaults(retries=2, retry_delay=timedelta(minutes=5)),
):

    @task
    def extract() -> list[dict[str, str]]:
        return [{"id": "1", "name": "Ada"}, {"id": "2", "name": "Grace"}]

    @task(timeout=timedelta(minutes=10))
    def load(rows: list[dict[str, str]]) -> int:
        print(f"loading {len(rows)} customers")
        return len(rows)

    rows = extract()  # Output[list[dict[str, str]]]
    count = load(rows)  # load depends on extract (data flow)

    CommandOperator(  # depends on load: its command uses `count`
        "notify",
        command=["echo", t"synced {count} customers for {dag_ctx.run.planned_for}"],
    )
```

Start schedr (the scheduler, the web UI and one worker, in one process, on SQLite):

```sh
SCHEDR_DAG_PATH=dags .venv/bin/schedr standalone --migrate
```

The web UI is at http://127.0.0.1:8080/. In a second terminal:

```sh
.venv/bin/schedr dags list                      # sync_customers, paused
.venv/bin/schedr runs trigger sync_customers    # a manual run
.venv/bin/schedr runs list sync_customers
.venv/bin/schedr dags unpause sync_customers    # daily at 03:00 from now on
```

Ctrl-C stops it; the next start keeps the DAGs, runs and logs
(`~/.local/share/schedr`).

## Next

- [Getting started](https://ptutak.github.io/schedr/01-start/index.html): these steps, explained
- [Writing DAGs](https://ptutak.github.io/schedr/02-dags/index.html): tasks, data flow,
  schedules, retries; `schedr dags check` and `schedr dags test` without a server
- [Deployment](https://ptutak.github.io/schedr/03-guide/10-deployment.html): the images and
  the Helm chart, Postgres, separate roles
- [Changelog](https://github.com/ptutak/schedr/blob/main/CHANGELOG.md)
