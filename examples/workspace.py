"""A resource: a scratch directory made for a run, and removed once nothing needs it.

The setup makes the directory; the tasks given its value use it; the cleanup removes it after
them, whether they succeeded or failed. ``report`` waits for the cleanup.
"""

import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

from schedr import Dag, dag_ctx, group, resource, task


@dataclass(frozen=True)
class Workspace:
    """What the setup made: the directory the run's tasks share."""

    path: str


@resource
def workspace() -> Workspace:
    """The setup: what it returns is the resource, passed to the tasks that use it."""
    path = tempfile.mkdtemp(prefix=f"schedr-{dag_ctx.dag_id}-")
    print(f"made {path}")
    return Workspace(path)


@workspace.cleanup(retries=3)
def remove_workspace(made: Workspace) -> None:
    """The cleanup: it gets what the setup returned, and runs once the users have ended."""
    shutil.rmtree(made.path, ignore_errors=True)
    print(f"removed {made.path}")


@task
def download(space: Workspace) -> list[str]:
    """Writes the day's files into the workspace."""
    names = ["customers.csv", "orders.csv"]
    for name in names:
        (Path(space.path) / name).write_text("id\n1\n2\n")
    return names


@task
def count_rows(space: Workspace, names: list[str]) -> int:
    """Reads them back."""
    return sum(len((Path(space.path) / name).read_text().splitlines()) - 1 for name in names)


@task
def report(rows: int) -> str:
    """Runs after the workspace is gone."""
    return f"imported {rows} rows"


with Dag("workspace_import"):
    with group("work") as work:
        space = workspace()
        rows = count_rows(space, download(space))
    # Waits for `work`, the workspace's cleanup included.
    work >> report(rows)
