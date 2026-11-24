"""120 DAGs in one file, for the DAGs page's numbered pages (decision 517)."""

from schedr import Dag, task


@task
def noop() -> None:
    """Nothing: the DAGs are listed, not run."""


for number in range(120):
    with Dag(f"page_{number:03}"):
        noop()
