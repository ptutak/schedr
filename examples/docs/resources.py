"""A cluster made for a run, and removed after everything that uses it."""

from dataclasses import dataclass

from schedr import Dag, EmptyOperator, group, resource, task


@dataclass
class Cluster:
    """What the setup made."""

    cluster_id: str


@resource
def cluster(size: int) -> Cluster:
    """The setup: what it returns is the resource."""
    return Cluster(f"cluster-{size}")


@cluster.cleanup(retries=5)
def delete_cluster(made: Cluster) -> None:
    """The cleanup: it gets what the setup returned."""
    print(f"deleting {made.cluster_id}")


@task
def load(made: Cluster) -> int:
    """Uses the resource through its value."""
    return len(made.cluster_id)


@task
def warm_cache() -> None:
    """Needs the resource, not its value."""


with Dag(
    "cluster_job",
    description="Makes a cluster for the run, and removes it once everything using it is done.",
):
    with group("work") as work:
        made = cluster(8)
        load(made)
        warm_cache.override(uses=[made])()
    # Waits for `work` and for the cluster's cleanup.
    work >> EmptyOperator("report")
