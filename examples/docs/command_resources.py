"""A command reports a resource value; its cleanup reads it three ways."""

from dataclasses import dataclass

from schedr import CommandResource, Dag, task


@dataclass
class Cluster:
    """Every field is reported, even those with defaults."""

    cluster_id: str
    region: str = "eu"
    size: int = 8


@task
def load(made: Cluster) -> int:
    """Uses the reported value, decoded as a Cluster."""
    return made.size


with Dag("command_cluster_job"):
    cluster = CommandResource(
        "cluster",
        [
            "schedr",
            "task-ctx",
            "resource",
            "set",
            "cluster_id=cluster-8",
            "region=eu",
            "size:=8",
        ],
        value=Cluster,
    )
    cluster.cleanup_command(
        [
            "sh",
            "-c",
            'schedr task-ctx resource get --field cluster_id && printf "deleting %s in %s\\n" '
            + '"$1" "$REGION"',
            "cleanup",
            t"{cluster.cluster_id}",
        ],
        env={"REGION": t"{cluster.value.region}"},
        retries=5,
    )
    load(cluster.value)
