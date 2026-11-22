"""The Kubernetes extension's demo (M5.2i7): ``make demo-kind`` runs these on a kind cluster.

Every 2 minutes a Job's pod counts a few rows, logging each (the attempt's log shows them while
it runs), and prints its output as its last line; one run in six fails, so the extension's page
has failed Jobs to show too. A second DAG runs a slower pod, so the "pods running" panel has
something running most of the time.
"""

from schedr import Cron, Dag, ExtensionTask, task

IMAGE = "busybox:1.36"

# Counts 3 to 8 rows a second apart; a run of 3 (one in six) exits 1 before its output.
COUNT = "; ".join(
    [
        "n=$(awk 'BEGIN { srand(); print 3 + int(rand() * 6) }')",
        'for i in $(seq 1 "$n"); do echo "row $i of $n"; sleep 1; done',
        'if [ "$n" = 3 ]; then echo "giving up on purpose"; exit 1; fi',
        'echo "SCHEDR_OUTPUT: {\\"rows\\": $n}"',
    ]
)

with Dag(
    "kind_demo",
    description="A Job every 2 minutes: its pod's rows in the log, its count the output.",
    trigger=Cron("*/2 * * * *"),
):
    counted = ExtensionTask(
        "count",
        name="kubernetes",
        version="1.0",
        payload={
            "image": IMAGE,
            "command": ["sh", "-c", COUNT],
            "resources": {"limits": {"memory": "32Mi", "cpu": "100m"}},
        },
        returns=dict[str, int],
        cleanup=True,
    )

    @task
    def report(found: dict[str, int]) -> str:
        """Says what the pod counted."""
        return f"{found['rows']} rows"

    report(counted.output)

with Dag(
    "kind_demo_slow",
    description="A slower pod every 2 minutes: 90 seconds of work.",
    trigger=Cron("1-59/2 * * * *"),
):
    ExtensionTask(
        "work",
        name="kubernetes",
        version="1.0",
        payload={
            "image": IMAGE,
            "command": ["sh", "-c", 'for i in $(seq 1 9); do echo "step $i"; sleep 10; done'],
            "resources": {"limits": {"memory": "32Mi", "cpu": "100m"}},
        },
        cleanup=True,
    )
