"""The Kubernetes extension (M5.2i2): each task a Job, run by ``kubernetes/schedr-ext-kubernetes``.

``scripts/kind-extension.sh`` runs these on a kind cluster: a worker configured with the plugin
(``[extensions.kubernetes]``) starts a Job per attempt, streams its pod's log into the
attempt's, and deletes it afterwards, or on a cancel or a timeout.
"""

from datetime import timedelta

from schedr import Dag, ExtensionTask, secrets, task

IMAGE = "busybox:1.36"

with Dag("pod_runs", description="A pod's task: its log streamed, its last line the output."):
    counted = ExtensionTask(
        "count",
        name="kubernetes",
        version="1.0",
        payload={
            "image": IMAGE,
            "command": [
                "sh",
                "-c",
                "; ".join(
                    [
                        'for i in 1 2 3; do echo "row $i"; sleep 1; done',
                        'echo "token is ${#TOKEN} characters"',
                        "echo 'SCHEDR_OUTPUT: {\"rows\": 3}'",
                    ]
                ),
            ],
            "secret_env": {"TOKEN": "etl/api#token"},
            "resources": {"limits": {"memory": "64Mi", "cpu": "250m"}},
        },
        returns=dict[str, int],
        secrets=[secrets["etl/api"]["token"]],
        cleanup=True,
    )

    @task
    def report(found: dict[str, int]) -> str:
        """Says what the pod counted."""
        return f"{found['rows']} rows"

    report(counted.output)

with Dag("pod_fails", description="A pod exiting 3: the task fails with the pod's reason."):
    ExtensionTask(
        "fail",
        name="kubernetes",
        version="1.0",
        payload={"image": IMAGE, "command": ["sh", "-c", "echo failing; exit 3"]},
        cleanup=True,
    )

with Dag("pod_timeout", description="A pod past the task's timeout: its Job deleted."):
    ExtensionTask(
        "hang",
        name="kubernetes",
        version="1.0",
        payload={"image": IMAGE, "command": ["sh", "-c", "echo waiting; sleep 600"]},
        timeout=timedelta(seconds=20),
        cleanup=True,
    )

with Dag("pod_long", description="A long pod: stop its worker and the cleanup finds it."):
    ExtensionTask(
        "long",
        name="kubernetes",
        version="1.0",
        payload={"image": IMAGE, "command": ["sh", "-c", "echo long; sleep 600"]},
        cleanup=True,
    )
