"""The dummy extension (decision 510) in DAGs, one for each thing it can show, and a Rust plugin.

Its plugin, ``examples/extensions/dummy/schedr-ext-dummy``, is found by ``make standalone`` (and
``standalone-separate-ui``, ``distributed``) through discovery: ``[extensions] discover = true``
with its folder on the worker's ``PATH``. Each task's payload is a list of steps for it.
"""

from datetime import timedelta

from schedr import Dag, ExtensionTask, secrets, task

with Dag("extension_runs", description="An extension task whose output a Python task reads."):
    job = ExtensionTask(
        "job",
        name="dummy",
        version="1.0",
        payload=[{"log": "working"}, {"sleep": 2}, {"return": {"rows": 3}}],
        returns=dict[str, int],
    )

    @task
    def report(found: dict[str, int]) -> str:
        """Says what the extension found."""
        return f"{found['rows']} rows"

    report(job.output)

with Dag("extension_secret", description="An extension task reading a declared secret."):
    ExtensionTask(
        "job",
        name="dummy",
        version="1.0",
        payload=[{"secret": ["etl/api", "token"]}, {"return": "read"}],
        secrets=[secrets["etl/api"]["token"]],
    )

with Dag("extension_cleanup", description="An extension task that fails; its cleanup runs."):
    ExtensionTask(
        "job",
        name="dummy",
        version="1.0",
        payload=[{"handle": {"id": "job-1"}}, {"fail": "the cluster said no"}],
        cleanup=True,
    )

with Dag("extension_timeout", description="An extension task that hangs, stopped at its timeout."):
    ExtensionTask(
        "job",
        name="dummy",
        version="1.0",
        payload={"hang": True},
        timeout=timedelta(seconds=5),
    )

with Dag("extension_unserved", description="A version no worker serves: it waits, unschedulable."):
    ExtensionTask("job", name="dummy", version="9.9", payload={"return": 1})

with Dag(
    "extension_two_languages",
    description="Two plugins in two languages: the Python dummy (discovered) and Rust's hello.",
):
    counted = ExtensionTask(
        "counted",
        name="dummy",
        version="1.0",
        payload=[{"log": "the Python plugin runs"}, {"return": {"rows": 3}}],
        returns=dict[str, int],
    )
    # crates/schedr-extension/examples/hello-plugin.rs, configured by path in make standalone.
    greeted = ExtensionTask(
        "greeted",
        name="hello",
        version="1.0",
        payload={"name": "schedr"},
        returns=dict[str, str],
    )

    @task
    def together(rows: dict[str, int], greeting: dict[str, str]) -> str:
        """What both plugins returned."""
        return f"{greeting['greeting']}: {rows['rows']} rows"

    together(counted.output, greeted.output)
