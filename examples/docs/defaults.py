"""Defaults for every task, with one task overriding two of them."""

from datetime import timedelta

from schedr import Dag, MiB, TaskDefaults, task

with Dag(
    "defaults_demo",
    trigger=None,
    defaults=TaskDefaults(
        retries=2,
        retry_delay=timedelta(minutes=5),
        timeout=timedelta(minutes=30),
        max_output_size=8 * MiB,
    ),
):

    @task
    def usual() -> None:
        print("usual")

    @task(timeout=timedelta(minutes=1), retries=0)
    def quick() -> None:
        print("quick")

    usual()
    quick()
