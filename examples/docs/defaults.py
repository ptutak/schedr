"""Defaults for every task, with one task overriding two of them."""

from datetime import timedelta

from schedr import Dag, MiB, StepDefaults, task

with Dag(
    "defaults_demo",
    description="Sets retries and timeouts for every task, with one task overriding two of them.",
    trigger=None,
    defaults=StepDefaults(
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
