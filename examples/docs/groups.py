"""Task groups: one function reused per source, shared settings, a group as one dependency."""

from datetime import timedelta

from schedr import Dag, EmptyOperator, TaskDefaults, group, task

with Dag("grouped_load", trigger=None):

    @task
    def extract(source: str) -> list[str]:
        return [f"{source}-1", f"{source}-2"]

    @task
    def load(rows: list[str]) -> int:
        print(f"loaded {len(rows)}")
        return len(rows)

    @task
    def report(eu: int, us: int) -> None:
        print(f"eu {eu}, us {us}")

    start = EmptyOperator("start")
    loaded: dict[str, int] = {}
    for source in ("eu", "us"):
        # Each group's tasks are `eu.extract`, `eu.load`, ...; retries for these only.
        defaults = TaskDefaults(retries=2, retry_delay=timedelta(seconds=30))
        with group(source, defaults=defaults) as per_source:
            loaded[source] = load(extract(source))
        start >> per_source  # before the group's roots: `eu.extract`
    report(loaded["eu"], loaded["us"])
