"""Start a chained command run per region and gather the run IDs."""

from dataclasses import dataclass

from schedr import (
    Chained,
    CommandOperator,
    Dag,
    Failed,
    TimedOut,
    TriggerDagOperator,
    dag_ctx,
    task,
)


@dataclass
class RegionParams:
    """The target's params: the region to greet."""

    region: str = "eu"


@task
def regions() -> list[dict[str, str]]:
    return [{"region": "eu"}, {"region": "us"}]


@task
def successful_ids(results: list[str | Failed | TimedOut]) -> list[str]:
    return [result for result in results if isinstance(result, str)]


with Dag("region_command", trigger=Chained(by=["mapped_regions"]), params=RegionParams):
    region: str = dag_ctx.params.region  # pyright: ignore[reportAny]
    CommandOperator("greet", command=["printf", "hello %s", region])

with Dag("mapped_regions"):
    runs = TriggerDagOperator.partial("start_regions", dag="region_command").expand(
        params=regions()
    )
    successful_ids(runs.output)
