"""Start a load from a report, passing its region to a chained DAG."""

from dataclasses import dataclass
from datetime import timedelta

from schedr import Chained, Dag, Failed, TimedOut, TriggerDagOperator, dag_ctx, task


@dataclass
class LoadParams:
    """What a load run is given; the default lets it run alone too."""

    region: str = "eu"


@task
def load(region: str) -> str:
    return f"loaded {region}"


@task
def report(run_id: str) -> str:
    return f"load run {run_id} succeeded"


@task
def failed(result: Failed) -> str:
    return f"load run {result.run_id} ended {result.state}"


@task
def late(result: TimedOut) -> str:
    del result
    return "load is still going or finished late"


with Dag(
    "chained_load",
    description="Loads the region a chained_report run asks for.",
    trigger=Chained(by=["chained_report"]),
    params=LoadParams,
):
    region: str = dag_ctx.params.region  # pyright: ignore[reportAny]
    load(region)

with Dag("chained_report", description="Builds a report, then starts chained_load for its region."):
    started = TriggerDagOperator(
        "start_load", dag="chained_load", params={"region": "eu"}, max_wait=timedelta(minutes=5)
    )
    report(started.on(str))
    failed(started.on(Failed))
    late(started.on(TimedOut))
