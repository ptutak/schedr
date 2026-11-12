"""Run parameters: a class on the DAG, a field passed into a task, the class read inside one."""

from dataclasses import dataclass, field
from datetime import date, timedelta

from schedr import CommandOperator, Cron, Dag, dag_ctx, task


@dataclass
class ReportParams:
    """The values a run of `report_params` is given."""

    region: str = "eu"
    full: bool = False
    day: date | None = None
    limits: list[int] = field(default_factory=list)
    window: timedelta = timedelta(hours=1)
    start: date = date(2026, 1, 1)


@task
def extract(region: str, day: date | None, limits: list[int]) -> str:
    text = "all" if day is None else day.isoformat()
    print(f"{region} {text} {limits}")
    return f"{region} {text}"


@task
def summary(params: ReportParams) -> str:
    text = f"{params.region} full={params.full} {len(params.limits)}"
    print(text)
    return text


with Dag(
    "report_params",
    description="Builds a nightly report; a manual run can pick its parameters.",
    trigger=Cron("0 3 * * *"),
    params=ReportParams,
):
    # A field is untyped (Any), so a strict checker reports the read; the names below are typed.
    region: str = dag_ctx.params.region  # pyright: ignore[reportAny]
    day: date | None = dag_ctx.params.day  # pyright: ignore[reportAny]
    limits: list[int] = dag_ctx.params.limits  # pyright: ignore[reportAny]
    start: date = dag_ctx.params.start  # pyright: ignore[reportAny]
    found = extract(region=region, day=day, limits=limits)
    summary(dag_ctx.params(ReportParams))
    CommandOperator(
        "publish",
        ["echo", t"{found}", t"{start:%Y-%m-%d}", t"{region}"],
    )
