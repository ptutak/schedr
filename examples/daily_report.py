"""A DAG whose tasks read the run's context: ``dag_ctx`` in task code and in a command."""

from dataclasses import dataclass
from datetime import date

from schedr import CommandOperator, Cron, Dag, dag_ctx, task


@dataclass(frozen=True)
class Report:
    """Which report a run builds."""

    day: date
    path: str


with Dag("daily_report", trigger=Cron("0 6 * * *", tz="Europe/Warsaw")):

    @task
    def plan() -> Report:
        day = dag_ctx.run.planned_for.date()
        return Report(day, f"reports/{dag_ctx.dag_id}/{day:%Y-%m-%d}.csv")

    @task
    def build(report: Report) -> int:
        print(f"building {report.path} for {report.day}")
        return 3

    rows = build(plan())

    CommandOperator(
        "announce",
        command=["echo", t"{rows} rows in the report for {dag_ctx.run.planned_for:%Y-%m-%d}"],
    )
