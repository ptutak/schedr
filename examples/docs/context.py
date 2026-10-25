"""The run's window passed into a task, and read from inside a task."""

from datetime import datetime

from schedr import Cron, Dag, dag_ctx, task

with Dag("report_window", trigger=Cron("0 3 * * *")):

    @task
    def bounds(since: datetime, until: datetime) -> str:
        text = f"{since.isoformat()} .. {until.isoformat()}"
        print(text)
        return text

    @task
    def label() -> str:
        day = dag_ctx.run.planned_for.date().isoformat()
        print(f"{dag_ctx.dag_id} {dag_ctx.run.kind} {day}")
        return day

    bounds(dag_ctx.run.since, dag_ctx.run.planned_for)
    label()
