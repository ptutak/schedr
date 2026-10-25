"""The same task on a cron, a preset, an interval, a single fire time and no trigger.

``Once`` and a DAG with no trigger have no previous fire time. Passing ``dag_ctx.run.since``
into a task is then an import error unless the DAG sets ``initial_since``.
"""

from datetime import datetime, timedelta

from schedr import DAILY, Cron, Dag, Every, Once, dag_ctx, task


@task
def bounds(since: datetime, until: datetime) -> str:
    text = f"{since.isoformat()} .. {until.isoformat()}"
    print(text)
    return text


with Dag(
    "nightly",
    trigger=Cron("0 3 * * *", tz="Europe/Warsaw", start="2026-01-01"),
    description="Nightly window",
    tags=["demo"],
):
    bounds(dag_ctx.run.since, dag_ctx.run.planned_for)

with Dag("daily_warsaw", trigger=DAILY.override(tz="Europe/Warsaw")):
    bounds(dag_ctx.run.since, dag_ctx.run.planned_for)

with Dag("every_hour", trigger=Every(timedelta(hours=1), start="2026-01-01")):
    bounds(dag_ctx.run.since, dag_ctx.run.planned_for)

with Dag(
    "once",
    trigger=Once("2026-12-24T23:00", tz="Europe/Warsaw"),
    initial_since="2026-12-24T22:00",
):
    bounds(dag_ctx.run.since, dag_ctx.run.planned_for)

with Dag("manual_only", trigger=None, initial_since="2026-01-01"):
    bounds(dag_ctx.run.since, dag_ctx.run.planned_for)
