"""Wait for a duration and for a moment, then use the times they ended."""

from datetime import UTC, datetime, timedelta

from schedr import Dag, task
from schedr.sensors import delay, until


@task
def report(ended: datetime) -> None:
    print(f"Wait ended at {ended.isoformat()}")


with Dag(
    "wait_times",
    description="Waits for a duration and for a moment, then uses the times they ended.",
):
    pause = delay(timedelta(milliseconds=100))
    scheduled = until(timedelta(milliseconds=100))
    past = until(datetime(2020, 1, 1, tzinfo=UTC))
    report(pause.on(datetime))
    report.override(task_id="report_scheduled")(scheduled.on(datetime))
    report.override(task_id="report_past")(past.on(datetime))
