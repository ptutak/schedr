"""Tests of the daily report DAG: a task reading ``dag_ctx`` under ``task_context``, and a run."""

from datetime import UTC, date, datetime
from pathlib import Path

from daily_report import Report, build, plan

from schedr.testing import run_dag, task_context

HERE = Path(__file__).parent
FOR = datetime(2026, 10, 15, 4, tzinfo=UTC)


def test_plan_names_the_report_after_the_runs_day() -> None:
    with task_context(planned_for=FOR, dag_id="daily_report"):
        assert plan() == Report(date(2026, 10, 15), "reports/daily_report/2026-10-15.csv")


def test_build_needs_no_run_context() -> None:
    assert build(Report(date(2026, 10, 15), "r.csv")) == 3


def test_a_whole_run() -> None:
    result = run_dag(HERE / "daily_report.py", "daily_report", for_=FOR)
    assert result.state == "success"
    assert result.planned_for == FOR
    assert result.tasks["plan"].output == Report(
        date(2026, 10, 15), "reports/daily_report/2026-10-15.csv"
    )
    assert "building reports/daily_report/2026-10-15.csv" in result.tasks["build"].log
    assert result.tasks["announce"].output.stdout == "3 rows in the report for 2026-10-15\n"
