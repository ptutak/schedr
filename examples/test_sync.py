"""Tests of the example DAG of PROJECT.md: plain calls of its tasks, and one whole run."""

from datetime import UTC, datetime
from pathlib import Path

from sync import extract, load

from schedr.testing import run_dag

HERE = Path(__file__).parent


def test_tasks_are_plain_functions_outside_a_run() -> None:
    rows = extract()
    assert rows == [{"id": "1", "name": "Ada"}, {"id": "2", "name": "Grace"}]
    assert load([{"id": "1", "name": "Ada"}]) == 1


def test_a_whole_run() -> None:
    result = run_dag(HERE / "sync.py", "sync_customers", for_=datetime(2026, 10, 15, 1, tzinfo=UTC))
    assert result.state == "success"
    assert result.tasks["load"].output == 2
    assert "loading 2 customers" in result.tasks["load"].log
    notify = result.tasks["notify"]
    assert notify.output.stdout == "synced 2 customers for 2026-10-15T01:00:00Z\n"
    assert [a.status for a in notify.attempts] == ["success"]


def test_the_example_is_the_one_in_project_md() -> None:
    example = (HERE / "sync.py").read_text()
    code = example[example.index("from datetime") :]
    assert f"```python\n{code}```" in (HERE.parent / "PROJECT.md").read_text()
