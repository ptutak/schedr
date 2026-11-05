"""Tests of the resource example: a plain call of a task, and one whole run."""

from pathlib import Path

from workspace import Workspace, count_rows, download

from schedr.testing import run_dag

HERE = Path(__file__).parent


def test_tasks_are_plain_functions_outside_a_run(tmp_path: Path) -> None:
    space = Workspace(str(tmp_path))
    assert count_rows(space, download(space)) == 4


def test_a_whole_run_removes_the_workspace_after_its_users() -> None:
    result = run_dag(HERE / "workspace.py", "workspace_import")
    assert result.state == "success"
    assert result.tasks["work.count_rows"].output == 4
    assert result.tasks["report"].output == "imported 4 rows"
    made = result.resources["work.workspace"]
    assert (made.cleanup, made.state) == ("work.workspace.cleanup", "cleaned")
    path = result.tasks["work.workspace"].output.path
    assert not Path(path).exists()
