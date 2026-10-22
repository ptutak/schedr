"""``schedr.testing.run_dag`` against the real binary: ``schedr dags test`` as a subprocess.

The binary is found as ``run_dag`` finds it (``SCHEDR_BIN``, else the one installed with
this Python's ``schedr``); ``make test-integration`` and CI install the current build.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from schedr.testing import RunDagError, run_dag

FOR = datetime(2026, 10, 15, 3, tzinfo=UTC)

FAILING = """
from datetime import timedelta

from schedr import Dag, TaskDefaults, task

with Dag("fails", defaults=TaskDefaults(retries=1, retry_delay=timedelta(0))):

    @task
    def extract() -> int:
        raise ValueError("source is down")

    @task
    def load(n: int) -> int:
        return n

    load(extract())
"""

TYPED = """
from dataclasses import dataclass

from schedr import Dag, task

@dataclass
class Batch:
    name: str
    size: int

with Dag("typed"):

    @task
    def make() -> Batch:
        return Batch("b-1", 3)

    make()
"""


def test_a_failed_run_shows_each_attempt_and_the_tasks_downstream(tmp_path: Path) -> None:
    (tmp_path / "fails.py").write_text(FAILING)
    result = run_dag(tmp_path / "fails.py", "fails", for_=FOR)
    assert result.state == "failed"
    extract = result.tasks["extract"]
    assert extract.state == "failed"
    assert [a.status for a in extract.attempts] == ["failed", "failed"]
    error = extract.attempts[-1].error
    assert error is not None
    assert (error["exception"], error["message"]) == ("ValueError", "source is down")
    assert all(a.duration is not None for a in extract.attempts)
    assert result.tasks["load"].state == "upstream_failed"
    assert result.tasks["load"].attempts == ()


def test_a_directory_is_a_root_and_outputs_are_decoded_from_its_file(tmp_path: Path) -> None:
    (tmp_path / "typed.py").write_text(TYPED)
    (tmp_path / "helpers.py").write_text("X = 1\n")
    batch = run_dag(tmp_path, "typed", for_=FOR).tasks["make"].output
    assert type(batch).__name__ == "Batch"
    assert (batch.name, batch.size) == ("b-1", 3)


def test_an_unknown_dag_is_an_error(tmp_path: Path) -> None:
    (tmp_path / "typed.py").write_text(TYPED)
    with pytest.raises(RunDagError, match="no DAG `nope`"):
        run_dag(tmp_path, "nope")
