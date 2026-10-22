import json
import subprocess
import sys
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from schedr import dag_ctx, task
from schedr.testing import (
    BINARY_ENV,
    Attempt,
    RunDagError,
    RunResult,
    TaskResult,
    run_dag,
    schedr_binary,
    task_context,
)
from schedr.types import ProcessResult

PLANNED = datetime(2026, 10, 15, 3, tzinfo=UTC)


@task
def report_name() -> str:
    return f"{dag_ctx.dag_id}/{dag_ctx.run.kind}/{dag_ctx.run.planned_for:%Y-%m-%d}"


def test_task_context_gives_dag_ctx_values_to_plain_calls() -> None:
    with task_context(planned_for=PLANNED, dag_id="reports", kind="scheduled") as values:
        assert report_name() == "reports/scheduled/2026-10-15"
        assert dag_ctx.run.since == PLANNED
        assert dag_ctx.run.id == values.run_id
    assert uuid.UUID(values.run_id).version == 7
    with pytest.raises(RuntimeError, match="must be run inside a schedr task"):
        report_name()


def test_task_context_defaults() -> None:
    before = datetime.now(UTC)
    with task_context() as values:
        pass
    assert values.dag_id == "test"
    assert values.kind == "manual"
    assert before <= values.planned_for <= datetime.now(UTC)
    assert values.since == values.planned_for


def test_task_context_keeps_given_values() -> None:
    since = PLANNED - timedelta(days=1)
    with task_context(planned_for=PLANNED, since=since, run_id="r-1") as values:
        assert (dag_ctx.run.id, dag_ctx.run.since) == ("r-1", since)
    assert values.planned_for == PLANNED


@pytest.mark.parametrize(
    ("arguments", "error", "message"),
    [
        ({"planned_for": datetime(2026, 10, 15)}, ValueError, "planned_for must have a time zone"),
        ({"since": "2026-10-14"}, TypeError, "since must be a datetime, got str"),
        (
            {"planned_for": PLANNED, "since": PLANNED + timedelta(seconds=1)},
            ValueError,
            "must not be after planned_for",
        ),
        ({"kind": "daily"}, ValueError, "unknown run kind `daily`"),
        ({"params": {"region": "eu"}}, NotImplementedError, "run parameters \\(milestone M4\\)"),
        ({"secrets": {"a#b": "x"}}, NotImplementedError, "secrets \\(milestone M4\\)"),
    ],
)
def test_task_context_rejects_bad_arguments(
    arguments: dict[str, Any], error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message), task_context(**arguments):
        pass


ATTEMPT = {
    "attempt_id": "a-1",
    "try_number": 1,
    "worker_id": "w",
    "status": "failed",
    "exit_code": 1,
    "error_kind": None,
    "error_summary": {"exception": "ValueError", "message": "bad", "traceback": "..."},
    "assigned_at": "2026-10-15T03:00:00Z",
    "started_at": "2026-10-15T03:00:01Z",
    "ended_at": "2026-10-15T03:00:03.5Z",
}


def test_attempts_are_read_with_times_and_duration() -> None:
    attempt = Attempt.from_json(ATTEMPT)
    assert attempt.status == "failed"
    assert attempt.error == {"exception": "ValueError", "message": "bad", "traceback": "..."}
    assert attempt.started_at == datetime(2026, 10, 15, 3, 0, 1, tzinfo=UTC)
    assert attempt.duration == timedelta(seconds=2.5)


def test_an_attempt_that_didnt_end_has_no_duration() -> None:
    attempt = Attempt.from_json(ATTEMPT | {"status": None, "ended_at": None})
    assert attempt.ended_at is None
    assert attempt.duration is None


DAG_FILE = """
from dataclasses import dataclass

from schedr import CommandOperator, Dag, EmptyOperator, task

@dataclass
class Rows:
    rows: list[str]

@task
def extract() -> Rows:
    return Rows(["a"])

with Dag("sync"):
    extract()
    CommandOperator("notify", command="echo hi")
    EmptyOperator("end")
"""


def report(dag_file: str = "sync.py", **run: Any) -> dict[str, Any]:
    def entry(task_id: str, output: object) -> dict[str, Any]:
        return {
            "task_id": task_id,
            "state": "success",
            "outcome": "SUCCESS",
            "try_number": 1,
            "output": output,
            "attempts": [ATTEMPT | {"status": "success"}],
            "log": f"{task_id} log\n",
        }

    return {
        "run": {
            "run_id": "r-1",
            "dag_id": "sync",
            "kind": "manual",
            "state": "success",
            "planned_for": "2026-10-15T03:00:00Z",
            "since": "2026-10-14T03:00:00Z",
        }
        | run,
        "location": {"root": "dags", "path": dag_file},
        "tasks": [
            entry("extract", {"rows": ["a"]}),
            entry("notify", {"stdout": "hi\n", "stderr": "", "exit_code": 0,
                             "stdout_truncated": False, "stderr_truncated": False}),
            entry("end", None),
        ],
    }  # fmt: skip


class FakeRun:
    """Stands in for ``subprocess.run``: records the command, answers with ``stdout``."""

    def __init__(self, stdout: str, returncode: int = 0, stderr: str = "") -> None:
        self.stdout, self.returncode, self.stderr = stdout, returncode, stderr
        self.command: Sequence[str] = ()
        self.env: dict[str, str] = {}

    def __call__(self, command: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.command, self.env = command, kwargs["env"]
        return subprocess.CompletedProcess(command, self.returncode, self.stdout, self.stderr)


@pytest.fixture
def dags(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "dags"
    root.mkdir()
    (root / "sync.py").write_text(DAG_FILE)
    monkeypatch.setenv(BINARY_ENV, "/opt/schedr")
    monkeypatch.delenv("SCHEDR_CONFIG__INTERPRETERS__PYTHON", raising=False)
    monkeypatch.setattr(sys, "path", list(sys.path))  # decoding may extend it
    return root


def fake(monkeypatch: pytest.MonkeyPatch, *args: Any, **kwargs: Any) -> FakeRun:
    run = FakeRun(*args, **kwargs)
    monkeypatch.setattr(subprocess, "run", run)
    return run


def test_run_dag_runs_dags_test_and_reads_its_report(
    dags: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = fake(monkeypatch, json.dumps(report()))
    result = run_dag(dags, "sync", for_=PLANNED)
    assert list(run.command) == [
        "/opt/schedr", "dags", "test", "sync", "--path", str(dags), "--json",
        "--for", "2026-10-15T03:00:00+00:00",
    ]  # fmt: skip
    assert run.env["SCHEDR_CONFIG__INTERPRETERS__PYTHON"] == sys.executable
    assert isinstance(result, RunResult)
    assert (result.run_id, result.dag_id, result.kind, result.state) == (
        "r-1",
        "sync",
        "manual",
        "success",
    )
    assert result.planned_for - result.since == timedelta(days=1)
    extract = result.tasks["extract"]
    assert isinstance(extract, TaskResult)
    assert (extract.state, extract.outcome, extract.try_number) == ("success", "SUCCESS", 1)
    assert extract.output_json == {"rows": ["a"]}
    assert [a.status for a in extract.attempts] == ["success"]
    assert extract.log == "extract log\n"


def test_outputs_are_decoded_by_the_tasks_types(
    dags: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake(monkeypatch, json.dumps(report()))
    result = run_dag(dags / "sync.py", "sync")
    rows = result.tasks["extract"].output
    assert type(rows).__name__ == "Rows"
    assert rows.rows == ["a"]
    assert result.tasks["notify"].output == ProcessResult("hi\n", "", 0, False, False)
    assert result.tasks["end"].output is None


def test_outputs_use_the_classes_of_a_dag_file_already_imported(
    dags: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    namespace: dict[str, Any] = {}
    exec(compile(DAG_FILE, str(dags / "sync.py"), "exec"), namespace)
    fake(monkeypatch, json.dumps(report()))
    rows = run_dag(dags, "sync").tasks["extract"].output
    assert rows == namespace["Rows"](["a"])


def test_without_a_location_or_a_known_task_outputs_are_decoded_untyped(
    dags: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = report()
    data["location"] = None
    fake(monkeypatch, json.dumps(data))
    assert run_dag(dags, "sync").tasks["extract"].output == {"rows": ["a"]}
    data = report()
    data["tasks"][0]["task_id"] = "gone"
    data["tasks"][0]["output"] = {"$date": "2026-10-15"}
    fake(monkeypatch, json.dumps(data))
    assert str(run_dag(dags, "sync").tasks["gone"].output) == "2026-10-15"


def test_a_dag_file_that_no_longer_imports_or_defines_the_dag_is_an_error(
    dags: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (dags / "broken.py").write_text("raise ValueError('nope')\n")
    (dags / "other.py").write_text("X = 1\n")
    fake(monkeypatch, json.dumps(report("broken.py")))
    with pytest.raises(RunDagError, match=r"can't import .*broken\.py.*ValueError: nope"):
        _ = run_dag(dags, "sync").tasks["extract"].output
    fake(monkeypatch, json.dumps(report("other.py")))
    with pytest.raises(RunDagError, match=r"other\.py doesn't define DAG `sync`"):
        _ = run_dag(dags, "sync").tasks["extract"].output


def test_a_failed_run_is_a_result(dags: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake(monkeypatch, json.dumps(report(state="failed")), returncode=1)
    assert run_dag(dags, "sync").state == "failed"


@pytest.mark.parametrize("stdout", ["", "not json", "[]", '{"files": []}'])
def test_no_report_is_an_error_with_what_schedr_said(
    dags: Path, monkeypatch: pytest.MonkeyPatch, stdout: str
) -> None:
    fake(monkeypatch, stdout, returncode=1, stderr="error: no DAG `sync` in the DAG files\n")
    expected = r"dags test sync` failed \(exit code 1\): error: no DAG"
    with pytest.raises(RunDagError, match=expected):
        run_dag(dags, "sync")


def test_the_environments_interpreter_setting_wins(
    dags: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SCHEDR_CONFIG__INTERPRETERS__PYTHON", "/opt/python")
    run = fake(monkeypatch, json.dumps(report()))
    run_dag(dags, "sync")
    assert run.env["SCHEDR_CONFIG__INTERPRETERS__PYTHON"] == "/opt/python"


@pytest.mark.parametrize(
    ("arguments", "error", "message"),
    [
        ({"for_": datetime(2026, 10, 15)}, ValueError, "for_ must have a time zone"),
        ({"params": {"a": 1}}, NotImplementedError, "run_dag\\(params=...\\)` comes with run"),
        ({"secrets": {"a#b": "x"}}, NotImplementedError, "run_dag\\(secrets=...\\)"),
        ({"step": True}, NotImplementedError, "step-by-step runs \\(milestone M4\\)"),
        ({"replace": {"a": 1}}, NotImplementedError, "replaced task results"),
    ],
)
def test_run_dag_rejects_bad_and_later_arguments(
    dags: Path, monkeypatch: pytest.MonkeyPatch, arguments: dict[str, Any],
    error: type[Exception], message: str,
) -> None:  # fmt: skip
    fake(monkeypatch, json.dumps(report()))
    with pytest.raises(error, match=message):
        run_dag(dags, "sync", **arguments)


@dataclass
class Found:
    scripts: Path
    which: str | None


@pytest.fixture
def lookup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Found:
    found = Found(tmp_path, None)
    monkeypatch.delenv(BINARY_ENV, raising=False)
    monkeypatch.setattr("sysconfig.get_path", lambda name: str(found.scripts))
    monkeypatch.setattr("shutil.which", lambda name: found.which)
    return found


def test_the_binary_comes_from_the_environment_variable_first(
    lookup: Found, monkeypatch: pytest.MonkeyPatch
) -> None:
    (lookup.scripts / "schedr").write_text("")
    monkeypatch.setenv(BINARY_ENV, "/custom/schedr")
    assert schedr_binary() == "/custom/schedr"


def test_the_binary_installed_with_the_package_comes_before_path(lookup: Found) -> None:
    (lookup.scripts / "schedr").write_text("")
    lookup.which = "/usr/bin/schedr"
    assert schedr_binary() == str(lookup.scripts / "schedr")


def test_the_binary_on_path_is_the_fallback(lookup: Found) -> None:
    lookup.which = "/usr/bin/schedr"
    assert schedr_binary() == "/usr/bin/schedr"


def test_no_binary_is_an_error_saying_what_to_do(lookup: Found) -> None:
    with pytest.raises(RunDagError, match="`pip install schedr`\\), or set SCHEDR_BIN"):
        schedr_binary()
