"""Per-task cleanups: ``cleanup=``, ``cleanup_handle`` and the runner's cleanup path."""

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from schedr import CommandOperator, Dag, TaskDefaults, cleanup_handle, secrets, task
from schedr._model import registry
from schedr.context import TaskValues, handle_classes
from schedr.runner.task import cleanup_call, handle_adder
from schedr.testing import task_context


@dataclass
class ExportJob:
    job_id: str


@dataclass
class TempDir:
    path: str


def undo(day: str, handles: list[ExportJob | TempDir]) -> None:
    del day, handles


@task(cleanup=undo)
def export(day: str, rows: int = 3) -> None:
    cleanup_handle(ExportJob(day))
    del rows


def _cleanup_json(dag: Dag, task_id: str) -> dict[str, Any]:
    tasks = {t.id: t for t in dag.model.tasks}
    found = tasks[task_id].to_json()["cleanup"]
    assert isinstance(found, dict)
    return found


def test_a_task_records_its_cleanup_with_the_defaults_and_its_handle_types() -> None:
    with Dag("d1") as dag:
        export("2026-11-04")
    assert _cleanup_json(dag, "export") == {
        "run": "python",
        "retries": 3,
        "retry_delay_ms": 30_000,
        "handles": ["ExportJob", "TempDir"],
    }


def test_the_dags_defaults_and_the_tasks_own_settings_apply() -> None:
    defaults = TaskDefaults(cleanup_retries=1, cleanup_retry_delay=timedelta(seconds=2))
    with Dag("d2", defaults=defaults) as dag:
        export.override(cleanup_timeout=timedelta(minutes=1))("2026-11-04")
    found = _cleanup_json(dag, "export")
    assert (found["timeout_ms"], found["retries"], found["retry_delay_ms"]) == (60_000, 1, 2_000)


def test_cleanup_options_without_a_cleanup_are_refused() -> None:
    @task(cleanup_retries=1)
    def plain() -> None: ...

    with Dag("d3"), pytest.raises(TypeError, match="without `cleanup=`"):
        plain()


def test_a_cleanup_parameter_the_task_lacks_is_refused() -> None:
    def other(missing: str) -> None:
        del missing

    @task(cleanup=other)
    def job() -> None: ...

    with Dag("d4"), pytest.raises(TypeError, match="isn't a parameter of the task"):
        job()


def test_a_parameter_of_another_type_is_refused() -> None:
    def other(day: int) -> None:
        del day

    @task(cleanup=other)
    def job(day: str) -> None:
        del day

    with Dag("d5"), pytest.raises(TypeError, match="the task's is"):
        job("x")


@pytest.mark.parametrize(
    ("annotation", "classes"),
    [(list[ExportJob], (ExportJob,)), (list[ExportJob | TempDir], (ExportJob, TempDir))],
)
def test_handle_classes_reads_a_list_of_dataclasses(annotation: object, classes: object) -> None:
    assert handle_classes(annotation) == classes


@pytest.mark.parametrize("annotation", [None, list, list[int], list[ExportJob | int], ExportJob])
def test_handle_classes_refuses_anything_else(annotation: object) -> None:
    with pytest.raises(TypeError, match="list\\[A \\| B\\]"):
        handle_classes(annotation)


def test_an_async_or_generator_cleanup_is_refused() -> None:
    async def later() -> None: ...

    def steps() -> Any:
        yield 1

    for cleanup, message in ((later, "async"), (steps, "generator"), (print, "needs a function")):
        with pytest.raises(TypeError, match=message):

            @task(cleanup=cleanup)
            def job() -> None: ...


def test_a_command_cleanup_is_its_line_and_refuses_secrets() -> None:
    with Dag("d6") as dag:
        CommandOperator("run", "true", cleanup=["./undo.sh"], cleanup_handles=["ExportJob"])
    found = _cleanup_json(dag, "run")
    assert found["run"] == {"command": {"argv": [[{"text": "./undo.sh"}]]}}
    assert found["handles"] == ["ExportJob"]
    with Dag("d9"), pytest.raises(TypeError, match="in the cleanup line"):
        CommandOperator("run", "true", cleanup=["./undo.sh", secrets["etl/db"]["password"]])


def test_a_function_without_handles_takes_none_and_a_command_any_by_default() -> None:
    def clean() -> None: ...

    @task(cleanup=clean)
    def job() -> None: ...

    with Dag("d10") as dag:
        job()
        CommandOperator("run", "true", cleanup="true")
    assert _cleanup_json(dag, "job")["handles"] == []
    assert "handles" not in _cleanup_json(dag, "run")


def test_cleanup_handles_without_a_cleanup_are_refused() -> None:
    with Dag("d7"), pytest.raises(TypeError, match="without `cleanup=`"):
        CommandOperator("run", "true", cleanup_handles=["ExportJob"])


def test_a_test_sees_what_a_task_registered() -> None:
    with task_context() as ctx:
        export("2026-11-04")
    assert ctx.handles == [ExportJob("2026-11-04")]


def test_outside_a_task_a_handle_is_refused() -> None:
    with pytest.raises(RuntimeError, match="must be run inside a schedr task"):
        cleanup_handle(ExportJob("j"))


@pytest.mark.parametrize("value", [1, "x", ExportJob])
def test_only_a_dataclass_instance_is_a_handle(value: object) -> None:
    with pytest.raises(TypeError, match="dataclass instance"):
        cleanup_handle(value)


class _Agent:
    """A channel answering with ``answer``, remembering what was asked."""

    def __init__(self, answer: dict[str, Any]) -> None:
        self.answer = answer
        self.asked: list[dict[str, Any]] = []

    def ask(self, message: dict[str, Any]) -> dict[str, Any]:
        self.asked.append(message)
        return self.answer


def test_a_handle_goes_to_the_agent_with_its_class_name() -> None:
    agent = _Agent({"type": "handle_added"})
    add = handle_adder(agent)  # type: ignore[arg-type]
    add(ExportJob("j"), "ExportJob", {"job_id": "j"})
    assert agent.asked == [
        {"type": "handle_add", "handle": {"handle_type": "ExportJob", "value": {"job_id": "j"}}}
    ]


def test_a_refused_handle_fails_where_it_was_registered() -> None:
    add = handle_adder(_Agent({"type": "handle_refused", "message": "too large"}))  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="too large"):
        add(ExportJob("j"), "ExportJob", {})


def _entry(cleanups: dict[str, Any]) -> registry.DefinedDag:
    with Dag("d8") as dag:
        pass
    return registry.DefinedDag(dag.model, {}, None, cleanups)


def test_a_cleanup_gets_the_inputs_it_names_defaults_and_its_handles() -> None:
    def clean(rows: int, handles: list[ExportJob | TempDir]) -> None:
        del rows, handles

    def job(day: str, rows: int = 3) -> None:
        del day, rows

    registered = [
        {"handle_type": "TempDir", "value": json.loads('{"path": "/tmp/a"}')},
        {"handle_type": "ExportJob", "value": {"job_id": "j"}},
    ]
    function, given = cleanup_call(
        _entry({"job": clean}), job, "job", {"day": "2026-11-04"}, registered
    )
    assert function is clean
    assert given == {"rows": 3, "handles": [TempDir("/tmp/a"), ExportJob("j")]}


def test_a_cleanup_that_is_gone_says_so() -> None:
    with pytest.raises(Exception, match="no `cleanup=` function"):
        cleanup_call(_entry({}), undo, "job", {}, [])


def test_a_handle_the_cleanup_no_longer_takes_says_so() -> None:
    def clean(handles: list[TempDir]) -> None:
        del handles

    with pytest.raises(Exception, match="doesn't take handles of type `ExportJob`"):
        cleanup_call(
            _entry({"job": clean}), undo, "job", {}, [{"handle_type": "ExportJob", "value": {}}]
        )


def test_task_values_take_no_handles_by_default() -> None:
    assert TaskValues("t", 1, None).handles is None
