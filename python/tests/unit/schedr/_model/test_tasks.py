from typing import Any

import pytest

from schedr._model.ids import IdError
from schedr._model.tasks import (
    ArgvCommand,
    CommandKind,
    EmptyKind,
    PythonKind,
    ShellCommand,
    Task,
    TaskSettings,
)
from schedr._model.types import ScalarType
from schedr._model.values import (
    DagCtxField,
    DagCtxRef,
    ListValue,
    LiteralValue,
    OutputRef,
    RefValue,
    Template,
    TemplateRef,
)

from .conftest import Validate

PLANNED_FOR = DagCtxRef(DagCtxField.PLANNED_FOR)


def test_default_settings_match_the_rust_defaults(validate: Validate) -> None:
    assert TaskSettings().to_json() == {
        "retries": 0,
        "retry_delay_ms": 300_000,
        "timeout_ms": None,
        "queue": "default",
        "max_output_size": 1_048_576,
    }
    validate("TaskSettings", TaskSettings().to_json())


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"retries": -1}, "must not be negative"),
        ({"retry_delay_ms": -1}, "must not be negative"),
        ({"max_output_size": -1}, "must not be negative"),
        ({"timeout_ms": 0}, "timeout must be positive"),
        ({"queue": ""}, "queue must not be empty"),
    ],
)
def test_settings_reject_values_out_of_range(kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        TaskSettings(**kwargs)


def test_python_task_serializes_flat(validate: Validate) -> None:
    task = Task(
        "load",
        PythonKind({"rows": RefValue(OutputRef("extract")), "limit": LiteralValue(5)}),
        ScalarType("int"),
        TaskSettings(retries=2, timeout_ms=60_000, queue="large"),
    )
    expected = {
        "id": "load",
        "kind": "python",
        "arguments": {
            "limit": {"literal": 5},
            "rows": {"ref": {"kind": "output", "task": "extract"}},
        },
        "settings": {
            "retries": 2,
            "retry_delay_ms": 300_000,
            "timeout_ms": 60_000,
            "queue": "large",
            "max_output_size": 1_048_576,
        },
        "output_type": {"type": "int"},
    }
    assert task.to_json() == expected
    validate("Task", task.to_json())


def test_command_tasks_with_shell_and_argv(validate: Validate) -> None:
    shell = Task("ls", CommandKind(ShellCommand(Template(["ls -l"]))), ScalarType("process_result"))
    assert shell.to_json()["command"] == {"shell": [{"text": "ls -l"}]}
    assert "env" not in shell.to_json()
    validate("Task", shell.to_json())

    argv = Task(
        "notify",
        CommandKind(
            ArgvCommand([Template(["./notify.sh"]), Template([TemplateRef(PLANNED_FOR)])]),
            {"TOKEN": Template(["x"])},
        ),
        ScalarType("process_result"),
    )
    assert argv.to_json()["command"] == {
        "argv": [[{"text": "./notify.sh"}], [{"ref": {"kind": "dag_ctx", "field": "planned_for"}}]]
    }
    assert argv.to_json()["env"] == {"TOKEN": [{"text": "x"}]}
    validate("Task", argv.to_json())


def test_argv_needs_a_program() -> None:
    with pytest.raises(ValueError, match="needs at least the program"):
        ArgvCommand([])


def test_empty_task(validate: Validate) -> None:
    task = Task("start", EmptyKind(), ScalarType("none"))
    assert task.to_json()["kind"] == "empty"
    assert task.references() == []
    validate("Task", task.to_json())


def test_python_references_come_from_the_arguments_by_name() -> None:
    kind = PythonKind({"b": RefValue(OutputRef("y")), "a": ListValue([RefValue(OutputRef("x"))])})
    assert kind.references() == [OutputRef("x"), OutputRef("y")]


def test_command_references_come_from_the_command_line_then_the_env() -> None:
    kind = CommandKind(
        ShellCommand(Template(["echo ", TemplateRef(PLANNED_FOR)])),
        {
            "B": Template([TemplateRef(OutputRef("b"))]),
            "A": Template([TemplateRef(OutputRef("a"))]),
        },
    )
    task = Task("echo", kind, ScalarType("process_result"))
    assert task.references() == [PLANNED_FOR, OutputRef("a"), OutputRef("b")]


def test_command_line_templates() -> None:
    assert ShellCommand(Template(["x"])).templates() == [Template(["x"])]
    args = [Template(["a"]), Template(["b"])]
    assert ArgvCommand(args).templates() == args


def test_task_validates_its_id() -> None:
    with pytest.raises(IdError):
        Task("bad id", EmptyKind(), ScalarType("none"))
