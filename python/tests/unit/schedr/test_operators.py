from collections.abc import Callable
from typing import Any

import pytest

from schedr import CommandOperator, Dag, EmptyOperator, dag_ctx, task
from schedr._model import (
    ArgvCommand,
    CommandKind,
    DagCtxField,
    DagCtxRef,
    EdgeKind,
    EmptyKind,
    OutputRef,
    ScalarType,
    ShellCommand,
    Template,
    TemplateRef,
    registry,
)
from schedr.operators import Operator
from schedr.types import ProcessResult

Validate = Callable[[str, Any], None]


@task
def count() -> int:
    return 3


def only_dag() -> Any:
    (dag,) = registry.take_defined()
    return dag


def test_a_shell_command(validate: Validate) -> None:
    with Dag("d"):
        op = CommandOperator("ls", "ls -l | wc -l")
    task = only_dag().tasks[0]
    assert task.kind == CommandKind(ShellCommand(Template(["ls -l | wc -l"])))
    assert task.output_type == ScalarType("process_result")
    assert op.task_id == "ls"
    assert repr(op) == "CommandOperator('ls')"
    validate("Task", task.to_json())


def test_an_argument_list_with_references_and_env(validate: Validate) -> None:
    with Dag("d"):
        n = count()
        CommandOperator(
            "notify",
            ["./notify.sh", dag_ctx.run.planned_for, n],
            env={"TOKEN": "x", "RUN": dag_ctx.run.id},
        )
    model = only_dag()
    kind = model.tasks[1].kind
    assert kind == CommandKind(
        ArgvCommand(
            [
                Template(["./notify.sh"]),
                Template([TemplateRef(DagCtxRef(DagCtxField.PLANNED_FOR))]),
                Template([TemplateRef(OutputRef("count"))]),
            ]
        ),
        {"TOKEN": Template(["x"]), "RUN": Template([TemplateRef(DagCtxRef(DagCtxField.RUN_ID))])},
    )
    assert [(e.from_, e.to, e.kind) for e in model.edges] == [("count", "notify", EdgeKind.DATA)]
    validate("Dag", model.to_json())


def test_stdout_and_stderr_reference_parts_of_the_output() -> None:
    @task
    def show(text: str) -> None: ...

    @task
    def show_all(result: ProcessResult) -> None: ...

    with Dag("d"):
        listing = CommandOperator("listing", ["ls"])
        show(listing.stdout)
        show.override(task_id="show_err")(listing.stderr)
        show_all(listing.output)
    tasks = only_dag().tasks
    assert tasks[1].kind.arguments["text"].reference == OutputRef("listing", ("stdout",))
    assert tasks[2].kind.arguments["text"].reference == OutputRef("listing", ("stderr",))
    assert tasks[3].kind.arguments["result"].reference == OutputRef("listing")


@pytest.mark.parametrize(
    ("kwargs", "error", "message"),
    [
        ({"command": 5}, TypeError, "command must be a string, a t-string or a list"),
        ({"command": []}, ValueError, "needs at least the program"),
        ({"command": ["ls", 5]}, TypeError, r"command\[1\]: expected a string, a t-string or a"),
        ({"command": "ls", "env": {"1BAD": "x"}}, ValueError, "not a valid environment variable"),
        ({"command": "ls", "env": {"A": 5}}, TypeError, r"env\['A'\]: expected a string"),
    ],
)
def test_bad_commands_are_rejected(
    kwargs: dict[str, Any], error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message), Dag("d"):
        CommandOperator("t", **kwargs)


def test_options_are_passed_to_the_task() -> None:
    with Dag("d"):
        CommandOperator("t", "ls", retries=4, queue="q", max_output_size=99)
    settings = only_dag().tasks[0].settings
    assert (settings.retries, settings.queue, settings.max_output_size) == (4, "q", 99)


def test_an_empty_operator(validate: Validate) -> None:
    with Dag("d"):
        op = EmptyOperator("start")
    task = only_dag().tasks[0]
    assert task.kind == EmptyKind()
    assert task.output_type == ScalarType("none")
    assert op.output._schedr_reference() == OutputRef("start")
    validate("Task", task.to_json())


@pytest.mark.parametrize("make", [lambda: CommandOperator("t", "ls"), lambda: EmptyOperator("t")])
def test_operators_need_a_dag(make: Callable[[], object]) -> None:
    with pytest.raises(RuntimeError, match="must be created inside `with Dag"):
        make()


def test_operators_cant_be_subclassed() -> None:
    with pytest.raises(TypeError, match="custom operators are not supported"):

        class MyOperator(CommandOperator):  # type: ignore[misc]
            pass

    with pytest.raises(TypeError, match="custom operators are not supported"):

        class Mine(Operator):
            pass


def test_operator_shift_operators() -> None:
    with Dag("d"):
        a, b, c = EmptyOperator("a"), EmptyOperator("b"), EmptyOperator("c")
        assert (a >> b) is b
        assert ([a] >> c) is c
        assert (c << b) is b
        assert ([c] << a) is a
    # `[c] << a` repeats a -> c, which is recorded once.
    assert [(e.from_, e.to) for e in only_dag().edges] == [("a", "b"), ("a", "c"), ("b", "c")]


def echo_whole_result() -> None:
    with Dag("d"):
        listing = CommandOperator("listing", ["ls"])
        CommandOperator("t", ["echo", listing.output])


def echo_missing_field() -> None:
    with Dag("d"):
        n = count()
        CommandOperator("t", ["echo", n.total])


def test_command_references_must_be_renderable() -> None:
    with pytest.raises(TypeError, match=r"task `t`: a value of type `ProcessResult` can't be"):
        echo_whole_result()


def test_command_reference_paths_must_exist() -> None:
    with pytest.raises(TypeError, match=r"`count\.total` doesn't exist"):
        echo_missing_field()


def test_a_t_string_shell_command(validate: Validate) -> None:
    region = "eu west"
    with Dag("d"):
        n = count()
        CommandOperator("t", t"./run.sh {region} {n} {dag_ctx.run.planned_for:%Y-%m-%d}")
    model = only_dag()
    assert model.tasks[1].kind.command == ShellCommand(
        Template(
            [
                "./run.sh 'eu west' ",
                TemplateRef(OutputRef("count")),
                " ",
                TemplateRef(DagCtxRef(DagCtxField.PLANNED_FOR), "%Y-%m-%d"),
            ]
        )
    )
    assert [(e.from_, e.to) for e in model.edges] == [("count", "t")]
    validate("Dag", model.to_json())


def test_t_strings_in_arguments_and_env_are_not_quoted() -> None:
    name = "a b"
    with Dag("d"):
        CommandOperator("t", ["./x", t"--name={name}"], env={"RUN": t"run-{dag_ctx.run.id}"})
    kind = only_dag().tasks[0].kind
    assert kind.command.args[1] == Template(["--name=a b"])
    assert kind.env["RUN"] == Template(["run-", TemplateRef(DagCtxRef(DagCtxField.RUN_ID))])


def test_a_strftime_spec_on_a_number_is_a_parse_error() -> None:
    with pytest.raises(TypeError, match=r"`:%Y` formats dates and times, but `count` is `int`"):
        echo_count_as_date()


def echo_count_as_date() -> None:
    with Dag("d"):
        n = count()
        CommandOperator("t", t"echo {n:%Y}")
