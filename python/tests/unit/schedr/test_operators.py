from collections.abc import Callable
from typing import Any

import pytest

from schedr import CommandOperator, Dag, EmptyOperator, ScriptOperator, dag_ctx, task
from schedr._model import (
    ArgvCommand,
    CommandKind,
    DagCtxField,
    DagCtxRef,
    EdgeKind,
    EmptyKind,
    OutputRef,
    ScalarType,
    SecretRef,
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


def shell_with_a_reference() -> None:
    with Dag("d"):
        n = count()
        CommandOperator("t", t"./run.sh {n}")


def shell_with_a_plain_value() -> None:
    region = "eu west"
    with Dag("d"):
        CommandOperator("t", t"./run.sh {region}")


def shell_with_dag_ctx() -> None:
    with Dag("d"):
        CommandOperator("t", t"./run.sh '{dag_ctx.run.planned_for:%Y-%m-%d}'")


@pytest.mark.parametrize(
    ("make", "placeholder"),
    [
        (shell_with_a_reference, "{n}"),
        (shell_with_a_plain_value, "{region}"),
        (shell_with_dag_ctx, "{dag_ctx.run.planned_for:%Y-%m-%d}"),
    ],
)
def test_a_shell_command_takes_no_placeholders(make: Callable[[], None], placeholder: str) -> None:
    """Decision 99: values never become shell source; the message shows the two rewrites."""
    with pytest.raises(TypeError) as raised:
        make()
    message = str(raised.value)
    assert message.startswith(f"task `t`: `{placeholder}` in a shell command:"), message
    assert 'command=["./notify.sh", out.report_url]' in message
    assert """command='./notify.sh "$URL"', env={"URL": out.report_url}""" in message


def test_a_shell_t_string_without_placeholders_is_its_text(validate: Validate) -> None:
    with Dag("d"):
        CommandOperator("t", t"echo '{{}}' $HOME")
    model = only_dag()
    assert model.tasks[0].kind.command == ShellCommand(Template(["echo '{}' $HOME"]))
    validate("Dag", model.to_json())


def test_raw_is_an_unknown_format_spec() -> None:
    with Dag("d"):
        n = count()
        with pytest.raises(ValueError, match=r"unknown format spec `:raw`"):
            CommandOperator("t", ["./run.sh", t"{n:raw}"])


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
        CommandOperator("t", ["echo", t"{n:%Y}"])


# ScriptOperator (decisions 293, 302): the declaration is asked of the agent while parsing.

_DECLARED: dict[str, Any] = {
    "type": "script_declaration",
    "job": "jobs:sales/schedr.toml#clean",
    "script": "sales/clean.py",
    "interpreter": "py311",
    "args": [[{"text": "--day"}], [{"ref": {"kind": "dag_ctx", "field": "since"}}]],
    "env": {"MODE": [{"text": "full"}], "LEVEL": [{"text": "info"}]},
    "secrets": [{"kind": "secret", "path": "etl/db", "key": "password"}],
    "working_dir": "sales",
    "retries": 2,
    "timeout_ms": 60000,
    "queue": "scripts",
}


def _asking(answer: dict[str, Any], asked: list[str] | None = None) -> Any:
    def ask(reference: str) -> dict[str, Any]:
        if asked is not None:
            asked.append(reference)
        return answer

    return registry.script_declarations(ask)


def test_a_script_operator_takes_its_declaration(
    validate: Validate, validate_runner_message: Callable[[Any], None]
) -> None:
    validate_runner_message(_DECLARED)
    asked: list[str] = []
    with _asking(_DECLARED, asked), Dag("d"):
        op = ScriptOperator("clean", "jobs:sales/schedr.toml#clean")
    assert asked == ["jobs:sales/schedr.toml#clean"]
    task = only_dag().tasks[0]
    shown = task.to_json()
    assert {
        k: shown[k] for k in ("kind", "job", "script", "interpreter", "working_dir", "args", "env")
    } == {
        "kind": "script",
        "job": "jobs:sales/schedr.toml#clean",
        "script": "sales/clean.py",
        "interpreter": "py311",
        "working_dir": "sales",
        "args": _DECLARED["args"],
        "env": {"LEVEL": [{"text": "info"}], "MODE": [{"text": "full"}]},
    }
    assert (task.settings.retries, task.settings.timeout_ms, task.settings.queue) == (
        2,
        60000,
        "scripts",
    )
    assert task.secrets == (SecretRef("etl/db", "password"),)
    assert task.output_type == ScalarType("process_result")
    assert repr(op) == "ScriptOperator('clean')"
    validate("Task", task.to_json())


def test_a_script_operators_own_settings_change_the_declared_ones(validate: Validate) -> None:
    with _asking(_DECLARED), Dag("d"):
        n = count()
        ScriptOperator(
            "clean",
            "jobs:sales/schedr.toml#clean",
            args=["--count", n],
            env={"MODE": "quick"},
            secrets=["etl/api#token"],
            retries=0,
            queue="fast",
        )
    model = only_dag()
    task = model.tasks[1]
    kind = task.to_json()
    # args= replaces, env= merges per variable (the operator's wins), secrets= adds (302).
    assert kind["args"] == [[{"text": "--count"}], [{"ref": {"kind": "output", "task": "count"}}]]
    assert kind["env"] == {"LEVEL": [{"text": "info"}], "MODE": [{"text": "quick"}]}
    assert task.secrets == (SecretRef("etl/db", "password"), SecretRef("etl/api", "token"))
    assert (task.settings.retries, task.settings.timeout_ms, task.settings.queue) == (
        0,
        60000,
        "fast",
    )
    assert [(e.from_, e.to, e.kind) for e in model.edges] == [("count", "clean", EdgeKind.DATA)]
    validate("Dag", model.to_json())


def test_a_script_the_worker_cant_find_is_an_import_error_saying_why() -> None:
    missing = {"type": "script_not_found", "message": "script root `jobs` isn't on this worker"}
    with (
        pytest.raises(LookupError, match=r"task `clean`: script root `jobs` isn't on this worker"),
        _asking(missing),
        Dag("d"),
    ):
        ScriptOperator("clean", "jobs:sales/schedr.toml#clean")
    registry.take_defined()


def test_outside_a_parse_a_script_operator_only_names_its_declaration() -> None:
    # A task's runner imports its DAG file to find its own task: no agent to ask.
    with Dag("d"):
        ScriptOperator("clean", "jobs:sales/schedr.toml#clean")
    kind = only_dag().tasks[0].to_json()
    assert (kind["job"], kind["script"]) == ("jobs:sales/schedr.toml#clean", "")


def test_a_script_operators_args_are_a_list() -> None:
    with pytest.raises(TypeError, match="args= is a list"), _asking(_DECLARED), Dag("d"):
        ScriptOperator("clean", "jobs:sales/schedr.toml#clean", args="--day")  # type: ignore[arg-type]
    registry.take_defined()
