from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from schedr import (
    CommandOperator,
    Dag,
    DefaultOutcome,
    EmptyOperator,
    Outcome,
    Resource,
    ScriptOperator,
    ScriptResource,
    TaskDefaults,
    dag_ctx,
    resource,
    task,
)
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


# M4.9c: a declaration's outcomes and cleanup (decisions 311-314), a script as a resource.

_RESOURCE_JOB = "jobs:sales/schedr.toml#clean"


def _script_task(answer: dict[str, Any], **options: Any) -> dict[str, Any]:
    with _asking(answer), Dag("d"):
        ScriptOperator("clean", _RESOURCE_JOB, **options)
    return only_dag().tasks[0].to_json()


_WITH_OUTCOMES = {
    **_DECLARED,
    "outcomes": [{"name": "SUCCESS", "exit_code": 0}, {"name": "NO_DATA", "exit_code": 3}],
}

_CLEANUP = {
    "script": "sales/undo.sh",
    "interpreter": "bash",
    "args": [[{"text": "--all"}]],
    "retries": 1,
    "timeout_ms": 5000,
    "handles": ["Job"],
}


class _Check(Outcome):
    SUCCESS = 0
    OTHER = 4


def test_declared_outcomes_apply_without_an_enum_and_an_enum_replaces_them() -> None:
    assert _script_task(_WITH_OUTCOMES)["outcomes"] == _WITH_OUTCOMES["outcomes"]
    assert _script_task(_WITH_OUTCOMES, outcomes=_Check)["outcomes"] == [
        {"name": "SUCCESS", "exit_code": 0},
        {"name": "OTHER", "exit_code": 4},
    ]


def test_routing_on_declared_outcomes_needs_an_enum() -> None:
    with _asking(_WITH_OUTCOMES), Dag("d"):
        op = ScriptOperator("clean", _RESOURCE_JOB)
        assert op.on(DefaultOutcome.SUCCESS) is not None
        with pytest.raises(TypeError, match=r"give the task `outcomes=` an `Outcome` enum"):
            op.on(_Check.OTHER)
        with pytest.raises(TypeError, match=r"`clean.outcome`: its `schedr.toml` declares"):
            _ = op.outcome


def test_success_cant_be_routed_on_when_its_not_declared() -> None:
    only = {**_DECLARED, "outcomes": [{"name": "NO_DATA", "exit_code": 3}]}
    with _asking(only), Dag("d"):
        op = ScriptOperator("clean", _RESOURCE_JOB)
        with pytest.raises(TypeError, match=r"its `schedr.toml` declares NO_DATA"):
            op.on(DefaultOutcome.SUCCESS)


def test_a_declared_cleanup_comes_with_its_settings() -> None:
    cleanup = _script_task({**_DECLARED, "cleanup": _CLEANUP})["cleanup"]
    assert cleanup == {
        "run": {
            "script": {
                "script": "sales/undo.sh",
                "interpreter": "bash",
                "args": [[{"text": "--all"}]],
            }
        },
        "retries": 1,
        "retry_delay_ms": 30000,
        "timeout_ms": 5000,
        "handles": ["Job"],
    }


def test_the_operator_changes_a_cleanups_settings_over_the_declared_and_the_defaults() -> None:
    answer = {**_DECLARED, "cleanup": _CLEANUP}
    cleanup = _script_task(
        answer,
        cleanup_retries=7,
        cleanup_timeout=timedelta(seconds=9),
        cleanup_retry_delay=timedelta(seconds=2),
        cleanup_anywhere=True,
    )["cleanup"]
    assert (cleanup["retries"], cleanup["timeout_ms"], cleanup["retry_delay_ms"]) == (7, 9000, 2000)
    assert cleanup["anywhere"] is True
    assert cleanup["run"]["script"]["script"] == "sales/undo.sh"
    defaults = TaskDefaults(cleanup_retries=5, cleanup_retry_delay=timedelta(seconds=1))
    with _asking(answer), Dag("d", defaults=defaults):
        ScriptOperator("clean", _RESOURCE_JOB)
    cleanup = only_dag().tasks[0].to_json()["cleanup"]
    assert (cleanup["retries"], cleanup["retry_delay_ms"]) == (1, 1000), "declared, then defaults"


def test_a_cleanup_setting_without_a_declared_cleanup_is_refused_in_a_parse_only() -> None:
    with pytest.raises(TypeError, match=r"`cleanup_retries`, but its declaration has no"):
        _script_task(_DECLARED, cleanup_retries=1)
    registry.take_defined()
    with Dag("d"):  # only named: a task's import must not fail (decision 306)
        ScriptOperator("clean", _RESOURCE_JOB, cleanup_retries=1)
    assert "cleanup" not in only_dag().tasks[0].to_json()


def _resource_tasks(answer: dict[str, Any], build: Any) -> dict[str, dict[str, Any]]:
    registry.take_defined()
    with _asking(answer), Dag("d"):
        build()
    return {task.id: task.to_json() for task in only_dag().tasks}


def test_a_script_resource_reports_its_value_and_keeps_its_setups_cleanup() -> None:
    @dataclass
    class Info:
        cluster_id: str

    tasks = _resource_tasks(
        {**_DECLARED, "cleanup": _CLEANUP},
        lambda: ScriptResource("cluster", _RESOURCE_JOB, value=Info),
    )
    assert tasks["cluster"]["resource"] == {"role": "setup"}
    assert tasks["cluster"]["output_type"]["name"] == "Info"
    assert tasks["cluster"]["cleanup"]["run"]["script"]["script"] == "sales/undo.sh"
    assert tasks["cluster.cleanup"]["kind"] == "empty"


def test_a_resource_refuses_a_declaration_with_outcomes() -> None:
    with pytest.raises(TypeError, match=r"declares `\[outcomes\]`.*only on exit code 0"):
        _resource_tasks(_WITH_OUTCOMES, lambda: ScriptResource("cluster", _RESOURCE_JOB))


def test_cleanup_script_refuses_outcomes_and_a_cleanup_of_its_own() -> None:
    def build() -> None:
        Resource("r").cleanup_script(_RESOURCE_JOB)

    with pytest.raises(TypeError, match=r"declares `\[outcomes\]`"):
        _resource_tasks(_WITH_OUTCOMES, build)
    with pytest.raises(TypeError, match=r"declares `\[cleanup\]`.*no cleanup of its own"):
        _resource_tasks({**_DECLARED, "cleanup": _CLEANUP}, build)


def test_cleanup_script_takes_the_setups_value_and_its_settings_in_order() -> None:
    @resource
    def cluster() -> str:
        return "c"

    def build() -> None:
        made = cluster()
        cluster.cleanup_script(
            _RESOURCE_JOB, args=[t"{cluster.value}"], env={"A": "1"}, retries=9, blocking=False
        )
        del made

    tasks = _resource_tasks(_DECLARED, build)
    drop = tasks["cluster.cleanup"]
    assert drop["kind"] == "script"
    assert drop["resource"] == {"role": "cleanup", "setup": "cluster"}
    assert drop["settings"]["retries"] == 9, "its own option over the declared 2"
    assert drop["settings"]["timeout_ms"] == 60000, "the declaration's"
    assert drop["settings"]["queue"] == "scripts", "the declaration's over the setup's"
    assert drop["env"]["A"] == [{"text": "1"}]
    assert drop["env"]["MODE"] == [{"text": "full"}]
    assert drop["args"] == [[{"ref": {"kind": "output", "task": "cluster"}}]]


def test_cleanup_script_without_settings_takes_a_cleanups_defaults() -> None:
    bare = {k: v for k, v in _DECLARED.items() if k not in {"retries", "timeout_ms", "queue"}}
    tasks = _resource_tasks(bare, lambda: Resource("r").cleanup_script(_RESOURCE_JOB))
    assert tasks["r.cleanup"]["settings"]["retries"] == 3


def test_a_resource_has_one_of_the_three_cleanups() -> None:
    def script_then_command() -> None:
        made = Resource("r")
        made.cleanup_script(_RESOURCE_JOB)
        made.cleanup_command("true")

    def command_then_script() -> None:
        made = Resource("r")
        made.cleanup_command("true")
        made.cleanup_script(_RESOURCE_JOB)

    def function_then_script() -> None:
        made = Resource("r")
        made.cleanup(lambda: None)
        made.cleanup_script(_RESOURCE_JOB)

    for build in (script_then_command, command_then_script, function_then_script):
        with pytest.raises(TypeError, match=r"resource `r` has a cleanup already"):
            _resource_tasks(_DECLARED, build)
