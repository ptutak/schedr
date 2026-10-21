from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest

from schedr import Dag, EmptyOperator, Output, chain, dag_ctx, task
from schedr._model import (
    DagCtxField,
    DagCtxRef,
    EdgeKind,
    IdError,
    ListValue,
    LiteralValue,
    OutputRef,
    PythonKind,
    RefValue,
    ScalarType,
    registry,
)

Validate = Callable[[str, Any], None]


@task
def extract() -> list[dict[str, str]]:
    return [{"id": "1"}]


@task(queue="large", retries=3)
def load(rows: list[dict[str, str]], limit: int = 10) -> int:
    return min(len(rows), limit)


def edges(model: Any) -> list[tuple[str, str, EdgeKind]]:
    return [(e.from_, e.to, e.kind) for e in model.edges]


def only_dag() -> Any:
    (dag,) = registry.take_defined()
    return dag


def test_outside_a_dag_a_task_is_the_plain_function() -> None:
    assert extract() == [{"id": "1"}]
    assert load([{"a": "b"}] * 20) == 10


def test_task_function_keeps_the_functions_identity() -> None:
    assert extract.__name__ == "extract"
    assert extract.task_id == "extract"
    assert load.__wrapped__.__name__ == "load"  # type: ignore[attr-defined]


def test_calls_inside_a_dag_record_tasks_and_data_edges(validate: Validate) -> None:
    with Dag("d"):
        rows = extract()
        count = load(rows, limit=5)
    assert isinstance(rows, Output)
    assert repr(count) == "Output(load)"
    model = only_dag()
    first, second = model.tasks
    assert first.kind == PythonKind({})
    assert second.kind == PythonKind(
        {"rows": RefValue(OutputRef("extract")), "limit": LiteralValue(5)}
    )
    assert second.settings.queue == "large"
    assert second.settings.retries == 3
    assert second.output_type == ScalarType("any")
    assert edges(model) == [("extract", "load", EdgeKind.DATA)]
    validate("Dag", model.to_json())


def test_defaults_of_parameters_are_not_stored() -> None:
    with Dag("d"):
        load(extract())
    assert set(only_dag().tasks[1].kind.arguments) == {"rows"}


def test_references_inside_containers_and_dag_ctx() -> None:
    @task
    def combine(parts: list[Any], day: Any) -> None: ...

    with Dag("d"):
        a = extract.override(task_id="a")()
        b = extract.override(task_id="b")()
        combine([a, b], dag_ctx.run.planned_for)
    model = only_dag()
    kind = model.tasks[2].kind
    assert kind.arguments["parts"] == ListValue(
        [RefValue(OutputRef("a")), RefValue(OutputRef("b"))]
    )
    assert kind.arguments["day"] == RefValue(DagCtxRef(DagCtxField.PLANNED_FOR))
    assert edges(model) == [("a", "combine", EdgeKind.DATA), ("b", "combine", EdgeKind.DATA)]


def test_paths_into_outputs() -> None:
    @task
    def use(value: Any) -> None: ...

    with Dag("d"):
        out = extract()
        use(out[0]["id"])
        use.override(task_id="use2")(out.rows)
    model = only_dag()
    assert model.tasks[1].kind.arguments["value"] == RefValue(OutputRef("extract", (0, "id")))
    assert model.tasks[2].kind.arguments["value"] == RefValue(OutputRef("extract", ("rows",)))
    assert repr(out[0].name) == "Output(extract[0].name)"


def test_output_paths_reject_bad_keys_and_private_names() -> None:
    out = Output[Any]("x")
    with pytest.raises(TypeError, match="output keys are str or int"):
        out[1.5]  # type: ignore[index]
    with pytest.raises(TypeError, match="output keys are str or int"):
        out[True]
    with pytest.raises(ValueError, match="negative indexes"):
        out[-1]
    with pytest.raises(AttributeError):
        _ = out._hidden


def test_an_output_cant_be_used_as_a_value() -> None:
    out = Output[Any]("x")
    with pytest.raises(TypeError, match="can't be used as a value in the DAG file"):
        bool(out)
    with pytest.raises(TypeError, match="can't be used as a value"):
        iter(out)
    with pytest.raises(TypeError, match="can't be used as a value"):
        str(out)


def test_override_changes_the_id_and_options_only_for_the_copy() -> None:
    eu = load.override(task_id="load_eu", queue="eu", timeout=timedelta(minutes=1))
    assert eu.task_id == "load_eu"
    assert load.task_id == "load"
    with Dag("d"):
        rows = extract()
        eu(rows)
        load(rows)
    _, eu_task, plain = only_dag().tasks
    assert (eu_task.id, eu_task.settings.queue, eu_task.settings.timeout_ms) == (
        "load_eu",
        "eu",
        60_000,
    )
    assert eu_task.settings.retries == 3
    assert (plain.settings.queue, plain.settings.timeout_ms) == ("large", None)


def test_override_keeps_the_id_when_none_is_given() -> None:
    assert load.override(retries=0).task_id == "load"


def call_extract_twice() -> None:
    with Dag("d"):
        extract()
        extract()


def test_calling_a_task_twice_needs_a_new_id() -> None:
    with pytest.raises(IdError, match=r"\.override\(task_id=\.\.\.\)"):
        call_extract_twice()


def test_wrong_arguments_name_the_task() -> None:
    with (
        pytest.raises(TypeError, match="task `load`: missing a required argument: 'rows'"),
        Dag("d"),
    ):
        load()  # type: ignore[call-arg]
    with (
        pytest.raises(TypeError, match="task `load`: got an unexpected keyword argument"),
        Dag("e"),
    ):
        load([], color="red")  # type: ignore[call-arg]


def test_unstorable_arguments_name_the_task_and_argument() -> None:
    with (
        pytest.raises(TypeError, match="task `load`, argument `rows`: values of type set"),
        Dag("d"),
    ):
        load({1})  # type: ignore[arg-type]


def test_the_return_type_must_be_annotated() -> None:
    def no_annotation():  # type: ignore[no-untyped-def]
        return 1

    with pytest.raises(TypeError, match="must annotate its return type"):
        task(no_annotation)


def test_forward_references_in_annotations_are_fine() -> None:
    def later() -> NotDefinedYet:  # type: ignore[name-defined]  # noqa: F821
        ...

    assert task(later).task_id == "later"


def test_var_args_are_rejected() -> None:
    def star(*args: int) -> None: ...

    def stars(**kwargs: int) -> None: ...

    with pytest.raises(TypeError, match=r"`\*args` isn't supported"):
        task(star)
    with pytest.raises(TypeError, match=r"`\*\*kwargs` isn't supported"):
        task(stars)


def test_only_plain_functions_can_be_tasks() -> None:
    async def coro() -> None: ...

    with pytest.raises(TypeError, match="async functions aren't supported"):
        task(coro)
    with pytest.raises(TypeError, match="@task needs a function, got int"):
        task(5)  # type: ignore[call-overload]


def test_task_id_option() -> None:
    @task(task_id="custom")
    def f() -> None: ...

    assert f.task_id == "custom"


def test_shift_operators_add_order_edges() -> None:
    with Dag("d"):
        a, b, c, d = (EmptyOperator(x) for x in "abcd")
        out = extract()
        out >> a
        [b, c] >> out
        a << d
        [c] << d
    assert edges(only_dag()) == [
        ("extract", "a", EdgeKind.ORDER),
        ("b", "extract", EdgeKind.ORDER),
        ("c", "extract", EdgeKind.ORDER),
        ("d", "a", EdgeKind.ORDER),
        ("d", "c", EdgeKind.ORDER),
    ]


def test_output_shift_returns_the_other_side_for_chaining() -> None:
    with Dag("d"):
        out = extract()
        a, b, c, d = (EmptyOperator(x) for x in "abcd")
        assert (out >> a) is a
        assert ([b] >> out) is out
        assert (out << c) is c
        assert ([d] << out) is out
    assert edges(only_dag()) == [
        ("extract", "a", EdgeKind.ORDER),
        ("b", "extract", EdgeKind.ORDER),
        ("c", "extract", EdgeKind.ORDER),
        ("extract", "d", EdgeKind.ORDER),
    ]


def test_chain_links_consecutive_items() -> None:
    with Dag("d"):
        a, b, c, d = (EmptyOperator(x) for x in "abcd")
        chain(a, [b, c], d)
    assert edges(only_dag()) == [
        ("a", "b", EdgeKind.ORDER),
        ("a", "c", EdgeKind.ORDER),
        ("b", "d", EdgeKind.ORDER),
        ("c", "d", EdgeKind.ORDER),
    ]


def test_a_repeated_dependency_is_recorded_once() -> None:
    with Dag("d"):
        a, b = EmptyOperator("a"), EmptyOperator("b")
        a >> b
        a >> b
    assert len(only_dag().edges) == 1
