from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from schedr import Dag, EmptyOperator, Output, chain, dag_ctx, task
from schedr._model import (
    DagCtxField,
    DagCtxRef,
    DictType,
    EdgeKind,
    IdError,
    ListType,
    ListValue,
    LiteralValue,
    OutputRef,
    PythonKind,
    RefValue,
    ScalarType,
    Template,
    TemplateRef,
    TemplateValue,
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
    assert first.output_type == ListType(DictType(ScalarType("str")))
    assert second.output_type == ScalarType("int")
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
    model = only_dag()
    assert model.tasks[1].kind.arguments["value"] == RefValue(OutputRef("extract", (0, "id")))
    assert repr(out[0].name) == "Output(extract[0].name)"


def test_a_path_that_doesnt_exist_is_a_parse_error() -> None:
    @task
    def use(value: Any) -> None: ...

    with (
        pytest.raises(
            TypeError,
            match=r"`extract\.rows` doesn't exist: the output of `extract` is "
            r"`list\[dict\[str, str\]\]`",
        ),
        Dag("d"),
    ):
        use(extract().rows)


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


def test_preload_and_fresh_process_default_to_forking_and_can_be_overridden() -> None:
    bare = load.override(task_id="bare", preload=False)
    fresh = load.override(task_id="fresh", fresh_process=True)
    with Dag("d"):
        rows = extract()
        load(rows)
        bare(rows)
        fresh(rows)
    starts = {t.id: (t.settings.preload, t.settings.fresh_process) for t in only_dag().tasks}
    assert starts == {
        "extract": (True, False),
        "load": (True, False),
        "bare": (False, False),
        "fresh": (True, True),
    }


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
        pytest.raises(TypeError, match="task `load`, argument `rows`: set is not supported"),
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

    async def events() -> AsyncIterator[int]:
        yield 1

    with pytest.raises(
        TypeError, match=r"async tasks come with milestone M4; to wait .* \(@sensor"
    ):
        task(coro)
    with pytest.raises(TypeError, match=r"is an async generator: .* trigger \(@trigger"):
        task(events)
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


def test_return_types_are_described_in_the_dag_json(validate: Validate) -> None:
    @task
    def rows() -> dict[str, list[int]]: ...

    with Dag("d"):
        rows()
    model = only_dag()
    assert model.tasks[0].output_type == DictType(ListType(ScalarType("int")))
    validate("Dag", model.to_json())


def test_an_unsupported_return_type_is_a_parse_error() -> None:
    @task
    def bad() -> dict[int, str]: ...

    with (
        pytest.raises(
            TypeError, match=r"task `bad`: return type `dict\[int, str\]`: .*keys must be `str`"
        ),
        Dag("d"),
    ):
        bad()


def test_annotations_are_resolved_when_the_task_is_called() -> None:
    @task
    def later() -> Later: ...  # noqa: F821

    with pytest.raises(TypeError, match="task `later`: can't resolve an annotation"), Dag("d"):
        later()


def test_an_output_must_fit_the_parameter() -> None:
    @task
    def wants_int(n: int) -> None: ...

    with (
        pytest.raises(
            TypeError,
            match=r"argument `n`: expects `int`, but `extract` is `list\[dict\[str, str\]\]`",
        ),
        Dag("d"),
    ):
        wants_int(extract())


def test_references_inside_containers_must_fit_their_element() -> None:
    @task
    def name() -> str: ...

    @task
    def total(values: list[int]) -> None: ...

    @task
    def by_key(values: dict[str, int]) -> None: ...

    @task
    def anything(values: list[Any]) -> None: ...

    @task
    def count() -> int: ...

    with (
        pytest.raises(TypeError, match=r"argument `values`: expects `int`, but `name` is `str`"),
        Dag("d"),
    ):
        total([1, name()])
    with (
        pytest.raises(TypeError, match=r"argument `values`: expects `int`, but `name` is `str`"),
        Dag("e"),
    ):
        by_key({"a": name()})
    with (
        pytest.raises(
            TypeError, match=r"expects `int`, but `dag_ctx\.run\.planned_for` is `datetime`"
        ),
        Dag("f"),
    ):
        total([dag_ctx.run.planned_for])
    with Dag("g"):
        anything([name(), dag_ctx.run.planned_for])
    # The literals beside a reference are checked too (Grok's review, 2026-10-29).
    with (
        pytest.raises(TypeError, match=r"argument `values`\[0\]: expected int, got bool"),
        Dag("h"),
    ):
        total([True, count()])


def test_parts_and_dag_ctx_are_checked_too() -> None:
    @task
    def wants_str(s: str) -> None: ...

    @task
    def wants_date(d: datetime) -> None: ...

    with Dag("d"):
        wants_str(extract()[0]["id"])
        wants_date(dag_ctx.run.planned_for)
    registry.take_defined()
    with (
        pytest.raises(
            TypeError, match=r"expects `int`, but `dag_ctx\.run\.planned_for` is `datetime`"
        ),
        Dag("e"),
    ):
        load.override(task_id="x")(rows=[], limit=dag_ctx.run.planned_for)


def test_literal_arguments_are_encoded_against_the_parameter() -> None:
    @task
    def at(when: datetime, pair: tuple[int, str]) -> None: ...

    with Dag("d"):
        at(datetime(2026, 1, 1, tzinfo=UTC), (1, "a"))
    arguments = only_dag().tasks[0].kind.arguments
    assert arguments["when"] == LiteralValue({"$datetime": "2026-01-01T00:00:00+00:00"})
    assert arguments["pair"] == LiteralValue({"$tuple": [1, "a"]})
    with pytest.raises(TypeError, match="argument `limit`: expected int, got str"), Dag("e"):
        load([], limit="ten")  # type: ignore[arg-type]


def test_an_unsupported_parameter_type_is_a_parse_error() -> None:
    @task
    def odd(values: set[int]) -> None: ...

    with (
        pytest.raises(TypeError, match=r"argument `values`: parameter type `set\[int\]`"),
        Dag("d"),
    ):
        odd([])  # type: ignore[arg-type]


def test_t_strings_in_task_arguments() -> None:
    @task
    def greet(message: str, lines: list[str]) -> None: ...

    with Dag("d"):
        rows = extract()
        greet(t"day {dag_ctx.run.planned_for:%Y-%m-%d}", [t"first {rows[0]['id']}", "plain"])
    model = only_dag()
    arguments = model.tasks[1].kind.arguments
    assert arguments["message"] == TemplateValue(
        Template(["day ", TemplateRef(DagCtxRef(DagCtxField.PLANNED_FOR), "%Y-%m-%d")])
    )
    assert arguments["lines"] == ListValue(
        [
            TemplateValue(Template(["first ", TemplateRef(OutputRef("extract", (0, "id")))])),
            LiteralValue("plain"),
        ]
    )
    assert edges(model) == [("extract", "greet", EdgeKind.DATA)]


def test_a_t_string_needs_a_parameter_that_accepts_str() -> None:
    with pytest.raises(TypeError, match="argument `limit`: expects `int`, but a t-string gives"):
        pass_t_string_as_int()


def pass_t_string_as_int() -> None:
    with Dag("d"):
        load([], limit=t"10")  # type: ignore[arg-type]


def test_references_in_argument_t_strings_must_render() -> None:
    with pytest.raises(TypeError, match=r"a value of type `list\[dict\[str, str\]\]` can't be"):
        render_list_in_t_string()


def render_list_in_t_string() -> None:
    @task
    def say(text: str) -> None: ...

    with Dag("d"):
        say(t"{extract()}")
