from datetime import timedelta

import pytest

from schedr import _build
from schedr._model import (
    DagCtxField,
    DagCtxRef,
    EdgeKind,
    EmptyKind,
    IdError,
    ListValue,
    LiteralValue,
    MapValue,
    OutputRef,
    RefValue,
    ScalarType,
    TaskSettings,
    Template,
    TemplateRef,
)
from schedr.tasks import Output


class FakeRef:
    def __init__(self, reference: object) -> None:
        self.reference = reference

    def _schedr_reference(self) -> object:
        return self.reference


def builder() -> _build.DagBuilder:
    return _build.DagBuilder("d", _build.TaskOptions())


def add(b: _build.DagBuilder, *task_ids: str) -> None:
    for task_id in task_ids:
        b.add_task(task_id, EmptyKind(), ScalarType("none"), _build.TaskOptions())


def test_is_reference_recognizes_reference_objects() -> None:
    assert _build.is_reference(Output("a"))
    assert not _build.is_reference("a")
    assert not _build.is_reference(None)


def test_options_fall_back_to_the_base() -> None:
    own = _build.TaskOptions(retries=3, queue="large")
    base = _build.TaskOptions(retries=1, retry_delay=timedelta(seconds=10), queue="default")
    assert own.over(base) == _build.TaskOptions(
        retries=3, retry_delay=timedelta(seconds=10), queue="large"
    )


def test_settings_use_the_builtin_defaults_where_nothing_is_set() -> None:
    assert _build.TaskOptions().settings() == TaskSettings()
    settings = _build.TaskOptions(
        retries=2, retry_delay=timedelta(minutes=1), timeout=timedelta(hours=1), max_output_size=7
    ).settings()
    assert settings == TaskSettings(
        retries=2, retry_delay_ms=60_000, timeout_ms=3_600_000, max_output_size=7
    )


@pytest.mark.parametrize(
    ("duration", "ms"),
    [(timedelta(0), 0), (timedelta(milliseconds=5), 5), (timedelta(days=1), 86_400_000)],
)
def test_to_ms_converts_whole_milliseconds(duration: timedelta, ms: int) -> None:
    assert _build.to_ms(duration, "x") == ms


def test_to_ms_rejects_bad_durations() -> None:
    with pytest.raises(TypeError, match="timeout must be a timedelta, got int"):
        _build.to_ms(5, "timeout")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="must not be negative"):
        _build.to_ms(timedelta(seconds=-1), "x")
    with pytest.raises(ValueError, match="whole number of milliseconds"):
        _build.to_ms(timedelta(microseconds=1500), "x")


def test_add_task_resolves_settings_against_the_dag_defaults() -> None:
    b = _build.DagBuilder("d", _build.TaskOptions(retries=4, queue="q"))
    b.add_task("t", EmptyKind(), ScalarType("none"), _build.TaskOptions(queue="own"))
    assert b.tasks["t"].settings == TaskSettings(retries=4, queue="own")


def test_add_task_rejects_duplicates_and_bad_ids() -> None:
    b = builder()
    add(b, "t")
    with pytest.raises(IdError, match=r"defined twice in DAG `d`.*\.override\(task_id=\.\.\.\)"):
        add(b, "t")
    with pytest.raises(IdError):
        add(b, "bad id")


def test_add_edge_dedupes_and_checks_both_ends() -> None:
    b = builder()
    add(b, "a", "b")
    b.add_edge("a", "b", EdgeKind.ORDER)
    b.add_edge("a", "b", EdgeKind.ORDER)
    b.add_edge("a", "b", EdgeKind.DATA)
    assert [(e.from_, e.to, e.kind) for e in b.edges] == [
        ("a", "b", EdgeKind.ORDER),
        ("a", "b", EdgeKind.DATA),
    ]
    with pytest.raises(ValueError, match="task `x` is not part of DAG `d`"):
        b.add_edge("a", "x", EdgeKind.ORDER)
    with pytest.raises(ValueError, match="can't depend on itself"):
        b.add_edge("a", "a", EdgeKind.ORDER)


def test_find_cycle() -> None:
    b = builder()
    add(b, "a", "b", "c", "d")
    b.add_edge("a", "b", EdgeKind.ORDER)
    b.add_edge("b", "c", EdgeKind.ORDER)
    b.add_edge("a", "d", EdgeKind.ORDER)
    assert b.find_cycle() is None
    b.add_edge("c", "a", EdgeKind.ORDER)
    assert b.find_cycle() == ["a", "b", "c", "a"]


def test_the_active_builder_stack() -> None:
    assert _build.active() is None
    with pytest.raises(RuntimeError, match="EmptyOperator must be created inside `with Dag"):
        _build.require_active("EmptyOperator")
    outer = builder()
    _build.push(outer)
    try:
        assert _build.active() is outer
        assert _build.require_active("x") is outer
        with pytest.raises(RuntimeError, match="DAGs can't be nested"):
            _build.push(_build.DagBuilder("inner", _build.TaskOptions()))
    finally:
        _build.pop()
    assert _build.active() is None


def test_to_value_keeps_literals_literal() -> None:
    upstream: list[str] = []
    assert _build.to_value(5, "x", upstream) == LiteralValue(5)
    assert _build.to_value({"a": [1, None, "s", 1.5, True]}, "x", upstream) == LiteralValue(
        {"a": [1, None, "s", 1.5, True]}
    )
    assert _build.to_value((1, 2), "x", upstream) == LiteralValue([1, 2])
    assert upstream == []


def test_to_value_keeps_references_visible_in_containers() -> None:
    upstream: list[str] = []
    value = _build.to_value(
        {"rows": Output("extract"), "day": FakeRef(DagCtxRef(DagCtxField.PLANNED_FOR)), "n": 1},
        "x",
        upstream,
    )
    assert value == MapValue(
        {
            "rows": RefValue(OutputRef("extract")),
            "day": RefValue(DagCtxRef(DagCtxField.PLANNED_FOR)),
            "n": LiteralValue(1),
        }
    )
    assert _build.to_value([Output("a"), 2], "x", upstream) == ListValue(
        [RefValue(OutputRef("a")), LiteralValue(2)]
    )
    assert upstream == ["extract", "a"]


@pytest.mark.parametrize(
    ("value", "error", "message"),
    [
        ({1: "a"}, TypeError, r"x: dictionary keys must be str, got int"),
        ({"a": {2: "b"}}, TypeError, r"x\['a'\]: dictionary keys must be str"),
        (float("nan"), ValueError, "NaN and infinity"),
        ([float("inf")], ValueError, r"x\[0\]: NaN and infinity"),
        ({1, 2}, TypeError, "values of type set can't be passed yet"),
    ],
)
def test_to_value_rejects_unstorable_values(
    value: object, error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        _build.to_value(value, "x", [])


def test_to_template() -> None:
    upstream: list[str] = []
    assert _build.to_template("ls", "x", upstream) == Template(["ls"])
    assert _build.to_template(Output("a", ("stdout",)), "x", upstream) == Template(
        [TemplateRef(OutputRef("a", ("stdout",)))]
    )
    assert _build.to_template(FakeRef(DagCtxRef(DagCtxField.SINCE)), "x", upstream) == Template(
        [TemplateRef(DagCtxRef(DagCtxField.SINCE))]
    )
    assert upstream == ["a"]
    with pytest.raises(TypeError, match="x: expected a string or a reference, got int"):
        _build.to_template(5, "x", upstream)


def test_task_ids_of_operands() -> None:
    assert _build.task_ids_of(Output("a"), ">>") == ["a"]
    assert _build.task_ids_of([Output("a"), Output("b", ("rows",))], ">>") == ["a", "b"]
    with pytest.raises(TypeError, match="`>>` needs tasks or outputs, got str"):
        _build.task_ids_of("a", ">>")


def test_order_adds_order_edges_between_all_pairs() -> None:
    b = builder()
    add(b, "a", "b", "c")
    _build.push(b)
    try:
        _build.order([Output("a"), Output("b")], Output("c"), ">>")
    finally:
        _build.pop()
    assert [(e.from_, e.to) for e in b.edges] == [("a", "c"), ("b", "c")]
    assert {e.kind for e in b.edges} == {EdgeKind.ORDER}


def test_order_outside_a_dag_fails() -> None:
    with pytest.raises(RuntimeError, match="`>>` must be created inside"):
        _build.order(Output("a"), Output("b"), ">>")


def test_any_type() -> None:
    assert _build.any_type() == ScalarType("any")
