import dataclasses
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest

from schedr import (
    CommandOperator,
    Dag,
    Data,
    DefaultOutcome,
    EmptyOperator,
    Outcome,
    RunIf,
    TaskDefaults,
    TypingHint,
    on,
    task,
)
from schedr._model import registry
from schedr._serialization import CodecError, decode, describe, encode
from schedr.outcomes import DEFAULT, for_command, from_return_type
from schedr.types import ProcessResult

type Validate = Callable[[str, Any], None]


class Rows(Data):
    rows: list[int]


class Unavailable(Data):
    reason: str


class Page[T](Data):
    items: list[T]


class SpecialRows(Rows):
    pass


class Check(Outcome):
    CHANGED = 0
    UNCHANGED = 3


class Other(Outcome):
    YES = 0


@task
def fetch() -> Rows | Unavailable:
    return Rows([1])


@task
def check() -> Check:
    return Check.CHANGED


@task
def count() -> int:
    return 1


@task
def load(rows: Rows | None) -> int:
    return 0 if rows is None else len(rows.rows)


@task
def load_strict(rows: Rows) -> int:
    return len(rows.rows)


@task
def either(value: Rows | Unavailable | None) -> None:
    pass


@task
def maybe(value: int | None) -> None:
    pass


@task
def numbers(values: list[int] | None) -> int:
    return 0 if values is None else len(values)


@task
def react(outcome: Check) -> str:
    return outcome.name


def only_dag() -> Any:
    (dag,) = registry.take_defined()
    return dag


def edges(model: Any) -> list[tuple[str, str, str, tuple[str, ...]]]:
    return [(e.from_, e.to, e.kind.value, e.on) for e in model.edges]


@pytest.mark.parametrize(
    ("annotation", "names"),
    [
        (Rows | Unavailable, ("Rows", "Unavailable")),
        (Rows | None, ("Rows", "None")),
        (Rows | Unavailable | None, ("Rows", "Unavailable", "None")),
        (Check, ("CHANGED", "UNCHANGED")),
        # Ordinary data: SUCCESS.
        (int, ("SUCCESS",)),
        (int | None, ("SUCCESS",)),
        (int | str, ("SUCCESS",)),
        (Rows | int, ("SUCCESS",)),
        (Rows, ("SUCCESS",)),
        (list[Rows | Unavailable], ("SUCCESS",)),
    ],
)
def test_outcomes_come_from_the_return_type(annotation: object, names: tuple[str, ...]) -> None:
    assert from_return_type(annotation, "f").names == names


def test_an_alias_of_an_outcome_union_is_one() -> None:
    type Fetched = Rows | Unavailable
    assert from_return_type(Fetched, "f").names == ("Rows", "Unavailable")


@pytest.mark.parametrize(
    ("annotation", "message"),
    [
        (Rows | SpecialRows, "is a subclass of `Rows`"),
        (Page[int] | Page[str], "appears with different type parameters"),
        (Check | Rows, "either a union of classes or one Outcome enum"),
    ],
)
def test_outcome_unions_the_runner_couldnt_tell_apart_are_refused(
    annotation: object, message: str
) -> None:
    with pytest.raises(TypeError, match=message):
        from_return_type(annotation, "f")


def test_two_classes_of_one_name_are_refused() -> None:
    elsewhere = type("Rows", (), {"__module__": "elsewhere"})
    elsewhere = dataclass(elsewhere)
    with pytest.raises(TypeError, match="two classes are named `Rows`"):
        from_return_type(Rows | elsewhere, "f")


def test_on_narrows_and_makes_an_edge_taken_on_that_outcome(validate: Validate) -> None:
    with Dag("d"):
        fetched = fetch()
        load(fetched.on(Rows))
        either(fetched.on(Rows, Unavailable))
    model = only_dag()
    assert edges(model) == [
        ("fetch", "load", "data", ("Rows",)),
        ("fetch", "either", "data", ("Rows", "Unavailable")),
    ]
    (fetch_task, load_task, _) = model.tasks
    assert fetch_task.to_json()["outcomes"] == [{"name": "Rows"}, {"name": "Unavailable"}]
    assert load_task.to_json()["arguments"]["rows"] == {
        "ref": {"kind": "output", "task": "fetch", "on": ["Rows"]}
    }
    validate("Dag", model.to_json())


def test_two_conditions_on_one_task_are_two_edges() -> None:
    @task
    def both(a: Rows | None, b: Unavailable | None) -> None:
        pass

    with Dag("d"):
        fetched = fetch()
        both(fetched.on(Rows), fetched.on(Unavailable))
    assert edges(only_dag()) == [
        ("fetch", "both", "data", ("Rows",)),
        ("fetch", "both", "data", ("Unavailable",)),
    ]


@pytest.mark.parametrize(
    ("narrow", "message"),
    [
        (lambda f, c, n: f.on(Check.CHANGED), "one of its outcomes"),
        (lambda f, c, n: f.on(ProcessResult), "`ProcessResult` isn't one of its outcomes"),
        (lambda f, c, n: c.on(Other.YES), "Check's"),
        (lambda f, c, n: c.on(Rows), "not one of its outcomes"),
        (lambda f, c, n: n.on(Check.CHANGED), "DefaultOutcome's"),
        (lambda f, c, n: f.on(), "at least one outcome"),
    ],
)
def test_on_takes_only_the_tasks_own_outcomes(narrow: Any, message: str) -> None:
    with Dag("d"), pytest.raises(TypeError, match=message):
        narrow(fetch(), check(), count())
    registry.take_defined()


def test_a_plain_task_takes_its_default_outcome() -> None:
    with Dag("d"):
        maybe(count().on(DefaultOutcome.SUCCESS))
    assert edges(only_dag()) == [("count", "maybe", "data", ("SUCCESS",))]


def test_a_path_into_an_outcome_union_needs_narrowing_first(validate: Validate) -> None:
    with Dag("d"), pytest.raises(TypeError, match=r"narrow it first, e.g. `fetch.on\(Rows\)"):
        numbers(fetch().rows)
    registry.take_defined()
    with Dag("d"):
        numbers(fetch().on(Rows).rows)
    validate("Dag", only_dag().to_json())


def test_on_narrows_the_whole_output_only() -> None:
    with Dag("d"), pytest.raises(TypeError, match="narrow the whole output first"):
        fetch().on(Rows).on(Rows)
    registry.take_defined()


def test_outcome_passes_an_enum_tasks_outcome_and_isnt_for_unions(validate: Validate) -> None:
    with Dag("d"):
        react(check().outcome)  # type: ignore[arg-type]
    model = only_dag()
    assert model.tasks[1].to_json()["arguments"]["outcome"] == {
        "ref": {"kind": "outcome", "task": "check"}
    }
    assert edges(model) == [("check", "react", "data", ())]
    # An enum task's output is None: the member is its outcome.
    assert model.tasks[0].output_type.to_json() == {"type": "none"}
    validate("Dag", model.to_json())
    with Dag("d"), pytest.raises(TypeError, match="union of classes"):
        fetch().outcome  # noqa: B018 (the property raises)
    registry.take_defined()


def test_a_task_fed_only_by_a_narrowed_value_is_skipped_so_needs_no_none() -> None:
    # Its only edge not taken: skipped, never given None.
    with Dag("d"):
        load_strict(fetch().on(Rows))
    assert edges(only_dag()) == [("fetch", "load_strict", "data", ("Rows",))]


def test_a_narrowed_value_must_allow_none_when_another_edge_can_run_the_task() -> None:
    @task
    def merge(primary: Rows, mirror: Rows | None) -> None:
        pass

    def define() -> None:
        with Dag("d"):
            merge(fetch().on(Rows), fetch.override(task_id="mirror")().on(Rows))

    # Either source alone runs it: the other is None.
    with pytest.raises(TypeError, match=r"argument `primary`: `fetch.on\(Rows\)`"):
        define()
    registry.take_defined()
    # ALL_SUCCEEDED: skipped instead, so no None.
    with Dag("d"):
        merge.override(run_if=RunIf.ALL_SUCCEEDED)(
            fetch().on(Rows), fetch.override(task_id="mirror")().on(Rows)
        )
    registry.take_defined()


def test_an_order_edge_added_later_is_counted() -> None:
    def define() -> None:
        with Dag("d"):
            strict = load_strict(fetch().on(Rows))
            count() >> strict

    with pytest.raises(TypeError, match="runs through another"):
        define()
    registry.take_defined()


def test_a_task_after_one_that_may_be_skipped_passes_none_through_another_edge() -> None:
    @task
    def take(n: int, m: int) -> int:
        return n + m

    def define() -> None:
        with Dag("d"):
            take(load(fetch().on(Rows)), count())

    with pytest.raises(TypeError, match=r"argument `n`: `load` is None"):
        define()
    registry.take_defined()


@task
def judge(outcome: Check, n: int) -> None:
    pass


@task
def judge_any(outcome: Check | None, n: int) -> None:
    pass


@task
def total(values: list[int], n: int) -> None:
    pass


@task
def total_any(values: list[int | None], n: int) -> None:
    pass


@task
def by_name(values: dict[str, Rows], n: int) -> None:
    pass


def test_none_is_checked_for_outcome_references_and_at_any_depth() -> None:
    def judged(judge: Any) -> None:
        checked = check()
        fetch().on(Unavailable) >> checked
        judge(checked.outcome, count())

    cases = [
        (judged, judge, judge_any, r"argument `outcome`: `check.outcome` is None"),
        (
            lambda total: total([load_strict(fetch().on(Rows))], count()),
            total,
            total_any,
            r"argument `values`: `load_strict` is None",
        ),
        (
            lambda by_name: by_name({"a": fetch().on(Rows)}, count()),
            by_name,
            None,
            r"argument `values`: `fetch.on\(Rows\)` is None",
        ),
    ]
    for define, strict, lenient, message in cases:
        # Checked when the DAG block ends.
        with pytest.raises(TypeError, match=message), Dag("d"):
            define(strict)
        registry.take_defined()
        if lenient is not None:
            with Dag("d"):
                define(lenient)
            registry.take_defined()


def test_a_task_with_an_edge_always_taken_isnt_skipped_under_none_failed() -> None:
    @task
    def keep(rows: Rows | None, n: int) -> int:
        return n

    @task
    def publish(kept: int, n: int) -> None:
        pass

    def define(rule: RunIf) -> None:
        with Dag("d"):
            n = count()
            publish(keep.override(run_if=rule)(fetch().on(Rows), n), n)

    # `keep` runs in every run that `count` succeeds in: `kept` is never None.
    define(RunIf.NONE_FAILED)
    registry.take_defined()
    # Under ALL_SUCCEEDED, `keep` is skipped without Rows, and `publish` runs through `count`.
    with pytest.raises(TypeError, match=r"argument `kept`: `keep` is None"):
        define(RunIf.ALL_SUCCEEDED)
    registry.take_defined()


def test_order_edges_take_conditions_too() -> None:
    with Dag("d"):
        fetched = fetch()
        done = EmptyOperator("done")
        fetched.on(Unavailable) >> done
    assert edges(only_dag()) == [("fetch", "done", "order", ("Unavailable",))]


def test_a_command_declares_outcomes_with_exit_codes(validate: Validate) -> None:
    with Dag("d"):
        probe = CommandOperator("probe", "./check.sh", outcomes=Check)
        EmptyOperator("notify", run_if=RunIf.ALL_SUCCEEDED)
        probe.on(Check.UNCHANGED) >> EmptyOperator("quiet")
        react(probe.outcome)
    model = only_dag()
    assert model.tasks[0].to_json()["outcomes"] == [
        {"name": "CHANGED", "exit_code": 0},
        {"name": "UNCHANGED", "exit_code": 3},
    ]
    assert model.tasks[1].to_json()["settings"]["run_if"] == "all_succeeded"
    # Neither outcomes nor run_if when they are the defaults.
    assert "outcomes" not in model.tasks[2].to_json()
    assert "run_if" not in model.tasks[2].to_json()["settings"]
    validate("Dag", model.to_json())


def test_outcomes_for_a_command_are_checked() -> None:
    assert for_command(None, "t") == DEFAULT

    class Twice(Outcome):
        A = 0
        B = 0

    with pytest.raises(TypeError, match="Outcome enum"):
        for_command(int, "t")  # type: ignore[arg-type]
    # IntEnum aliases B to A: one name, so no clash to report.
    assert for_command(Twice, "t").names == ("A",)


def test_task_defaults_give_run_if() -> None:
    with Dag("d", defaults=TaskDefaults(run_if=RunIf.ALL_SUCCEEDED)):
        count()
    assert only_dag().tasks[0].settings.run_if == "all_succeeded"


def test_an_outcome_is_stored_by_its_members_name() -> None:
    assert describe(Check).to_json() == {"type": "str"}
    assert encode(Check.UNCHANGED, Check) == "UNCHANGED"
    assert decode("UNCHANGED", Check) is Check.UNCHANGED
    with pytest.raises(CodecError, match="`GONE` isn't one of Check's outcomes"):
        decode("GONE", Check)
    with pytest.raises(CodecError, match="expected a member of Check"):
        encode(Other.YES, Check)


@pytest.mark.parametrize(
    ("returns", "refused"),
    [
        ("Holder", True),
        ("dict[str, int]", True),
        ("Any", True),
        ("int", False),
        ("Rows", False),
        ("ProcessResult", False),
    ],
)
def test_outcome_is_refused_where_the_output_may_have_a_field_of_that_name(
    returns: str, refused: bool
) -> None:
    @dataclass
    class Holder:
        outcome: str

    kinds = {
        "Holder": Holder,
        "dict[str, int]": dict[str, int],
        "Any": Any,
        "int": int,
        "Rows": Rows,
        "ProcessResult": ProcessResult,
    }

    def plain() -> Any:
        return None

    plain.__annotations__["return"] = kinds[returns]
    with Dag("d"):
        out = task(plain)()
        if refused:
            with pytest.raises(TypeError, match=r'write `plain\["outcome"\]` for the field'):
                out.outcome  # noqa: B018 (the property raises)
        else:
            assert repr(out.outcome) == "Output(plain.outcome)"
    registry.take_defined()


def test_an_outcome_goes_only_where_a_str_or_outcome_enum_goes() -> None:
    @task
    def wants_int(n: int) -> None:
        pass

    with Dag("d"), pytest.raises(TypeError, match=r"expects `int`, but `check.outcome` is `str`"):
        wants_int(check().outcome)  # type: ignore[arg-type]
    registry.take_defined()


def test_a_data_subclass_is_a_dataclass_with_its_options() -> None:
    class Frozen(Data, frozen=True, kw_only=True):
        rows: list[int]
        note: str = ""

    value = Frozen(rows=[1])
    assert dataclasses.is_dataclass(value)
    assert value == Frozen(rows=[1], note="")
    with pytest.raises(dataclasses.FrozenInstanceError):
        value.rows = []  # type: ignore[misc]
    with pytest.raises(TypeError):
        Frozen([1])  # type: ignore[misc]  # kw_only
    # A subclass of one is a dataclass of its own fields too.
    assert [f.name for f in dataclasses.fields(SpecialRows)] == ["rows"]
    assert Rows([1]) == Rows([1])


def test_data_refuses_options_that_arent_dataclass_options() -> None:
    with pytest.raises(TypeError, match=r"`Bad\(Data, \.\.\.\)`: unknown option\(s\) slots"):

        class Bad(Data, slots=True):  # type: ignore[call-arg]
            x: int


def test_plain_outcome_classes_run_but_get_a_typing_hint() -> None:
    @dataclass
    class PlainRows:
        rows: list[int]

    @dataclass
    class PlainGone:
        reason: str

    with pytest.warns(TypingHint) as hints:
        spec = from_return_type(PlainRows | PlainGone | None, "task `fetch`")
    assert spec.names == ("PlainRows", "PlainGone", "None")
    assert [str(h.message) for h in hints] == [
        "task `fetch`: outcome class `PlainRows` derives from neither schedr.Data nor "
        "schedr.pydantic.Model, so type checkers reject `.on(PlainRows)`; derive it from "
        "schedr.Data, or write `on(output, PlainRows)`",
        "task `fetch`: outcome class `PlainGone` derives from neither schedr.Data nor "
        "schedr.pydantic.Model, so type checkers reject `.on(PlainGone)`; derive it from "
        "schedr.Data, or write `on(output, PlainGone)`",
    ]


def test_data_outcome_classes_and_data_that_isnt_an_outcome_get_no_hint() -> None:
    @dataclass
    class Plain:
        rows: list[int]

    with warnings.catch_warnings():
        warnings.simplefilter("error", TypingHint)
        assert from_return_type(Rows | Unavailable | None, "task `a`").names == (
            "Rows",
            "Unavailable",
            "None",
        )
        # Ordinary data: no outcomes, so nothing to narrow.
        assert from_return_type(Plain, "task `b`") is DEFAULT
        assert from_return_type(list[Plain | None], "task `c`") is DEFAULT


def test_the_on_function_narrows_as_the_method_does() -> None:
    @dataclass
    class PlainRows:
        rows: list[int]

    @dataclass
    class PlainGone:
        reason: str

    @task
    def plain_fetch() -> PlainRows | PlainGone:
        return PlainRows([1])

    @task
    def plain_load(rows: PlainRows) -> int:
        return len(rows.rows)

    with Dag("d"), pytest.warns(TypingHint):
        plain_load(on(plain_fetch(), PlainRows))
    assert edges(only_dag()) == [("plain_fetch", "plain_load", "data", ("PlainRows",))]


def test_the_on_function_refuses_what_isnt_an_output() -> None:
    class Gone(Data):
        reason: str

    with pytest.raises(TypeError, match=r"on\(\): expects a task's output in a DAG body, got 3"):
        on(3, Rows)
    with Dag("d"), pytest.raises(TypeError, match=r"`Gone` isn't one of its outcomes"):
        on(fetch(), Gone)
