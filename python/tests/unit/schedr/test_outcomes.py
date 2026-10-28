from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest

from schedr import (
    CommandOperator,
    Dag,
    DefaultOutcome,
    EmptyOperator,
    Outcome,
    RunIf,
    TaskDefaults,
    task,
)
from schedr._model import registry
from schedr._serialization import CodecError, decode, describe, encode
from schedr.outcomes import DEFAULT, for_command, from_return_type
from schedr.types import ProcessResult

type Validate = Callable[[str, Any], None]


@dataclass
class Rows:
    rows: list[int]


@dataclass
class Unavailable:
    reason: str


@dataclass
class Page[T]:
    items: list[T]


@dataclass
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
        probe.output.on(Check.UNCHANGED) >> EmptyOperator("quiet")
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
