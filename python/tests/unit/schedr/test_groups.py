from collections.abc import Iterator
from typing import Any

import pytest

from schedr import (
    Dag,
    EmptyOperator,
    Group,
    RunIf,
    TaskDefaults,
    chain,
    group,
    task,
)
from schedr._model import Group as GroupModel
from schedr._model import IdError, registry


@task
def extract() -> int:
    return 1


@task
def transform(n: int) -> int:
    return n


@task
def audit() -> None: ...


@pytest.fixture(autouse=True)
def clean_registry() -> Iterator[None]:
    registry.take_defined()
    yield
    registry.take_defined()


def only_dag() -> Any:
    (dag,) = registry.take_defined()
    return dag


def edges(model: Any) -> list[tuple[str, str, str]]:
    return sorted((e.from_, e.to, e.kind.value) for e in model.edges)


def test_a_groups_tasks_are_prefixed_and_belong_to_it() -> None:
    with Dag("d"), group("load"):
        transform(extract())
        with group("eu"):
            audit()
    model = only_dag()
    assert [(t.id, t.group) for t in model.tasks] == [
        ("load.extract", "load"),
        ("load.transform", "load"),
        ("load.eu.audit", "load.eu"),
    ]
    assert model.groups == [GroupModel("load"), GroupModel("load.eu", "load")]
    assert model.to_json()["groups"] == [{"id": "load"}, {"id": "load.eu", "parent": "load"}]
    assert edges(model) == [("load.extract", "load.transform", "data")]


def test_one_task_function_in_two_groups_needs_no_override() -> None:
    with Dag("d"):
        for source in ("eu", "us"):
            with group(source):
                transform(extract())
    assert [t.id for t in only_dag().tasks] == [
        "eu.extract",
        "eu.transform",
        "us.extract",
        "us.transform",
    ]


def test_settings_come_from_the_dag_the_groups_and_the_task_in_that_order() -> None:
    with (
        Dag("d", defaults=TaskDefaults(retries=1, queue="dag")),
        group("outer", defaults=TaskDefaults(retries=2, queue="outer"), run_if=RunIf.ALL_SUCCEEDED),
    ):
        extract()
        with group("inner", defaults=TaskDefaults(retries=3)):
            audit()
            transform.override(retries=4)(5)
    settings = {t.id: t.settings for t in only_dag().tasks}
    assert settings["outer.extract"].retries == 2
    assert settings["outer.extract"].queue == "outer"
    assert settings["outer.extract"].run_if == "all_succeeded"
    assert settings["outer.inner.audit"].retries == 3
    assert settings["outer.inner.audit"].queue == "outer"
    assert settings["outer.inner.audit"].run_if == "all_succeeded"
    assert settings["outer.inner.transform"].retries == 4


def test_a_dependency_on_a_group_goes_through_its_roots_and_leaves() -> None:
    with Dag("d"):
        start = EmptyOperator("start")
        with group("load") as load:
            transform(extract())
            audit()
        start >> load >> EmptyOperator("publish")
    assert edges(only_dag()) == [
        ("load.audit", "publish", "order"),
        ("load.extract", "load.transform", "data"),
        ("load.transform", "publish", "order"),
        ("start", "load.audit", "order"),
        ("start", "load.extract", "order"),
    ]


def test_a_dependency_written_inside_the_group_is_made_when_it_closes() -> None:
    with Dag("d"):
        start = EmptyOperator("start")
        with group("load") as load:
            start >> load
            transform(extract())  # extract becomes a root after the line above
    assert edges(only_dag()) == [
        ("load.extract", "load.transform", "data"),
        ("start", "load.extract", "order"),
    ]


def test_groups_lists_chain_and_left_shift() -> None:
    with Dag("d"):
        with group("a") as a:
            extract()
        with group("b") as b:
            audit()
        last = EmptyOperator("last")
        a >> b
        chain(b, [last])
        EmptyOperator("first") << a
    assert edges(only_dag()) == [
        ("a.extract", "b.audit", "order"),
        ("a.extract", "first", "order"),
        ("b.audit", "last", "order"),
    ]


def test_a_dependency_on_an_empty_group_is_refused() -> None:
    with Dag("d"):
        with group("empty") as empty:
            pass
        first = EmptyOperator("x")
        with pytest.raises(ValueError, match=r"`>>`: group `empty` has no tasks"):
            first >> empty


def test_a_group_name_is_one_id_part() -> None:
    with Dag("d"), pytest.raises(IdError, match=r"group name 'a\.b' must be one ID part"):
        group("a.b").__enter__()
    registry.take_defined()
    # The same name nested in itself is a different ID.
    with Dag("e"), group("x"), group("x"):
        extract()
    assert [t.id for t in only_dag().tasks] == ["x.x.extract"]


def test_a_group_and_a_task_cant_share_an_id() -> None:
    with Dag("d"):
        audit()
        with pytest.raises(IdError, match=r"group `audit` has the ID of a task"):
            group("audit").__enter__()
    with Dag("e"):
        with group("load"):
            extract()
        with pytest.raises(IdError, match=r"task ID `load` is a group's ID"):
            EmptyOperator("load")
    with Dag("f"):
        with group("twice"):
            extract()
        with pytest.raises(IdError, match=r"group `twice` is defined twice"):
            group("twice").__enter__()


def test_a_group_needs_a_dag_and_its_block() -> None:
    with pytest.raises(RuntimeError, match=r"group\(\) must be created inside"):
        group("x").__enter__()
    handle = Group("later")
    with pytest.raises(RuntimeError, match="used before its `with` block"):
        _ = handle.group_id
    assert repr(handle) == "Group('later')"
    with Dag("d"), group("named") as named:
        assert named.group_id == "named"
        assert repr(named) == "Group('named')"
        extract()
