import inspect
from collections.abc import Iterator
from typing import Any

import pytest

from schedr import (
    Dag,
    Data,
    EmptyOperator,
    Group,
    RunIf,
    Task,
    TaskDefaults,
    branch,
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
            transform(extract())  # extract is a first task, the line above notwithstanding
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
    def define() -> None:
        with Dag("d"):
            with group("empty") as empty:
                pass
            first = EmptyOperator("x")
            first >> empty

    # Reported when the DAG ends: a group's ends are known once every edge is.
    with pytest.raises(ValueError, match=r"`>>`: group `empty` has no tasks"):
        define()


class Rows(Data):
    n: int


class Missing(Data):
    why: str


@task
def fetch() -> Rows | Missing:
    return Rows(1)


def test_a_groups_ends_count_edges_written_after_its_block() -> None:
    # `store` runs only on `Rows`, an edge inside the group written after its block: it is
    # no first task of the group, so `start` doesn't run it on its own (decision 56A).
    with Dag("d"):
        start = EmptyOperator("start")
        with group("load") as load:
            fetched = fetch()
            store = EmptyOperator("store")
        fetched.on(Rows) >> store
        start >> load
    assert sorted((e.from_, e.to, e.on) for e in only_dag().edges) == [
        ("load.fetch", "load.store", ("Rows",)),
        ("start", "load.fetch", ()),
    ]


def test_a_dependency_inside_a_group_counts_for_the_groups_ends() -> None:
    # `a >> b` between two groups inside `outer`, written last: `outer`'s first task is `a`'s.
    with Dag("d"):
        start = EmptyOperator("start")
        with group("outer") as outer:
            with group("a") as a:
                extract()
            with group("b") as b:
                audit()
        start >> outer
        a >> b
    assert edges(only_dag()) == [
        ("outer.a.extract", "outer.b.audit", "order"),
        ("start", "outer.a.extract", "order"),
    ]


@branch
def pick() -> list[Task]:
    return []


def test_a_dependency_on_a_group_ending_with_a_branch_is_a_plain_edge() -> None:
    # Decision 94: the branch's own choices stay choices; the edge the group's dependency
    # makes is taken once the branch has run, whatever it chose.
    with Dag("d"):
        with group("route") as route:
            chosen = pick()
        yes, no = EmptyOperator("yes"), EmptyOperator("no")
        chosen >> [yes, no]
        route >> EmptyOperator("then")
    assert edges(only_dag()) == [
        ("route.pick", "no", "selection"),
        ("route.pick", "then", "order"),
        ("route.pick", "yes", "selection"),
    ]


def test_a_group_whose_block_raised_is_closed() -> None:
    with Dag("d"):
        try:
            with group("load") as load:
                extract()
                raise RuntimeError("caught in the DAG file")
        except RuntimeError:
            pass
        load >> EmptyOperator("after")
    model = only_dag()
    assert [(t.id, t.group) for t in model.tasks] == [
        ("load.extract", "load"),
        ("after", None),
    ]
    assert edges(model) == [("load.extract", "after", "order")]


def test_a_cycle_through_a_whole_group_is_named() -> None:
    def define() -> None:
        with Dag("d"):
            with group("g") as g:
                a, b = EmptyOperator("a"), EmptyOperator("b")
            a >> b >> a
            g >> EmptyOperator("after")

    with pytest.raises(ValueError, match=r"group `g`'s tasks form a cycle: g\.a -> g\.b -> g\.a"):
        define()


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


def _statement_order_edges(start_first: bool) -> list[tuple[str, str, str]]:
    """Failure A of the fix-up review: `extracted >> [store, notify]` adds an edge inside
    `load` and one out of it; `start >> load` reads `load`'s first tasks."""
    with Dag("d"):
        start, notify = EmptyOperator("start"), EmptyOperator("notify")
        with group("load") as load:
            with group("extract") as extracted:
                e = EmptyOperator("e", run_if=RunIf.ALL_SUCCEEDED)
            store = EmptyOperator("store")
        fetch().on(Rows) >> e
        if start_first:
            start >> load
            extracted >> [store, notify]
        else:
            extracted >> [store, notify]
            start >> load
    return edges(only_dag())


def test_a_groups_ends_dont_depend_on_the_order_of_statements() -> None:
    first = _statement_order_edges(start_first=True)
    assert first == _statement_order_edges(start_first=False)
    # `store` comes after `e` inside `load`: no first task of it.
    assert ("start", "load.store", "order") not in first


def test_a_dependency_reaching_out_of_a_group_still_counts_for_its_ends() -> None:
    # Failure B: `a >> [tb, outside]` adds `ta -> tb` inside `left` (and one out of `outer`):
    # `ta` is then no last task of `left`, whatever the statements' order.
    with Dag("d"):
        outside = EmptyOperator("outside")
        with group("outer"):
            with group("left") as left:
                with group("a") as a:
                    EmptyOperator("ta")
                tb = EmptyOperator("tb", run_if=RunIf.ALL_SUCCEEDED)
            c = EmptyOperator("c")
        fetch().on(Rows) >> tb
        left >> c
        a >> [tb, outside]
    assert ("outer.left.a.ta", "outer.c", "order") not in edges(only_dag())


def test_a_cycle_through_a_group_is_its_own_not_another_one() -> None:
    def define() -> None:
        with Dag("d"):
            x, y = EmptyOperator("x"), EmptyOperator("y")
            x >> y >> x
            with group("g") as g:
                a, b = EmptyOperator("a"), EmptyOperator("b")
            a >> b >> a
            g >> EmptyOperator("after")

    with pytest.raises(ValueError, match=r"group `g`'s tasks form a cycle: g\.a -> g\.b -> g\.a"):
        define()


def test_a_group_dependency_error_names_the_line_it_was_written_on() -> None:
    lines: list[int] = []

    def define() -> None:
        with Dag("d"):
            with group("empty") as empty:
                pass
            first = EmptyOperator("x")
            lines.append(inspect.currentframe().f_lineno + 1)  # type: ignore[union-attr]
            first >> empty

    # Reported when the DAG ends, so the message says where the dependency was written.
    with pytest.raises(ValueError, match=r"`>>`: group `empty` has no tasks") as raised:
        define()
    assert str(raised.value).startswith(f"test_groups.py:{lines[0]}: "), raised.value
