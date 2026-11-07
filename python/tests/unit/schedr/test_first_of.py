"""`first_of` and `at_least`: what a DAG file says."""

from typing import Any

import pytest

from schedr import Dag, Data, EmptyOperator, at_least, branch, first_of, group, task
from schedr._model import registry
from schedr.runner.task import _narrowed


@task
def one() -> str:
    """A value."""
    return "1"


@task
def two() -> str:
    """Another value."""
    return "2"


@task
def three() -> str:
    """A third value."""
    return "3"


@task
def takes(value: str, winner: str) -> str:
    """Reads the node."""
    return f"{winner}={value}"


@task
def many(values: list[str], winners: list[str]) -> str:
    """Reads the node."""
    return f"{sorted(values)} {sorted(winners)}"


class Rows(Data):
    """One outcome."""


class Missing(Data):
    """Another outcome."""


@task
def either() -> Rows | Missing:
    """Ends with one of two outcomes."""
    return Rows()


def _json() -> dict[str, Any]:
    (dag,) = registry.take_defined()
    return dag.to_json()


def _task(model: dict[str, Any], task_id: str) -> dict[str, Any]:
    tasks: list[dict[str, Any]] = model["tasks"]
    return next(t for t in tasks if t["id"] == task_id)


def test_first_of_is_a_node_with_an_edge_from_each_task() -> None:
    with Dag("d"):
        value, winner = first_of(one(), either().on(Rows), with_winner=True)
        takes(value, winner)
    model = _json()
    assert _task(model, "first_of")["kind"] == "first_of"
    edges = [e for e in model["edges"] if e["to"] == "first_of"]
    assert [(e["from"], e.get("on", [])) for e in edges] == [("one", []), ("either", ["Rows"])]
    arguments = _task(model, "takes")["arguments"]
    assert arguments["value"]["ref"]["path"] == ["$value"]
    assert arguments["winner"]["ref"]["path"] == ["winner"]


def test_without_the_winner_it_is_the_value() -> None:
    with Dag("d"):
        takes(first_of(one(), two()), "w")
    assert _task(_json(), "takes")["arguments"]["value"]["ref"]["path"] == ["$value"]


def test_a_node_chooses_from_another_but_not_from_its_winner() -> None:
    with Dag("d"):
        inner, winner = first_of(one(), two(), with_winner=True)
        other = three()
        first_of(inner, other)
        with pytest.raises(TypeError, match="part of an output"):
            first_of(winner, other)
    model = _json()
    edges = [(e["from"], e["to"]) for e in model["edges"]]
    assert ("first_of", "first_of_2") in edges


def test_at_least_says_its_count() -> None:
    with Dag("d"):
        at_least(2, one(), two(), three())
    assert _task(_json(), "at_least")["at_least"] == 2


def test_nodes_are_numbered_or_named() -> None:
    with Dag("d"):
        a, b = one(), two()
        first_of(a, b)
        first_of(a, b)
        first_of(a, b, task_id="mine")
    ids = {t["id"] for t in _json()["tasks"]}
    assert {"first_of", "first_of_2", "mine"} <= ids


@pytest.mark.parametrize("count", [0, 4, True])
def test_a_count_it_cant_reach_is_refused(count: int) -> None:
    with Dag("d"), pytest.raises(ValueError, match="can't reach"):
        at_least(count, one(), two(), three())


def test_what_it_cant_choose_from_is_refused() -> None:
    with Dag("d"):
        a = one()
        b = two()
        with pytest.raises(TypeError, match="at least two"):
            first_of(a)
        with pytest.raises(TypeError, match="twice"):
            first_of(a, a)
        with pytest.raises(TypeError, match="isn't the output"):
            first_of(a, "text")
        with pytest.raises(TypeError, match="part of an output"):
            first_of(a, b["x"])


def test_nothing_else_can_run_before_the_node() -> None:
    with Dag("d"):
        first = first_of(one(), two())
        with pytest.raises(TypeError, match="the tasks it was given"):
            three() >> first


def test_what_follows_it_is_ordered_with_shift() -> None:
    with Dag("d"):
        after = EmptyOperator("after")
        first_of(one(), two()) >> after
    edges = _json()["edges"]
    assert {"from": "first_of", "to": "after", "kind": "order"} in edges


def test_outside_a_dag_it_is_refused() -> None:
    with pytest.raises(RuntimeError):
        first_of(1, 2)


@branch
def choose() -> str:
    """Takes one path."""
    return "one"


def test_it_joins_the_paths_of_a_branch() -> None:
    with Dag("d"):
        chosen = choose()
        a, b = one(), two()
        chosen >> [a, b]
        value, winner = first_of(a, b, with_winner=True)
        takes(value, winner)
    assert _task(_json(), "takes")["kind"] == "python"


def test_ordering_a_group_holding_a_node_before_it_is_refused() -> None:
    def define() -> None:
        with Dag("d"):
            gate = EmptyOperator("gate")
            a, b = one(), two()
            with group("g") as g:
                first_of(a, b)
            gate >> g

    with pytest.raises(TypeError, match="the tasks it was"):
        define()


def test_a_first_of_reads_as_its_winner_did() -> None:
    annotation = Rows | Missing
    reference = {"kind": "output", "task": "first_of", "path": ["$value"]}
    assert _narrowed(reference, annotation, {"first_of": "Missing"}) is Missing
    a_field = {"kind": "output", "task": "t", "path": ["value"]}
    assert _narrowed(a_field, annotation, {"t": "Missing"}) == annotation
