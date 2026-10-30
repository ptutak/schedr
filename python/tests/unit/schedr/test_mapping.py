from collections.abc import Iterator
from typing import Any

import pytest

from schedr import Dag, Data, Task, branch, task
from schedr._model import registry


class Rows(Data):
    count: int


class Missing(Data):
    reason: str


@task
def list_files() -> list[str]:
    return ["a", "b"]


@task
def one_file() -> str:
    return "a"


@task
def load(bucket: str, path: str) -> int:
    return len(path)


@task
def size(n: int) -> int:
    return n


@task
def total(sizes: list[int]) -> int:
    return sum(sizes)


@task
def maybe_total(sizes: list[int | None]) -> int:
    return 0


@task
def fetch(path: str) -> Rows | Missing:
    return Rows(1)


@task
def count(rows: Rows) -> int:
    return rows.count


@pytest.fixture(autouse=True)
def clean_registry() -> Iterator[None]:
    registry.take_defined()
    yield
    registry.take_defined()


def only_dag() -> Any:
    (dag,) = registry.take_defined()
    return dag


def task_json(model: Any, task_id: str) -> dict[str, Any]:
    return next(t.to_json() for t in model.tasks if t.id == task_id)


def test_a_partial_and_expand_are_recorded_as_arguments_and_expand() -> None:
    with Dag("d"):
        load.partial(bucket="raw").expand(path=list_files())
    model = only_dag()
    loaded = task_json(model, "load")
    assert loaded["arguments"] == {"bucket": {"literal": "raw"}}
    assert loaded["expand"] == {
        "product": [{"name": "path", "value": {"ref": {"kind": "output", "task": "list_files"}}}]
    }
    assert [(e.from_, e.to, e.kind.value) for e in model.edges] == [("list_files", "load", "data")]


def test_mapped_literals_are_checked_element_by_element() -> None:
    with Dag("d"):
        size.expand(n=[1, 2, 3])
    assert task_json(only_dag(), "size")["expand"] == {
        "product": [{"name": "n", "value": {"literal": [1, 2, 3]}}]
    }
    with (
        Dag("e"),
        pytest.raises(TypeError, match=r"mapped argument `n`: expected int, got str"),
    ):
        size.expand(n=[1, "two"])


def test_what_is_mapped_over_must_be_a_list_of_the_parameters_type() -> None:
    with (
        Dag("d"),
        pytest.raises(
            TypeError,
            match=r"mapped argument `path`: expects `list\[str\]`, but `one_file` is `str`",
        ),
    ):
        load.partial(bucket="x").expand(path=one_file())
    with (
        Dag("e"),
        pytest.raises(TypeError, match=r"expects `list\[int\]`, but `list_files` is `list\[str\]`"),
    ):
        size.expand(n=list_files())


def test_a_mapped_tasks_output_is_the_list_of_its_instances_outputs() -> None:
    with Dag("d"):
        sizes = size.expand(n=[1, 2])
        total(sizes)
        size.override(task_id="first")(sizes[0])  # an element by index
        size.override(task_id="again").expand(n=sizes)  # element by element
    model = only_dag()
    assert task_json(model, "again")["expand"] == {
        "product": [{"name": "n", "value": {"ref": {"kind": "output", "task": "size"}}}]
    }
    with (
        Dag("e"),
        pytest.raises(TypeError, match=r"expects `str`, but `size` is `list\[int\]`"),
    ):
        load(bucket="x", path=size.expand(n=[1]))  # type: ignore[arg-type]


def test_a_field_of_every_instance_is_refused() -> None:
    with Dag("d"):
        results = fetch.expand(path=list_files())
        with pytest.raises(
            TypeError,
            match=r"`fetch\.on\(Rows\)\.count`: `fetch` is mapped, so its output is a list",
        ):
            size(results.on(Rows).count)  # type: ignore[attr-defined]


@task
def every(rows: list[Rows]) -> None: ...


def test_on_and_outcome_apply_element_by_element_inside_expand_only() -> None:
    with Dag("d"):
        results = fetch.expand(path=list_files())
        count.expand(rows=results.on(Rows))  # type: ignore[attr-defined]
    edges = [(e.from_, e.to, e.on) for e in only_dag().edges]
    assert ("fetch", "count", ("Rows",)) in edges
    with Dag("e"):
        results = fetch.expand(path=list_files())
        with pytest.raises(
            TypeError, match=r"`\.on\(\.\.\.\)` on the mapped task `fetch`'s whole output"
        ):
            every(results.on(Rows))  # type: ignore[attr-defined]


def test_a_list_from_partly_skipped_instances_must_allow_none() -> None:
    def summed(receiver: Any) -> None:
        with Dag("d"):
            results = fetch.expand(path=list_files())
            receiver(count.expand(rows=results.on(Rows)))  # type: ignore[attr-defined]
        registry.take_defined()

    summed(maybe_total)
    # Checked once the DAG is complete, when its block ends.
    with pytest.raises(
        TypeError,
        match=r"some instances of the mapped task `count` may be skipped.*allow None "
        r"\(`int \| None` in the list\)",
    ):
        summed(total)


def test_expand_names_are_checked() -> None:
    with Dag("d"), pytest.raises(TypeError, match=r"`\.partial\(\)`: no parameter `nope`"):
        load.partial(nope=1)
    with Dag("e"), pytest.raises(TypeError, match=r"`\.expand\(\)`: no parameter `nope`"):
        load.partial(bucket="x").expand(nope=[1])
    with Dag("f"), pytest.raises(TypeError, match="`path` is both fixed"):
        load.partial(bucket="x", path="y").expand(path=["z"])
    with Dag("g"), pytest.raises(TypeError, match="missing a required argument: 'bucket'"):
        load.expand(path=["a"])
    with Dag("h"), pytest.raises(TypeError, match="needs at least one mapped argument"):
        load.partial(bucket="x").expand()


def test_the_product_keeps_the_written_order() -> None:
    with Dag("d"):
        load.expand(path=["a"], bucket=["raw", "clean"])
    assert [m["name"] for m in task_json(only_dag(), "load")["expand"]["product"]] == [
        "path",
        "bucket",
    ]


def test_expand_kwargs_maps_over_a_list_of_dicts() -> None:
    with Dag("d"):
        load.expand_kwargs([{"bucket": "raw", "path": "a"}])
    assert task_json(only_dag(), "load")["expand"] == {
        "kwargs": {"literal": [{"bucket": "raw", "path": "a"}]}
    }
    with Dag("e"), pytest.raises(TypeError, match=r"`\.expand_kwargs\(\)`"):
        load.expand_kwargs(list_files())


def test_a_branch_cant_be_mapped() -> None:
    @branch
    def pick(n: int) -> list[Task]:
        return []

    with Dag("d"), pytest.raises(TypeError, match=r"a @branch task can't be mapped"):
        pick.expand(n=[1])


def test_outside_a_dag_expand_is_refused() -> None:
    with pytest.raises(RuntimeError, match=r"`size\.expand\(\)` must be created inside"):
        size.expand(n=[1])


def test_an_order_edge_between_mapped_outputs_is_the_barrier() -> None:
    with Dag("d"):
        sizes = size.expand(n=[1, 2])
        again = size.override(task_id="again").expand(n=sizes)
        sizes >> again
    kinds = sorted((e.from_, e.to, e.kind.value) for e in only_dag().edges)
    assert kinds == [("size", "again", "data"), ("size", "again", "order")]
