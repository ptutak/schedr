"""Acting on the first of several tasks: ``first_of`` and ``at_least`` (decision 347).

A task starts after all its upstream tasks end. These are nodes of their own, which the master ends
as soon as enough of the tasks given have succeeded: no process, no slot, no polling. Nothing is
cancelled: the others run on, and one failing later fails the run, not the node or what follows it.
"""

from __future__ import annotations

from typing import Final, Literal, overload

from schedr import _build
from schedr._model import EdgeKind, FirstOfKind, ScalarType
from schedr.outcomes import FIRST_OF_VALUE
from schedr.tasks import Output

_VALUE: Final = FIRST_OF_VALUE
"""The field of the node's stored output with the winner's output."""

_WINNER: Final = "winner"
"""The field of the node's stored output with the winner's task ID."""


_FEWEST_OPTIONS: Final = 2
"""With fewer tasks to choose from there is no first."""


def _free_id(builder: _build.DagBuilder, name: str) -> str:
    """The node's ID: ``name``, numbered from the second on in the same group.

    Args:
        builder: The DAG being defined.
        name: ``first_of`` or ``at_least``.

    Returns:
        An ID no task has yet.
    """
    task_id = builder.qualified(name)
    number = 1
    while task_id in builder.tasks:
        number += 1
        task_id = builder.qualified(f"{name}_{number}")
    return task_id


def _option(builder: _build.DagBuilder, what: str, option: object) -> tuple[str, tuple[str, ...]]:
    """The task an option stands for, with the outcomes its edge is taken on.

    Args:
        builder: The DAG being defined.
        what: ``first_of`` or ``at_least``, for messages.
        option: What was given.

    Returns:
        The task's ID and its ``.on(...)`` outcomes.

    Raises:
        TypeError: For what isn't a whole task's output (another node's value is one).
    """
    if not callable(getattr(option, "_schedr_reference", None)):
        raise TypeError(f"{what}: {option!r} isn't the output of a task")
    found = _build.task_ids_of(option, what)
    if len(found) != 1:
        raise TypeError(f"{what}: {option!r} isn't the output of one task")
    path = tuple(getattr(option, "_path", ()))
    node = builder.tasks.get(found[0])
    another_node = node is not None and isinstance(node.kind, FirstOfKind)
    if path and not (another_node and path == (_VALUE,)):
        raise TypeError(f"{what}: {option!r} is a part of an output; give it the whole output")
    return found[0], tuple(getattr(option, "_schedr_on", ()))


def _add_node(
    what: str, options: tuple[object, ...], at_least: int | None, task_id: str | None
) -> str:
    """Records the node and an edge from each option.

    Args:
        what: ``first_of`` or ``at_least``, for messages.
        options: The tasks it chooses from.
        at_least: How many must succeed; ``None``: one, with one value.
        task_id: The node's ID, if given.

    Returns:
        The node's ID.

    Raises:
        TypeError: For fewer than two different tasks.
    """
    builder = _build.require_active(what)
    if len(options) < _FEWEST_OPTIONS:
        raise TypeError(f"{what}: give it at least two tasks to choose from")
    upstream = [_option(builder, what, option) for option in options]
    names = [name for name, _ in upstream]
    if len(set(names)) != len(names):
        raise TypeError(f"{what}: the same task is given twice; `.on(...)` takes several outcomes")
    node = builder.qualified(task_id) if task_id is not None else _free_id(builder, what)
    builder.add_task(node, FirstOfKind(at_least), ScalarType("any"), _build.TaskOptions())
    for name, on in upstream:
        builder.add_edge(name, node, EdgeKind.DATA, on)
    return node


def _result(node: str, *, with_winner: bool) -> object:
    """The node's value as a reference, with its winner's if asked for.

    Args:
        node: The node's ID.
        with_winner: Whether to give the winner too.

    Returns:
        The value, or ``(value, winner)``.
    """
    builder = _build.require_active("first_of")
    value = Output[object](node, (_VALUE,), dag=builder)
    if not with_winner:
        return value
    return value, Output[object](node, (_WINNER,), dag=builder)


@overload
def first_of[A, B](
    a: A, b: B, /, *, with_winner: Literal[False] = False, task_id: str | None = None
) -> A | B: ...
@overload
def first_of[A, B](
    a: A, b: B, /, *, with_winner: Literal[True], task_id: str | None = None
) -> tuple[A | B, str]: ...
@overload
def first_of[A, B, C](
    a: A, b: B, c: C, /, *, with_winner: Literal[False] = False, task_id: str | None = None
) -> A | B | C: ...
@overload
def first_of[A, B, C](
    a: A, b: B, c: C, /, *, with_winner: Literal[True], task_id: str | None = None
) -> tuple[A | B | C, str]: ...
@overload
def first_of[T](
    *options: T, with_winner: Literal[False] = False, task_id: str | None = None
) -> T: ...
@overload
def first_of[T](
    *options: T, with_winner: Literal[True], task_id: str | None = None
) -> tuple[T, str]: ...
def first_of(*options: object, with_winner: bool = False, task_id: str | None = None) -> object:
    """A node that ends as soon as one of these tasks has succeeded, with its value.

    ``publish(first_of(a, b))`` passes the first value on; ``value, winner = first_of(a, b,
    with_winner=True)`` also gives the winner's task ID. Narrow a task with ``.on(...)`` to say
    which of its outcomes count. A task that was skipped doesn't count, so this also joins the
    paths of a ``@branch``.

    The others aren't cancelled: they run on as any task, and one failing later fails the run, not
    this node or what follows it. It is skipped when none of them is taken, and ``upstream_failed``
    when one failed and none can still succeed.

    Args:
        *options: The tasks' outputs, at least two.
        with_winner: Also give the ID of the task that came first.
        task_id: The node's ID; ``first_of``, numbered from the second on, if not given.

    Returns:
        The value as a reference to pass to a task, or ``(value, winner)``.
    """
    node = _add_node("first_of", options, None, task_id)
    return _result(node, with_winner=with_winner)


@overload
def at_least[A, B](
    n: int, a: A, b: B, /, *, with_winner: Literal[False] = False, task_id: str | None = None
) -> list[A | B]: ...
@overload
def at_least[A, B](
    n: int, a: A, b: B, /, *, with_winner: Literal[True], task_id: str | None = None
) -> tuple[list[A | B], list[str]]: ...
@overload
def at_least[A, B, C](
    n: int, a: A, b: B, c: C, /, *, with_winner: Literal[False] = False, task_id: str | None = None
) -> list[A | B | C]: ...
@overload
def at_least[A, B, C](
    n: int, a: A, b: B, c: C, /, *, with_winner: Literal[True], task_id: str | None = None
) -> tuple[list[A | B | C], list[str]]: ...
@overload
def at_least[T](
    n: int, /, *options: T, with_winner: Literal[False] = False, task_id: str | None = None
) -> list[T]: ...
@overload
def at_least[T](
    n: int, /, *options: T, with_winner: Literal[True], task_id: str | None = None
) -> tuple[list[T], list[str]]: ...
def at_least(
    n: int, /, *options: object, with_winner: bool = False, task_id: str | None = None
) -> object:
    """A node that ends as soon as ``n`` of these tasks have succeeded, with their values.

    ``accepted(at_least(2, a, b, c))`` passes the values on, in the order the tasks ended;
    ``values, winners = at_least(2, a, b, c, with_winner=True)`` also gives their task IDs.
    Everything else is as in ``first_of``.

    Args:
        n: How many must succeed: at least 1, at most as many as are given.
        *options: The tasks' outputs, at least two.
        with_winner: Also give the IDs of the tasks that made it.
        task_id: The node's ID; ``at_least``, numbered from the second on, if not given.

    Returns:
        The values as a reference to pass to a task, or ``(values, winners)``.

    Raises:
        ValueError: For an ``n`` the tasks can't reach.
    """
    if isinstance(n, bool) or not 1 <= n <= len(options):
        raise ValueError(f"at_least: can't reach {n} with {len(options)} tasks")
    node = _add_node("at_least", options, n, task_id)
    return _result(node, with_winner=with_winner)
