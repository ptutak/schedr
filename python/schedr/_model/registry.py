"""The DAGs defined while importing DAG files, and where they are defined.

The runner (``parse`` mode) imports each file inside :func:`parse_root` and then takes the
DAGs with :func:`take_defined`. Outside the runner (tests, an interactive session) DAGs are
collected the same way, located as ``local:<file name>``.
"""

import contextlib
from collections.abc import Callable, Iterator, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from schedr._model.dag import Dag, DagLocation

LOCAL_ROOT = "local"
"""The root name of DAGs defined outside any configured DAG root."""

_ROOT: ContextVar[tuple[str, Path] | None] = ContextVar("schedr_parse_root", default=None)


@dataclass(frozen=True, slots=True)
class DefinedDag:
    """A DAG whose ``with Dag(...)`` block finished, with its tasks' Python functions.

    Attributes:
        model: The DAG as DAG JSON types.
        functions: The function of each Python task, by task ID (the runner calls them).
    """

    model: Dag
    functions: Mapping[str, Callable[..., Any]] = field(default_factory=dict)


_DEFINED: list[DefinedDag] = []


@contextlib.contextmanager
def parse_root(name: str, path: Path) -> Iterator[None]:
    """Locates DAGs defined inside the block relative to a DAG root.

    Args:
        name: The root's name, e.g. ``etl-repo``.
        path: The root's directory.

    Yields:
        Nothing; DAGs defined in files under ``path`` get locations relative to it.
    """
    token = _ROOT.set((name, path.resolve()))
    try:
        yield
    finally:
        _ROOT.reset(token)


def location_for(filename: str) -> DagLocation:
    """Where a DAG defined in ``filename`` is, given the current parse root.

    Args:
        filename: The DAG file's path, as Python reports it (``__file__``).

    Returns:
        The root and the path relative to it, with ``/`` separators; ``local`` and the file
        name when there is no parse root or the file isn't under it.
    """
    root = _ROOT.get()
    file = Path(filename).resolve()
    if root is not None and file.is_relative_to(root[1]):
        return DagLocation(root[0], PurePosixPath(file.relative_to(root[1])).as_posix())
    return DagLocation(LOCAL_ROOT, file.name)


def register(dag: Dag, functions: Mapping[str, Callable[..., Any]] | None = None) -> None:
    """Records a DAG whose ``with Dag(...)`` block finished.

    Args:
        dag: The DAG.
        functions: The function of each of its Python tasks, by task ID.
    """
    _DEFINED.append(DefinedDag(dag, dict(functions or {})))


def take_entries() -> list[DefinedDag]:
    """Returns the DAGs defined since the last call, with their functions, and forgets them.

    Returns:
        The DAGs, in definition order.
    """
    entries = list(_DEFINED)
    _DEFINED.clear()
    return entries


def take_defined() -> list[Dag]:
    """Returns the DAGs defined since the last call, in definition order, and forgets them.

    Returns:
        The DAGs (their models).
    """
    return [entry.model for entry in take_entries()]
