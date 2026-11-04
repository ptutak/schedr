"""The DAGs defined while importing DAG files, and where they are defined.

The runner (``parse`` mode) imports each file inside :func:`parse_root` and then takes the
DAGs with :func:`take_defined`. Outside the runner (tests, an interactive session) DAGs are
collected the same way, located as ``local:<file name>``.
"""

import contextlib
import os
from collections.abc import Callable, Iterator, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from schedr._model.dag import Dag, DagLocation

LOCAL_ROOT = "local"
"""The root name of DAGs defined outside any configured DAG root."""

_ROOT: ContextVar[tuple[str, Path, Path] | None] = ContextVar("schedr_parse_root", default=None)
"""The parse root: its name, its absolute path as given, and that path resolved."""


@dataclass(frozen=True, slots=True)
class DefinedDag:
    """A DAG whose ``with Dag(...)`` block finished, with its tasks' Python functions.

    Attributes:
        model: The DAG as DAG JSON types.
        functions: The function of each Python task, by task ID (the runner calls them).
        params_class: The class its run parameters are declared with; ``None``: none.
        cleanups: The ``cleanup=`` function of each Python task with one, by task ID.
    """

    model: Dag
    functions: Mapping[str, Callable[..., Any]] = field(default_factory=dict)
    params_class: type | None = None
    cleanups: Mapping[str, Callable[..., Any]] = field(default_factory=dict)


_DEFINED: list[DefinedDag] = []

_BY_FILE: dict[tuple[Path, str], DefinedDag] = {}
"""The latest definition of each DAG by (resolved file, DAG ID); never cleared."""


@contextlib.contextmanager
def parse_root(name: str, path: Path) -> Iterator[None]:
    """Locates DAGs defined inside the block relative to a DAG root.

    Args:
        name: The root's name, e.g. ``etl-repo``.
        path: The root's directory.

    Yields:
        Nothing; DAGs defined in files under ``path`` get locations relative to it.
    """
    token = _ROOT.set((name, _absolute(path), path.resolve()))
    try:
        yield
    finally:
        _ROOT.reset(token)


def parse_root_active() -> bool:
    """Whether a parse root is set (the runner parsing a DAG folder).

    Returns:
        ``True`` inside :func:`parse_root`.
    """
    return _ROOT.get() is not None


def location_for(filename: str) -> DagLocation:
    """Where a DAG defined in ``filename`` is, given the current parse root.

    Args:
        filename: The DAG file's path, as Python reports it (``__file__``).

    Returns:
        The root and the path relative to it, with ``/`` separators; ``local`` and the file
        name when there is no parse root or the file isn't under it.
    """
    return root_location(filename) or DagLocation(LOCAL_ROOT, Path(filename).resolve().name)


def root_location(filename: str) -> DagLocation | None:
    """Where ``filename`` is in the current parse root, or ``None`` outside it (or without one).

    Unlike :func:`location_for`, a root that is itself named ``local`` is told apart.

    Args:
        filename: The file's path, as Python reports it (``__file__``).

    Returns:
        The root and the path relative to it, with ``/`` separators, or ``None``.
    """
    root = _ROOT.get()
    if root is None:
        return None
    name, given, resolved = root
    # As scanned, through links (B61: a file in a linked directory was located by its
    # target's name), else resolved (a root reached through a link itself).
    for base, path in ((given, _absolute(Path(filename))), (resolved, Path(filename).resolve())):
        if path.is_relative_to(base):
            return DagLocation(name, PurePosixPath(path.relative_to(base)).as_posix())
    return None


def _absolute(path: Path) -> Path:
    """``path`` made absolute and normalized, links kept."""
    return Path(os.path.normpath(os.path.abspath(path)))


def register(
    dag: Dag,
    functions: Mapping[str, Callable[..., Any]] | None = None,
    filename: str | None = None,
    params_class: type | None = None,
    cleanups: Mapping[str, Callable[..., Any]] | None = None,
) -> None:
    """Records a DAG whose ``with Dag(...)`` block finished.

    Args:
        dag: The DAG.
        functions: The function of each of its Python tasks, by task ID.
        filename: The file defining it (``__file__``), remembered for :func:`defined_in`.
        params_class: The class its run parameters are declared with.
        cleanups: The ``cleanup=`` function of each Python task with one, by task ID.
    """
    entry = DefinedDag(dag, dict(functions or {}), params_class, dict(cleanups or {}))
    _DEFINED.append(entry)
    if filename is not None:
        _BY_FILE[Path(filename).resolve(), dag.id] = entry


def defined_in(filename: str | Path, dag_id: str) -> DefinedDag | None:
    """The latest definition of a DAG in a file, if this process has imported it.

    Unlike :func:`take_entries`, this remembers every DAG defined so far (e.g. by a test
    importing a DAG file), so its task functions and their types can be found again.

    Args:
        filename: The DAG file.
        dag_id: The DAG's ID.

    Returns:
        The DAG with its task functions, or ``None``.
    """
    return _BY_FILE.get((Path(filename).resolve(), dag_id))


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
