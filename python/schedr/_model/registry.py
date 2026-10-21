"""The DAGs defined while importing DAG files, and where they are defined.

The runner (``parse`` mode) imports each file inside :func:`parse_root` and then takes the
DAGs with :func:`take_defined`. Outside the runner (tests, an interactive session) DAGs are
collected the same way, located as ``local:<file name>``.
"""

import contextlib
from collections.abc import Iterator
from contextvars import ContextVar
from pathlib import Path, PurePosixPath

from schedr._model.dag import Dag, DagLocation

LOCAL_ROOT = "local"
"""The root name of DAGs defined outside any configured DAG root."""

_ROOT: ContextVar[tuple[str, Path] | None] = ContextVar("schedr_parse_root", default=None)
_DEFINED: list[Dag] = []


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


def register(dag: Dag) -> None:
    """Records a DAG whose ``with Dag(...)`` block finished.

    Args:
        dag: The DAG.
    """
    _DEFINED.append(dag)


def take_defined() -> list[Dag]:
    """Returns the DAGs defined since the last call, in definition order, and forgets them.

    Returns:
        The DAGs.
    """
    dags = list(_DEFINED)
    _DEFINED.clear()
    return dags
