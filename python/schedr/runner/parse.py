"""Parse mode: import DAG files by their ordinary names, and report the DAGs they define.

A file is imported as Python would (``team/sync.py`` is ``team.sync``), each in a process of
its own, and a task's process imports it the same way (decision 326). Files that define no DAG
report none; a file that raises reports an import error and doesn't stop the others.
"""

import importlib
import importlib.util
import os
import sys
import traceback
import warnings
from pathlib import Path, PurePosixPath
from typing import Any

from schedr._model import Dag, DagLocation, registry
from schedr.connections import take_connections
from schedr.outcomes import DagHint, TypingHint
from schedr.runner.protocol import Channel
from schedr.runner.sources import (
    clash,
    forgotten_roots,
    import_from_source,
    registered_roots,
    search_for_clashes,
)

TRACEBACK_LINES = 20
"""How many traceback lines an import error keeps."""


def module_name(root: str, path: str) -> str:
    """The name a DAG file is imported under: the ordinary one, from the root it is in.

    ``team/sync.py`` is ``team.sync``, so relative imports, ``__init__.py``, cycles and
    ``sys.modules`` work as Python's do (decision 326). The root is a label, not a package.

    Args:
        root: The DAG root's name.
        path: The file's path relative to the root.

    Returns:
        The module's name.

    Raises:
        ImportError: For a path with no name of its own (``a.b.py`` would mean ``a/b.py``).
    """
    del root
    relative = PurePosixPath(path)
    if relative.is_absolute() or not relative.parts or relative.suffix != ".py":
        raise ImportError(f"can't import {path}: not a Python source file")
    bits = list(relative.parts)
    if bits[-1] == "__init__.py" and len(bits) > 1:
        bits.pop()
    else:
        bits[-1] = bits[-1].removesuffix(".py")
    if any("." in part for part in bits):
        raise ImportError(
            f"can't import {path}: a dot in a file or folder name has no import name; rename it"
        )
    return ".".join(bits)


def _same_place(found: str, wanted: Path) -> bool:
    """Whether two paths are one place, as given or through links."""
    return os.path.abspath(found) == os.path.abspath(wanted) or os.path.realpath(
        found
    ) == os.path.realpath(wanted)


def _collision(name: str, wanted: Path, found: object) -> ImportError:
    return ImportError(
        f"`{name}` must come from {wanted}, but Python finds {found}: another package has that "
        "name; rename it, or list it in `.schedrignore`"
    )


def _import_checked(root_dir: Path, path: str, name: str) -> None:
    """Imports the module, each parent first, refusing one that resolves somewhere else."""
    bits = name.split(".")
    if (why := clash(bits[0])) is not None:
        raise ImportError(why)
    for size in range(1, len(bits) + 1):
        prefix = ".".join(bits[:size])
        loaded = sys.modules.get(prefix)
        spec = loaded.__spec__ if loaded is not None else importlib.util.find_spec(prefix)
        if size == len(bits):
            wanted = root_dir / path
            origin = None if spec is None else spec.origin
            if origin is None or not _same_place(origin, wanted):
                raise _collision(prefix, wanted, origin)
        else:
            wanted = root_dir.joinpath(*bits[:size])
            places = [] if spec is None else list(spec.submodule_search_locations or ())
            if not any(_same_place(place, wanted) for place in places):
                raise _collision(prefix, wanted, places)
        importlib.import_module(prefix)


def import_dag_entries(root: str, root_dir: Path, path: str) -> list[registry.DefinedDag]:
    """Imports one DAG file and returns the DAGs it defined, with their task functions.

    Args:
        root: The DAG root's name.
        root_dir: The root's directory.
        path: The file's path relative to the root.

    Returns:
        The DAGs, in definition order (empty for a helper module).

    Raises:
        BaseException: Whatever importing the file raised (its partial DAGs are dropped).
    """
    name = module_name(root, path)
    if os.path.abspath(root_dir) not in sys.path:
        extend_sys_path([*registered_roots(), str(root_dir)])
    # Imported again in one process (a test's): its top level runs again, for its DAGs.
    sys.modules.pop(name, None)
    with registry.parse_root(root, root_dir):
        try:
            _import_checked(root_dir, path, name)
        except BaseException:
            registry.take_entries()
            raise
    # Only this file's own: a module it imports may define DAGs too, found when that module's
    # file is parsed (B110: they made the master refuse this file).
    here = DagLocation(root, PurePosixPath(path).as_posix())
    return [entry for entry in registry.take_entries() if entry.model.location == here]


def import_dag_file(root: str, root_dir: Path, path: str) -> list[Dag]:
    """Imports one DAG file and returns the DAGs it defined.

    Args:
        root: The DAG root's name.
        root_dir: The root's directory.
        path: The file's path relative to the root.

    Returns:
        The DAGs, in definition order (empty for a helper module).
    """
    return [entry.model for entry in import_dag_entries(root, root_dir, path)]


_SCHEDR_DIR = str(Path(__file__).resolve().parents[1]) + os.sep


_IMPORTLIB_DIR = str(Path(importlib.__file__).resolve().parent) + os.sep


def _internal(filename: str) -> bool:
    return filename.startswith((_SCHEDR_DIR, _IMPORTLIB_DIR, "<frozen importlib"))


def import_error(error: BaseException) -> dict[str, str]:
    """An import error (or a task's failure) as sent to the agent.

    The traceback starts at the user's code: the runner's own frames and ``importlib``'s before
    it are left out (all of them are kept when every frame is schedr's).

    Args:
        error: The exception.

    Returns:
        Its class name, message and the last :data:`TRACEBACK_LINES` lines of the traceback.
    """
    tb = error.__traceback__
    while tb is not None and _internal(tb.tb_frame.f_code.co_filename):
        tb = tb.tb_next
    shown = error.__traceback__ if tb is None else tb
    lines = "".join(traceback.format_exception(type(error), error, shown)).splitlines()
    return {
        "exception": type(error).__name__,
        "message": str(error),
        "traceback": "\n".join(lines[-TRACEBACK_LINES:]),
    }


def file_result(root: str, root_dir: Path, path: str) -> dict[str, Any]:
    """Imports one file and builds its ``file_parsed`` message.

    Args:
        root: The DAG root's name.
        root_dir: The root's directory.
        path: The file's path relative to the root.

    Returns:
        The message, with the DAG JSON of each DAG, or the import error, and the warnings
        importing it raised (``Category: message``; a ``TypingHint`` or ``DagHint`` always,
        others as the interpreter's warning filters let them through).
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", TypingHint)
        warnings.simplefilter("always", DagHint)
        take_connections()
        try:
            dags = import_dag_file(root, root_dir, path)
            documents = [d.to_json() for d in dags]
            connections = own_connections(root, path)
            error = None
        except (Exception, SystemExit) as e:
            take_connections()
            documents, connections, error = [], [], import_error(e)
    message: dict[str, Any] = {"type": "file_parsed", "path": path, "dags": documents}
    message["error"] = error
    if connections:
        message["connections"] = connections
    shown = [f"{w.category.__name__}: {w.message}" for w in caught]
    if shown:
        message["warnings"] = list(dict.fromkeys(shown))
    return message


def own_connections(root: str, path: str) -> list[dict[str, Any]]:
    """The connections the file just imported defines itself, as reported.

    A module it imports may define connections too, reported when that module's own file is
    parsed (as its DAGs, B110).

    Args:
        root: The DAG root's name.
        path: The file's path relative to the root.

    Returns:
        Each connection's ``{"name", "params"}``, in definition order.

    Raises:
        ValueError: When the file defines two connections of one name.
    """
    here = DagLocation(root, PurePosixPath(path).as_posix())
    own: dict[str, dict[str, Any]] = {}
    for defined in take_connections():
        if defined.location != here:
            continue
        if defined.name in own:
            raise ValueError(f"connection `{defined.name}` is defined twice in {path}")
        own[defined.name] = defined.to_json()
    return list(own.values())


def extend_sys_path(entries: list[str]) -> None:
    """Makes these directories the DAG roots on ``sys.path``, first and in this order.

    Modules found there are imported from their source, never from bytecode (B59). A name
    that can't be told apart fails what uses it, saying why (decision 328). In a
    process used for several roots (a test's), what the earlier ones gave is forgotten.

    Args:
        entries: The directories (every DAG root).
    """
    ordered = list(dict.fromkeys(os.path.abspath(entry) for entry in entries))
    gone = forgotten_roots(ordered)
    for name, loaded in tuple(sys.modules.items()):
        place = getattr(loaded, "__file__", None) or next(
            iter(getattr(loaded, "__path__", ()) or ()), None
        )
        if isinstance(place, str) and any(
            os.path.abspath(place).startswith(root + os.sep) for root in gone
        ):
            del sys.modules[name]
    others = [p for p in sys.path if os.path.abspath(p or os.curdir) not in {*ordered, *gone}]
    sys.path[:] = ordered + others
    import_from_source(ordered)
    search_for_clashes(ordered, others)


def run_parse(channel: Channel, request: dict[str, Any]) -> None:
    """Handles a ``parse`` request: imports each file, reporting progress and results.

    Args:
        channel: The channel to the agent.
        request: The ``parse`` message (root, root directory, files, ``sys.path`` entries).
    """
    extend_sys_path(request["sys_path"])
    root, root_dir = request["root"], Path(request["root_dir"])

    def ask(reference: str) -> dict[str, Any]:
        return channel.ask({"type": "script_declaration", "reference": reference})

    with registry.script_declarations(ask):
        for path in request["files"]:
            channel.send({"type": "file_started", "path": path})
            channel.send(file_result(root, root_dir, path))
    channel.send({"type": "parse_finished"})
