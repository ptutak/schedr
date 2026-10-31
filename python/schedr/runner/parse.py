"""Parse mode: import the files of one DAG root and report the DAGs they define.

Each file is imported under a stable module name derived from its root and path, and kept in
``sys.modules`` (dataclasses need their module there for their type hints to resolve). Task
mode imports a DAG file under the same name. Files that define no DAG report none; a file that
raises reports an import error and doesn't stop the others.
"""

import hashlib
import importlib.machinery
import importlib.util
import os
import re
import sys
import traceback
import warnings
from pathlib import Path
from typing import Any

from schedr._model import Dag, registry
from schedr.outcomes import TypingHint
from schedr.runner.protocol import Channel
from schedr.runner.sources import SourceOnlyLoader, import_from_source

TRACEBACK_LINES = 20
"""How many traceback lines an import error keeps."""


def module_name(root: str, path: str) -> str:
    """The module name a DAG file is imported under.

    Args:
        root: The DAG root's name.
        path: The file's path relative to the root.

    Returns:
        A valid, unique module name, e.g. ``_schedr_dag_etl_repo_team_a_sync_1a2b3c4d``.
    """
    readable = re.sub(r"\W", "_", f"{root}/{path.removesuffix('.py')}", flags=re.ASCII)
    digest = hashlib.sha256(f"{root}\0{path}".encode()).hexdigest()[:8]
    return f"_schedr_dag_{readable}_{digest}"


def import_dag_entries(root: str, root_dir: Path, path: str) -> list[registry.DefinedDag]:
    """Imports one DAG file and returns the DAGs it defined, with their task functions.

    Args:
        root: The DAG root's name.
        root_dir: The root's directory.
        path: The file's path relative to the root.

    Returns:
        The DAGs, in definition order (empty for a helper module).

    Raises:
        BaseException: Whatever importing the file raised (the module is then removed from
            ``sys.modules`` and its partial DAGs are dropped).
    """
    name = module_name(root, path)
    # From its source, never stale bytecode (B59); a given loader would take any file.
    location = str(root_dir / path)
    if not location.endswith(tuple(importlib.machinery.SOURCE_SUFFIXES)):
        raise ImportError(f"can't import {path}: not a Python source file")
    spec = importlib.util.spec_from_file_location(
        name, location, loader=SourceOnlyLoader(name, location)
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"can't import {path}: not a Python source file")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        with registry.parse_root(root, root_dir):
            spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        registry.take_entries()
        raise
    return registry.take_entries()


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


def _internal(filename: str) -> bool:
    return filename.startswith((_SCHEDR_DIR, "<frozen importlib"))


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
        importing it raised (``Category: message``; a ``TypingHint`` always, others as the
        interpreter's warning filters let them through).
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", TypingHint)
        try:
            dags = import_dag_file(root, root_dir, path)
            documents = [d.to_json() for d in dags]
            error = None
        except (Exception, SystemExit) as e:
            documents, error = [], import_error(e)
    message: dict[str, Any] = {"type": "file_parsed", "path": path, "dags": documents}
    message["error"] = error
    shown = [f"{w.category.__name__}: {w.message}" for w in caught]
    if shown:
        message["warnings"] = list(dict.fromkeys(shown))
    return message


def extend_sys_path(entries: list[str]) -> None:
    """Puts directories first on ``sys.path``, in order, each at most once.

    Modules found there are imported from their source, never from bytecode (B59).

    Args:
        entries: The directories (every DAG root).
    """
    import_from_source(entries)
    for entry in reversed(list(dict.fromkeys(entries))):
        if entry not in sys.path:
            sys.path.insert(0, entry)


def run_parse(channel: Channel, request: dict[str, Any]) -> None:
    """Handles a ``parse`` request: imports each file, reporting progress and results.

    Args:
        channel: The channel to the agent.
        request: The ``parse`` message (root, root directory, files, ``sys.path`` entries).
    """
    extend_sys_path(request["sys_path"])
    root, root_dir = request["root"], Path(request["root_dir"])
    for path in request["files"]:
        channel.send({"type": "file_started", "path": path})
        channel.send(file_result(root, root_dir, path))
    channel.send({"type": "parse_finished"})
