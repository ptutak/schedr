"""Parse mode: import the files of one DAG root and report the DAGs they define.

Each file is imported under a stable module name derived from its root and path, and kept in
``sys.modules`` (dataclasses need their module there for their type hints to resolve). Task
mode imports a DAG file under the same name. Files that define no DAG report none; a file that
raises reports an import error and doesn't stop the others.
"""

import hashlib
import importlib.util
import re
import sys
import traceback
from pathlib import Path
from typing import Any

from schedr._model import Dag, registry
from schedr.runner.protocol import Channel

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


def import_dag_file(root: str, root_dir: Path, path: str) -> list[Dag]:
    """Imports one DAG file and returns the DAGs it defined.

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
    spec = importlib.util.spec_from_file_location(name, root_dir / path)
    if spec is None or spec.loader is None:
        raise ImportError(f"can't import {path}: not a Python source file")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        with registry.parse_root(root, root_dir):
            spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        registry.take_defined()
        raise
    return registry.take_defined()


def import_error(error: BaseException) -> dict[str, str]:
    """An import error as sent to the agent.

    Args:
        error: The exception.

    Returns:
        Its class name, message and the last :data:`TRACEBACK_LINES` lines of the traceback.
    """
    lines = "".join(traceback.format_exception(error)).splitlines()
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
        The message, with the DAG JSON of each DAG, or the import error.
    """
    try:
        dags = import_dag_file(root, root_dir, path)
        documents = [d.to_json() for d in dags]
    except (Exception, SystemExit) as e:
        return {"type": "file_parsed", "path": path, "dags": [], "error": import_error(e)}
    return {"type": "file_parsed", "path": path, "dags": documents, "error": None}


def run_parse(channel: Channel, request: dict[str, Any]) -> None:
    """Handles a ``parse`` request: imports each file, reporting progress and results.

    Args:
        channel: The channel to the agent.
        request: The ``parse`` message (root, root directory, files, ``sys.path`` entries).
    """
    for entry in reversed(request["sys_path"]):
        if entry not in sys.path:
            sys.path.insert(0, entry)
    root, root_dir = request["root"], Path(request["root_dir"])
    for path in request["files"]:
        channel.send({"type": "file_started", "path": path})
        channel.send(file_result(root, root_dir, path))
    channel.send({"type": "parse_finished"})
