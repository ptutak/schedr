"""DAG roots are imported from their source, never from bytecode (B59).

Python's source loader reuses a ``.pyc`` whose recorded modification time (whole seconds) and
size match the source. git-sync switching a root to a new revision can change a file without
changing either (an edit of the same length within the same second), so a fresh runner would
run the old code while the agent hashed, and reports, the new. Code under a DAG root is
compiled from its source at each import instead, and no bytecode is written there; other
imports (schedr, the libraries) keep their caches.
"""

from __future__ import annotations

import ast
import importlib.machinery
import os
import sys
from contextvars import ContextVar
from functools import cache
from pathlib import Path
from types import CodeType, ModuleType

_ROOTS: set[str] = set()
"""The DAG root directories registered so far, as absolute paths."""


IMPORTING: ContextVar[str | None] = ContextVar("schedr_importing", default=None)
"""The DAG root's file whose top level is running, if one is."""


_SUFFIXES = (*importlib.machinery.EXTENSION_SUFFIXES, *importlib.machinery.SOURCE_SUFFIXES)
"""What makes a file a module."""


_SEARCHED: list[list[str]] = [[], []]
"""The DAG roots of this process, and the rest of ``sys.path`` where libraries are."""


def search_for_clashes(roots: list[str], others: list[str]) -> None:
    """Sets the DAG roots whose names :func:`clash` looks at, forgetting what it knew.

    Args:
        roots: The DAG roots, in ``sys.path`` order.
        others: The rest of ``sys.path``.
    """
    _SEARCHED[:] = [roots, others]
    clash.cache_clear()


@cache
def clash(name: str) -> str | None:
    """Why a top-level name can't be told apart: in two roots, or also a library's.

    Only what uses one fails (decision 328): a stray ``queue.py`` must not stop a root's other
    files, nor a deployment's tasks. Each name is looked at when first imported: a process
    imports a few, and every file and every task starts one, whatever the size of the root.

    Args:
        name: The name, without dots.

    Returns:
        What to tell the author, or ``None`` if the name can be used.
    """
    roots, others = _SEARCHED
    places = [place for root in roots if (place := _top_place(root, name)) is not None]
    if not places:
        return None
    if len(places) > 1:
        return (
            f"the name `{name}` is in several DAG roots ({', '.join(places)}); rename all "
            "but one, or list it in `.schedrignore`"
        )
    library = importlib.machinery.PathFinder.find_spec(name, others)
    if _is_namespace(places[0]) and (library is None or library.origin is not None):
        return None  # No package, so a package or module of its name wins however it starts.
    cached = sys.modules.get(name)
    outside = getattr(cached, "__file__", None) if cached is not None else None
    if (
        name in sys.stdlib_module_names
        or library is not None
        or (isinstance(outside, str) and not _inside(outside, places[0]))
    ):
        return (
            f"`{name}` ({places[0]}) has the name of a library, so which one you get would "
            "depend on how the process starts; rename it, or list it in `.schedrignore`"
        )
    return None


def _top_place(root: str, name: str) -> str | None:
    """What gives the import name `name` at a DAG root's top level, in Python's order."""
    folder = Path(root, name)
    if any((folder / f"__init__{suffix}").is_file() for suffix in _SUFFIXES):
        return str(folder)
    for suffix in _SUFFIXES:
        if (file := Path(root, name + suffix)).is_file():
            return str(file)
    try:
        if folder.is_dir() and any(found.name.endswith(_SUFFIXES) for found in folder.rglob("*")):
            return str(folder)
    except OSError:
        pass  # Not readable: importing from it says so.
    return None


def _is_namespace(place: str) -> bool:
    """Whether what gives a name is a folder of modules without ``__init__``."""
    folder = Path(place)
    return folder.is_dir() and not any(
        (folder / f"__init__{suffix}").is_file() for suffix in _SUFFIXES
    )


def _inside(found: str, place: str) -> bool:
    real = Path(os.path.realpath(found))
    return real == Path(os.path.realpath(place)) or real.is_relative_to(os.path.realpath(place))


def _clash_in(tree: ast.AST) -> str | None:
    """Why a module can't be run: it imports, anywhere in it, a name that clashes."""
    for node in ast.walk(tree):
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module is not None:
            names = [node.module]
        for name in names:
            if (why := clash(name.partition(".")[0])) is not None:
                return why
    return None


class NameGuard:
    """A meta path finder: a clashing name is never a DAG root's, for a library importing it.

    A root's own files importing one fail when compiled (see ``SourceOnlyLoader``), so this
    leaves libraries working whatever a root holds, under any start method.
    """

    @staticmethod
    def find_spec(
        name: str, path: object = None, target: ModuleType | None = None
    ) -> importlib.machinery.ModuleSpec | None:
        """The library's module of a clashing name; ``None`` for any other name.

        Args:
            name: The module's name.
            path: Its parent's search path.
            target: The module being reloaded.

        Returns:
            Its spec, or ``None``: the next finders then look.

        Raises:
            ModuleNotFoundError: For a clashing name that no library has.
        """
        del path
        if "." in name or (why := clash(name)) is None:
            return None
        for finder in (importlib.machinery.BuiltinImporter, importlib.machinery.FrozenImporter):
            if (spec := finder.find_spec(name, None, target)) is not None:
                return spec
        others = [p for p in sys.path if not _under_a_root(p or os.curdir)]
        spec = importlib.machinery.PathFinder.find_spec(name, others, target)
        if spec is None:
            raise ModuleNotFoundError(why, name=name)
        return spec


class SourceOnlyLoader(importlib.machinery.SourceFileLoader):
    """A source file loader that compiles the source at every import, never using bytecode."""

    def exec_module(self, module: ModuleType) -> None:
        """Runs the module, saying so: the ``Dag`` its top level enters belongs to it.

        Args:
            module: The module being imported.
        """
        token = IMPORTING.set(self.path)
        try:
            super().exec_module(module)
        finally:
            IMPORTING.reset(token)

    def get_code(self, fullname: str) -> CodeType:
        """The module's code, compiled from its source now.

        Args:
            fullname: The module's name.

        Returns:
            The compiled code.
        """
        del fullname
        source = self.get_data(self.path)
        tree = ast.parse(source, self.path)
        if (why := _clash_in(tree)) is not None:
            raise ImportError(why)
        return compile(tree, self.path, "exec", dont_inherit=True)


def _under_a_root(path: str) -> bool:
    """Whether `path` (a ``sys.path`` entry or a package's directory) is in a DAG root."""
    absolute = os.path.abspath(path)
    return any(absolute == root or absolute.startswith(root + os.sep) for root in _ROOTS)


def _finder(path: str) -> importlib.machinery.FileFinder:
    """The path hook: a finder of extension modules and source files, for a DAG root.

    Source files are compiled at each import, never read as bytecode; the order is the default
    finder's.

    Raises:
        ImportError: For any other directory, which the next hook then handles.
    """
    if not _under_a_root(path):
        raise ImportError("not in a DAG root")
    return importlib.machinery.FileFinder(
        path,
        (importlib.machinery.ExtensionFileLoader, importlib.machinery.EXTENSION_SUFFIXES),
        (SourceOnlyLoader, importlib.machinery.SOURCE_SUFFIXES),
    )


def registered_roots() -> list[str]:
    """The DAG root directories registered so far, in their order on ``sys.path``.

    Returns:
        The directories.
    """
    return [path for path in sys.path if path in _ROOTS]


def forgotten_roots(kept: list[str]) -> set[str]:
    """Forgets the DAG roots registered so far but these.

    Args:
        kept: The directories to keep, as absolute paths.

    Returns:
        The ones forgotten.
    """
    gone = _ROOTS - set(kept)
    _ROOTS.difference_update(gone)
    return gone


def import_from_source(roots: list[str]) -> None:
    """Makes imports from these DAG root directories compile their source (see the module).

    Args:
        roots: The directories, as on ``sys.path``.
    """
    if _finder not in sys.path_hooks:
        sys.path_hooks.insert(0, _finder)
    if NameGuard not in sys.meta_path:
        sys.meta_path.insert(0, NameGuard)
    _ROOTS.update(os.path.abspath(root) for root in roots)
    # A finder made for a root before this would still read bytecode.
    for cached in list(sys.path_importer_cache):
        if _under_a_root(cached):
            del sys.path_importer_cache[cached]
