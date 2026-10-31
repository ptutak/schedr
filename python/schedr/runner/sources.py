"""DAG roots are imported from their source, never from bytecode (B59).

Python's source loader reuses a ``.pyc`` whose recorded modification time (whole seconds) and
size match the source. git-sync switching a root to a new revision can change a file without
changing either (an edit of the same length within the same second), so a fresh runner would
run the old code while the agent hashed, and reports, the new. Code under a DAG root is
compiled from its source at each import instead, and no bytecode is written there; other
imports (schedr, the libraries) keep their caches.
"""

from __future__ import annotations

import importlib.machinery
import os
import sys
from types import CodeType

_ROOTS: set[str] = set()
"""The DAG root directories registered so far, as absolute paths."""


class SourceOnlyLoader(importlib.machinery.SourceFileLoader):
    """A source file loader that compiles the source at every import, never using bytecode."""

    def get_code(self, fullname: str) -> CodeType:
        """The module's code, compiled from its source now.

        Args:
            fullname: The module's name.

        Returns:
            The compiled code.
        """
        del fullname
        return self.source_to_code(self.get_data(self.path), self.path)


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


def import_from_source(roots: list[str]) -> None:
    """Makes imports from these DAG root directories compile their source (see the module).

    Args:
        roots: The directories, as on ``sys.path``.
    """
    if _finder not in sys.path_hooks:
        sys.path_hooks.insert(0, _finder)
    _ROOTS.update(os.path.abspath(root) for root in roots)
    # A finder made for a root before this would still read bytecode.
    for cached in list(sys.path_importer_cache):
        if _under_a_root(cached):
            del sys.path_importer_cache[cached]
