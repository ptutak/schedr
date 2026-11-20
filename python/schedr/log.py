"""Structured log lines (decision 514), for every task kind.

One JSON object a line on stderr, shown in the Logs tab with its level and fields.

A Python task's ``logging`` becomes structured with no change to its code: the runner makes
:class:`StructuredHandler` the handler ``logging`` falls back to (``logging.lastResort``) before
the task's DAG file is imported, at ``INFO``. A task that configures logging itself
(``basicConfig``, a handler of its own) keeps only its own.

Any process may write one with :func:`line`; ``schedr task-ctx log`` does it for a script.
"""

from __future__ import annotations

import functools
import json
import logging
import sys
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TextIO

__all__ = ["KEY", "StructuredHandler", "install", "line"]

#: The key a structured line starts with.
KEY = "schedr_log"

#: The format's version.
VERSION = 1

_LEVELS = {
    logging.DEBUG: "debug",
    logging.INFO: "info",
    logging.WARNING: "warning",
    logging.ERROR: "error",
    logging.CRITICAL: "critical",
}

# A record's own attributes: anything else on it is an `extra=` field.
_RECORD_KEYS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {
    "message",
    "asctime",
    "taskName",
}


def _level_name(levelno: int) -> str:
    """The format's level for a ``logging`` level: the nearest one at or below it."""
    for number in sorted(_LEVELS, reverse=True):
        if levelno >= number:
            return _LEVELS[number]
    return "debug"


def _jsonable(value: object) -> object:
    """``value`` as JSON can hold it: itself when it can, else its text."""
    try:
        json.dumps(value, allow_nan=False)
    except TypeError, ValueError:
        return str(value)
    return value


def line(
    level: str, message: str, fields: dict[str, object] | None = None, *, time: datetime | None
) -> str:
    """One structured line, without its end of line.

    Args:
        level: ``debug``, ``info``, ``warning``, ``error`` or ``critical``.
        message: What it says.
        fields: More about it, each a JSON value (another is written as its text).
        time: When; ``None`` leaves it out.

    Returns:
        The line.
    """
    record: dict[str, object] = {KEY: VERSION, "level": level, "message": message}
    if fields:
        record["fields"] = {name: _jsonable(value) for name, value in fields.items()}
    if time is not None:
        record["time"] = (
            time.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        )
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"))


class StructuredHandler(logging.Handler):
    """Writes each record as a structured line to a stream (stderr by default)."""

    def __init__(self, stream: TextIO | None = None) -> None:
        """A handler writing to ``stream``; ``None``: stderr, as it is at each record."""
        super().__init__()
        self._stream = stream

    def emit(self, record: logging.LogRecord) -> None:
        """Writes ``record``: its logger's name and its ``extra=`` as fields, a traceback too."""
        try:
            fields: dict[str, object] = {"logger": record.name}
            fields.update(
                (key, value) for key, value in vars(record).items() if key not in _RECORD_KEYS
            )
            if record.exc_info:
                fields["traceback"] = "".join(traceback.format_exception(*record.exc_info))
            written = line(
                _level_name(record.levelno),
                record.getMessage(),
                fields,
                time=datetime.fromtimestamp(record.created, UTC),
            )
            stream = self._stream if self._stream is not None else sys.stderr
            stream.write(written + "\n")
            stream.flush()
        except Exception:  # logging's own way: a record that fails is reported, never raised
            self.handleError(record)


def install(stream: TextIO | None = None) -> StructuredHandler:
    """Makes structured lines what ``logging`` writes while nothing else is configured.

    The handler becomes ``logging.lastResort``, the one ``logging`` uses when a record finds no
    handler, and the one ``logging.basicConfig()`` installs when called with no arguments, as
    ``logging.info()`` and its kind do when nothing is configured. A task configuring logging
    itself (``basicConfig(...)`` with arguments, a handler of its own) keeps its own, whenever
    it does it: this handler is taken off the root logger first. The root logger is set to
    ``INFO`` unless it has a level of its own (its default, ``WARNING``, would show no
    ``logging.info``).

    Args:
        stream: Where the lines go; ``None``: stderr, as it is when each line is written.

    Returns:
        The handler.
    """
    handler = StructuredHandler(stream)
    logging.lastResort = handler
    root = logging.getLogger()
    if root.level == logging.WARNING:
        root.setLevel(logging.INFO)
    # Its keywords as `logging`'s own take them: passed on unread.
    original: Callable[..., None] = getattr(logging.basicConfig, "__wrapped__", logging.basicConfig)

    @functools.wraps(original)
    def basic_config(**kwargs: object) -> None:
        # Codex's review of M5.2i5: `logging.info()` called `basicConfig()`, a plain handler.
        if not kwargs:
            if not root.handlers:
                root.addHandler(handler)
            return
        for installed in list(root.handlers):
            if isinstance(installed, StructuredHandler):
                root.removeHandler(installed)
        original(**kwargs)

    logging.basicConfig = basic_config
    return handler
