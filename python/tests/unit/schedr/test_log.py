"""Structured log lines (decision 514): their format, and logging falling back to them."""

import io
import json
import logging
import subprocess
import sys
from datetime import UTC, datetime

import pytest

from schedr.log import StructuredHandler, line


def _run(code: str) -> subprocess.CompletedProcess[str]:
    """``code`` in a fresh interpreter (pytest's own logging handlers aside), the runner's
    structured logging installed first."""
    prelude = "import logging, sys\nfrom schedr.log import install\ninstall()\n"
    return subprocess.run(
        [sys.executable, "-c", prelude + code], capture_output=True, text=True, check=True
    )


def test_a_line_is_one_json_object_with_its_level_message_fields_and_time() -> None:
    written = line(
        "info",
        "pod scheduled",
        {"node": "n1", "odd": object()},
        time=datetime(2026, 11, 21, 10, 0, 3, 112000, UTC),
    )
    parsed = json.loads(written)
    assert parsed["schedr_log"] == 1
    assert (parsed["level"], parsed["message"]) == ("info", "pod scheduled")
    assert parsed["fields"]["node"] == "n1"
    assert parsed["fields"]["odd"].startswith("<object")
    assert parsed["time"] == "2026-11-21T10:00:03.112Z"
    assert "\n" not in written


def test_a_tasks_logging_is_structured_with_its_extras_and_traceback() -> None:
    ran = _run(
        "log = logging.getLogger('etl.load')\n"
        "log.info('loaded %d rows', 3, extra={'table': 'sales'})\n"
        "log.debug('not shown at INFO')\n"
        "try:\n    raise ValueError('no')\n"
        "except ValueError:\n    log.exception('failed')\n"
    )
    first, second = (json.loads(text) for text in ran.stderr.splitlines())
    assert (first["level"], first["message"]) == ("info", "loaded 3 rows")
    assert first["fields"] == {"logger": "etl.load", "table": "sales"}
    assert second["level"] == "error"
    assert "ValueError: no" in second["fields"]["traceback"]


def test_a_task_configuring_logging_itself_keeps_its_own() -> None:
    ran = _run(
        "logging.basicConfig(stream=sys.stdout, format='%(levelname)s %(message)s')\n"
        "logging.getLogger('t').warning('mine')\n"
    )
    assert ran.stdout == "WARNING mine\n"
    assert ran.stderr == ""


def test_module_level_logging_is_structured_too() -> None:
    """``logging.info()`` calls ``basicConfig()`` when nothing is configured: ours then (Codex's
    review of M5.2i5: a plain handler came first, and every logger after used it)."""
    ran = _run(
        "logging.info('hello', extra={'rows': 3})\n"
        "logging.getLogger('named').warning('after root')\n"
    )
    lines = [json.loads(text) for text in ran.stderr.splitlines()]
    assert [(r["level"], r["message"]) for r in lines] == [
        ("info", "hello"),
        ("warning", "after root"),
    ]
    assert lines[0]["fields"]["rows"] == 3


def test_a_task_configuring_logging_after_logging_keeps_its_own() -> None:
    """Its own ``basicConfig(...)`` after a ``logging.info()``: its handler alone."""
    ran = _run(
        "logging.info('before')\n"
        "logging.basicConfig(stream=sys.stdout, format='%(levelname)s %(message)s', force=False)\n"
        "logging.getLogger('t').warning('mine')\n"
    )
    assert ran.stdout == "WARNING mine\n"
    assert json.loads(ran.stderr)["message"] == "before"


def test_a_failing_record_never_raises(capsys: pytest.CaptureFixture[str]) -> None:
    class Broken(io.StringIO):
        def write(self, text: str) -> int:
            raise OSError("full")

    handler = StructuredHandler(Broken())
    handler.emit(logging.LogRecord("x", logging.INFO, "f", 1, "m", (), None))
    assert "OSError" in capsys.readouterr().err
