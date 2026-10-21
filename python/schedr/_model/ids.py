"""DAG IDs and task IDs, validated with the same rules as ``schedr-model`` (``ids.rs``)."""

import re

MAX_ID_LEN = 250
"""The longest allowed identifier, in bytes (UTF-8)."""

_DAG_ID_CHARS = re.compile(r"[A-Za-z0-9_.:/#-]")
_TASK_ID_CHARS = re.compile(r"[A-Za-z0-9_.-]")
_DAG_ID_ALLOWED = "ASCII letters, digits and `_ . - : / #`"
_TASK_ID_ALLOWED = "ASCII letters, digits and `_ . -`"


class IdError(ValueError):
    """An identifier that isn't valid; the message says why."""


def _check(kind: str, value: str, chars: re.Pattern[str], allowed: str) -> None:
    if not value:
        raise IdError(f"{kind} is empty")
    if len(value.encode()) > MAX_ID_LEN:
        raise IdError(f"{kind} `{value}` is longer than {MAX_ID_LEN} bytes")
    for ch in value:
        if not chars.fullmatch(ch):
            raise IdError(f"{kind} `{value}` contains `{ch}`; allowed are {allowed}")


def validate_dag_id(value: str) -> str:
    """Validates a DAG ID.

    Args:
        value: A native DAG's ID, or a standalone job's reference
            (``etl-repo:team_a/schedr.toml#report_daily``).

    Returns:
        The same string, so the call can be used inline.

    Raises:
        IdError: If it is empty, too long, or contains a character that isn't allowed.
    """
    _check("DAG ID", value, _DAG_ID_CHARS, _DAG_ID_ALLOWED)
    return value


def validate_task_id(value: str) -> str:
    """Validates a task ID (``task`` or ``group.task``).

    Args:
        value: The task ID.

    Returns:
        The same string, so the call can be used inline.

    Raises:
        IdError: If it is empty, too long, contains a character that isn't allowed, or has an
            empty part around a ``.``.
    """
    _check("task ID", value, _TASK_ID_CHARS, _TASK_ID_ALLOWED)
    if any(part == "" for part in value.split(".")):
        raise IdError(f"task ID `{value}` has an empty group or task name around `.`")
    return value
