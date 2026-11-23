"""What an output may be when stored (B76): its size and its nesting.

Checked the same way by a task's runner and an extension's plugin.
"""

import json

__all__ = ["MAX_DEPTH", "CodecError", "check_size", "encoded_size"]


class CodecError(ValueError):
    """A value that doesn't fit its type, or can't be stored.

    Attributes:
        at: Where in the value, e.g. ``[3].created`` (empty for the value itself).
        reason: What is wrong.
    """

    def __init__(self, at: str, reason: str) -> None:
        """Records where and why.

        Args:
            at: Where in the value.
            reason: What is wrong.
        """
        super().__init__(f"at `{at or '.'}`: {reason}")
        self.at = at
        self.reason = reason


MAX_DEPTH = 100
"""How deeply a stored value may nest lists and dicts: under what the agent's JSON reader takes
(128 levels, with the messages wrapping an output), B76."""


def encoded_size(data: object) -> int:
    """The size of encoded data as stored: compact JSON, in UTF-8 bytes.

    Args:
        data: JSON-compatible data (from :func:`encode`).

    Returns:
        The size in bytes.
    """
    return len(json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode())


def check_size(data: object, limit: int) -> None:
    """Checks encoded data against an output size limit, and its nesting (:data:`MAX_DEPTH`).

    Args:
        data: JSON-compatible data.
        limit: The limit in bytes.

    Raises:
        CodecError: If it is larger, or nested more deeply.
    """
    _check_depth(data)
    size = encoded_size(data)
    if size > limit:
        raise CodecError("", f"output is {size} bytes, over the limit of {limit} bytes")


def _check_depth(data: object) -> None:
    # Iterative: a value nested past Python's recursion limit is refused too.
    stack: list[tuple[object, int]] = [(data, 0)]
    while stack:
        value, depth = stack.pop()
        if isinstance(value, list | dict):
            if depth == MAX_DEPTH:
                raise CodecError(
                    "", f"output is nested more than {MAX_DEPTH} levels deep (lists and dicts)"
                )
            items = value.values() if isinstance(value, dict) else value
            stack.extend((item, depth + 1) for item in items)
