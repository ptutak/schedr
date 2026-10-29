"""Types for task outputs: ``ProcessResult``, the ``Json`` alias, and size constants.

``Json`` is the bound type checkers use for ``@task`` return types; the guarantee is the
parse-time check (see PROJECT.md, Output serialization).
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, Self

KiB = 1024
"""One kibibyte, in bytes."""

MiB = 1024 * KiB
"""One mebibyte, in bytes."""


class DataclassInstance(Protocol):
    """Any dataclass instance (for type checkers)."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


type Json = (
    bool
    | int
    | float
    | str
    | datetime
    | date
    | timedelta
    | Sequence[Json]
    | Mapping[str, object]
    | DataclassInstance
    | None
)
"""What a ``@task`` may return, for type checkers.

``Sequence`` and ``Mapping`` (not ``list`` / ``dict``, which are invariant) accept e.g.
``list[int]``; ``Mapping[str, object]`` accepts ``TypedDict`` classes, whose values the type
checker can't check (the parse-time check does).
"""


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """The output of a ``command`` or ``script`` task.

    Attributes:
        stdout: The standard output (its last part, if truncated).
        stderr: The standard error (its last part, if truncated).
        exit_code: The process's exit code.
        stdout_truncated: Whether ``stdout`` exceeded the output size limit.
        stderr_truncated: Whether ``stderr`` exceeded the output size limit.
    """

    stdout: str
    stderr: str
    exit_code: int
    stdout_truncated: bool = False
    stderr_truncated: bool = False

    if TYPE_CHECKING:
        # In a DAG body a command's output is a reference (an ``Output``), which orders.

        def __rshift__(self, other: object, /) -> object:
            """``self >> other``: ``other`` runs after the command."""
            ...

        def __rrshift__(self, other: object, /) -> Self:
            """``[a, b] >> self``: the command runs after the tasks in the list."""
            ...

        def __lshift__(self, other: object, /) -> object:
            """``self << other``: the command runs after ``other``."""
            ...

        def __rlshift__(self, other: object, /) -> Self:
            """``[a, b] << self``: the tasks in the list run after the command."""
            ...
