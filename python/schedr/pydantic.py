"""``schedr.pydantic.Model``: the base of Pydantic outcome models (needs Pydantic installed).

Importing this module imports Pydantic; nothing else in schedr does, so schedr never depends
on it. A Pydantic model that is an outcome (a member of a union a task returns) derives from
``Model`` instead of ``BaseModel``, as a dataclass outcome derives from ``schedr.Data``: type
checkers then accept ``.on(Remote)`` and ``>>`` on it (see ``schedr.Data``). Ordinary data
stays a plain ``BaseModel``.
"""

from typing import TYPE_CHECKING, ClassVar, Self, overload

from pydantic import BaseModel

from schedr.outcomes import Done

__all__ = ["Model"]


class Model(BaseModel):
    """The base of Pydantic outcome models: ``class Remote(Model): url: str``."""

    _schedr_outcome_model: ClassVar[bool] = True

    if TYPE_CHECKING:
        # The call is a reference at run time (an ``Output``), which has these.

        @overload
        def on[U](self, kind: type[U], /) -> U: ...
        @overload
        def on[U, V](self, kind: type[U], other: type[V], /) -> U | V: ...
        @overload
        def on(self, kind: None, /) -> Done: ...
        def on(self, *kinds: object) -> object:
            """Narrows to some of the task's outcome classes (see ``Output.on``)."""
            ...

        def __rshift__(self, other: object, /) -> object:
            """``self >> other``: ``other`` runs after it."""
            ...

        def __rrshift__(self, other: object, /) -> Self:
            """``[a, b] >> self``: it runs after the tasks in the list."""
            ...

        def __lshift__(self, other: object, /) -> object:
            """``self << other``: it runs after ``other``."""
            ...

        def __rlshift__(self, other: object, /) -> Self:
            """``[a, b] << self``: the tasks in the list run after it."""
            ...
