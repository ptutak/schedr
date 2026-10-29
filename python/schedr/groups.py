"""Task groups: ``with group("load") as load:`` (see PROJECT.md, Native API: task groups).

A group organises tasks while the DAG is written; it isn't something that runs. Its tasks'
IDs get its name as a prefix (``load.extract``), they take its ``defaults`` and ``run_if``
unless they set their own, and a dependency on the group goes through its roots and leaves
(decision 56A): ``start >> load`` makes ``start`` run before every task of ``load`` with no
upstream task inside it, ``load >> publish`` makes ``publish`` run after every task of
``load`` with no downstream task inside it. The master sees ordinary tasks only.
"""

from types import TracebackType
from typing import Self

from schedr import _build
from schedr.dag import TaskDefaults
from schedr.outcomes import RunIf

__all__ = ["Group", "group"]


class Group:
    """A group of tasks: ``with group("load") as load:``; ``>>`` orders it as a whole.

    Args:
        name: Its name, one ID part (no ``.``): its tasks' IDs start with it.
        defaults: The defaults its tasks get, over the DAG's (a task's own arguments win).
        run_if: Its tasks' trigger rule unless they set their own.

    Raises:
        RuntimeError: Outside ``with Dag(...)``.
        IdError: For an invalid name, one used twice in the same parent, or a task's ID.
    """

    def __init__(
        self, name: str, *, defaults: TaskDefaults | None = None, run_if: RunIf | None = None
    ) -> None:
        """Records the group's settings; it opens when its ``with`` block starts."""
        self._name = name
        own = (defaults or TaskDefaults()).options()
        self._options = _build.TaskOptions(run_if=run_if).over(own)
        self._scope: _build.GroupScope | None = None

    @property
    def group_id(self) -> str:
        """The group's ID (its parents' names before its own), once its block started.

        Raises:
            RuntimeError: Before its ``with`` block started.
        """
        return self._open_scope().id

    @property
    def _schedr_group(self) -> _build.GroupScope:
        return self._open_scope()

    def _open_scope(self) -> _build.GroupScope:
        if self._scope is None:
            raise RuntimeError(f"group `{self._name}` is used before its `with` block")
        return self._scope

    def __enter__(self) -> Self:
        """Opens the group: tasks created in the block belong to it.

        Returns:
            The group, to depend on.
        """
        builder = _build.require_active("group()")
        self._scope = builder.open_group(self._name, self._options)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Closes the group: its roots and leaves are known, and dependencies on it are made."""
        builder = _build.active()
        if builder is None or exc_type is not None or self._scope is None:
            return
        builder.close_group(self._scope)

    def __rshift__(self, other: object, /) -> object:
        """``group >> other``: ``other`` runs after the group's leaves.

        Returns:
            ``other``, so dependencies can be chained.
        """
        _build.order(self, other, ">>")
        return other

    def __rrshift__(self, other: object, /) -> Self:
        """``other >> group``: the group's roots run after ``other``.

        Returns:
            This group.
        """
        _build.order(other, self, ">>")
        return self

    def __lshift__(self, other: object, /) -> object:
        """``group << other``: the group's roots run after ``other``.

        Returns:
            ``other``.
        """
        _build.order(other, self, "<<")
        return other

    def __rlshift__(self, other: object, /) -> Self:
        """``other << group``: ``other`` runs after the group's leaves.

        Returns:
            This group.
        """
        _build.order(self, other, "<<")
        return self

    def __repr__(self) -> str:
        """A readable form, e.g. ``Group('load')``."""
        name = self._scope.id if self._scope is not None else self._name
        return f"Group({name!r})"


group = Group
"""``with group("load", defaults=TaskDefaults(retries=3)) as load:`` (see :class:`Group`)."""
