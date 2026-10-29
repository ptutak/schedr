"""Outcomes: which kind of success a task had, and what routes on it (``.on(...)``).

Every task has an outcome enum (see PROJECT.md, Task results and outcomes):

- ``DefaultOutcome`` (``SUCCESS``) for an ordinary return type, an empty task and a command
  without ``outcomes=``;
- a declared ``Outcome`` enum: ``def check() -> Check`` (the member is the outcome, the output
  ``None``), or ``CommandOperator(..., outcomes=Check)`` (the members' values are exit codes);
- an implicit one from an outcome union: a return type that is a union of two or more
  dataclasses, Pydantic models and ``None`` (``-> Rows | Unavailable``), one outcome per
  class, named after it (``"None"`` for ``None``). Pydantic models are recognised here, but
  outputs of them come later (B36): the type model refuses them for now.

In the backend an outcome is a name; the classes and enums exist only here.
"""

import dataclasses
import types
import typing
import warnings
from collections.abc import Sequence
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    Self,
    TypeAliasType,
    Union,
    dataclass_transform,
    get_args,
    get_origin,
    overload,
)

__all__ = ["Data", "DefaultOutcome", "Done", "Outcome", "RunIf", "TypingHint"]


class Done(Protocol):
    """What a call of a task without data gives, for type checkers: something to order.

    A task returning ``None``, an edge on an ``Outcome`` member (``check().on(Check.CHANGED)``)
    and ``.on(None)`` are typed ``Done``: ``>>`` orders them, nothing can be passed.
    """

    def __rshift__(self, other: object, /) -> object:
        """``self >> other``: ``other`` runs after it."""
        ...

    def __rrshift__(self, other: object, /) -> Done:
        """``[a, b] >> self``: it runs after the tasks in the list."""
        ...

    def __lshift__(self, other: object, /) -> object:
        """``self << other``: it runs after ``other``."""
        ...

    def __rlshift__(self, other: object, /) -> Done:
        """``[a, b] << self``: the tasks in the list run after it."""
        ...


class Outcome(IntEnum):
    """The base of a task's declared outcomes: subclass it, one member per outcome.

    For a ``command`` task each member's value is the exit code meaning it (a declared code
    is a success, any other a failure); for a Python task returning a member the values are
    ignored. One enum can serve both, so a check can move between Bash and Python without
    changing any edge::

        class Check(Outcome):
            CHANGED = 0
            UNCHANGED = 3

    In a DAG body, a call of a task returning one is typed as the enum (its output is
    ``None``): ``check().on(Check.CHANGED) >> notify``, ``react(check().outcome)``.
    """

    if TYPE_CHECKING:
        # The call is a reference at run time (an ``Output``), which has these.

        def on(self, *members: Self) -> Done:
            """The edge taken on these outcomes (see ``Output.on``)."""
            ...

        @property
        def outcome(self) -> Self:
            """The outcome itself as data, for a task taking the enum."""
            ...

        def __rshift__(self, other: object, /) -> object:  # type: ignore[override]
            """``self >> other``: ``other`` runs after it."""
            ...

        def __lshift__(self, other: object, /) -> object:  # type: ignore[override]
            """``self << other``: it runs after ``other``."""
            ...


class TypingHint(UserWarning):
    """A DAG that runs as written, but that type checkers will read wrongly.

    E.g. an outcome class deriving from neither ``schedr.Data`` nor ``schedr.pydantic.Model``:
    ``.on(Cls)`` works when the DAG runs, but type checkers reject it (see ``schedr.on``).
    ``schedr dags check`` prints these; pytest lists them in its warnings summary.
    """


_DATACLASS_OPTIONS = frozenset({"frozen", "eq", "order", "kw_only", "unsafe_hash", "repr"})


@dataclass_transform()
class Data:
    """The base of outcome classes: a subclass is a dataclass, and type checkers see ``.on``.

    ``class Rows(Data, frozen=True): rows: list[Row]`` is what ``@dataclass(frozen=True)``
    would make, with nothing else at run time. Type checkers read a call of a task returning
    ``Rows | Unavailable`` as that union, so ``.on(Rows)`` (typed ``Rows``) and ``>>`` must
    come from the class: ``Data`` declares them for type checkers only. Dataclass options
    are class keywords (``frozen``, ``eq``, ``order``, ``kw_only``, ``unsafe_hash``,
    ``repr``); ``slots`` isn't one (it makes a new class).

    Ordinary data (not an outcome) stays a plain ``@dataclass``.
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Makes the subclass a dataclass with the given options.

        Raises:
            TypeError: For an option that isn't a dataclass option this base takes.
        """
        options = {k: kwargs.pop(k) for k in list(kwargs) if k in _DATACLASS_OPTIONS}
        unknown = sorted(kwargs)
        if unknown:
            raise TypeError(
                f"`{cls.__name__}(Data, ...)`: unknown option(s) {', '.join(unknown)}; Data "
                f"takes the dataclass options {', '.join(sorted(_DATACLASS_OPTIONS))}"
            )
        super().__init_subclass__()
        dataclasses.dataclass(cls, **options)

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


class DefaultOutcome(Outcome):
    """The outcome of a task without outcomes of its own."""

    SUCCESS = 0


class RunIf(StrEnum):
    """When a task runs, from its upstream edges (its trigger rule).

    An edge not taken (an ``.on(...)`` whose outcome didn't happen, or a skipped upstream
    task) counts as skipped.
    """

    NONE_FAILED = "none_failed"
    """Every edge that wasn't skipped succeeded and at least one did (the default): skipped
    when all were skipped; a reference whose edge wasn't taken passes ``None``."""

    ALL_SUCCEEDED = "all_succeeded"
    """Every edge succeeded: skipped when one was skipped, so its data is never ``None``."""


@dataclass(frozen=True, slots=True)
class OutcomeSpec:
    """A task's outcomes, as its declaration gives them.

    Attributes:
        names: The outcome names, in declaration order.
        classes: For an outcome union, the class each name stands for (``NoneType`` for
            ``"None"``); empty otherwise.
        enum: The declared ``Outcome`` enum (``DefaultOutcome`` for a plain task).
        exit_codes: Whether the names carry exit codes (a command's).
    """

    names: tuple[str, ...]
    classes: tuple[type, ...] = ()
    enum: type[Outcome] = DefaultOutcome
    exit_codes: bool = False

    @property
    def is_union(self) -> bool:
        """Whether the outcomes come from a union of classes."""
        return bool(self.classes)

    def declared(self) -> tuple[tuple[str, int | None], ...]:
        """The outcomes as stored in the DAG JSON: each name and, for a command, its code.

        Returns:
            ``(name, exit_code)`` pairs, ``exit_code`` ``None`` but for commands.
        """
        if self.exit_codes:
            return tuple((m.name, int(m.value)) for m in self.enum)
        return tuple((name, None) for name in self.names)


DEFAULT = OutcomeSpec(names=("SUCCESS",), exit_codes=True)
"""A plain task's outcomes: ``SUCCESS`` (exit code 0 for a command)."""


def _resolve_alias(annotation: object) -> object:
    while isinstance(annotation, TypeAliasType):
        annotation = annotation.__value__
    return annotation


def _is_outcome_member(member: object) -> bool:
    if member is types.NoneType:
        return True
    target = get_origin(member) or member
    if not isinstance(target, type):
        return False
    if dataclasses.is_dataclass(target):
        return True
    return any(
        base.__module__.startswith("pydantic") and base.__name__ == "BaseModel"
        for base in target.__mro__
    )


def _member_name(member: object) -> str:
    if member is types.NoneType:
        return "None"
    target = get_origin(member) or member
    return str(getattr(target, "__name__", target))


def outcome_enum(annotation: object) -> type[Outcome] | None:
    """The ``Outcome`` enum a return type is, if it is one.

    Args:
        annotation: The return annotation.

    Returns:
        The enum (a subclass of ``Outcome``), or ``None``.
    """
    annotation = _resolve_alias(annotation)
    if isinstance(annotation, type) and issubclass(annotation, Outcome):
        return annotation
    return None


def from_return_type(annotation: object, where: str) -> OutcomeSpec:
    """A Python task's outcomes, from its return annotation.

    Args:
        annotation: The return annotation (type aliases are resolved; only the top level
            counts, so ``list[Rows | Unavailable]`` is data).
        where: Where it is declared, for messages.

    Returns:
        The outcomes: the members of an outcome union, an ``Outcome`` enum's, or ``SUCCESS``.

    Raises:
        TypeError: For an outcome union whose members can't be told apart by their class
            (one a subclass of another, one class with different type parameters, two classes
            of one name), or one mixing an ``Outcome`` enum in.
    """
    annotation = _resolve_alias(annotation)
    enum = outcome_enum(annotation)
    if enum is not None:
        return OutcomeSpec(names=tuple(m.name for m in enum), enum=enum)
    if get_origin(annotation) not in {Union, types.UnionType}:
        return DEFAULT
    members = [_resolve_alias(m) for m in get_args(annotation)]
    if any(outcome_enum(m) is not None for m in members):
        raise TypeError(
            f"{where}: a task's outcomes are either a union of classes or one Outcome enum, "
            "never both; for data or nothing, return an empty dataclass or None"
        )
    if len(members) < 2 or not all(_is_outcome_member(m) for m in members):  # noqa: PLR2004
        return DEFAULT
    _check_distinct(members, where)
    _hint_plain_classes(members, where)
    return OutcomeSpec(
        names=tuple(_member_name(m) for m in members),
        classes=tuple(typing.cast("type", m) for m in members),
    )


def _hint_plain_classes(members: Sequence[object], where: str) -> None:
    """Warns about outcome classes type checkers won't see ``.on`` on (see ``TypingHint``)."""
    for member in members:
        target = get_origin(member) or member
        if member is types.NoneType or not isinstance(target, type):
            continue
        if issubclass(target, Data) or getattr(target, "_schedr_outcome_model", False):
            continue
        base = "schedr.Data" if dataclasses.is_dataclass(target) else "schedr.pydantic.Model"
        warnings.warn(
            TypingHint(
                f"{where}: outcome class `{target.__name__}` derives from neither schedr.Data "
                f"nor schedr.pydantic.Model, so type checkers reject `.on({target.__name__})`; "
                f"derive it from {base}, or write `on(output, {target.__name__})`"
            ),
            stacklevel=2,
        )


def _check_distinct(members: Sequence[object], where: str) -> None:
    """Refuses union members the runner couldn't tell apart by their class."""
    origins = [get_origin(m) or m for m in members]
    for i, a in enumerate(origins):
        for j, b in enumerate(origins):
            if i == j or a is types.NoneType or b is types.NoneType:
                continue
            if i < j and a is b:
                raise TypeError(
                    f"{where}: `{_member_name(members[i])}` appears with different type "
                    "parameters; the outcome is its class, so they can't be told apart"
                )
            assert isinstance(a, type)
            assert isinstance(b, type)
            if a is not b and issubclass(a, b):
                raise TypeError(
                    f"{where}: `{a.__name__}` is a subclass of `{b.__name__}`; an outcome "
                    "union's classes must not derive from one another"
                )
            if i < j and a is not b and a.__name__ == b.__name__:
                raise TypeError(
                    f"{where}: two classes are named `{a.__name__}` ({a.__module__}, "
                    f"{b.__module__}); outcomes are stored by name, so rename one"
                )


def for_command(outcomes: type[Outcome] | None, where: str) -> OutcomeSpec:
    """A command task's outcomes from its ``outcomes=`` argument.

    Args:
        outcomes: The declared enum, or ``None`` for ``SUCCESS`` on exit code 0.
        where: Where it is declared, for messages.

    Returns:
        The outcomes, each name with its exit code.

    Raises:
        TypeError: If ``outcomes`` isn't an ``Outcome`` enum, or two members share a code.
    """
    if outcomes is None:
        return DEFAULT
    if not (isinstance(outcomes, type) and issubclass(outcomes, Outcome)):
        raise TypeError(f"{where}: outcomes= takes an Outcome enum, got {outcomes!r}")
    codes = [int(m.value) for m in outcomes]
    if len(set(codes)) != len(codes):
        raise TypeError(f"{where}: two outcomes of `{outcomes.__name__}` share an exit code")
    return OutcomeSpec(names=tuple(m.name for m in outcomes), enum=outcomes, exit_codes=True)


def names_for(spec: OutcomeSpec, kinds: Sequence[Any], where: str) -> tuple[str, ...]:
    """The outcome names ``.on(*kinds)`` narrows to, checked against the task's own outcomes.

    Args:
        spec: The upstream task's outcomes.
        kinds: Classes (and ``None``) of its outcome union, or members of its enum.
        where: The reference, for messages.

    Returns:
        The names, in the order given.

    Raises:
        TypeError: For no kinds, or one that isn't among the task's own outcomes (an edge that
            could never be taken would skip the downstream task in every run).
    """
    if not kinds:
        raise TypeError(f"{where}.on(): give at least one outcome")
    names: list[str] = []
    for kind in kinds:
        if spec.is_union:
            member = types.NoneType if kind is None else kind
            if member not in spec.classes:
                raise TypeError(
                    f"{where}.on({_label(kind)}): `{_label(kind)}` isn't one of its outcomes "
                    f"({', '.join(spec.names)})"
                )
            names.append(_member_name(member))
        elif isinstance(kind, Outcome) and type(kind) is spec.enum:
            names.append(kind.name)
        else:
            raise TypeError(
                f"{where}.on({_label(kind)}): not one of its outcomes; its outcomes are "
                f"{spec.enum.__name__}'s ({', '.join(spec.names)})"
            )
    return tuple(dict.fromkeys(names))


def _label(kind: object) -> str:
    if kind is None:
        return "None"
    if isinstance(kind, Outcome):
        return f"{type(kind).__name__}.{kind.name}"
    return getattr(kind, "__name__", repr(kind))
