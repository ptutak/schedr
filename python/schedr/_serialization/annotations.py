"""Reading type annotations: resolving aliases and wrappers, and naming types in messages."""

import collections.abc
import types
import typing
from collections.abc import Callable
from typing import (
    Annotated,
    Any,
    NewType,
    NotRequired,
    ReadOnly,
    Required,
    TypeAliasType,
    Union,
    get_args,
    get_origin,
)

_QUALIFIERS = (Annotated, NotRequired, Required, ReadOnly)


class TypeModelError(TypeError):
    """A type that can't be an output or input type; the message says which part and why."""


def unwrap(annotation: object) -> object:
    """Resolves what only renames or qualifies a type.

    That is ``type`` aliases, ``Annotated``, ``NewType``, and the ``TypedDict`` qualifiers
    ``NotRequired``, ``Required`` and ``ReadOnly``.

    Args:
        annotation: A type annotation.

    Returns:
        The underlying type; ``NoneType`` becomes ``None``.

    Raises:
        TypeModelError: For a generic ``type`` alias with parameters (not supported yet).
    """
    while True:
        if isinstance(annotation, TypeAliasType):
            annotation = annotation.__value__
        elif isinstance(get_origin(annotation), TypeAliasType):
            raise TypeModelError(f"generic type alias `{label(annotation)}` isn't supported yet")
        elif get_origin(annotation) in _QUALIFIERS:
            annotation = get_args(annotation)[0]
        elif isinstance(annotation, NewType):
            annotation = annotation.__supertype__
        else:
            break
    return None if annotation is type(None) else annotation


def is_union(annotation: object) -> bool:
    """Whether ``annotation`` is a union (``A | B``, ``Optional[A]``, ``Union[A, B]``).

    Args:
        annotation: A type annotation.

    Returns:
        True for unions.
    """
    return get_origin(annotation) in (Union, types.UnionType)


def tuple_items(args: tuple[Any, ...]) -> tuple[Any, ...]:
    """The element types of ``tuple[...]`` from its arguments (``tuple[()]`` has none).

    Args:
        args: ``get_args`` of the tuple type.

    Returns:
        The element types.
    """
    return () if args in ((), ((),)) else args


SEQUENCE_ORIGINS = (list, collections.abc.Sequence)
"""Origins accepted as lists."""

MAPPING_ORIGINS = (dict, collections.abc.Mapping)
"""Origins accepted as dictionaries."""


def label(annotation: object) -> str:
    """A type as written in code, for messages (``dict[int, str]``, ``Rows``, ``None``).

    Args:
        annotation: A type annotation.

    Returns:
        A readable name.
    """
    if annotation is None or annotation is type(None):
        return "None"
    if annotation is Any:
        return "Any"
    if isinstance(annotation, type) and not get_args(annotation):
        return annotation.__name__
    return str(annotation).replace("typing.", "").replace("collections.abc.", "")


def type_hints(target: Callable[..., Any] | type, what: str) -> dict[str, Any]:
    """The resolved annotations of a function or class.

    Args:
        target: The function or class.
        what: What it is, for messages (e.g. ``task `load```).

    Returns:
        Names (and ``return``) to resolved types.

    Raises:
        TypeModelError: If an annotation can't be resolved (e.g. an undefined name).
    """
    try:
        return typing.get_type_hints(target, include_extras=True)
    except Exception as e:  # get_type_hints raises NameError, TypeError, AttributeError, ...
        raise TypeModelError(f"{what}: can't resolve an annotation: {e}") from e
