"""Turning annotations into type descriptors (DAG JSON ``TypeDesc``), at parse time.

This is the parse-time guarantee that an output can be serialized (see PROJECT.md, Output
serialization): any type outside the supported set is an import error naming the part.
"""

import dataclasses
from datetime import date, datetime, timedelta
from typing import Any, Literal, get_args, get_origin, is_typeddict

from schedr._model import (
    DataclassType,
    DictType,
    Field,
    ListType,
    LiteralType,
    ModelType,
    ScalarType,
    TupleType,
    TypedDictType,
    TypeDesc,
    UnionType,
)
from schedr._model.types import ScalarName
from schedr._serialization import models
from schedr._serialization.annotations import (
    MAPPING_ORIGINS,
    SEQUENCE_ORIGINS,
    TypeModelError,
    is_union,
    label,
    tuple_items,
    type_hints,
    unwrap,
)
from schedr._serialization.models import Side
from schedr.outcomes import Outcome
from schedr.types import ProcessResult

_SCALARS: dict[object, ScalarName] = {
    bool: "bool",
    int: "int",
    float: "float",
    str: "str",
    datetime: "datetime",
    date: "date",
    timedelta: "timedelta",
}

_HINTS: dict[object, str] = {
    set: "use list",
    frozenset: "use list",
    bytes: "encode it, e.g. base64 in a str",
    object: "use a concrete type, or Any to skip the check",
}


def describe(annotation: object, side: Side = "output") -> TypeDesc:
    """The type descriptor of an output or input type.

    Args:
        annotation: A resolved annotation.
        side: An output's type or a parameter's: a Pydantic model is described by its
            serialization or its validation schema (decision 59).

    Returns:
        Its descriptor.

    Raises:
        TypeModelError: For an unsupported type or part of a type.
    """
    return _describe(annotation, frozenset(), side)


def _describe(annotation: object, seen: frozenset[type], side: Side) -> TypeDesc:  # noqa: PLR0911, PLR0912
    annotation = unwrap(annotation)
    if models.is_model(annotation):
        assert isinstance(annotation, type)
        return ModelType(annotation.__name__, models.schema(annotation, side))
    if annotation is Any:
        return ScalarType("any")
    if annotation is None:
        return ScalarType("none")
    if isinstance(annotation, type) and issubclass(annotation, Outcome):
        # An outcome is stored by its member's name.
        return ScalarType("str")
    if annotation is ProcessResult:
        return ScalarType("process_result")
    if annotation in _SCALARS:
        return ScalarType(_SCALARS[annotation])
    origin, args = get_origin(annotation), get_args(annotation)
    if is_union(annotation):
        return UnionType([_describe(a, seen, side) for a in args])
    if origin is Literal:
        return _literal(annotation, args)
    if origin in SEQUENCE_ORIGINS or annotation in SEQUENCE_ORIGINS:
        return ListType(_describe(args[0], seen, side) if args else ScalarType("any"))
    if annotation is tuple:
        raise TypeModelError("`tuple` needs element types, e.g. tuple[int, str]")
    if origin is tuple:
        return _tuple(annotation, args, seen, side)
    if origin in MAPPING_ORIGINS or annotation in MAPPING_ORIGINS:
        return _dict(annotation, args, seen, side)
    if isinstance(annotation, type) and is_typeddict(annotation):
        return _typed_dict(annotation, seen, side)
    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        return _dataclass(annotation, seen, side)
    hint = _HINTS.get(origin or annotation)
    suffix = f" ({hint})" if hint else ""
    raise TypeModelError(f"`{label(annotation)}` is not supported{suffix}")


def _literal(annotation: object, args: tuple[Any, ...]) -> TypeDesc:
    for value in args:
        if not (value is None or isinstance(value, str | int | bool)):
            raise TypeModelError(
                f"`{label(annotation)}`: literal values must be str, int, bool or None"
            )
    return LiteralType(list(args))


def _tuple(
    annotation: object, args: tuple[Any, ...], seen: frozenset[type], side: Side
) -> TypeDesc:
    if len(args) == 2 and args[1] is Ellipsis:  # noqa: PLR2004 - tuple[T, ...]
        raise TypeModelError(f"`{label(annotation)}` isn't supported; use list[...] instead")
    return TupleType([_describe(a, seen, side) for a in tuple_items(args)])


def _dict(annotation: object, args: tuple[Any, ...], seen: frozenset[type], side: Side) -> TypeDesc:
    if not args:
        return DictType(ScalarType("any"))
    if unwrap(args[0]) is not str:
        raise TypeModelError(f"`{label(annotation)}`: output keys must be `str`")
    return DictType(_describe(args[1], seen, side))


def _fields(cls: type, seen: frozenset[type], required: set[str] | None, side: Side) -> list[Field]:
    if cls in seen:
        raise TypeModelError(f"`{cls.__name__}` refers to itself; recursive types aren't supported")
    hints = type_hints(cls, f"`{cls.__name__}`")
    names: list[str]
    if required is None:  # a dataclass
        names = []
        required = set()
        for f in dataclasses.fields(cls):
            if not f.init:
                raise TypeModelError(
                    f"`{cls.__name__}.{f.name}`: fields with init=False aren't supported"
                )
            names.append(f.name)
            no_default = f.default is dataclasses.MISSING
            if no_default and f.default_factory is dataclasses.MISSING:
                required.add(f.name)
        init_vars = [n for n, h in hints.items() if isinstance(h, dataclasses.InitVar)]
        if init_vars:
            raise TypeModelError(f"`{cls.__name__}.{init_vars[0]}`: InitVar isn't supported")
    else:
        names = list(hints)
    fields: list[Field] = []
    for name in names:
        try:
            desc = _describe(hints[name], seen | {cls}, side)
        except TypeModelError as e:
            raise TypeModelError(f"`{cls.__name__}.{name}`: {e}") from None
        fields.append(Field(name, desc, required=name in required))
    return fields


def _typed_dict(cls: type, seen: frozenset[type], side: Side) -> TypeDesc:
    required = set(getattr(cls, "__required_keys__", frozenset()))
    return TypedDictType(cls.__name__, _fields(cls, seen, required, side))


def _dataclass(cls: type, seen: frozenset[type], side: Side) -> TypeDesc:
    return DataclassType(cls.__name__, _fields(cls, seen, None, side))
