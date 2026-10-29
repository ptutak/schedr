"""Parse-time checks between type descriptors.

Paths into outputs, and whether an upstream output fits a parameter.
"""

from collections.abc import Sequence

from schedr._model import (
    DataclassType,
    DictType,
    ListType,
    LiteralType,
    ModelType,
    ScalarType,
    TupleType,
    TypedDictType,
    TypeDesc,
    UnionType,
)
from schedr._serialization import schemas

_PROCESS_RESULT_FIELDS: dict[str, TypeDesc] = {
    "stdout": ScalarType("str"),
    "stderr": ScalarType("str"),
    "exit_code": ScalarType("int"),
    "stdout_truncated": ScalarType("bool"),
    "stderr_truncated": ScalarType("bool"),
}


def type_name(desc: TypeDesc) -> str:  # noqa: PLR0911
    """A descriptor written like a Python type, for messages.

    Args:
        desc: The descriptor.

    Returns:
        E.g. ``list[int]``, ``Rows | None``, ``Literal['eu', 'us']``.
    """
    match desc:
        case ScalarType(name="process_result"):
            return "ProcessResult"
        case ScalarType(name="none"):
            return "None"
        case ScalarType(name="any"):
            return "Any"
        case ScalarType(name=name):
            return name
        case ListType(item=item):
            return f"list[{type_name(item)}]"
        case TupleType(items=items):
            return f"tuple[{', '.join(type_name(i) for i in items) or '()'}]"
        case DictType(value=value):
            return f"dict[str, {type_name(value)}]"
        case LiteralType(values=values):
            return f"Literal[{', '.join(repr(v) for v in values)}]"
        case UnionType(variants=variants):
            return " | ".join(type_name(v) for v in variants)
        case TypedDictType(name=name) | DataclassType(name=name) | ModelType(name=name):
            return name


def follow_path(desc: TypeDesc, path: tuple[str | int, ...]) -> TypeDesc | None:
    """The type at ``path`` inside an output of type ``desc``.

    Args:
        desc: The output's type.
        path: Field names / keys and indexes.

    Returns:
        The part's type, or ``None`` if the path doesn't exist in the type. ``Any`` stays
        ``Any``.
    """
    found: TypeDesc | None
    for step in path:
        match desc, step:
            case ScalarType(name="any"), _:
                return desc
            case ModelType(), _:
                found = schemas.part(desc, step)
            case ScalarType(name="process_result"), str():
                found = _PROCESS_RESULT_FIELDS.get(step)
            case DictType(value=value), str():
                found = value
            case TypedDictType(fields=fields) | DataclassType(fields=fields), str():
                found = next((f.type for f in fields if f.name == step), None)
            case ListType(item=item), int():
                found = item
            case TupleType(items=items), int():
                found = items[step] if step < len(items) else None
            case UnionType(variants=variants), _:
                # There in every member (`.on(Rows, Unavailable).source`): any of their types.
                parts = [follow_path(v, (step,)) for v in variants]
                found = None if None in parts else _union_of(parts)
            case _:
                found = None
        if found is None:
            return None
        desc = found
    return desc


def _union_of(types: Sequence[TypeDesc | None]) -> TypeDesc:
    """One type, or the union of the different ones."""
    distinct: list[TypeDesc] = []
    for t in types:
        assert t is not None
        if t not in distinct:
            distinct.append(t)
    return distinct[0] if len(distinct) == 1 else UnionType(tuple(distinct))


def assignment_problem(source: TypeDesc, target: TypeDesc) -> str | None:
    """Why a value of type ``source`` can't be passed where ``target`` is expected.

    Args:
        source: The upstream output's type.
        target: The parameter's type.

    Returns:
        ``None`` when it fits. A Pydantic model on either side gives the schema comparison's
        reason (decision 59); otherwise an empty string (the types alone say why).
    """
    if schemas.contains_model(source) or schemas.contains_model(target):
        return schemas.fit_problem(source, target)
    return None if is_assignable(source, target) else ""


def is_assignable(source: TypeDesc, target: TypeDesc) -> bool:  # noqa: PLR0911
    """Whether a value of type ``source`` can be passed where ``target`` is expected.

    ``int`` fits ``float``; ``bool`` doesn't fit ``int`` (no coercion, as at run time);
    containers are checked element by element; classes match by name and fields. A
    Pydantic model on either side is compared by JSON Schema, strictly (decision 59).

    Args:
        source: The upstream output's type.
        target: The parameter's type.

    Returns:
        True if every value of ``source`` is a valid ``target``.
    """
    if ScalarType("any") in (source, target):
        return True
    if schemas.contains_model(source) or schemas.contains_model(target):
        return schemas.fit_problem(source, target) is None
    if isinstance(source, UnionType):
        return all(is_assignable(v, target) for v in source.variants)
    if isinstance(source, LiteralType):
        if isinstance(target, LiteralType):
            return all(v in target.values for v in source.values)
        return all(is_assignable(_literal_type(v), target) for v in source.values)
    if isinstance(target, UnionType):
        return any(is_assignable(source, v) for v in target.variants)
    match source, target:
        case ScalarType(name="int"), ScalarType(name="float"):
            return True
        case ListType(item=a), ListType(item=b):
            return is_assignable(a, b)
        case TupleType(items=a), TupleType(items=b):
            return len(a) == len(b) and all(is_assignable(x, y) for x, y in zip(a, b, strict=True))
        case DictType(value=a), DictType(value=b):
            return is_assignable(a, b)
        case _:
            return source == target


def _literal_type(value: object) -> TypeDesc:
    if value is None:
        return ScalarType("none")
    if isinstance(value, bool):
        return ScalarType("bool")
    if isinstance(value, int):
        return ScalarType("int")
    return ScalarType("str")
