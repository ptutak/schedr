"""Type descriptors: the type of a task's output in DAG JSON (``schedr-model``,
``types.rs``)."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal

type JsonScalar = str | int | bool | None
"""A value allowed in a ``Literal[...]`` type."""

ScalarName = Literal[
    "any", "none", "bool", "int", "float", "str", "datetime", "date", "timedelta", "process_result"
]
"""The types without parameters."""


@dataclass(frozen=True, slots=True)
class ScalarType:
    """A type without parameters (``int``, ``str``, ``datetime``, ``ProcessResult``, ...).

    Attributes:
        name: Which type.
    """

    name: ScalarName

    def to_json(self) -> dict[str, Any]:
        """The type as DAG JSON.

        Returns:
            ``{"type": name}``.
        """
        return {"type": self.name}


@dataclass(frozen=True, slots=True)
class ListType:
    """``list[T]``.

    Attributes:
        item: The element type.
    """

    item: TypeDesc

    def to_json(self) -> dict[str, Any]:
        """The type as DAG JSON.

        Returns:
            ``{"type": "list", "item": ...}``.
        """
        return {"type": "list", "item": self.item.to_json()}


@dataclass(frozen=True, slots=True)
class TupleType:
    """``tuple[A, B, ...]``.

    Attributes:
        items: The element types, in order.
    """

    items: Sequence[TypeDesc]

    def to_json(self) -> dict[str, Any]:
        """The type as DAG JSON.

        Returns:
            ``{"type": "tuple", "items": [...]}``.
        """
        return {"type": "tuple", "items": [i.to_json() for i in self.items]}


@dataclass(frozen=True, slots=True)
class DictType:
    """``dict[str, T]``.

    Attributes:
        value: The value type (keys are always strings).
    """

    value: TypeDesc

    def to_json(self) -> dict[str, Any]:
        """The type as DAG JSON.

        Returns:
            ``{"type": "dict", "value": ...}``.
        """
        return {"type": "dict", "value": self.value.to_json()}


@dataclass(frozen=True, slots=True)
class LiteralType:
    """``Literal[...]``.

    Attributes:
        values: The allowed values.
    """

    values: Sequence[JsonScalar]

    def to_json(self) -> dict[str, Any]:
        """The type as DAG JSON.

        Returns:
            ``{"type": "literal", "values": [...]}``.
        """
        return {"type": "literal", "values": list(self.values)}


@dataclass(frozen=True, slots=True)
class UnionType:
    """A union, including ``T | None``.

    Attributes:
        variants: The member types.
    """

    variants: Sequence[TypeDesc]

    def to_json(self) -> dict[str, Any]:
        """The type as DAG JSON.

        Returns:
            ``{"type": "union", "variants": [...]}``.
        """
        return {"type": "union", "variants": [v.to_json() for v in self.variants]}


@dataclass(frozen=True, slots=True)
class Field:
    """A named field of a ``TypedDict`` or dataclass.

    Attributes:
        name: The field or key name.
        type: The field's type.
        required: Whether it must be present (``TypedDict``) or has no default (dataclass).
    """

    name: str
    type: TypeDesc
    required: bool = True

    def to_json(self) -> dict[str, Any]:
        """The field as DAG JSON.

        Returns:
            ``{"name": ..., "type": ..., "required": ...}``.
        """
        return {"name": self.name, "type": self.type.to_json(), "required": self.required}


@dataclass(frozen=True, slots=True)
class TypedDictType:
    """A ``TypedDict``.

    Attributes:
        name: The class name.
        fields: The keys, in declaration order.
    """

    name: str
    fields: Sequence[Field]

    def to_json(self) -> dict[str, Any]:
        """The type as DAG JSON.

        Returns:
            ``{"type": "typed_dict", "name": ..., "fields": [...]}``.
        """
        return {
            "type": "typed_dict",
            "name": self.name,
            "fields": [f.to_json() for f in self.fields],
        }


@dataclass(frozen=True, slots=True)
class DataclassType:
    """A dataclass.

    Attributes:
        name: The class name (also the outcome name in outcome unions).
        fields: The fields, in declaration order.
    """

    name: str
    fields: Sequence[Field]

    def to_json(self) -> dict[str, Any]:
        """The type as DAG JSON.

        Returns:
            ``{"type": "dataclass", "name": ..., "fields": [...]}``.
        """
        return {
            "type": "dataclass",
            "name": self.name,
            "fields": [f.to_json() for f in self.fields],
        }


type TypeDesc = (
    ScalarType
    | ListType
    | TupleType
    | DictType
    | LiteralType
    | UnionType
    | TypedDictType
    | DataclassType
)
"""Any type descriptor."""
