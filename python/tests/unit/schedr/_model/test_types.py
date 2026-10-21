from collections.abc import Callable as _Callable
from typing import Any

import pytest

from schedr._model.types import (
    DataclassType,
    DictType,
    Field,
    ListType,
    LiteralType,
    ScalarType,
    TupleType,
    TypedDictType,
    TypeDesc,
    UnionType,
)

Validate = _Callable[[str, Any], None]

ROWS = DataclassType(
    "Rows",
    [
        Field("rows", ListType(ScalarType("str"))),
        Field("source", ScalarType("str"), required=False),
    ],
)


@pytest.mark.parametrize(
    ("ty", "expected"),
    [
        (ScalarType("any"), {"type": "any"}),
        (ScalarType("process_result"), {"type": "process_result"}),
        (ListType(ScalarType("int")), {"type": "list", "item": {"type": "int"}}),
        (
            TupleType([ScalarType("int"), ScalarType("str")]),
            {"type": "tuple", "items": [{"type": "int"}, {"type": "str"}]},
        ),
        (DictType(ScalarType("float")), {"type": "dict", "value": {"type": "float"}}),
        (
            LiteralType(["eu", 1, None, True]),
            {"type": "literal", "values": ["eu", 1, None, True]},
        ),
        (
            UnionType([ScalarType("int"), ScalarType("none")]),
            {"type": "union", "variants": [{"type": "int"}, {"type": "none"}]},
        ),
        (
            ROWS,
            {
                "type": "dataclass",
                "name": "Rows",
                "fields": [
                    {
                        "name": "rows",
                        "type": {"type": "list", "item": {"type": "str"}},
                        "required": True,
                    },
                    {"name": "source", "type": {"type": "str"}, "required": False},
                ],
            },
        ),
        (
            TypedDictType("Row", [Field("id", ScalarType("int"))]),
            {
                "type": "typed_dict",
                "name": "Row",
                "fields": [{"name": "id", "type": {"type": "int"}, "required": True}],
            },
        ),
    ],
)
def test_types_serialize_to_the_schema(
    ty: TypeDesc, expected: dict[str, Any], validate: Validate
) -> None:
    assert ty.to_json() == expected
    validate("TypeDesc", ty.to_json())


@pytest.mark.parametrize(
    "name",
    [
        "any",
        "none",
        "bool",
        "int",
        "float",
        "str",
        "datetime",
        "date",
        "timedelta",
        "process_result",
    ],
)
def test_every_scalar_name_is_in_the_schema(name: Any, validate: Validate) -> None:
    validate("TypeDesc", ScalarType(name).to_json())


def test_field_serializes_with_its_type(validate: Validate) -> None:
    field = Field("id", ScalarType("int"), required=False)
    assert field.to_json() == {"name": "id", "type": {"type": "int"}, "required": False}
    validate("Field", field.to_json())
