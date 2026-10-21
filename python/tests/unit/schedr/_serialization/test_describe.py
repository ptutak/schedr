from collections.abc import Mapping, Sequence
from dataclasses import InitVar, dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, NotRequired, TypedDict

import pytest

from schedr._model import (
    DataclassType,
    DictType,
    Field,
    ListType,
    LiteralType,
    ScalarType,
    TupleType,
    TypedDictType,
    UnionType,
)
from schedr._serialization.annotations import TypeModelError
from schedr._serialization.describe import describe
from schedr.types import ProcessResult


@dataclass
class Rows:
    rows: list[str]
    source: str = "primary"
    tags: list[str] = field(default_factory=list)


class Row(TypedDict):
    id: int
    note: NotRequired[str]


@dataclass
class Bad:
    tags: set[str]


@dataclass
class NoInit:
    x: int = field(init=False, default=0)


@dataclass
class WithInitVar:
    x: int
    secret: InitVar[str]


@dataclass
class Node:
    child: Node | None


@dataclass
class Wrapper:
    inner: Bad


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (Any, ScalarType("any")),
        (None, ScalarType("none")),
        (bool, ScalarType("bool")),
        (int, ScalarType("int")),
        (float, ScalarType("float")),
        (str, ScalarType("str")),
        (datetime, ScalarType("datetime")),
        (date, ScalarType("date")),
        (timedelta, ScalarType("timedelta")),
        (ProcessResult, ScalarType("process_result")),
        (list[int], ListType(ScalarType("int"))),
        (Sequence[int], ListType(ScalarType("int"))),
        (list, ListType(ScalarType("any"))),
        (tuple[int, str], TupleType([ScalarType("int"), ScalarType("str")])),
        (tuple[()], TupleType([])),
        (dict[str, float], DictType(ScalarType("float"))),
        (Mapping[str, int], DictType(ScalarType("int"))),
        (dict, DictType(ScalarType("any"))),
        (Literal["eu", "us"], LiteralType(["eu", "us"])),
        (int | None, UnionType([ScalarType("int"), ScalarType("none")])),
    ],
)
def test_supported_types(annotation: object, expected: object) -> None:
    assert describe(annotation) == expected


def test_dataclass_fields_with_required_flags() -> None:
    assert describe(Rows) == DataclassType(
        "Rows",
        [
            Field("rows", ListType(ScalarType("str")), required=True),
            Field("source", ScalarType("str"), required=False),
            Field("tags", ListType(ScalarType("str")), required=False),
        ],
    )


def test_typed_dict_fields_with_required_flags() -> None:
    assert describe(Row) == TypedDictType(
        "Row", [Field("id", ScalarType("int"), True), Field("note", ScalarType("str"), False)]
    )


@pytest.mark.parametrize(
    ("annotation", "message"),
    [
        (dict[int, str], r"`dict\[int, str\]`: output keys must be `str`"),
        (set[int], r"`set\[int\]` is not supported \(use list\)"),
        (bytes, r"`bytes` is not supported \(encode it"),
        (object, "use a concrete type, or Any"),
        (Decimal, "`Decimal` is not supported"),
        (tuple[int, ...], r"isn't supported; use list\[\.\.\.\] instead"),
        (tuple, "`tuple` needs element types"),
        (Literal[1.5], "literal values must be str, int, bool or None"),
        (Bad, r"`Bad\.tags`: `set\[str\]` is not supported"),
        (Wrapper, r"`Wrapper\.inner`: `Bad\.tags`: `set\[str\]`"),
        (NoInit, r"`NoInit\.x`: fields with init=False aren't supported"),
        (WithInitVar, r"`WithInitVar\.secret`: InitVar isn't supported"),
        (Node, "`Node` refers to itself; recursive types aren't supported"),
    ],
)
def test_unsupported_types_name_the_part(annotation: object, message: str) -> None:
    with pytest.raises(TypeModelError, match=message):
        describe(annotation)
