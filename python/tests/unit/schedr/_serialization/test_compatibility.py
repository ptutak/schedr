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
    TypeDesc,
    UnionType,
)
from schedr._serialization.compatibility import follow_path, is_assignable, type_name

INT, STR, FLOAT, BOOL, NONE, ANY = (
    ScalarType(n) for n in ("int", "str", "float", "bool", "none", "any")
)
ROWS = DataclassType("Rows", [Field("rows", ListType(STR)), Field("n", INT)])
ROW = TypedDictType("Row", [Field("id", INT)])


@pytest.mark.parametrize(
    ("desc", "text"),
    [
        (INT, "int"),
        (NONE, "None"),
        (ANY, "Any"),
        (ScalarType("process_result"), "ProcessResult"),
        (ListType(INT), "list[int]"),
        (TupleType([INT, STR]), "tuple[int, str]"),
        (TupleType([]), "tuple[()]"),
        (DictType(FLOAT), "dict[str, float]"),
        (LiteralType(["eu", 1]), "Literal['eu', 1]"),
        (UnionType([ROWS, NONE]), "Rows | None"),
        (ROW, "Row"),
    ],
)
def test_type_name(desc: TypeDesc, text: str) -> None:
    assert type_name(desc) == text


@pytest.mark.parametrize(
    ("desc", "path", "expected"),
    [
        (ROWS, (), ROWS),
        (ROWS, ("rows",), ListType(STR)),
        (ROWS, ("rows", 0), STR),
        (ROWS, ("missing",), None),
        (ROWS, (0,), None),
        (ROW, ("id",), INT),
        (DictType(INT), ("any_key",), INT),
        (TupleType([INT, STR]), (1,), STR),
        (TupleType([INT, STR]), (2,), None),
        (ListType(INT), ("x",), None),
        (ScalarType("process_result"), ("stdout",), STR),
        (ScalarType("process_result"), ("exit_code",), INT),
        (ScalarType("process_result"), ("nope",), None),
        (ANY, ("a", 0, "b"), ANY),
        (INT, ("a",), None),
    ],
)
def test_follow_path(
    desc: TypeDesc, path: tuple[str | int, ...], expected: TypeDesc | None
) -> None:
    assert follow_path(desc, path) == expected


@pytest.mark.parametrize(
    ("source", "target", "expected"),
    [
        (INT, INT, True),
        (INT, FLOAT, True),
        (FLOAT, INT, False),
        (BOOL, INT, False),
        (STR, INT, False),
        (ANY, INT, True),
        (INT, ANY, True),
        (INT, UnionType([INT, NONE]), True),
        (NONE, UnionType([INT, NONE]), True),
        (UnionType([INT, NONE]), INT, False),
        (UnionType([INT, FLOAT]), FLOAT, True),
        (ListType(INT), ListType(FLOAT), True),
        (ListType(STR), ListType(INT), False),
        (TupleType([INT, STR]), TupleType([FLOAT, STR]), True),
        (TupleType([INT]), TupleType([INT, INT]), False),
        (DictType(INT), DictType(FLOAT), True),
        (LiteralType(["eu", "us"]), STR, True),
        (LiteralType(["eu", 1]), STR, False),
        (LiteralType([True, None]), UnionType([BOOL, NONE]), True),
        (LiteralType(["eu"]), LiteralType(["eu", "us"]), True),
        (LiteralType(["de"]), LiteralType(["eu", "us"]), False),
        (STR, LiteralType(["eu"]), False),
        (ROWS, ROWS, True),
        (ROWS, DataclassType("Rows", []), False),
        (ROW, ROW, True),
    ],
)
def test_is_assignable(source: TypeDesc, target: TypeDesc, expected: bool) -> None:
    assert is_assignable(source, target) is expected
