from typing import Any

import pytest

from schedr._model import (
    DataclassType,
    DictType,
    Field,
    ListType,
    LiteralType,
    ModelType,
    ScalarType,
    TupleType,
    UnionType,
)
from schedr._serialization.schemas import contains_model, fit_problem, from_desc, part


def model(schema: dict[str, Any], name: str = "M") -> ModelType:
    return ModelType(name, schema)


def problem(out: dict[str, Any], in_: dict[str, Any]) -> str | None:
    return fit_problem(model(out), model(in_))


def test_contains_model_looks_inside_containers_and_classes() -> None:
    inner = model({"type": "string"})
    assert contains_model(inner)
    assert contains_model(ListType(inner))
    assert contains_model(UnionType([ScalarType("none"), DictType(inner)]))
    assert contains_model(DataclassType("D", [Field("m", inner)]))
    assert not contains_model(ListType(ScalarType("int")))


def test_from_desc_turns_schedrs_types_into_schema() -> None:
    dc = DataclassType("D", [Field("a", ScalarType("int")), Field("b", ScalarType("str"), False)])
    assert from_desc(dc, output=True)["required"] == ["a", "b"]
    assert from_desc(dc, output=False) == {
        "type": "object",
        "properties": {"a": {"type": "integer"}, "b": {"type": "string"}},
        "required": ["a"],
        "additionalProperties": False,
    }
    assert from_desc(ScalarType("datetime"), output=True) == {"$schedr": "datetime"}
    assert from_desc(LiteralType(["eu", "us"]), output=True) == {"enum": ["eu", "us"]}
    assert from_desc(ScalarType("any"), output=True) == {}
    assert from_desc(TupleType([ScalarType("bool")]), output=True) == {
        "$schedr": "tuple",
        "prefixItems": [{"type": "boolean"}],
    }


def test_part_follows_fields_items_unions_and_refs() -> None:
    schema = {
        "type": "object",
        "properties": {
            "inner": {"$ref": "#/$defs/Inner"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "pair": {"type": "array", "prefixItems": [{"type": "integer"}]},
            "either": {"anyOf": [{"$ref": "#/$defs/Inner"}, {"$ref": "#/$defs/Inner"}]},
            "map": {"type": "object", "additionalProperties": {"type": "number"}},
        },
        "$defs": {"Inner": {"type": "object", "properties": {"x": {"type": "integer"}}}},
    }
    root = model(schema, "Outer")
    inner = part(root, "inner")
    assert inner is not None
    assert inner.name == "Outer.inner"
    x = part(inner, "x")
    assert x is not None
    assert x.schema["type"] == "integer"
    tags = part(root, "tags")
    assert tags is not None
    assert part(tags, 3) == ModelType("Outer.tags[3]", {"type": "string", "$defs": schema["$defs"]})
    pair = part(root, "pair")
    assert pair is not None
    assert part(pair, 1) is None
    either = part(root, "either")
    assert either is not None
    assert part(either, "x") is not None
    mapping = part(root, "map")
    assert mapping is not None
    assert part(mapping, "any_key") is not None
    assert part(root, "missing") is None


@pytest.mark.parametrize(
    ("out", "in_", "expected"),
    [
        # Anything fits a parameter without constraints; an output of Any fits anything.
        ({"type": "integer"}, {}, None),
        ({}, {"type": "integer", "minimum": 0}, None),
        ({"type": "integer"}, {"type": "number"}, None),
        (
            {"type": "number"},
            {"type": "integer"},
            "the output may be a float; the parameter takes `integer`",
        ),
        (
            {"type": ["string", "null"]},
            {"type": "string"},
            "the output may be None; the parameter takes `string`",
        ),
        # Numbers: bounds, exclusive bounds, multiples.
        ({"type": "integer", "minimum": 5}, {"type": "integer", "minimum": 0}, None),
        (
            {"type": "integer", "minimum": 0},
            {"type": "integer", "exclusiveMinimum": 0},
            "the parameter needs exclusiveMinimum 0; the output has minimum 0",
        ),
        ({"type": "integer", "exclusiveMinimum": 0}, {"type": "integer", "minimum": 0}, None),
        (
            {"type": "integer", "maximum": 10},
            {"type": "integer", "maximum": 5},
            "the parameter needs maximum 5; the output has maximum 10",
        ),
        (
            {"type": "integer"},
            {"type": "integer", "maximum": 5},
            "the parameter needs maximum 5; the output has none",
        ),
        ({"type": "number", "multipleOf": 0.5}, {"type": "number", "multipleOf": 0.25}, None),
        (
            {"type": "number", "multipleOf": 0.3},
            {"type": "number", "multipleOf": 0.25},
            "the parameter needs multipleOf 0.25; the output has 0.3",
        ),
        (
            {"type": "integer"},
            {"type": "integer", "multipleOf": 2},
            "the parameter needs multipleOf 2; the output has none",
        ),
        # Strings: formats, lengths.
        ({"type": "string", "format": "uri"}, {"type": "string", "format": "uri"}, None),
        (
            {"type": "string", "format": "email"},
            {"type": "string", "format": "uri"},
            "the parameter needs format `uri`; the output has format `email`",
        ),
        ({"type": "string", "minLength": 2}, {"type": "string", "minLength": 1}, None),
        (
            {"type": "string"},
            {"type": "string", "minLength": 1},
            "the parameter needs minLength 1; the output has none",
        ),
        (
            {"type": "string", "maxLength": 9},
            {"type": "string", "maxLength": 3},
            "the parameter needs maxLength 3; the output has 9",
        ),
        # Enums and literals.
        ({"enum": ["eu"]}, {"enum": ["eu", "us"]}, None),
        ({"enum": ["eu", "asia"]}, {"enum": ["eu", "us"]}, "'asia' isn't one of ['eu', 'us']"),
        ({"const": 3}, {"type": "integer", "minimum": 0}, None),
        ({"const": -1}, {"type": "integer", "minimum": 0}, "-1: outside the parameter's minimum 0"),
        ({"const": 3}, {"type": "integer", "multipleOf": 2}, "3: not a multiple of 2"),
        (
            {"const": "abc"},
            {"type": "string", "maxLength": 2},
            "'abc': its length is outside the parameter's",
        ),
        (
            {"const": "a"},
            {"type": "string", "format": "uri"},
            "'a': the parameter's format or pattern can't be checked on a literal; "
            "make them the same type",
        ),
        ({"const": True}, {"type": "integer"}, "True: the parameter takes `integer`"),
        (
            {"type": "string"},
            {"enum": ["eu"]},
            "the parameter allows only ['eu']; the output may be any `string`",
        ),
        # Arrays.
        (
            {"type": "array", "items": {"type": "integer"}},
            {"type": "array", "items": {"type": "number"}},
            None,
        ),
        (
            {"type": "array", "items": {"type": "string"}},
            {"type": "array", "items": {"type": "integer"}},
            "`[]`: the output may be a str; the parameter takes `integer`",
        ),
        (
            {"type": "array", "items": {}},
            {"type": "array", "minItems": 1},
            "the parameter needs minItems 1; the output has none",
        ),
        (
            {"type": "array"},
            {"type": "array", "uniqueItems": True},
            "the parameter needs unique items; the output doesn't promise them",
        ),
        (
            {"type": "array", "prefixItems": [{"type": "integer"}]},
            {"type": "array", "prefixItems": [{"type": "number"}]},
            None,
        ),
        (
            {"type": "array", "prefixItems": [{"type": "string"}]},
            {"type": "array", "prefixItems": [{"type": "integer"}]},
            "`[0]`: the output may be a str; the parameter takes `integer`",
        ),
        (
            {"type": "array", "items": {}},
            {"type": "array", "prefixItems": [{}]},
            "the parameter takes exactly 1 item(s); the output may have another number",
        ),
        # Objects.
        (
            {"type": "object", "properties": {"a": {"type": "integer"}}, "required": ["a"]},
            {"type": "object", "properties": {"a": {"type": "number"}}, "required": ["a"]},
            None,
        ),
        (
            {"type": "object", "properties": {"a": {"type": "integer"}}},
            {"type": "object", "properties": {"a": {}}, "required": ["a"]},
            "field `a` may be missing in the output; the parameter needs it",
        ),
        (
            {"type": "object", "properties": {"a": {}, "b": {}}, "required": ["a", "b"]},
            {"type": "object", "properties": {"a": {}}, "additionalProperties": False},
            "the output has field `b`, which the parameter doesn't take",
        ),
        (
            {"type": "object", "properties": {"b": {"type": "string"}}, "required": ["b"]},
            {"type": "object", "additionalProperties": {"type": "integer"}},
            "`b`: the output may be a str; the parameter takes `integer`",
        ),
        (
            {"type": "object", "additionalProperties": {"type": "integer"}},
            {"type": "object", "additionalProperties": {"type": "number"}},
            None,
        ),
        (
            {"type": "object", "additionalProperties": True},
            {"type": "object", "additionalProperties": False},
            "the output may have more fields; the parameter takes only its own",
        ),
        (
            {"type": "object", "properties": {"a": {"type": "string"}}, "required": ["a"]},
            {"type": "object", "properties": {"a": {"type": "integer"}}},
            "`a`: the output may be a str; the parameter takes `integer`",
        ),
        # Unions on either side.
        (
            {"anyOf": [{"type": "integer"}, {"type": "null"}]},
            {"anyOf": [{"type": "integer"}, {"type": "null"}, {"type": "string"}]},
            None,
        ),
        (
            {"anyOf": [{"type": "integer"}, {"type": "string"}]},
            {"type": "integer"},
            "the output may be a str; the parameter takes `integer`",
        ),
        (
            {"type": "boolean"},
            {"anyOf": [{"type": "integer"}, {"type": "string"}]},
            "`boolean` fits none of the parameter's `integer` | `string`",
        ),
        # What can't be compared: identical only (59.2).
        ({"allOf": [{"type": "integer"}]}, {"allOf": [{"type": "integer"}]}, None),
        (
            {"allOf": [{"type": "integer"}]},
            {"type": "integer"},
            "`allOf` can't be compared and the two sides differ "
            "(a value of another kind against `integer`); make them the same type",
        ),
        ({"not": {"type": "null"}}, {}, None),
        ({"title": "only annotations"}, {"type": "integer"}, None),
        ({"description": "no type"}, {"type": "integer", "minimum": 1}, None),
        (
            {"minProperties": 1, "type": "object"},
            {"type": "object"},
            "`minProperties` can't be compared and the two sides differ "
            "(`object` (minProperties 1) against `object`); make them the same type",
        ),
        (
            {"type": "object", "properties": {}},
            {"minProperties": 1},
            "`minProperties` can't be compared and the two sides differ "
            "(`object` against a value of another kind); make them the same type",
        ),
        (
            {"format": "x"},
            {"type": "integer"},
            "can't tell the output's type (a value of another kind) fits `integer`",
        ),
    ],
)
def test_fit_rules(out: dict[str, Any], in_: dict[str, Any], expected: str | None) -> None:
    assert problem(out, in_) == expected


def test_recursive_definitions_fit_themselves() -> None:
    tree = {
        "$ref": "#/$defs/Tree",
        "$defs": {
            "Tree": {
                "type": "object",
                "properties": {"kids": {"type": "array", "items": {"$ref": "#/$defs/Tree"}}},
                "required": ["kids"],
            }
        },
    }
    assert problem(tree, tree) is None


def test_a_models_dates_and_schedrs_fit_each_other() -> None:
    stamped = model({"type": "string", "format": "date-time"})
    day = model({"type": "string", "format": "date"})
    span = model({"type": "string", "format": "duration"})
    # Both ways (decision 60): the runner converts when the task receives the value.
    assert fit_problem(stamped, ScalarType("datetime")) is None
    assert fit_problem(ScalarType("datetime"), stamped) is None
    assert fit_problem(day, ScalarType("date")) is None
    assert fit_problem(span, ScalarType("timedelta")) is None
    # Plain text proves nothing about being a date; a date isn't a day.
    assert fit_problem(ScalarType("str"), ScalarType("datetime")) is not None
    assert fit_problem(day, ScalarType("datetime")) == (
        "`string` (format date) is stored differently from schedr's `datetime` (schedr tags "
        "its own tuples and command results; Pydantic writes them as lists and objects): "
        "pass the model whole, or take the part as Pydantic writes it"
    )


def test_tuples_compare_element_by_element() -> None:
    stamped = model({"type": "string", "format": "date-time"})
    assert fit_problem(ScalarType("datetime"), ScalarType("datetime")) is None
    pair = TupleType([ScalarType("int"), stamped])
    assert fit_problem(pair, TupleType([ScalarType("float"), stamped])) is None
    assert fit_problem(pair, TupleType([ScalarType("int")])) == (
        "a tuple of 2 doesn't fit a tuple of 1"
    )
    assert fit_problem(TupleType([stamped]), TupleType([ScalarType("int")])) == (
        "`[0]`: the output may be a str; the parameter takes `integer`"
    )
