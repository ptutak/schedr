from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Literal, NotRequired, TypedDict

import pytest

from schedr._serialization.annotations import TypeModelError
from schedr._serialization.codec import (
    CodecError,
    check_size,
    decode,
    decode_any,
    encode,
    encode_any,
    encoded_size,
)
from schedr.types import ProcessResult

MOMENT = datetime(2026, 10, 19, 3, tzinfo=UTC)


@dataclass(frozen=True)
class Customer:
    name: str
    created: datetime
    tags: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Unavailable:
    reason: str


class Row(TypedDict):
    id: int
    note: NotRequired[str]


@pytest.mark.parametrize(
    ("value", "annotation", "stored"),
    [
        (None, None, None),
        (True, bool, True),
        (5, int, 5),
        (1.5, float, 1.5),
        ("x", str, "x"),
        (MOMENT, datetime, {"$datetime": "2026-10-19T03:00:00+00:00"}),
        (date(2026, 10, 19), date, {"$date": "2026-10-19"}),
        (timedelta(hours=1, microseconds=5), timedelta, {"$timedelta": 3_600_000_005}),
        ((1, "a"), tuple[int, str], {"$tuple": [1, "a"]}),
        ((), tuple[()], {"$tuple": []}),
        ([1, 2], list[int], [1, 2]),
        ({"a": 1}, dict[str, int], {"a": 1}),
        ("eu", Literal["eu", "us"], "eu"),
        (None, int | None, None),
        (3, int | None, 3),
        ({"id": 1}, Row, {"id": 1}),
        ({"id": 1, "note": "n"}, Row, {"id": 1, "note": "n"}),
        (
            Customer("Ann", MOMENT, ["vip"]),
            Customer,
            {"name": "Ann", "created": {"$datetime": "2026-10-19T03:00:00+00:00"}, "tags": ["vip"]},
        ),
        (
            ProcessResult("out", "", 0),
            ProcessResult,
            {
                "stdout": "out",
                "stderr": "",
                "exit_code": 0,
                "stdout_truncated": False,
                "stderr_truncated": False,
            },
        ),
        ([1, "a", None], list[Any], [1, "a", None]),
    ],
)
def test_round_trips(value: object, annotation: object, stored: object) -> None:
    assert encode(value, annotation) == stored
    assert decode(stored, annotation) == value


def test_int_is_accepted_for_float_and_decoded_as_float() -> None:
    assert encode(3, float) == 3
    decoded = decode(3, float)
    assert decoded == 3.0
    assert isinstance(decoded, float)


def test_datetime_keeps_its_offset() -> None:
    moment = datetime(2026, 10, 19, 5, tzinfo=timezone(timedelta(hours=2)))
    assert decode(encode(moment, datetime), datetime) == moment


@pytest.mark.parametrize(
    ("value", "annotation", "at", "reason"),
    [
        (True, int, "", "expected int, got bool"),
        ("5", int, "", "expected int, got str"),
        (1, bool, "", "expected bool, got int"),
        (float("nan"), float, "", "can't be stored (NaN and infinity"),
        (1, str, "", "expected str, got int"),
        (1, None, "", "expected None, got int"),
        (datetime(2026, 1, 1), datetime, "", "naive datetime"),
        ("2026", datetime, "", "expected datetime, got str"),
        (MOMENT, date, "", "expected date, got datetime"),
        (5, timedelta, "", "expected timedelta, got int"),
        ((1,), list[int], "", "expected a list, got tuple"),
        ([1, "a"], list[int], "[1]", "expected int, got str"),
        ((1,), tuple[int, str], "", "expected a tuple of 2"),
        ({1: "a"}, dict[str, str], "", "dict keys must be str"),
        ([1], dict[str, str], "", "expected a dict, got list"),
        ("de", Literal["eu", "us"], "", "'de' is not one of ['eu', 'us']"),
        (True, Literal[1], "", "True is not one of [1]"),
        ("x", int | None, "", "str matches none of int | None"),
        ({"note": "n"}, Row, "", "Row: missing key 'id'"),
        ({"id": 1, "x": 2}, Row, "", "Row: unknown key 'x'"),
        ([], Row, "", "expected Row (a dict), got list"),
        (Unavailable("x"), Customer, "", "expected Customer, got Unavailable"),
        (Customer("a", datetime(2026, 1, 1)), Customer, ".created", "naive datetime"),
    ],
)
def test_encode_errors_name_the_location(
    value: object, annotation: object, at: str, reason: str
) -> None:
    with pytest.raises(CodecError) as info:
        encode(value, annotation)
    assert info.value.at == at
    assert reason in info.value.reason
    assert str(info.value) == f"at `{at or '.'}`: {info.value.reason}"


def test_encode_rejects_unsupported_annotations() -> None:
    with pytest.raises(TypeModelError, match="`Decimal` is not supported"):
        encode(Decimal(1), Decimal)


@pytest.mark.parametrize(
    ("value", "stored"),
    [
        (None, None),
        (True, True),
        (5, 5),
        (1.5, 1.5),
        ("s", "s"),
        (MOMENT, {"$datetime": "2026-10-19T03:00:00+00:00"}),
        (date(2026, 1, 2), {"$date": "2026-01-02"}),
        (timedelta(seconds=1), {"$timedelta": 1_000_000}),
        ([1, (2, 3)], [1, {"$tuple": [2, 3]}]),
        ({"a": [MOMENT]}, {"a": [{"$datetime": "2026-10-19T03:00:00+00:00"}]}),
        (Unavailable("down"), {"reason": "down"}),
    ],
)
def test_encode_any_uses_run_time_types(value: object, stored: object) -> None:
    assert encode_any(value) == stored
    assert encode(value, Any) == stored


@pytest.mark.parametrize(
    ("value", "at", "reason"),
    [
        (Decimal("1.5"), "", "Decimal is not supported"),
        ([{1, 2}], "[0]", "set is not supported"),
        ({"a": float("inf")}, "['a']", "can't be stored"),
        (object, "", "type is not supported"),
    ],
)
def test_encode_any_errors(value: object, at: str, reason: str) -> None:
    with pytest.raises(CodecError) as info:
        encode_any(value)
    assert info.value.at == at
    assert reason in info.value.reason


@pytest.mark.parametrize(
    ("stored", "value"),
    [
        ([1, {"$tuple": [2]}], [1, (2,)]),
        ({"$datetime": "2026-10-19T03:00:00+00:00"}, MOMENT),
        ({"$date": "2026-01-02"}, date(2026, 1, 2)),
        ({"$timedelta": 5}, timedelta(microseconds=5)),
        ({"reason": "down"}, {"reason": "down"}),
        ({"$datetime": "x", "other": 1}, {"$datetime": "x", "other": 1}),
        ("s", "s"),
    ],
)
def test_decode_any_restores_tagged_types(stored: object, value: object) -> None:
    assert decode_any(stored) == value
    assert decode(stored, Any) == value


@pytest.mark.parametrize(
    ("stored", "annotation", "reason"),
    [
        ({"$datetime": "soon"}, datetime, "bad $datetime value 'soon'"),
        ({"$date": 5}, date, "bad $date value 5"),
        ({"$timedelta": "1s"}, timedelta, "a $timedelta holds whole microseconds"),
        ({"$timedelta": True}, timedelta, "a $timedelta holds whole microseconds"),
        ("2026-01-01", date, "expected a $date value, got str"),
        ([1, 2], tuple[int, int], "expected a $tuple value, got list"),
        ({"$tuple": [1]}, tuple[int, int], "expected a tuple of 2"),
        ({"a": 1}, list[int], "expected a list, got dict"),
        ([], dict[str, int], "expected a dict, got list"),
        ("x", None, "expected None, got str"),
        ("x", int | None, "the stored value matches none of"),
        ([], Customer, "expected Customer (an object), got list"),
        (
            {"name": "a", "created": {"$datetime": "2026-01-01T00:00:00+00:00"}, "age": 3},
            Customer,
            "unknown field 'age' (the class changed?)",
        ),
        (
            {"created": {"$datetime": "2026-01-01T00:00:00+00:00"}},
            Customer,
            "missing 1 required positional argument",
        ),
        ({"id": 1, "x": 1}, Row, "Row: unknown key 'x'"),
    ],
)
def test_decode_errors(stored: object, annotation: object, reason: str) -> None:
    with pytest.raises(CodecError) as info:
        decode(stored, annotation)
    assert reason in info.value.reason


def test_decode_rejects_unsupported_annotations() -> None:
    with pytest.raises(TypeModelError, match="`set` is not supported"):
        decode([], set)


def test_decode_follows_the_receiving_annotation_for_unions() -> None:
    assert decode({"reason": "down"}, Customer | Unavailable) == Unavailable("down")


def test_encoded_size_counts_compact_utf8() -> None:
    assert encoded_size({"a": [1, 2]}) == len('{"a":[1,2]}')
    assert encoded_size("é") == 4  # the two quotes and two bytes


def test_check_size() -> None:
    check_size("x" * 10, 12)
    with pytest.raises(CodecError, match="output is 13 bytes, over the limit of 12 bytes"):
        check_size("x" * 11, 12)
