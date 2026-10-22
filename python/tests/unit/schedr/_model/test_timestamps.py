from datetime import UTC, date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from schedr._model.timestamps import format_optional_timestamp, format_timestamp, parse_moment


@pytest.mark.parametrize(
    ("moment", "expected"),
    [
        (datetime(2026, 10, 15, 3, tzinfo=UTC), "2026-10-15T03:00:00Z"),
        (datetime(2026, 10, 15, 5, tzinfo=ZoneInfo("Europe/Warsaw")), "2026-10-15T03:00:00Z"),
        (datetime(2026, 10, 14, 23, tzinfo=timezone(timedelta(hours=-4))), "2026-10-15T03:00:00Z"),
        (datetime(2026, 10, 15, 3, 0, 0, 250_000, tzinfo=UTC), "2026-10-15T03:00:00.250000Z"),
    ],
)
def test_formats_in_utc_with_z(moment: datetime, expected: str) -> None:
    assert format_timestamp(moment) == expected


def test_rejects_naive_datetimes() -> None:
    with pytest.raises(ValueError, match="naive datetime 2026-10-15T03:00:00: give it a time zone"):
        format_timestamp(datetime(2026, 10, 15, 3))


WARSAW = ZoneInfo("Europe/Warsaw")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (datetime(2026, 1, 2, 3, tzinfo=UTC), datetime(2026, 1, 2, 3, tzinfo=UTC)),
        (datetime(2026, 1, 2, 3), datetime(2026, 1, 2, 3, tzinfo=WARSAW)),
        (date(2026, 1, 2), datetime(2026, 1, 2, tzinfo=WARSAW)),
        ("2026-01-02T03:00", datetime(2026, 1, 2, 3, tzinfo=WARSAW)),
        ("2026-01-02T03:00+00:00", datetime(2026, 1, 2, 3, tzinfo=UTC)),
    ],
)
def test_parse_moment_reads_naive_values_in_the_zone(value: object, expected: datetime) -> None:
    assert parse_moment(value, "Europe/Warsaw", "start") == expected  # type: ignore[arg-type]


def test_parse_moment_rejects_other_values() -> None:
    with pytest.raises(ValueError, match="start: `soon` is not an ISO 8601 date or date-time"):
        parse_moment("soon", "UTC", "start")
    with pytest.raises(TypeError, match="start must be a datetime, a date or an ISO string"):
        parse_moment(3, "UTC", "start")  # type: ignore[arg-type]


def test_an_optional_timestamp_is_formatted_or_none() -> None:
    assert format_optional_timestamp(None) is None
    assert format_optional_timestamp(datetime(2026, 1, 1, tzinfo=UTC)) == "2026-01-01T00:00:00Z"
