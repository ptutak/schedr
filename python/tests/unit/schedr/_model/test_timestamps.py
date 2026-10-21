from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from schedr._model.timestamps import format_timestamp


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
