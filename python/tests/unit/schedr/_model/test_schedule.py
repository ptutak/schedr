from collections.abc import Callable as _Callable
from datetime import UTC, datetime
from typing import Any

import pytest

from schedr._model.schedule import (
    CronSchedule,
    EverySchedule,
    NoSchedule,
    OnceSchedule,
    Schedule,
)

Validate = _Callable[[str, Any], None]


@pytest.mark.parametrize(
    ("schedule", "expected"),
    [
        (NoSchedule(), {"kind": "none"}),
        (
            CronSchedule("0 3 * * *", "Europe/Warsaw"),
            {"kind": "cron", "expression": "0 3 * * *", "timezone": "Europe/Warsaw"},
        ),
        (CronSchedule("@daily"), {"kind": "cron", "expression": "@daily", "timezone": "UTC"}),
        (EverySchedule(3_600_000), {"kind": "every", "interval_ms": 3_600_000}),
        (
            OnceSchedule(datetime(2026, 12, 24, 22, tzinfo=UTC)),
            {"kind": "once", "at": "2026-12-24T22:00:00Z"},
        ),
    ],
)
def test_schedules_serialize_to_the_schema(
    schedule: Schedule, expected: dict[str, Any], validate: Validate
) -> None:
    assert schedule.to_json() == expected
    validate("Schedule", schedule.to_json())


@pytest.mark.parametrize("interval_ms", [0, -1])
def test_every_needs_a_positive_interval(interval_ms: int) -> None:
    with pytest.raises(ValueError, match="interval must be positive"):
        EverySchedule(interval_ms)
