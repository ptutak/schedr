from collections.abc import Callable as _Callable
from datetime import UTC, datetime
from typing import Any

import pytest

from schedr._model.trigger import (
    CronTrigger,
    EveryTrigger,
    NoTrigger,
    OnceTrigger,
    Trigger,
)

Validate = _Callable[[str, Any], None]


@pytest.mark.parametrize(
    ("trigger", "expected"),
    [
        (NoTrigger(), {"kind": "none"}),
        (
            CronTrigger("0 3 * * *", "Europe/Warsaw"),
            {
                "kind": "cron",
                "expression": "0 3 * * *",
                "timezone": "Europe/Warsaw",
                "catchup": False,
                "start": None,
                "end": None,
            },
        ),
        (
            CronTrigger("@daily", catchup=True, start=datetime(2026, 1, 1, 1, tzinfo=UTC)),
            {
                "kind": "cron",
                "expression": "@daily",
                "timezone": "UTC",
                "catchup": True,
                "start": "2026-01-01T01:00:00Z",
                "end": None,
            },
        ),
        (
            EveryTrigger(3_600_000, end=datetime(2026, 12, 31, tzinfo=UTC)),
            {
                "kind": "every",
                "interval_ms": 3_600_000,
                "catchup": False,
                "start": None,
                "end": "2026-12-31T00:00:00Z",
            },
        ),
        (
            OnceTrigger(datetime(2026, 12, 24, 22, tzinfo=UTC)),
            {"kind": "once", "at": "2026-12-24T22:00:00Z"},
        ),
    ],
)
def test_triggers_serialize_to_the_schema(
    trigger: Trigger, expected: dict[str, Any], validate: Validate
) -> None:
    assert trigger.to_json() == expected
    validate("Trigger", trigger.to_json())


@pytest.mark.parametrize("interval_ms", [0, -1])
def test_every_needs_a_positive_interval(interval_ms: int) -> None:
    with pytest.raises(ValueError, match="interval must be positive"):
        EveryTrigger(interval_ms)
