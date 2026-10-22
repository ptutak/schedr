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
            },
        ),
        (
            CronTrigger("@daily", catchup=True),
            {"kind": "cron", "expression": "@daily", "timezone": "UTC", "catchup": True},
        ),
        (
            EveryTrigger(3_600_000),
            {"kind": "every", "interval_ms": 3_600_000, "catchup": False},
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
