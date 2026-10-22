from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from schedr._model import CronTrigger, EveryTrigger, OnceTrigger
from schedr.triggers import (
    DAILY,
    HOURLY,
    MONTHLY,
    PRESETS,
    WEEKLY,
    YEARLY,
    Cron,
    Every,
    Once,
    Trigger,
)


@pytest.mark.parametrize("expression", ["0 3 * * *", "0 0 3 * * *", "  @daily ", "*/15 * * * *"])
def test_cron_accepts_5_or_6_fields_and_presets(expression: str) -> None:
    assert Cron(expression).to_model() == CronTrigger(expression.strip(), "UTC")


def test_cron_keeps_its_time_zone() -> None:
    assert Cron("0 3 * * *", tz="Europe/Warsaw").to_model() == CronTrigger(
        "0 3 * * *", "Europe/Warsaw"
    )


@pytest.mark.parametrize(
    ("expression", "message"),
    [
        ("0 3 * *", "must have 5 fields"),
        ("0 0 0 3 * * *", "must have 5 fields"),
        ("@sometimes", "unknown cron preset `@sometimes`"),
    ],
)
def test_cron_rejects_malformed_expressions(expression: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        Cron(expression)


@pytest.mark.parametrize("tz", ["Mars/Olympus", "", "../etc"])
def test_cron_rejects_unknown_time_zones(tz: str) -> None:
    with pytest.raises(ValueError, match="unknown time zone"):
        Cron("@daily", tz=tz)


def test_cron_carries_catchup() -> None:
    assert Cron("@daily", catchup=True).to_model() == CronTrigger("@daily", "UTC", catchup=True)


def test_every_converts_to_milliseconds() -> None:
    assert Every(timedelta(hours=1)).to_model() == EveryTrigger(3_600_000)
    assert Every(timedelta(hours=1), catchup=True).to_model() == EveryTrigger(3_600_000, True)


@pytest.mark.parametrize(
    ("interval", "error"),
    [
        (timedelta(0), ValueError),
        (timedelta(seconds=-5), ValueError),
        (timedelta(microseconds=1), ValueError),
        (60, TypeError),
    ],
)
def test_every_rejects_bad_intervals(interval: object, error: type[Exception]) -> None:
    with pytest.raises(error):
        Every(interval)  # type: ignore[arg-type]


def test_presets_are_utc_cron_presets() -> None:
    assert [p.expression for p in (HOURLY, DAILY, WEEKLY, MONTHLY, YEARLY)] == [
        "@hourly",
        "@daily",
        "@weekly",
        "@monthly",
        "@yearly",
    ]
    assert {p.tz for p in (HOURLY, DAILY, WEEKLY, MONTHLY, YEARLY)} == {"UTC"}
    assert {"@annually", "@midnight"} <= PRESETS


def test_once_reads_a_naive_time_in_its_zone() -> None:
    once = Once("2026-12-24T23:00", tz="Europe/Warsaw")
    expected = datetime(2026, 12, 24, 23, tzinfo=ZoneInfo("Europe/Warsaw"))
    assert once.to_model() == OnceTrigger(expected)
    assert Once(datetime(2026, 12, 24, tzinfo=UTC)).to_model().at == datetime(
        2026, 12, 24, tzinfo=UTC
    )


def test_once_rejects_bad_values() -> None:
    with pytest.raises(ValueError, match="unknown time zone `Mars/Base`"):
        Once("2026-12-24", tz="Mars/Base")
    with pytest.raises(ValueError, match="at: `tonight` is not an ISO 8601"):
        Once("tonight")


def test_triggers_share_a_base_class() -> None:
    assert all(
        isinstance(t, Trigger) for t in (DAILY, Every(timedelta(hours=1)), Once("2026-12-24"))
    )
    with pytest.raises(NotImplementedError):
        Trigger().to_model()


def test_trigger_classes_cant_be_subclassed_outside_schedr() -> None:
    with pytest.raises(TypeError, match="custom trigger classes are not supported \\(`Mine`\\)"):

        class Mine(Trigger):
            pass
