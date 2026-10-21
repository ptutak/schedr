"""Schedules: when the scheduler creates runs of a DAG.

``Cron`` fires at the times a cron expression describes, in a time zone (default UTC);
``Every`` fires at a fixed interval measured in real time. A DAG without a schedule
(``schedule=None``) has manual runs only. See PROJECT.md, Native API.
"""

from dataclasses import dataclass
from datetime import timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from schedr._build import to_ms
from schedr._model import CronSchedule, EverySchedule

PRESETS = frozenset(
    {"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly"}
)
"""The cron presets accepted instead of an expression."""


@dataclass(frozen=True, slots=True)
class Cron:
    """A cron schedule, e.g. ``Cron("0 3 * * *", tz="Europe/Warsaw")``.

    Attributes:
        expression: 5 fields (minute, hour, day of month, month, day of week), 6 with seconds
            first, or a preset such as ``@daily``.
        tz: The IANA time zone the expression is evaluated in.
    """

    expression: str
    tz: str = "UTC"

    def __post_init__(self) -> None:
        """Checks the expression's shape and the time zone.

        Raises:
            ValueError: For an unknown preset, a wrong number of fields, or an unknown time
                zone.
        """
        text = self.expression.strip()
        if text.startswith("@"):
            if text not in PRESETS:
                raise ValueError(
                    f"unknown cron preset `{text}` (known: {', '.join(sorted(PRESETS))})"
                )
        elif len(text.split()) not in (5, 6):
            raise ValueError(
                f"cron expression `{self.expression}` must have 5 fields (or 6 with seconds)"
            )
        try:
            ZoneInfo(self.tz)
        except (ZoneInfoNotFoundError, ValueError) as e:
            raise ValueError(f"unknown time zone `{self.tz}`") from e

    def to_model(self) -> CronSchedule:
        """The schedule in DAG JSON.

        Returns:
            The cron schedule.
        """
        return CronSchedule(self.expression.strip(), self.tz)


@dataclass(frozen=True, slots=True)
class Every:
    """A fixed interval in real time, e.g. ``Every(timedelta(hours=1))``.

    Daylight-saving changes don't affect it.

    Attributes:
        interval: The time between runs; positive, a whole number of milliseconds.
    """

    interval: timedelta

    def __post_init__(self) -> None:
        """Checks the interval.

        Raises:
            TypeError: If it isn't a ``timedelta``.
            ValueError: If it isn't positive or has a fraction of a millisecond.
        """
        if to_ms(self.interval, "interval") == 0:
            raise ValueError("interval must be positive")

    def to_model(self) -> EverySchedule:
        """The schedule in DAG JSON.

        Returns:
            The interval schedule.
        """
        return EverySchedule(to_ms(self.interval, "interval"))


HOURLY = Cron("@hourly")
"""Every hour, on the hour (UTC)."""

DAILY = Cron("@daily")
"""Every day at midnight (UTC)."""

WEEKLY = Cron("@weekly")
"""Every Sunday at midnight (UTC)."""

MONTHLY = Cron("@monthly")
"""The first day of every month at midnight (UTC)."""

YEARLY = Cron("@yearly")
"""January 1st at midnight (UTC)."""

type Schedule = Cron | Every
"""A schedule for ``Dag(schedule=...)``."""
