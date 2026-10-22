"""Triggers: what starts runs of a DAG, ``Dag(trigger=...)``.

``Cron`` fires at the times a cron expression describes, in a time zone (default UTC);
``Every`` fires at a fixed interval measured in real time; ``Once`` fires once, at a given
moment. A DAG without a trigger (``trigger=None``) has manual runs only. Event triggers
(``@trigger``) come with milestone M4. See PROJECT.md, Native API.
"""

import dataclasses
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from schedr._build import to_ms
from schedr._model import CronTrigger, EveryTrigger, OnceTrigger, parse_moment
from schedr._model import Trigger as TriggerModel

PRESETS = frozenset(
    {"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly"}
)
"""The cron presets accepted instead of an expression."""


class Trigger:
    """The base of the trigger classes; not for subclassing outside schedr."""

    __slots__ = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Refuses subclasses outside schedr.

        Raises:
            TypeError: For a subclass defined outside the ``schedr`` package.
        """
        if not cls.__module__.startswith("schedr."):
            raise TypeError(
                f"custom trigger classes are not supported (`{cls.__name__}`): use Cron, "
                "Every or Once (event triggers come with milestone M4)"
            )
        super().__init_subclass__(**kwargs)

    def to_model(self) -> TriggerModel:
        """The trigger in DAG JSON.

        Raises:
            NotImplementedError: Always; every trigger class defines it.
        """
        raise NotImplementedError

    def override(self, **changes: Any) -> Self:
        """The same trigger with some options changed, e.g. ``DAILY.override(start=...)``.

        Args:
            **changes: The options to change, by name (as the class takes them).

        Returns:
            A new trigger.

        Raises:
            TypeError: For an option the trigger doesn't have.
            ValueError: For a value its class rejects.
        """
        if not dataclasses.is_dataclass(self):
            raise TypeError(f"`{type(self).__name__}` has no options to override")
        names = {f.name for f in dataclasses.fields(self)}
        unknown = sorted(set(changes) - names)
        if unknown:
            raise TypeError(
                f"`{type(self).__name__}` has no option `{unknown[0]}` "
                f"(options: {', '.join(sorted(names))})"
            )
        return dataclasses.replace(self, **changes)


Moment = datetime | date | str | None
"""A ``start`` / ``end``: aware datetime, or a naive one / a date / an ISO string, read in the
trigger's time zone."""


@dataclass(frozen=True, slots=True)
class Cron(Trigger):
    """A cron trigger, e.g. ``Cron("0 3 * * *", tz="Europe/Warsaw")``.

    Attributes:
        expression: 5 fields (minute, hour, day of month, month, day of week), 6 with seconds
            first, or a preset such as ``@daily``.
        tz: The IANA time zone the expression is evaluated in, and naive ``start`` / ``end``
            are read in.
        catchup: Whether missed fire times get runs in order (default: only the most recent).
        start: The first run is for the first fire time at or after it; without it, from
            when the master first stored the DAG.
        end: The last run is for the last fire time at or before it.
    """

    expression: str
    tz: str = "UTC"
    catchup: bool = False
    start: Moment = None
    end: Moment = None

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
        _check_zone(self.tz)
        _window(self.start, self.end, self.tz)

    def to_model(self) -> CronTrigger:
        """The trigger in DAG JSON.

        Returns:
            The cron trigger.
        """
        start, end = _window(self.start, self.end, self.tz)
        return CronTrigger(self.expression.strip(), self.tz, self.catchup, start, end)


@dataclass(frozen=True, slots=True)
class Every(Trigger):
    """A fixed interval in real time, e.g. ``Every(timedelta(hours=1))``.

    Daylight-saving changes don't affect it.

    Attributes:
        interval: The time between runs; positive, a whole number of milliseconds.
        catchup: Whether missed fire times get runs in order (default: only the most recent).
        start: The first run is for the first fire time at or after it, and fire times are
            whole intervals from it (from the Unix epoch without it, so ``timedelta(hours=1)``
            fires on the hour); naive values are UTC.
        end: The last run is for the last fire time at or before it.
    """

    interval: timedelta
    catchup: bool = False
    start: Moment = None
    end: Moment = None

    def __post_init__(self) -> None:
        """Checks the interval.

        Raises:
            TypeError: If it isn't a ``timedelta``.
            ValueError: If it isn't positive or has a fraction of a millisecond.
        """
        if to_ms(self.interval, "interval") == 0:
            raise ValueError("interval must be positive")
        _window(self.start, self.end, "UTC")

    def to_model(self) -> EveryTrigger:
        """The trigger in DAG JSON.

        Returns:
            The interval trigger.
        """
        start, end = _window(self.start, self.end, "UTC")
        return EveryTrigger(to_ms(self.interval, "interval"), self.catchup, start, end)


@dataclass(frozen=True, slots=True)
class Once(Trigger):
    """A single fire time, e.g. ``Once("2026-12-24T23:00", tz="Europe/Warsaw")``.

    The way to run a DAG once at a future time: an ordinary trigger, so ``end`` and pausing
    work as for any other. It has no previous fire time, so a DAG whose tasks use
    ``dag_ctx.run.since`` must set ``initial_since``.

    Attributes:
        at: The fire time: an aware datetime, or a naive one / a date / an ISO string, read in
            ``tz``.
        tz: The IANA time zone of a naive ``at`` (and of the DAG's naive ``initial_since``).
    """

    at: datetime | date | str
    tz: str = "UTC"

    def __post_init__(self) -> None:
        """Checks the time zone and reads the fire time.

        Raises:
            ValueError: For an unknown time zone or a malformed string.
            TypeError: For a fire time of another type.
        """
        _check_zone(self.tz)
        parse_moment(self.at, self.tz, "at")

    def to_model(self) -> OnceTrigger:
        """The trigger in DAG JSON.

        Returns:
            The single fire time.
        """
        return OnceTrigger(parse_moment(self.at, self.tz, "at"))


def _window(start: Moment, end: Moment, tz: str) -> tuple[datetime | None, datetime | None]:
    first = None if start is None else parse_moment(start, tz, "start")
    last = None if end is None else parse_moment(end, tz, "end")
    if first is not None and last is not None and first > last:
        raise ValueError(f"start ({first.isoformat()}) is after end ({last.isoformat()})")
    return first, last


def _check_zone(tz: str) -> None:
    try:
        ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise ValueError(f"unknown time zone `{tz}`") from e


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
