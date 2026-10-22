"""A DAG's trigger in DAG JSON (``schedr-model``, ``trigger.rs``)."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from schedr._model.timestamps import format_optional_timestamp, format_timestamp


@dataclass(frozen=True, slots=True)
class NoTrigger:
    """Nothing starts runs: manual runs only."""

    def to_json(self) -> dict[str, Any]:
        """The trigger as DAG JSON.

        Returns:
            ``{"kind": "none"}``.
        """
        return {"kind": "none"}


@dataclass(frozen=True, slots=True)
class CronTrigger:
    """A cron expression evaluated in a time zone.

    Attributes:
        expression: 5 or 6 fields, or a preset such as ``@daily``.
        timezone: An IANA time zone name, e.g. ``Europe/Warsaw``.
        catchup: Whether missed fire times get runs in order.
        start: The first run is for the first fire time at or after it (aware).
        end: The last run is for the last fire time at or before it (aware).
    """

    expression: str
    timezone: str = "UTC"
    catchup: bool = False
    start: datetime | None = None
    end: datetime | None = None

    def to_json(self) -> dict[str, Any]:
        """The trigger as DAG JSON.

        Returns:
            ``{"kind": "cron", "expression": ..., "timezone": ..., "catchup": ...}`` with
            ``start`` and ``end`` (UTC, or null).
        """
        return {
            "kind": "cron",
            "expression": self.expression,
            "timezone": self.timezone,
            "catchup": self.catchup,
            "start": format_optional_timestamp(self.start),
            "end": format_optional_timestamp(self.end),
        }


@dataclass(frozen=True, slots=True)
class EveryTrigger:
    """A fixed interval measured in real time (UTC).

    Attributes:
        interval_ms: The interval in milliseconds; must be positive.
        catchup: Whether missed fire times get runs in order.
        start: The first run is for the first fire time at or after it, and fire times are
            whole intervals from it (from the Unix epoch without it); aware.
        end: The last run is for the last fire time at or before it (aware).
    """

    interval_ms: int
    catchup: bool = False
    start: datetime | None = None
    end: datetime | None = None

    def __post_init__(self) -> None:
        """Checks that the interval is positive.

        Raises:
            ValueError: If it isn't.
        """
        if self.interval_ms <= 0:
            raise ValueError(f"interval must be positive, got {self.interval_ms} ms")

    def to_json(self) -> dict[str, Any]:
        """The trigger as DAG JSON.

        Returns:
            ``{"kind": "every", "interval_ms": ..., "catchup": ...}`` with ``start`` and
            ``end`` (UTC, or null).
        """
        return {
            "kind": "every",
            "interval_ms": self.interval_ms,
            "catchup": self.catchup,
            "start": format_optional_timestamp(self.start),
            "end": format_optional_timestamp(self.end),
        }


@dataclass(frozen=True, slots=True)
class OnceTrigger:
    """A single fire time.

    Attributes:
        at: The fire time (aware).
    """

    at: datetime

    def to_json(self) -> dict[str, Any]:
        """The trigger as DAG JSON.

        Returns:
            ``{"kind": "once", "at": ...}`` in UTC.
        """
        return {"kind": "once", "at": format_timestamp(self.at)}


type Trigger = NoTrigger | CronTrigger | EveryTrigger | OnceTrigger
"""Any trigger."""
