"""A DAG's schedule in DAG JSON (``schedr-model``, ``schedule.rs``)."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from schedr._model.timestamps import format_timestamp


@dataclass(frozen=True, slots=True)
class NoSchedule:
    """No scheduled runs: manual runs only."""

    def to_json(self) -> dict[str, Any]:
        """The schedule as DAG JSON.

        Returns:
            ``{"kind": "none"}``.
        """
        return {"kind": "none"}


@dataclass(frozen=True, slots=True)
class CronSchedule:
    """A cron expression evaluated in a time zone.

    Attributes:
        expression: 5 or 6 fields, or a preset such as ``@daily``.
        timezone: An IANA time zone name, e.g. ``Europe/Warsaw``.
    """

    expression: str
    timezone: str = "UTC"

    def to_json(self) -> dict[str, Any]:
        """The schedule as DAG JSON.

        Returns:
            ``{"kind": "cron", "expression": ..., "timezone": ...}``.
        """
        return {"kind": "cron", "expression": self.expression, "timezone": self.timezone}


@dataclass(frozen=True, slots=True)
class EverySchedule:
    """A fixed interval measured in real time (UTC).

    Attributes:
        interval_ms: The interval in milliseconds; must be positive.
    """

    interval_ms: int

    def __post_init__(self) -> None:
        """Checks that the interval is positive.

        Raises:
            ValueError: If it isn't.
        """
        if self.interval_ms <= 0:
            raise ValueError(f"interval must be positive, got {self.interval_ms} ms")

    def to_json(self) -> dict[str, Any]:
        """The schedule as DAG JSON.

        Returns:
            ``{"kind": "every", "interval_ms": ...}``.
        """
        return {"kind": "every", "interval_ms": self.interval_ms}


@dataclass(frozen=True, slots=True)
class OnceSchedule:
    """A single fire time.

    Attributes:
        at: The fire time (aware).
    """

    at: datetime

    def to_json(self) -> dict[str, Any]:
        """The schedule as DAG JSON.

        Returns:
            ``{"kind": "once", "at": ...}`` in UTC.
        """
        return {"kind": "once", "at": format_timestamp(self.at)}


type Schedule = NoSchedule | CronSchedule | EverySchedule | OnceSchedule
"""Any schedule."""
