"""A DAG's schedule in DAG JSON (``schedr-model``, ``schedule.rs``)."""

from dataclasses import dataclass
from typing import Any


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


type Schedule = NoSchedule | CronSchedule | EverySchedule
"""Any schedule."""
