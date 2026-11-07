"""A DAG's trigger in DAG JSON (``schedr-model``, ``trigger.rs``)."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from schedr._model.tasks import Task
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


@dataclass(frozen=True, slots=True)
class EventTrigger:
    """A ``@trigger`` function: its events start runs.

    Attributes:
        task: Its function, as a task of kind ``sensor`` that isn't in the graph.
        restart_if_idle_ms: Started again from its bookmark when no event came for this long.
        start: It runs from this moment on (aware).
        end: It stops at this moment (aware).
    """

    task: Task
    restart_if_idle_ms: int | None = None
    start: datetime | None = None
    end: datetime | None = None

    def to_json(self) -> dict[str, Any]:
        """The trigger as DAG JSON.

        Returns:
            ``{"kind": "event", "task": {...}, "start": ..., "end": ...}``, and
            ``restart_if_idle_ms`` when it is set.
        """
        result: dict[str, Any] = {
            "kind": "event",
            "task": self.task.to_json(),
            "start": format_optional_timestamp(self.start),
            "end": format_optional_timestamp(self.end),
        }
        if self.restart_if_idle_ms is not None:
            result["restart_if_idle_ms"] = self.restart_if_idle_ms
        return result


@dataclass(frozen=True, slots=True)
class RunOfTrigger:
    """``trigger=RunOf(...)``: a run per run of another DAG that succeeded (decision 353).

    Attributes:
        dag: That DAG.
        task: Its task; ``None``: the whole run.
        lag_ms: The new run is planned this long after that one; ``None``: when it is seen.
    """

    dag: str
    task: str | None = None
    lag_ms: int | None = None

    def to_json(self) -> dict[str, Any]:
        """The trigger as DAG JSON.

        Returns:
            ``{"kind": "run_of", "run_of": {"dag": ...}}``, with what is set.
        """
        run_of: dict[str, Any] = {"dag": self.dag}
        if self.task is not None:
            run_of["task"] = self.task
        if self.lag_ms is not None:
            run_of["lag_ms"] = self.lag_ms
        return {"kind": "run_of", "run_of": run_of}


type Trigger = NoTrigger | CronTrigger | EveryTrigger | OnceTrigger | EventTrigger | RunOfTrigger
"""Any trigger."""
