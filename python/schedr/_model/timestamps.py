"""Timestamps in DAG JSON: always UTC, written with ``Z``.

See PROJECT.md, Times and time zones.
"""

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo


def format_timestamp(moment: datetime) -> str:
    """Formats an aware datetime as RFC 3339 in UTC, e.g. ``2026-10-15T03:00:00Z``.

    Microseconds are kept when present (``2026-10-15T03:00:00.250000Z``).

    Args:
        moment: An aware datetime, in any zone.

    Returns:
        The moment in UTC with a ``Z`` suffix.

    Raises:
        ValueError: If ``moment`` is naive (has no time zone).
    """
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(f"naive datetime {moment.isoformat()}: give it a time zone")
    utc = moment.astimezone(UTC)
    text = utc.strftime("%Y-%m-%dT%H:%M:%S")
    if utc.microsecond:
        text += f".{utc.microsecond:06d}"
    return text + "Z"


def parse_moment(value: datetime | date | str, tz: str, name: str) -> datetime:
    """Reads a moment given in DAG code: naive values (and dates) are in the time zone ``tz``.

    Args:
        value: An aware or naive datetime, a date (midnight), or an ISO 8601 string.
        tz: The IANA time zone of naive values (the trigger's).
        name: The argument's name, for messages.

    Returns:
        An aware datetime.

    Raises:
        TypeError: For another type.
        ValueError: For a string that isn't ISO 8601.
    """
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError as e:
            raise ValueError(f"{name}: `{value}` is not an ISO 8601 date or date-time") from e
    if not isinstance(value, datetime):
        if not isinstance(value, date):
            raise TypeError(f"{name} must be a datetime, a date or an ISO string")
        value = datetime(value.year, value.month, value.day)
    if value.tzinfo is None:
        value = value.replace(tzinfo=ZoneInfo(tz))
    return value
