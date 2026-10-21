"""Timestamps in DAG JSON: always UTC, written with ``Z``.

See PROJECT.md, Times and time zones.
"""

from datetime import UTC, datetime


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
