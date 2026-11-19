#!/usr/bin/env python3
r"""The master's round trace as a table: each timed point's count, mean, median, p99 and total.

The trace is `SCHEDR_HIDDEN__MASTER__ROUND_TRACE`'s file, one `<part>/<point>\t<nanoseconds>`
line per measurement (crates/schedr-db/src/trace.rs; docs/03-guide/19-masters-round.md).
Times are shown in milliseconds, totals in seconds; points are grouped by part, in the order
they were first seen.

Usage: round_trace.py FILE; `--check` runs its own small self-test.
"""

from __future__ import annotations

import statistics
import sys
from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class Point:
    """One point's measurements, in milliseconds."""

    name: str
    count: int
    mean: float
    median: float
    p99: float
    total: float


def quantile(ordered: list[float], q: float) -> float:
    """The `q`-quantile of `ordered` (sorted), by nearest rank.

    The smallest value with at least a `q` share of the values at or below it.
    """
    if not ordered:
        return 0.0
    rank = max(1, -(-int(q * len(ordered) * 1000) // 1000))
    return ordered[min(rank, len(ordered)) - 1]


def points(lines: Iterable[str]) -> list[Point]:
    """Each name's measurements from trace lines, grouped by part in first-seen order.

    A line that doesn't read is skipped.
    """
    values: dict[str, list[float]] = {}
    for line in lines:
        name, _, nanos = line.rstrip("\n").partition("\t")
        if not name or not nanos.isdigit():
            continue
        values.setdefault(name, []).append(int(nanos) / 1e6)
    parts: dict[str, int] = {}
    for name in values:
        parts.setdefault(name.split("/")[0], len(parts))
    names = sorted(values, key=lambda n: parts[n.split("/")[0]])
    result = []
    for name in names:
        ordered = sorted(values[name])
        result.append(
            Point(
                name,
                len(ordered),
                statistics.fmean(ordered),
                quantile(ordered, 0.5),
                quantile(ordered, 0.99),
                sum(ordered),
            )
        )
    return result


def table(found: list[Point]) -> str:
    """The points as a Markdown table."""
    rows = [
        "| Point | Count | Mean ms | Median ms | p99 ms | Total s |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    rows.extend(
        f"| `{p.name}` | {p.count} | {p.mean:.3f} | {p.median:.3f} | {p.p99:.3f}"
        f" | {p.total / 1000:.3f} |"
        for p in found
    )
    return "\n".join(rows)


def check() -> None:
    """A small self-test."""
    four = [1.0, 2.0, 3.0, 4.0]
    assert quantile(four, 0.5) == four[1]
    assert quantile(four, 0.99) == four[-1]
    assert not quantile([], 0.5)
    found = points(["b/x\t2000000", "a/y\t1000000", "b/x\t4000000", "junk", "a/z\tno"])
    assert [p.name for p in found] == ["b/x", "a/y"], found
    assert (found[0].count, found[0].mean, found[0].total) == (2, 3.0, 6.0)


def main(argv: list[str]) -> int:
    """Prints the table of the trace named in `argv`."""
    if argv[1:] == ["--check"]:
        check()
        return 0
    if len(argv) != len(["round_trace.py", "FILE"]):
        print(__doc__, file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as lines:
        print(table(points(lines)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
