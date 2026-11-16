#!/usr/bin/env python3
"""The stress suite's results as one Markdown table, for the run's page on GitHub.

A row per scenario from `target/stress/<scenario>.json` (crates/schedr/tests/stress/check.rs
writes them), then each scenario's every number folded under its name.

Usage: stress_summary.py [DIR] (default target/stress); it prints the page, and appends it to
`$GITHUB_STEP_SUMMARY` when that is set. `--check` runs its own small self-test.
"""

from __future__ import annotations

import json
import math
import os
import sys
from pathlib import Path

#: The columns after the scenario and its result: a title and the metric shown.
COLUMNS = [
    ("Tasks", "tasks"),
    ("Tasks/s", "tasks_per_s"),
    ("Run p50", "run_p50_s"),
    ("Run p99", "run_p99_s"),
    ("Start p99", "assigned_to_started_p99_s"),
    ("Round p99", "round_p99_s"),
    ("Wall", "wall_s"),
]

#: Under this many seconds a time is shown in milliseconds.
MILLISECONDS_BELOW = 0.1
#: Under this many seconds, two decimals; from it, one.
TWO_DECIMALS_BELOW = 10
#: From this many seconds a time is shown in minutes and seconds.
MINUTES_FROM = 120


def number(value: object, key: str) -> str:
    """A metric as people read it; an empty cell for none.

    Counts whole, rates to one decimal, times as `25.6 ms`, `0.41 s`, `31.8 s` or `2m 05s`.
    """
    if not isinstance(value, int | float) or isinstance(value, bool) or math.isnan(value):
        return ""
    if key == "tasks":
        return f"{value:,.0f}"
    if key == "tasks_per_s":
        return f"{value:.1f}"
    if 0 < value < MILLISECONDS_BELOW:
        return f"{value * 1000:.1f} ms"
    if value >= MINUTES_FROM:
        minutes, seconds = divmod(round(value), 60)
        return f"{minutes}m {seconds:02d}s"
    return f"{value:.2f} s" if value < TWO_DECIMALS_BELOW else f"{value:.1f} s"


def cell(text: str) -> str:
    """Text safe in a Markdown table's cell: no `|`, no line breaks."""
    return " ".join(text.replace("|", "\\|").split())


def row(report: dict[str, object]) -> str:
    """One scenario's line: its name, ✅ or ❌ with its first failure, its numbers."""
    metrics = report.get("metrics")
    metrics = metrics if isinstance(metrics, dict) else {}
    failures = report.get("failures")
    failures = [str(f) for f in failures] if isinstance(failures, list) else []
    if not failures:
        result = "✅"
    else:
        more = f" (+{len(failures) - 1} more)" if len(failures) > 1 else ""
        result = f"❌ {cell(failures[0])}{more}"
    seed = report.get("seed")
    name = f"`{report.get('scenario', '?')}`" + (f" (seed {seed})" if seed is not None else "")
    numbers = [number(metrics.get(key), key) for _, key in COLUMNS]
    return "| " + " | ".join([name, result, *numbers]) + " |"


def every_number(report: dict[str, object]) -> str:
    """A scenario's every metric, folded under its name (`<details>`, which GitHub shows).

    For the scenarios measuring other things than the table's columns (parses, ratios,
    commands); empty without metrics.
    """
    metrics = report.get("metrics")
    if not isinstance(metrics, dict) or not metrics:
        return ""
    lines = [
        f"<details><summary><code>{report.get('scenario', '?')}</code></summary>",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key, value in sorted(metrics.items()):
        if key.endswith("_s") or key in ("tasks", "tasks_per_s"):
            shown = number(value, key)
        elif isinstance(value, float):
            shown = f"{value:,.3g}"
        else:
            shown = str(value)
        lines.append(f"| `{key}` | {cell(shown)} |")
    lines += ["", "</details>"]
    return "\n".join(lines)


def table(directory: Path) -> str:
    """The page for every report in `directory`, in the scenarios' order.

    A line saying none were found when there are none.
    """
    reports = []
    for path in sorted(directory.glob("*.json")):
        try:
            report = json.loads(path.read_text())
        except (OSError, ValueError) as error:
            report = {"scenario": path.stem, "failures": [f"report unreadable: {error}"]}
        if isinstance(report, dict):
            reports.append(report)
    if not reports:
        return f"## Stress results\n\nNo scenario reports in `{directory}`.\n"
    failed = sum(1 for r in reports if r.get("failures"))
    profiles = sorted({str(r["profile"]) for r in reports if r.get("profile")})
    lines = [
        "## Stress results",
        "",
        f"{len(reports)} scenarios, {failed} failed"
        + (f"; profile {', '.join(profiles)}" if profiles else "")
        + ". p50 and p99 over the scenario's runs or attempts.",
        "",
        "| Scenario | Result | " + " | ".join(title for title, _ in COLUMNS) + " |",
        "|---|---|" + "---:|" * len(COLUMNS),
        *(row(r) for r in reports),
        "",
        "### All numbers",
        "",
        *(part for r in reports if (part := every_number(r))),
        "",
    ]
    return "\n".join(lines)


def check() -> None:
    """A self-test: the cells, a failing report, the folded numbers."""
    assert number(2501.0, "tasks") == "2,501"
    assert number(49.888, "tasks_per_s") == "49.9"
    assert number(0.4068, "run_p99_s") == "0.41 s"
    assert number(0.0256, "journal_process_p50_s") == "25.6 ms"
    assert number(0.0, "round_triggers_p50_s") == "0.00 s"
    assert number(31.775, "run_p50_s") == "31.8 s"
    assert number(125.4, "wall_s") == "2m 05s"
    assert number(None, "wall_s") == ""
    assert number(float("nan"), "wall_s") == ""
    assert number(True, "tasks") == ""
    line = row({"scenario": "x", "failures": ["a | b\nc", "d"], "metrics": {"tasks": 3}, "seed": 7})
    assert line.startswith("| `x` (seed 7) | ❌ a \\| b c (+1 more) | 3 |"), line
    assert row({"scenario": "y", "failures": [], "metrics": {}}).startswith("| `y` | ✅ |")
    folded = every_number({"scenario": "z", "metrics": {"round_mean_ratio": 1.04821, "parses": 12}})
    assert "| `parses` | 12 |" in folded, folded
    assert "| `round_mean_ratio` | 1.05 |" in folded, folded
    assert every_number({"scenario": "z", "metrics": {}}) == ""


def main(argv: list[str]) -> int:
    """Print the page for `argv[1]` (or target/stress), or run the self-test with `--check`."""
    if argv[1:] == ["--check"]:
        check()
        print("ok")
        return 0
    directory = Path(argv[1] if len(argv) > 1 else "target/stress")
    text = table(directory)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as out:
            out.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
