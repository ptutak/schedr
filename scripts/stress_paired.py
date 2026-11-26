#!/usr/bin/env python3
"""A paired speed comparison: one scenario run on two builds in turn on one runner.

The Stress workflow's paired job (M5.3i) runs a scenario on the base build and the new one
alternately (base, new, base, new, ...) and keeps each run's report as
`DIR/base/<n>/<scenario>.json` and `DIR/new/<n>/<scenario>.json`. Runners differ in speed by
some 10 % between machines; pairs on one machine cancel that out. This prints, per metric, the
base and new medians, the median change of the pairs and its spread (the lowest and highest
pair), and a verdict: slower or faster only when every pair agrees and the median change is past
the threshold; otherwise within the noise.

Usage: stress_paired.py DIR [--threshold PERCENT] (default 5); it prints the page, appends it to
`$GITHUB_STEP_SUMMARY` when that is set, and exits 1 when a headline metric is slower or a run
failed. `--check` runs its own small self-test.
"""

from __future__ import annotations

import json
import math
import os
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path

#: The metrics a verdict is drawn from (the job fails on one of them slower); the others shown.
HEADLINE = [
    "tasks_per_s",
    "run_p50_s",
    "run_p99_s",
    "assigned_to_started_p99_s",
    "round_p99_s",
    "wall_s",
]
#: Higher is better for these; lower for every time (`*_s`).
HIGHER_IS_BETTER = {"tasks_per_s"}
#: A change below this, in percent, is noise whatever the pairs say.
DEFAULT_THRESHOLD = 5.0
#: Times under this many seconds aren't judged: a millisecond's jitter is a large share of them.
SMALLEST_JUDGED_S = 0.005
#: Fewer pairs than this give no verdict.
FEWEST_PAIRS = 2
#: Under this many seconds a time is shown in milliseconds.
MILLISECONDS_BELOW = 0.1


@dataclass(frozen=True)
class Pair:
    """One pair's reports of a scenario: the base build's and the new one's."""

    base: dict[str, object]
    new: dict[str, object]


@dataclass(frozen=True)
class Change:
    """One metric across the pairs."""

    key: str
    base: float
    new: float
    change: float
    low: float
    high: float
    verdict: str


def load(path: Path) -> dict[str, object]:
    """A report, or one standing for a run that left none or an unreadable one."""
    try:
        report = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        return {"scenario": path.stem, "failures": [f"report missing or unreadable: {error}"]}
    return report if isinstance(report, dict) else {"failures": ["report not an object"]}


def pairs(directory: Path) -> dict[str, list[Pair]]:
    """Each scenario's pairs, in run order, from `directory`'s `base/<n>` and `new/<n>`."""
    found: dict[str, list[Pair]] = {}
    runs = sorted(
        (p for p in (directory / "base").glob("*") if p.is_dir() and p.name.isdigit()),
        key=lambda p: int(p.name),
    )
    for run in runs:
        for base in sorted(run.glob("*.json")):
            new = directory / "new" / run.name / base.name
            found.setdefault(base.stem, []).append(Pair(load(base), load(new)))
    return found


def metric(report: dict[str, object], key: str) -> float | None:
    """A report's number for `key`, or none."""
    metrics = report.get("metrics")
    value = metrics.get(key) if isinstance(metrics, dict) else None
    if not isinstance(value, int | float) or isinstance(value, bool) or math.isnan(value):
        return None
    return float(value)


def percent(base: float, new: float) -> float:
    """`new` against `base`, in percent (0 for two zeros)."""
    if base == 0:
        return 0.0 if new == 0 else math.inf
    return (new - base) / base * 100


def worse(key: str, change: float) -> bool:
    """Whether a change of `key` by `change` percent is for the worse."""
    return change < 0 if key in HIGHER_IS_BETTER else change > 0


def compare(key: str, runs: list[Pair], threshold: float) -> Change | None:
    """`key` across `runs`: the medians, the pairs' changes and the verdict.

    None if no pair has it on both sides.
    """
    both = [
        (b, n)
        for pair in runs
        if (b := metric(pair.base, key)) is not None and (n := metric(pair.new, key)) is not None
    ]
    if not both:
        return None
    changes = [percent(b, n) for b, n in both]
    base = statistics.median(b for b, _ in both)
    new = statistics.median(n for _, n in both)
    change = statistics.median(changes)
    if key.endswith("_s") and max(base, new) < SMALLEST_JUDGED_S:
        verdict = "too small to judge"
    elif abs(change) < threshold or len(both) < FEWEST_PAIRS:
        verdict = "within noise"
    elif all(worse(key, c) for c in changes):
        verdict = "slower"
    elif not any(worse(key, c) or c == 0 for c in changes):
        verdict = "faster"
    else:
        verdict = "within noise"
    return Change(key, base, new, change, min(changes), max(changes), verdict)


def shown(value: float, key: str) -> str:
    """A metric's value as people read it."""
    if key == "tasks_per_s":
        return f"{value:.1f}"
    if key.endswith("_s"):
        return f"{value * 1000:.1f} ms" if value < MILLISECONDS_BELOW else f"{value:.2f} s"
    return f"{value:,.3g}"


def signed(value: float) -> str:
    """A change in percent, with its sign."""
    return "∞" if math.isinf(value) else f"{value:+.1f} %"


def section(scenario: str, runs: list[Pair], threshold: float) -> tuple[str, bool]:
    """A scenario's part of the page, and whether it failed.

    It failed when a run did or a headline metric is slower.
    """
    failed_runs = [
        (side, i, str(failures[0]))
        for i, pair in enumerate(runs, 1)
        for side, report in (("base", pair.base), ("new", pair.new))
        if isinstance(failures := report.get("failures"), list) and failures
    ]
    noisy = sum(
        1 for pair in runs for report in (pair.base, pair.new) if report.get("noisy_runner")
    )
    keys = sorted(
        {
            key
            for pair in runs
            for report in (pair.base, pair.new)
            if isinstance(metrics := report.get("metrics"), dict)
            for key in metrics
        }
    )
    changes = [c for key in keys if (c := compare(key, runs, threshold)) is not None]
    headline = [c for c in changes if c.key in HEADLINE]
    slower = [c.key for c in headline if c.verdict == "slower"]
    lines = [f"### `{scenario}`: {len(runs)} pairs", ""]
    if failed_runs:
        lines += [f"- ❌ {side} run {i}: {failure}" for side, i, failure in failed_runs] + [""]
    if noisy:
        lines += [f"- ⚠️ {noisy} runs report a runner stalled on I/O: their timings are rough", ""]
    if slower:
        lines += [f"**Slower:** {', '.join(f'`{k}`' for k in slower)}", ""]
    header = ["| Metric | Base | New | Change | Pairs | Verdict |", "|---|---:|---:|---:|---|---|"]

    def table_row(c: Change) -> str:
        return (
            f"| `{c.key}` | {shown(c.base, c.key)} | {shown(c.new, c.key)} | "
            f"{signed(c.change)} | {signed(c.low)} .. {signed(c.high)} | {c.verdict} |"
        )

    lines += [*header, *(table_row(c) for c in headline), ""]
    rest = [c for c in changes if c.key not in HEADLINE]
    if rest:
        lines += [
            "<details><summary>Every metric</summary>",
            "",
            *header,
            *(table_row(c) for c in rest),
            "",
            "</details>",
            "",
        ]
    return "\n".join(lines), bool(failed_runs or slower)


def page(directory: Path, threshold: float) -> tuple[str, bool]:
    """The whole page for `directory`, and whether any scenario failed."""
    found = pairs(directory)
    if not found:
        return f"## Paired speed\n\nNo pairs in `{directory}`.\n", True
    parts = [section(s, runs, threshold) for s, runs in sorted(found.items())]
    failed = any(f for _, f in parts)
    head = [
        "## Paired speed: the new build against the base, in turn on one runner",
        "",
        f"Change: the median of the pairs' changes; Pairs: the lowest and highest. Slower or "
        f"faster only when every pair agrees and the change passes {threshold:g} %.",
        "",
    ]
    return "\n".join(head + [text for text, _ in parts]), failed


def check() -> None:
    """A self-test: the verdicts, a failed run, the directory's layout."""

    def report(**metrics: float) -> dict[str, object]:
        return {"scenario": "x", "failures": [], "metrics": metrics}

    runs = [
        Pair(report(tasks_per_s=100, run_p99_s=1.0), report(tasks_per_s=90, run_p99_s=1.2)),
        Pair(report(tasks_per_s=104, run_p99_s=1.1), report(tasks_per_s=93, run_p99_s=1.3)),
    ]
    slow = compare("tasks_per_s", runs, 5)
    assert slow is not None, slow
    assert slow.verdict == "slower", slow
    assert (slow.base, slow.new) == (102, 91.5), slow
    later = compare("run_p99_s", runs, 5)
    assert later is not None, later
    assert later.verdict == "slower", later
    mixed = [
        Pair(report(run_p99_s=1.0), report(run_p99_s=1.2)),
        Pair(report(run_p99_s=1.0), report(run_p99_s=0.9)),
    ]
    noise = compare("run_p99_s", mixed, 5)
    assert noise is not None, noise
    assert noise.verdict == "within noise", noise
    fast = [Pair(report(wall_s=10), report(wall_s=8)), Pair(report(wall_s=11), report(wall_s=9))]
    quick = compare("wall_s", fast, 5)
    assert quick is not None, quick
    assert quick.verdict == "faster", quick
    tiny = [Pair(report(lock_wait_round_p50_s=0.0001), report(lock_wait_round_p50_s=0.0003))] * 2
    small = compare("lock_wait_round_p50_s", tiny, 5)
    assert small is not None, small
    assert small.verdict == "too small to judge", small
    assert compare("absent", runs, 5) is None
    text, failed = section("x", runs, 5)
    assert failed, text
    assert "**Slower:** `run_p99_s`, `tasks_per_s`" in text, text
    broken = [Pair(report(wall_s=1), {"failures": ["it broke"]})]
    text, failed = section("x", broken, 5)
    assert failed, text
    assert "❌ new run 1: it broke" in text, text
    assert percent(0, 0) == 0
    assert math.isinf(percent(0, 1))


def main(argv: list[str]) -> int:
    """Print the page for `argv[1]`, or run the self-test with `--check`."""
    args = argv[1:]
    if args == ["--check"]:
        check()
        print("ok")
        return 0
    threshold = DEFAULT_THRESHOLD
    if "--threshold" in args:
        i = args.index("--threshold")
        threshold = float(args[i + 1])
        del args[i : i + 2]
    if len(args) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    text, failed = page(Path(args[0]), threshold)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as out:
            out.write(text)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
