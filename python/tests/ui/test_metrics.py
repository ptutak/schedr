"""The Metrics page (M5.1j; decisions 465, 466) in a browser: a day of samples seeded for a
worker with tasks and a master beside the standalone's own, and an hour of attempts (decision
494), then the charts, the table, a process's panel and the ranges.
`SCHEDR_UI_SCREENSHOTS=<dir>` saves the page in light and dark.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from playwright.sync_api import Page, expect

GIB = 1024**3
MIB = 1024**2


def _at(when: datetime) -> str:
    return when.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@pytest.fixture(scope="module")
def seeded(standalone: dict[str, str]) -> dict[str, str]:
    """A day of minute samples of two processes, stored as the leader stores them."""
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    parts = {
        "fork_servers_bytes": 300 * MIB,
        "watcher_hosts_bytes": 100 * MIB,
        "tasks_bytes": 3 * GIB,
        "largest_tasks": [
            {
                "attempt_id": "a1",
                "dag_id": "etl",
                "run_id": "run-1",
                "task_id": "load_orders",
                "bytes": 2 * GIB,
            }
        ],
    }
    rows = []
    # Every minute, as a running process is sampled: one sampled one minute in three counts in
    # a bucket for its share of the deployment's sampled minutes, so the newest bucket's
    # "tasks" moved with where the standalone's own start fell (CI, 2026-11-15: 1.5 GiB).
    for minute in range(1, 24 * 60):
        at = _at(now - timedelta(minutes=minute))
        wave = (minute % 120) * MIB
        worker_resident = 180 * MIB + wave
        whole = worker_resident + 300 * MIB + 100 * MIB + 3 * GIB
        rows.append(
            (
                "seed-worker",
                60,
                at,
                json.dumps({"worker": "w-seed"}),
                worker_resident,
                worker_resident,
                whole,
                whole,
                1,
                1,
                1,
                1,
                10.0,
                0.5,
                0.5,
                whole + 50 * MIB,
                whole + 50 * MIB,
                8 * GIB,
                json.dumps(parts),
                300 * MIB,
                100 * MIB,
                3 * GIB,
            )
        )
        rows.append(
            (
                "seed-master",
                60,
                at,
                json.dumps({"master": "0190a0a0-seed-master"}),
                96 * MIB,
                110 * MIB,
                96 * MIB,
                110 * MIB,
                1,
                1,
                0,
                1,
                5.0,
                0.05,
                0.1,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
        )
    with sqlite3.connect(standalone["db"]) as db:
        db.executemany(
            "INSERT OR IGNORE INTO samples (process_id, step_s, at, roles, resident_avg, "
            "resident_peak, memory_avg, memory_peak, minutes, cpu_minutes, container_minutes, "
            "covered, cpu_seconds, cpu_avg, cpu_peak, "
            "container_avg, container_peak, container_limit, parts, forks_avg, hosts_avg, "
            "tasks_avg) VALUES (" + ", ".join("?" * 22) + ")",
            rows,
        )
        # An hour of attempts, one every two minutes, a tenth failed: their own rows, not
        # tied to a run (the tests' database leaves foreign keys off).
        attempts = []
        for n in range(30):
            assigned = now - timedelta(minutes=2 * n + 1)
            attempts.append(
                (
                    f"seed-attempt-{n:03}",
                    f"seed-instance-{n:03}",
                    "failed" if n % 10 == 0 else "success",
                    _at(assigned),
                    _at(assigned + timedelta(milliseconds=100 + 40 * n)),
                    _at(assigned + timedelta(seconds=30)),
                )
            )
        db.executemany(
            "INSERT OR IGNORE INTO attempts (id, task_instance_id, try_number, status, "
            "assigned_at, started_at, ended_at) VALUES (?, ?, 1, ?, ?, ?, ?)",
            attempts,
        )
    return standalone


def _shot(page: Page, name: str) -> None:
    folder = os.environ.get("SCHEDR_UI_SCREENSHOTS")
    if folder:
        page.screenshot(path=str(Path(folder) / f"{name}.png"), full_page=True)


def test_the_metrics_page_charts_each_process_with_its_tasks(
    page: Page, seeded: dict[str, str]
) -> None:
    page.emulate_media(color_scheme="light")
    page.goto(seeded["url"] + "/metrics")
    expect(page.get_by_role("heading", name="Metrics", exact=True)).to_be_visible()
    # Memory, memory by kind and CPU; attempts ended and their start latency.
    expect(page.locator("svg.chart")).to_have_count(5)
    expect(page.locator("svg.chart path.line")).not_to_have_count(0)
    expect(page.locator("svg.chart path.area")).not_to_have_count(0)
    legend = page.locator(".chart-legend").nth(1)
    expect(legend).to_contain_text("tasks 3.0 GiB")
    # The worker's line is its whole memory: its tasks with it.
    row = page.locator("table.metrics-table tr", has_text="worker w-seed")
    expect(row.locator("td").nth(2)).to_have_text("3.6 GiB")
    expect(row.locator("td").nth(4)).to_have_text("3.0 GiB")
    expect(row).to_contain_text("8.0 GiB")
    _shot(page, "metrics-light")
    # A row opens its panel with the largest task, linking to its run.
    row.get_by_role("link").click()
    panel = page.locator("#process-panel")
    expect(panel).to_contain_text("worker w-seed")
    expect(panel.get_by_role("link", name="etl.load_orders")).to_have_attribute(
        "href", "/d/default/runs/run-1"
    )
    expect(panel.locator("svg.chart path.area")).not_to_have_count(0)
    _shot(page, "metrics-panel-light")
    page.locator("html").evaluate("e => e.setAttribute('data-theme', 'dark')")
    _shot(page, "metrics-panel-dark")
    # The series colours follow the theme.
    stroke = page.evaluate("getComputedStyle(document.querySelector('svg.chart path.line')).stroke")
    assert stroke == "rgb(57, 135, 229)", stroke


@pytest.mark.parametrize("range_", ["1h", "24h", "7d", "30d"])
def test_every_range_renders(page: Page, seeded: dict[str, str], range_: str) -> None:
    page.goto(seeded["url"] + f"/metrics?range={range_}")
    expect(page.locator(".choices a.current").first).to_have_text(
        range_.replace("h", " h").replace("d", " d")
    )
    # Every range charts the seeded day: 7 and 30 days from the rollups the sampler makes of it
    # and the minutes not rolled up yet (Codex's review: these ranges passed with no chart).
    expect(page.locator("svg.chart")).to_have_count(5)
    expect(page.locator("#live")).to_have_attribute("hx-trigger", "every 60s")


def test_the_attempts_ended_and_their_start_latency_are_charted(
    page: Page, seeded: dict[str, str]
) -> None:
    page.emulate_media(color_scheme="light")
    page.goto(seeded["url"] + "/metrics?range=1h")
    flow = page.locator(".metrics-flow")
    expect(flow.get_by_role("heading", name="Attempts ended")).to_be_visible()
    expect(flow.get_by_role("heading", name="Start latency")).to_be_visible()
    # The seeded hour, and whatever the other tests ran beside it.
    keys = flow.locator(".legend .key")
    expect(keys).to_have_count(5)
    succeeded = int(keys.nth(0).inner_text().split()[-1])
    failed = int(keys.nth(1).inner_text().split()[-1])
    assert succeeded >= 27, succeeded
    assert failed >= 3, failed
    expect(keys.nth(4)).to_contain_text("p95, highest")
    expect(flow.locator("svg.chart rect.area")).not_to_have_count(0)
    expect(flow.locator("svg.chart path.line")).not_to_have_count(0)
    _shot(page, "metrics-attempts-light")
    page.locator("html").evaluate("e => e.setAttribute('data-theme', 'dark')")
    _shot(page, "metrics-attempts-dark")
