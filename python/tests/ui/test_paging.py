"""The Runs and DAGs pages in numbered pages (M5.2j, decision 517 as DN23 corrects it), on a
standalone of their own: 120 DAGs, and 61 runs (one created, 60 copied into its database)."""

from __future__ import annotations

import re
import shutil
import sqlite3
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from playwright.sync_api import Page, expect

from .conftest import _api, _wait, serve

RUNS = 61


@pytest.fixture(scope="module")
def paged(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """The UI's address."""
    work = tmp_path_factory.mktemp("paged")
    (work / "dags").mkdir()
    shutil.copy(Path(__file__).parent / "many_dags.py", work / "dags" / "many_dags.py")
    with serve(work) as (server, log):

        def listed() -> bool:
            return len(_api(server, "GET", "/api/v1/dags")["dags"]) == 120  # type: ignore[index]

        def finished() -> bool:
            runs = _api(server, "GET", "/api/v1/runs")["runs"]  # type: ignore[index]
            return bool(runs) and runs[0]["state"] == "success"  # type: ignore[index]

        _wait("the DAGs listed", listed, log)
        _api(server, "POST", "/api/v1/dags/page_000/runs", {})
        _wait("the run finished", finished, log)
        # Copies of it, triggered before it, ended: listed, nothing left for the master to do;
        # all 61 succeeded.
        with sqlite3.connect(work / "schedr.db", timeout=30) as db:
            columns = [row[1] for row in db.execute("PRAGMA table_info(runs)")]
            (source,) = db.execute("SELECT id FROM runs").fetchone()
            now = datetime.now(UTC)
            for i in range(RUNS - 1):
                at = (now - timedelta(minutes=10 + i)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                given = {
                    "id": f"'{uuid.uuid4()}'",
                    "kind": "'manual'",
                    "state": "'success'",
                    "planned_for": f"'{at}'",
                    "triggered_at": f"'{at}'",
                    "started_at": f"'{at}'",
                    "ended_at": f"'{at}'",
                }
                names = ", ".join(columns)
                chosen = ", ".join(given.get(c, c) for c in columns)
                copy = f"INSERT INTO runs ({names}) SELECT {chosen} FROM runs WHERE id = ?"
                db.execute(copy, (source,))
        yield server


def test_the_dags_page_is_numbered_and_a_filter_starts_again_at_the_first(
    page: Page, paged: str
) -> None:
    page.goto(f"{paged}/?trigger=")
    rows = page.locator("table.dags tbody tr")
    pager = page.locator("nav.pager")
    expect(rows).to_have_count(50)
    expect(pager.locator(".page.current")).to_have_text("1")
    expect(pager.locator("a.page:not(.next)")).to_have_text(["2", "3"])
    pager.get_by_role("link", name="3", exact=True).click()
    expect(page).to_have_url(re.compile(r"page=3"))
    expect(rows).to_have_count(20)
    expect(rows.first.locator("td:first-child a")).to_have_text("page_100")
    expect(pager.locator("a[rel=next]")).to_have_count(0)
    expect(pager.locator("a.page:not(.previous)")).to_have_text(["1", "2"])
    # A filter lists anew, from the first page: page_000 to page_099, two pages.
    page.get_by_label("Filter DAGs by name").fill("page_0")
    expect(page).not_to_have_url(re.compile(r"page=3"))
    expect(pager.locator(".page.current")).to_have_text("1")
    expect(rows).to_have_count(50)
    pager.get_by_role("link", name="Next page").click()
    expect(page).to_have_url(re.compile(r"name=page_0.*page=2"))
    expect(rows).to_have_count(50)
    expect(rows.last.locator("td:first-child a")).to_have_text("page_099")
    expect(page.get_by_label("Filter DAGs by name")).to_have_value("page_0")


def test_the_runs_page_is_numbered_to_its_last(page: Page, paged: str) -> None:
    page.goto(f"{paged}/runs")
    rows = page.locator("table.runs tbody tr")
    pager = page.locator("nav.pager")
    expect(rows).to_have_count(50)
    expect(pager.locator(".page.current")).to_have_text("1")
    expect(page.get_by_text("Older runs")).to_have_count(0)
    pager.get_by_role("link", name="Next page").click()
    expect(page).to_have_url(re.compile(r"page=2"))
    expect(rows).to_have_count(RUNS - 50)
    expect(pager.locator(".page.current")).to_have_text("2")
    expect(pager.locator("a[rel=next]")).to_have_count(0)
    pager.get_by_role("link", name="Previous page").click()
    expect(rows).to_have_count(50)
    # A filter keeps the pages.
    page.goto(f"{paged}/runs?state=success&page=2")
    expect(pager.locator(".page.current")).to_have_text("2")
    expect(rows).to_have_count(RUNS - 50)
    expect(pager.locator("a[rel=prev]")).to_have_attribute("href", re.compile(r"state=success"))
