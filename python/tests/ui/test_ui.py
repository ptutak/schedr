"""The UI in a browser: what only a browser can show (27A)."""

from __future__ import annotations

import re
from collections.abc import Callable

from playwright.sync_api import BrowserContext, Page, expect


def test_the_theme_toggle_switches_and_is_remembered(
    page: Page, standalone: dict[str, str]
) -> None:
    page.emulate_media(color_scheme="light")
    page.goto(standalone["url"] + "/")
    root = page.locator("html")
    # Before a choice: the system's.
    expect(root).to_have_attribute("data-theme", "light")
    page.get_by_role("button", name="Switch between light and dark").click()
    expect(root).to_have_attribute("data-theme", "dark")
    background = page.evaluate("getComputedStyle(document.body).backgroundColor")
    assert background == "rgb(21, 24, 29)"
    # Remembered on the next page, whatever the system prefers.
    page.goto(standalone["url"] + "/runs")
    expect(root).to_have_attribute("data-theme", "dark")


def test_times_show_in_the_browsers_zone_once_it_is_known(
    context: BrowserContext, standalone: dict[str, str]
) -> None:
    # A fresh context in a zone of its own: the first visit sets the cookie and reloads once.
    browser = context.browser
    assert browser is not None
    zoned = browser.new_context(timezone_id="Asia/Tokyo")
    page = zoned.new_page()
    page.goto(f"{standalone['url']}/d/default/runs/{standalone['run']}")
    expect(page.locator("dl.facts dd").first).to_contain_text("2026-10-15 01:00 UTC · 10:00 JST")
    expect(page.locator("footer.note")).to_have_count(0)
    cookies = {c["name"]: c["value"] for c in zoned.cookies()}
    assert cookies["schedr_tz"] == "Asia%2FTokyo"
    zoned.close()


def test_a_tasks_panel_opens_beside_the_graph_without_leaving_the_page(
    page: Page, standalone: dict[str, str]
) -> None:
    page.goto(f"{standalone['url']}/d/default/runs/{standalone['run']}")
    expect(page.locator("h1 .state")).to_have_text("success")
    page.evaluate("window.notReloaded = true")
    page.locator('svg.dag a.node[data-task="load"]').click()
    panel = page.locator("#task-panel")
    expect(panel.locator(".panel-head h2")).to_have_text("load")
    expect(page).to_have_url(re.compile(r"\?task=load$"))
    # The graph marks it.
    expect(page.locator("svg.dag a.node.selected")).to_have_attribute("data-task", "load")
    # Swapped in by htmx: the page wasn't loaded again.
    assert page.evaluate("window.notReloaded") is True
    panel.locator('.tabs a[data-tab="logs"]').click()
    expect(panel.locator("pre.log")).to_contain_text("loading 2 customers")
    panel.locator('.tabs a[data-tab="output"]').click()
    expect(panel.locator("pre.output")).to_have_text("2")
    assert page.evaluate("window.notReloaded") is True


def test_the_runs_list_shows_the_run_and_links_it(page: Page, standalone: dict[str, str]) -> None:
    page.goto(standalone["url"] + "/runs")
    # Unpaused, the DAG has its scheduled runs too: the one triggered is among them.
    link = page.locator(f'table.runs tbody td:first-child a[href$="/runs/{standalone["run"]}"]')
    expect(link).to_have_text(standalone["run"])
    link.click()
    expect(page.locator("svg.dag a.node")).to_have_count(3)


def test_a_new_run_appears_on_its_dags_page_without_a_reload(
    page: Page, standalone: dict[str, str], trigger: Callable[[str], None]
) -> None:
    dag = standalone["dag"]
    page.goto(f"{standalone['url']}/d/default/dags/{dag}")
    rows = page.locator("table.runs tbody tr")
    expect(rows.first).to_be_visible()
    before = rows.count()
    page.evaluate("window.notReloaded = true")
    # Live: the page follows its DAG's changes.
    expect(page.locator("body")).to_have_attribute(
        "sse-connect", f"/events?deployment=default&dag={dag}"
    )
    trigger("2026-10-16T01:00:00Z")
    new_row = page.locator("table.runs tbody td", has_text="2026-10-16 01:00 UTC").first
    expect(new_row).to_be_visible(timeout=15_000)
    assert rows.count() > before
    assert page.evaluate("window.notReloaded") is True
