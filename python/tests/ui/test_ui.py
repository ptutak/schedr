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
    expect(page.locator("dl.facts dd").first).to_contain_text("2026-10-15 01:00 UTC (10:00 JST)")
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


def test_a_run_is_triggered_from_its_dags_page_and_shown_until_applied(
    page: Page, standalone: dict[str, str]
) -> None:
    dag = standalone["dag"]
    page.goto(f"{standalone['url']}/d/default/dags/{dag}")
    page.get_by_role("button", name="Trigger").first.click()
    dialog = page.locator("dialog.confirm[open]")
    expect(dialog).to_be_visible()
    dialog.locator("input[name=for]").fill("2026-10-17T01:00:00Z")
    expect(dialog.locator(".moment")).to_contain_text("2026-10-17 01:00 UTC")
    dialog.get_by_role("button", name="Trigger").click()
    expect(dialog).to_be_hidden()
    # Sent from the page itself (not refused), then read until the master applied it.
    result = page.locator("#action-results .command").first
    expect(result).to_contain_text("Trigger")
    expect(result).to_have_class(re.compile(r"\bapplied\b"), timeout=15_000)
    result.locator("a").click()
    expect(page.locator("dl.facts dd").first).to_contain_text("2026-10-17 01:00 UTC")


def test_an_open_dialog_survives_a_live_update(
    page: Page, standalone: dict[str, str], trigger: Callable[[str], None]
) -> None:
    dag = standalone["dag"]
    page.goto(f"{standalone['url']}/d/default/dags/{dag}")
    rows = page.locator("table.runs tbody tr")
    expect(rows.first).to_be_visible()
    before = rows.count()
    page.get_by_role("button", name="Pause").first.click()
    dialog = page.locator("dialog.confirm[open]")
    expect(dialog).to_be_visible()
    # A run appears meanwhile: the page waits while the dialog is open...
    trigger("2026-10-18T01:00:00Z")
    page.wait_for_timeout(4_000)
    expect(dialog).to_be_visible()
    assert rows.count() == before
    # ...and reads itself again once it is closed.
    dialog.get_by_role("button", name="Cancel").click()
    expect(
        page.locator("table.runs tbody td", has_text="2026-10-18 01:00 UTC").first
    ).to_be_visible(timeout=15_000)


def test_pausing_a_dag_turns_its_button_into_unpause_without_a_reload(
    page: Page, standalone: dict[str, str]
) -> None:
    page.goto(f"{standalone['url']}/d/default/dags/{standalone['dag']}")
    page.evaluate("window.notReloaded = true")
    for action, then in [("Pause", "Unpause"), ("Unpause", "Pause")]:
        page.locator(".actions > button.action", has_text=action).click()
        page.locator("dialog.confirm[open] button[type=submit]").click()
        expect(page.locator(".actions > button.action", has_text=then)).to_be_visible(
            timeout=15_000
        )
    assert page.evaluate("window.notReloaded") is True


def _set_visibility(page: Page, state: str) -> None:
    """Make the page's document report `state` and tell it, as switching tabs would."""
    page.evaluate(
        """state => {
            Object.defineProperty(document, "visibilityState", {value: state, configurable: true});
            document.dispatchEvent(new Event("visibilitychange"));
        }""",
        state,
    )


def test_hidden_pages_give_their_connections_back_and_reload_when_shown(
    context: BrowserContext, standalone: dict[str, str]
) -> None:
    # Six live pages hold the six connections a browser opens to one host over HTTP/1.1 (B25).
    live = f"{standalone['url']}/d/default/dags/{standalone['dag']}"
    tabs = []
    for _ in range(6):
        tab = context.new_page()
        tab.goto(live)
        expect(tab.locator("body")).to_have_attribute("sse-connect", re.compile("/events"))
        tab.evaluate("schedrHiddenClose = 100")
        tabs.append(tab)
    for tab in tabs:
        _set_visibility(tab, "hidden")
    # In the background they close their streams: another page loads.
    seventh = context.new_page()
    seventh.goto(f"{standalone['url']}/runs", timeout=10_000)
    expect(seventh.locator("table")).to_be_visible()
    # Shown again, a page reloads (its state is in its URL) and follows its changes again.
    tabs[0].evaluate("window.notReloaded = true")
    _set_visibility(tabs[0], "visible")
    expect(tabs[0].locator("body")).to_have_attribute("sse-connect", re.compile("/events"))
    tabs[0].wait_for_function("window.notReloaded === undefined")
