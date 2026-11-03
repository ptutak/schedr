"""The UI in a browser: what only a browser can show (27A)."""

from __future__ import annotations

import re
import time
from collections.abc import Callable

from playwright.sync_api import Browser, BrowserContext, Page, Route, expect


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


def test_a_running_log_is_read_part_by_part_without_holding_a_connection(
    page: Page, standalone: dict[str, str]
) -> None:
    # Decision 175: a running try's log is polled, each answer the next part and the element
    # reading the one after it (the tail's answers faked here; the UI's are its unit tests').
    page.goto(f"{standalone['url']}/d/default/runs/{standalone['run']}?task=load&tab=logs")
    log = page.locator("#task-panel pre.log")
    expect(log).to_contain_text("loading 2 customers")
    answers = {
        "from=1": 'step 2\n<span class="log-tail" hx-get="/fake/tail?from=2" '
        'hx-trigger="load delay:0ms, retry delay:1s" hx-target="this" hx-swap="outerHTML"></span>',
        "from=2": '<span class="fg-1">done</span>\n',
    }
    asked: list[str] = []

    def answer(route: Route) -> None:
        part = route.request.url.rsplit("?", 1)[1]
        asked.append(part)
        if part == "from=1" and asked.count(part) == 1:
            route.abort()  # the UI didn't answer: read again a second later
            return
        if part == "from=2" and asked.count(part) == 1:
            # An error answer (a proxy while the UI restarts) is kept out, and read again.
            route.fulfill(status=502, content_type="text/html", body="<h1>502 Bad Gateway</h1>")
            return
        route.fulfill(content_type="text/html", body=answers[part])

    page.route("**/fake/tail?*", answer)
    page.evaluate(
        """() => {
            const log = document.querySelector("#task-panel pre.log");
            log.insertAdjacentHTML("beforeend",
                '<span class="log-tail" hx-get="/fake/tail?from=1" '
                + 'hx-trigger="load delay:0ms, retry delay:1s" hx-target="this" '
                + 'hx-swap="outerHTML"></span>');
            htmx.process(log.lastElementChild);
        }"""
    )
    expect(log).to_contain_text("step 2\ndone\n")
    expect(log.locator(".fg-1")).to_have_text("done")
    expect(log.locator(".log-tail")).to_have_count(0)
    expect(log).not_to_contain_text("Bad Gateway")
    assert asked == ["from=1", "from=1", "from=2", "from=2"]


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
    # (Its stream also says when the page was made: a change after it is sent at once, B144.)
    expect(page.locator("body")).to_have_attribute(
        "data-events", re.compile(rf"^/events\?deployment=default&dag={dag}&since=\d+$")
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


def test_a_run_is_triggered_with_params_from_the_form(
    page: Page, standalone: dict[str, str]
) -> None:
    page.goto(f"{standalone['url']}/d/default/dags/form_params")
    page.get_by_role("button", name="Trigger").first.click()
    dialog = page.locator("dialog.confirm[open]")
    expect(dialog).to_be_visible()
    # Pre-filled with the defaults; the secret one's default never reaches the page.
    expect(dialog.locator("input[name='p:rows']")).to_have_value("10")
    expect(dialog.locator("select[name='p:mode']")).to_have_value('"fast"')
    expect(dialog.locator("input[name='p:api_token']")).to_have_attribute("type", "password")
    expect(dialog.locator("input[name='p:api_token']")).to_have_value("")
    assert "hunter2" not in page.content()
    dialog.locator("input[name='p:region']").fill("us")
    dialog.locator("input[name='p:rows']").fill("25")
    dialog.locator("input[name='none:day']").uncheck()
    dialog.locator("input[name='p:day']").fill("2026-10-20")
    dialog.locator("select[name='p:mode']").select_option('"slow"')
    dialog.get_by_role("button", name="Trigger").click()
    expect(dialog).to_be_hidden()
    result = page.locator("#action-results .command").first
    expect(result).to_have_class(re.compile(r"\bapplied\b"), timeout=15_000)
    result.locator("a").click()
    shown = page.locator("dd.params")
    expect(shown).to_contain_text("region = us")
    expect(shown).to_contain_text("rows = 25")
    expect(shown).to_contain_text("mode = slow")
    expect(shown).to_contain_text("day = 2026-10-20")


def test_a_refused_param_shows_the_masters_reason(page: Page, standalone: dict[str, str]) -> None:
    page.goto(f"{standalone['url']}/d/default/dags/form_params")
    page.get_by_role("button", name="Trigger").first.click()
    dialog = page.locator("dialog.confirm[open]")
    dialog.locator("textarea[name='p:tags']").fill("[1, 2]")
    dialog.get_by_role("button", name="Trigger").click()
    result = page.locator("#action-results .command").first
    expect(result).to_have_class(re.compile(r"\brejected\b"), timeout=15_000)
    expect(result).to_contain_text("tags")


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


def test_a_browsers_live_pages_share_one_stream_and_leave_its_connections_free(
    context: BrowserContext, standalone: dict[str, str], trigger: Callable[[str], None]
) -> None:
    # B31, B32: a browser opens six HTTP/1.1 connections to a host, and each live page held
    # one. Its pages now share one stream (decision 175): eight live pages, and a page still
    # loads at once.
    url, dag = standalone["url"], standalone["dag"]
    tabs = []
    for _ in range(8):
        tab = context.new_page()
        tab.goto(f"{url}/d/default/dags/{dag}")
        expect(tab.locator("table.runs tbody tr").first).to_be_visible()
        tabs.append(tab)
    started = time.monotonic()
    another = context.new_page()
    another.goto(f"{url}/runs", timeout=5_000)
    expect(another.locator("table")).to_be_visible()
    assert time.monotonic() - started < 2, time.monotonic() - started
    # Each of them still hears of a change concerning it.
    trigger("2026-10-19T01:00:00Z")
    for tab in tabs:
        expect(
            tab.locator("table.runs tbody td", has_text="2026-10-19 01:00 UTC").first
        ).to_be_visible(timeout=5_000)
    # The last page of the UI closed, the next opens the stream again.
    for tab in [*tabs, another]:
        tab.close()
    page = context.new_page()
    page.goto(f"{url}/d/default/dags/{dag}")
    expect(page.locator("table.runs tbody tr").first).to_be_visible()
    trigger("2026-10-20T01:00:00Z")
    expect(
        page.locator("table.runs tbody td", has_text="2026-10-20 01:00 UTC").first
    ).to_be_visible(timeout=15_000)


def test_without_shared_workers_hidden_pages_give_their_connections_back(
    browser: Browser, standalone: dict[str, str]
) -> None:
    # Where a browser has no SharedWorker, each page holds its own stream, given back after a
    # while hidden (B25), and reloads when shown again (its state is in its URL).
    context = browser.new_context()
    context.add_init_script("delete window.SharedWorker")
    live = f"{standalone['url']}/d/default/dags/{standalone['dag']}"
    tabs = []
    for _ in range(6):
        tab = context.new_page()
        tab.goto(live)
        expect(tab.locator("table.runs tbody tr").first).to_be_visible()
        tab.evaluate("schedrHiddenClose = 100")
        tabs.append(tab)
    for tab in tabs:
        _set_visibility(tab, "hidden")
    # In the background they close their streams: another page loads.
    seventh = context.new_page()
    seventh.goto(f"{standalone['url']}/runs", timeout=10_000)
    expect(seventh.locator("table")).to_be_visible()
    tabs[0].evaluate("window.notReloaded = true")
    _set_visibility(tabs[0], "visible")
    tabs[0].wait_for_function("window.notReloaded === undefined")
    context.close()


def test_a_live_read_answered_while_a_dialog_is_open_doesnt_replace_it(
    page: Page, standalone: dict[str, str]
) -> None:
    url = f"{standalone['url']}/d/default/dags/{standalone['dag']}"
    page.goto(url)
    held: list[Route] = []

    def hold(route: Route) -> None:
        held.append(route)

    # The page's next live read is held in flight (as on a slow network) ...
    page.route(url, hold)
    page.evaluate("""htmx.trigger(document.getElementById("live"), "live:change")""")
    page.wait_for_function("() => document.querySelector('#live.htmx-request') !== null")
    assert len(held) == 1
    # ... while the user opens an action's dialog; then the read's answer arrives.
    page.locator(".actions > button.action").first.click()
    dialog = page.locator("dialog.confirm[open]")
    expect(dialog).to_be_visible()
    held[0].continue_()
    page.unroute(url)
    page.wait_for_function("() => document.querySelector('#live.htmx-request') === null")
    # The dialog is still the one the user opened, and once closed the part is read again.
    expect(dialog).to_be_visible()
    dialog.get_by_role("button", name="Cancel").click()
    expect(dialog).to_be_hidden()


def test_a_deployment_nodes_panel_opens_on_click_and_stays_open_across_a_refresh(
    page: Page, standalone: dict[str, str]
) -> None:
    # Its master and worker known first (run alone, the page could be made before they were).
    deadline = time.monotonic() + 30
    while True:
        known = page.request.get(standalone["url"] + "/api/v1/deployment").json()
        if known["masters"] and known["workers"]:
            break
        assert time.monotonic() < deadline, f"no master or worker: {known}"
        page.wait_for_timeout(200)
    page.goto(standalone["url"] + "/deployments")
    # Standalone: one process holding the UI, the webserver, the master and the worker.
    expect(page.locator("svg.deployment g.process")).to_have_count(1)
    expect(page.locator("svg.deployment a.node")).to_have_count(4)
    worker = page.locator("svg.deployment a.node.worker")
    expect(worker.locator("text.sub").last).to_have_text("queues: default")
    panel = page.locator("section.node-config.open")
    expect(panel).to_have_count(0)
    worker.click()
    expect(panel.locator(".panel-head h2")).to_contain_text("Worker ")
    # Beside the graph, the clicked node marked, its address in the URL.
    graph = page.locator(".graph-scroll").bounding_box()
    beside = panel.bounding_box()
    assert graph is not None
    assert beside is not None
    assert beside["x"] >= graph["x"] + graph["width"]
    expect(worker).to_have_class(re.compile(r"\bselected\b"))
    assert page.url.endswith("#node.default.worker.localhost")
    # Its configuration as TOML.
    expect(panel.locator("pre.config")).to_contain_text("[worker]")
    # A live refresh replaces the panels: the open one is open again.
    with page.expect_response(re.compile(r"/deployments$")):
        page.evaluate("htmx.trigger(document.getElementById('live'), 'live:change')")
    expect(panel.locator(".panel-head h2")).to_contain_text("Worker ")
    # A refresh leaves where the page is scrolled to alone (it jumped to the open panel).
    page.set_viewport_size({"width": 1200, "height": 300})
    page.evaluate("window.scrollTo(0, 0)")
    with page.expect_response(re.compile(r"/deployments$")):
        page.evaluate("htmx.trigger(document.getElementById('live'), 'live:change')")
    page.wait_for_function("() => document.querySelector('#live.htmx-request') === null")
    page.wait_for_timeout(300)
    assert page.evaluate("window.scrollY") == 0
    expect(panel.locator(".panel-head h2")).to_contain_text("Worker ")
    # The UI's panel: its connection to the webserver, in process.
    page.locator("svg.deployment a.node.ui").click()
    expect(panel.locator("table.connections tbody td").nth(3)).to_have_text("in process")


def test_a_runs_tasks_show_their_ends_soon_after_them(
    page: Page, standalone: dict[str, str]
) -> None:
    # B31: a finished task stayed `scheduled` on the run page for up to 15 s (a change during
    # a read was dropped, B141, until the next refresh).
    url, dag = standalone["url"], standalone["dag"]
    command = page.request.post(
        f"{url}/api/v1/dags/{dag}/runs", data={"planned_for": "2026-10-18T01:00:00Z"}
    ).json()
    deadline = time.monotonic() + 30
    run = None
    while run is None and time.monotonic() < deadline:
        decided = page.request.get(f"{url}/api/v1/commands/{command['command_id']}").json()
        run = (decided.get("result") or {}).get("run_id")
        time.sleep(0.05)
    assert run is not None
    page.goto(f"{url}/d/default/runs/{run}")
    while time.monotonic() < deadline:
        state = page.request.get(f"{url}/api/v1/runs/{run}").json()["run"]["state"]
        if state not in {"running", "queued"}:
            break
        time.sleep(0.05)
    assert state == "success"
    # Shown within a few seconds of the end (the database's changes are read every second),
    # not at the next refresh.
    expect(page.locator("svg.dag rect.box.success")).to_have_count(3, timeout=4_000)
    expect(page.locator("h1 .state")).to_have_text("success", timeout=1_000)
