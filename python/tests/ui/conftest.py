"""The web UI's browser tests (decision 27A): a `schedr standalone` with the example DAG and
one finished run, its UI driven by Playwright's Chromium.

`make test-ui` runs them: it builds the binary, installs the `ui-test` group and the browser.
`SCHEDR_BIN` names the binary (default: `target/debug/schedr`); the Python running these
tests is the one standalone runs the DAG with.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
DAG_ID = "sync_customers"
PLANNED_FOR = "2026-10-15T01:00:00Z"


def _api(server: str, method: str, path: str, body: object | None = None) -> object:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        f"{server}{path}",
        data=data,
        method=method,
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def _wait(what: str, check: object, log: Path, seconds: float = 120) -> object:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = check()  # type: ignore[operator]
        except OSError:
            value = None
        if value:
            return value
        time.sleep(0.5)
    raise AssertionError(
        f"{what} didn't happen in {seconds} s; the log:\n{log.read_text()[-4000:]}"
    )


@pytest.fixture(scope="session")
def standalone(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, str]]:
    """The UI's address and the run's ID."""
    work = tmp_path_factory.mktemp("standalone")
    dags = work / "dags"
    dags.mkdir()
    shutil.copy(REPO / "examples" / "sync.py", dags / "sync.py")
    binary = os.environ.get("SCHEDR_BIN", str(REPO / "target" / "debug" / "schedr"))
    log = work / "standalone.log"
    env = {
        "PATH": f"{Path(sys.executable).parent}:/usr/bin:/bin",
        "HOME": str(work),
        "TZ": "UTC",
        "SCHEDR_DAG_PATH": str(dags),
        "SCHEDR_CONFIG__DATABASE__URL": f"sqlite://{work}/schedr.db",
        "SCHEDR_CONFIG__WEBSERVER__PORT": "0",
        "SCHEDR_CONFIG__MASTER__GRPC_PORT": "0",
        "SCHEDR_CONFIG__WORKER__LOG_PORT": "0",
        "SCHEDR_CONFIG__OPS__PORT": "0",
        "SCHEDR_CONFIG__WORKER__DATA_DIR": str(work / "data"),
    }
    with log.open("w") as out:
        process = subprocess.Popen(
            [binary, "standalone", "--migrate"], env=env, stdout=out, stderr=subprocess.STDOUT
        )
    try:

        def banner() -> str | None:
            found = re.search(r"API on (http://[0-9.:]+)", log.read_text())
            if process.poll() is not None:
                raise AssertionError(f"standalone exited:\n{log.read_text()[-4000:]}")
            return found.group(1) if found else None

        server = str(_wait("the banner", banner, log))

        def listed() -> bool:
            dags_list = _api(server, "GET", "/api/v1/dags")
            return any(d["dag_id"] == DAG_ID for d in dags_list["dags"])  # type: ignore[index]

        _wait("the DAG listed", listed, log)
        _api(server, "POST", f"/api/v1/dags/{DAG_ID}/unpause")
        command = _api(server, "POST", f"/api/v1/dags/{DAG_ID}/runs", {"planned_for": PLANNED_FOR})
        command_id = command["command_id"]  # type: ignore[index]

        def run_id() -> str | None:
            decided = _api(server, "GET", f"/api/v1/commands/{command_id}")
            result = decided.get("result") or {}  # type: ignore[union-attr]
            return result.get("run_id")  # type: ignore[no-any-return]

        run = str(_wait("the run created", run_id, log))

        def finished() -> bool:
            state = _api(server, "GET", f"/api/v1/runs/{run}")["run"]["state"]  # type: ignore[index]
            if state in ("failed", "cancelled"):
                raise AssertionError(f"the run ended {state}:\n{log.read_text()[-4000:]}")
            return state == "success"  # type: ignore[no-any-return]

        _wait("the run finished", finished, log)
        yield {"url": server, "run": run, "dag": DAG_ID}
    finally:
        process.terminate()
        process.wait(timeout=30)


@pytest.fixture
def trigger(standalone: dict[str, str]) -> Callable[[str], None]:
    """Triggers a run of the DAG for a moment (RFC 3339) through the API."""

    def go(planned_for: str) -> None:
        _api(
            standalone["url"],
            "POST",
            f"/api/v1/dags/{DAG_ID}/runs",
            {"planned_for": planned_for},
        )

    return go
