"""The greenhouse showcase: each DAG run for real, in a folder of the test's own."""

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from schedr import Event
from schedr.testing import run_dag

DAGS = Path(__file__).parent / "dags"
CONFIG = Path(__file__).parent.parent / "standalone.toml"


def _last_fire_time() -> datetime:
    """The last five-minute mark before now: `dags test` takes no `--for` in the future."""
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    return now - timedelta(minutes=now.minute % 5 + 5)


FOR = _last_fire_time()
TOKEN = {"greenhouse/devices#token": "s3cret-device-token"}


@pytest.fixture(autouse=True)
def _config(monkeypatch: pytest.MonkeyPatch) -> None:
    # The pager's local URL allowed; the waits of seconds.
    monkeypatch.setenv("SCHEDR_CONFIG_FILE", str(CONFIG))
    monkeypatch.setenv("GREENHOUSE_FAST", "1")
    # A worker's processes get a clean environment: the variable passed on by name.
    monkeypatch.setenv("SCHEDR_CONFIG__WORKER__PASS_ENV", '["GREENHOUSE_FAST"]')


def _db(tmp_path: Path) -> dict[str, dict[str, object]]:
    return {"readings_db": {"path": str(tmp_path / "greenhouse.db")}}


def _write(inbox: Path, name: str, lines: list[dict[str, object]] | str) -> str:
    inbox.mkdir(parents=True, exist_ok=True)
    path = inbox / name
    text = lines if isinstance(lines, str) else "".join(json.dumps(line) + "\n" for line in lines)
    path.write_text(text)
    return str(path)


def test_ingest_stores_good_files_and_quarantines_a_corrupt_one(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    at = (FOR - timedelta(minutes=2)).isoformat()
    stamp = f"{FOR:%Y-%m-%dT%H%M}"
    files = [
        _write(
            inbox,
            f"{stamp}-a.jsonl",
            [
                {"device": "dev-a1", "at": at, "celsius": 35.5, "humidity": 40.0},
                {"device": "dev-a2", "at": at, "celsius": 24.0, "humidity": 55.0},
            ],
        ),
        _write(
            inbox,
            f"{stamp}-b.jsonl",
            [
                {"device": "dev-b1", "at": at, "celsius": 22.5, "humidity": 60.0},
            ],
        ),
        _write(inbox, f"{stamp}-c.jsonl", "{not json\n"),
    ]
    batch = [Event(Path(f).name, {"path": f}, at=FOR) for f in files]
    result = run_dag(
        DAGS,
        "readings_ingest",
        for_=FOR,
        event=Event(batch[-1].key, batch, at=FOR),
        connections=_db(tmp_path),
    )
    assert result.state == "success", result
    assert result.tasks["summary"].output == "3 readings stored, 1 files quarantined, 1 anomalies"
    assert [a.device for a in result.tasks["detect"].output] == ["dev-a1"]
    rows = (
        sqlite3.connect(tmp_path / "greenhouse.db")
        .execute("select device, zone from readings order by device")
        .fetchall()
    )
    assert rows == [("dev-a1", "zone_a"), ("dev-a2", "zone_a"), ("dev-b1", "zone_b")]
    assert sorted(p.name for p in (tmp_path / "quarantine").iterdir()) == [
        f"{stamp}-c.jsonl",
        f"{stamp}-c.why",
    ]
    assert list(inbox.iterdir()) == [], "stored files leave the inbox"


def test_response_pages_when_no_one_acknowledges() -> None:
    result = run_dag(
        DAGS,
        "anomaly_response",
        for_=FOR,
        event=Event(f"dev-a1@{FOR}", {"device": "dev-a1", "celsius": 35.5}, at=FOR),
    )
    assert result.state == "success", result
    assert result.tasks["diagnose"].output.startswith("dev-a1 ")
    assert result.tasks["escalate"].state == "success"
    assert result.tasks["start_maintenance"].state == "skipped"


@pytest.mark.parametrize(
    ("device", "ran", "skipped"),
    [("dev-a1", "fixed_report", "visit_ticket"), ("dev-b2", "visit_ticket", "fixed_report")],
)
def test_maintenance_routes_by_the_command_s_exit_code(device: str, ran: str, skipped: str) -> None:
    result = run_dag(DAGS, "device_maintenance", for_=FOR, params={"device": device}, secrets=TOKEN)
    assert result.state == "success", result
    assert (result.tasks[ran].state, result.tasks[skipped].state) == ("success", "skipped")
    assert result.tasks["closed"].state == "success"


def test_rollup_summarizes_each_zone_and_opens_the_warm_one_s_vents(tmp_path: Path) -> None:
    db = sqlite3.connect(tmp_path / "greenhouse.db")
    db.execute(
        "create table readings (device text, zone text, at text, celsius real, humidity real,"
        " primary key (device, at))"
    )
    inside = (FOR - timedelta(minutes=2)).isoformat()
    db.executemany(
        "insert into readings values (?, ?, ?, ?, ?)",
        [
            ("dev-a1", "zone_a", inside, 31.0, 40.0),
            ("dev-b1", "zone_b", inside, 22.0, 60.0),
        ],
    )
    db.commit()
    result = run_dag(DAGS, "greenhouse_rollup", for_=FOR, connections=_db(tmp_path))
    assert result.state == "success", result
    assert result.tasks["zone_a.summarize"].output.max_celsius == 31.0
    assert result.tasks["ventilate"].output == ["zone_a"]
    assert result.tasks["all_normal"].state == "skipped"


def test_rollup_of_a_window_without_readings_ends_quiet(tmp_path: Path) -> None:
    result = run_dag(DAGS, "greenhouse_rollup", for_=FOR, connections=_db(tmp_path))
    assert result.state == "success", result
    assert result.tasks["quiet"].output.startswith("no readings from")
    assert result.tasks["zone_a.summarize"].state == "skipped"
