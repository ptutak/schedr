"""Documentation examples: each DAG's JSON matches the schema, and each docs DAG runs."""

import http.server
import importlib
import json
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from schedr import Event
from schedr._model import registry
from schedr.testing import run_dag

HERE = Path(__file__).parent
ROOT = HERE.parent
SCHEMA = json.loads((ROOT / "schema" / "dag.schema.json").read_text())
VALIDATOR = Draft202012Validator(SCHEMA)
DOC_EXAMPLES = sorted((HERE / "docs").glob("*.py"))
QUOTED = [HERE / "sync.py", HERE / "daily_report.py", *DOC_EXAMPLES]
FOR = datetime(2026, 10, 15, 3, tzinfo=UTC)
# The values a docs DAG's secrets are given (`schedr dags test --secret`).
SECRETS = {
    "async_tasks.py": {"etl/api#token": "s3cret-api-token"},
    "secrets_demo.py": {
        "etl/db#password": "s3cret-db-password",
        "etl/api#token": "s3cret-api-token",
        "tenants/acme#api_key": "s3cret-acme-key",
    },
    "connections.py": {"etl/api#token": "s3cret-api-token"},
    "typed_connections.py": {},
}


# The event a docs DAG's run brings, when a trigger starts its runs.
EVENTS: dict[str, Event[Any]] = {
    "event_triggers.py": Event("a.csv", {"path": "/in/a.csv", "size": 3}, at=FOR),
}


class _RunApi(http.server.BaseHTTPRequestHandler):
    """Stands in for the webserver `sensors.py` asks: its slow task and a wait step succeeded."""

    def do_GET(self) -> None:
        tasks = [
            {"task_id": "slow_task", "state": "success"},
            {"task_id": "sees_slow_task", "state": "success"},
        ]
        body = json.dumps({"tasks": tasks}).encode()
        self.send_response(200)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: object) -> None:
        pass


@pytest.fixture
def run_api() -> Iterator[str]:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _RunApi)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()


def _module_name(path: Path) -> str:
    return ".".join(path.relative_to(HERE).with_suffix("").parts)


def _load(path: Path) -> list:
    # The file's own module name. A second import under an alias registers its classes
    # again, and run_dag decodes outputs with that newer class.
    importlib.import_module(_module_name(path))
    resolved = path.resolve()
    dags = [entry.model for (file, _dag_id), entry in registry._BY_FILE.items() if file == resolved]
    assert dags, f"{path.name} defines no DAG"
    return dags


@pytest.mark.parametrize("path", QUOTED, ids=lambda path: path.name)
def test_example_dag_json_matches_the_schema(path: Path) -> None:
    for dag in _load(path):
        VALIDATOR.validate(dag.to_json())


@pytest.mark.parametrize("path", DOC_EXAMPLES, ids=lambda path: path.name)
def test_doc_example_runs(path: Path, run_api: str) -> None:
    connections = {"schedr_api": {"base_url": run_api}} if path.name == "sensors.py" else None
    for dag in _load(path):
        result = run_dag(
            path,
            dag.id,
            for_=FOR,
            secrets=SECRETS.get(path.name),
            connections=connections,
            event=EVENTS.get(path.name),
        )
        assert result.state == "success", dag.id
        ended = [
            (task.task_id, task.state)
            for task in result.tasks.values()
            if task.state not in {"success", "skipped"}
        ]
        assert ended == [], dag.id
        if path.name == "sensors.py":
            assert result.tasks["sees_slow_task"].output == "Task slow_task success"
            assert result.tasks["sees_absent_task"].outcome == "TimedOut"
            assert result.tasks["full_report"].output == (
                "sees_slow_task: Task slow_task success; sees_absent_task: timed out"
            )
            assert result.tasks["sees_either"].output == "sees_slow_task"
            assert result.tasks["fast_report"].output == "sees_slow_task ended first"
        if path.name == "event_triggers.py":
            assert result.tasks["load"].output == "a.csv: 3 bytes in /in/a.csv"
        if path.name == "async_tasks.py":
            # Ten pages, each the base URL's length and its number.
            assert result.tasks["fetch_all"].output == 10 * len("https://api.example.com") + 45
        if path.name == "secrets_demo.py":
            # Masked when stored: a secret's value never reaches the database.
            assert result.tasks["connect"].output == "postgresql://etl:****@db.example.com/etl"
            assert result.tasks["call_api"].output == "acme: a key of 15 characters, a token of 16"
        if path.name == "typed_connections.py":
            assert result.tasks["bucket_names"].output == ["eu-west/raw", "eu-west/clean"]
            listed = result.tasks["list_buckets"].output
            assert listed.stdout == 'eu-west: ["raw","clean"]\n'  # type: ignore[union-attr]
        if path.name == "connections.py":
            assert result.tasks["fetch_orders"].output == (
                "GET https://api.example.com/orders with a token of 16 characters"
            )
