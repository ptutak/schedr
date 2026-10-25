"""Documentation examples: each DAG's JSON matches the schema, and each docs DAG runs."""

import importlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from schedr._model import registry
from schedr.testing import run_dag

HERE = Path(__file__).parent
ROOT = HERE.parent
SCHEMA = json.loads((ROOT / "schema" / "dag.schema.json").read_text())
VALIDATOR = Draft202012Validator(SCHEMA)
DOC_EXAMPLES = sorted((HERE / "docs").glob("*.py"))
QUOTED = [HERE / "sync.py", HERE / "daily_report.py", *DOC_EXAMPLES]
FOR = datetime(2026, 10, 15, 3, tzinfo=UTC)


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
def test_doc_example_runs(path: Path) -> None:
    for dag in _load(path):
        result = run_dag(path, dag.id, for_=FOR)
        assert result.state == "success", dag.id
        failed = [task.task_id for task in result.tasks.values() if task.state != "success"]
        assert failed == [], dag.id
