"""Shared fixtures: schema validation of produced JSON, and a clean DAG registry per test."""

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

SCHEMA_DIR = Path(__file__).parents[4] / "schema"
SCHEMA_PATH = SCHEMA_DIR / "dag.schema.json"

type Validate = Callable[[str, Any], None]


@pytest.fixture(scope="session")
def dag_schema() -> dict[str, Any]:
    """The committed DAG JSON schema."""
    schema: dict[str, Any] = json.loads(SCHEMA_PATH.read_text())
    return schema


@pytest.fixture(scope="session")
def validate(dag_schema: dict[str, Any]) -> Validate:
    """Validates an instance against the whole schema (``"Dag"``) or one of its definitions."""

    def check(definition: str, instance: Any) -> None:
        if definition == "Dag":
            schema = dag_schema
        else:
            schema = {"$ref": f"#/$defs/{definition}", "$defs": dag_schema["$defs"]}
        Draft202012Validator(schema).validate(instance)

    return check


@pytest.fixture(scope="session")
def validate_runner_message() -> Callable[[Any], None]:
    """Validates one agent <-> runner message against ``schema/runner.schema.json``."""
    schema = json.loads((SCHEMA_DIR / "runner.schema.json").read_text())
    validator = Draft202012Validator(schema)
    return validator.validate


@pytest.fixture(autouse=True)
def clean_registry() -> Iterator[None]:
    """Forgets the DAGs a test defined, so tests don't see each other's DAGs."""
    from schedr._model import registry  # noqa: PLC0415 - imported where it is used

    registry.take_defined()
    yield
    registry.take_defined()
