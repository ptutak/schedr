"""Validation of produced JSON against the committed ``schema/dag.schema.json``."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

SCHEMA_PATH = Path(__file__).parents[5] / "schema" / "dag.schema.json"

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
