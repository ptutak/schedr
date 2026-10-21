import re

import pytest

from schedr._model.ids import MAX_ID_LEN, IdError, validate_dag_id, validate_task_id


@pytest.mark.parametrize(
    "value", ["sync_customers", "etl-repo:team_a/schedr.toml#report_daily", "a.b-c_D9"]
)
def test_valid_dag_ids_are_returned(value: str) -> None:
    assert validate_dag_id(value) == value


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("", "DAG ID is empty"),
        ("with space", "DAG ID `with space` contains ` `"),
        ("ünicode", "contains `ü`"),
        ("a" * (MAX_ID_LEN + 1), f"is longer than {MAX_ID_LEN} bytes"),
    ],
)
def test_invalid_dag_ids_are_rejected(value: str, message: str) -> None:
    with pytest.raises(IdError, match=message):
        validate_dag_id(value)


def test_the_length_limit_counts_utf8_bytes() -> None:
    assert validate_dag_id("a" * MAX_ID_LEN)
    with pytest.raises(IdError, match="longer than"):
        validate_task_id("é" * 126)


@pytest.mark.parametrize("value", ["extract", "group.extract", "outer.inner.load-2"])
def test_valid_task_ids_are_returned(value: str) -> None:
    assert validate_task_id(value) == value


@pytest.mark.parametrize("value", [".extract", "extract.", "group..extract"])
def test_task_ids_with_empty_parts_are_rejected(value: str) -> None:
    with pytest.raises(IdError, match="empty group or task name"):
        validate_task_id(value)


@pytest.mark.parametrize("value", ["a:b", "a/b", "a#b"])
def test_task_ids_reject_dag_only_characters(value: str) -> None:
    with pytest.raises(IdError, match=re.escape("allowed are ASCII letters, digits and `_ . -`")):
        validate_task_id(value)


def test_empty_task_id_is_rejected() -> None:
    with pytest.raises(IdError, match="task ID is empty"):
        validate_task_id("")


def test_id_error_is_a_value_error() -> None:
    assert issubclass(IdError, ValueError)
