"""Pydantic models as outputs and inputs (decision 59; ``pip install schedr[pydantic]``).

schedr never imports Pydantic itself: a model is recognised by its base class's module and
name, and Pydantic is used only through the model's own methods, so a DAG without models
never loads it. A model is stored as ``model_dump(mode="json", by_alias=False)`` and read back
with ``model_validate(..., by_name=True)``: Pydantic's own JSON, keyed by attribute names (so
``out.url`` is a path by attribute name, as type checkers read it).
"""

import sys
from typing import Any, Literal

from schedr._serialization.annotations import TypeModelError

type Side = Literal["output", "input"]
"""An output's type (what is written) or a parameter's (what is read)."""

MIN_VERSION = (2, 11)
"""The oldest Pydantic supported: ``model_validate(..., by_name=True)`` came with 2.11."""

_INSTALL = "install `schedr[pydantic]` (Pydantic >=2.11,<3)"


def is_model(annotation: object) -> bool:
    """Whether ``annotation`` is a Pydantic model class (checked without importing Pydantic).

    Args:
        annotation: A resolved annotation.

    Returns:
        True for a subclass of ``pydantic.BaseModel``.
    """
    return isinstance(annotation, type) and any(
        base.__name__ == "BaseModel" and base.__module__.startswith("pydantic")
        for base in annotation.__mro__
    )


def is_model_value(value: object) -> bool:
    """Whether ``value`` is an instance of a Pydantic model.

    Args:
        value: Any value.

    Returns:
        True for a model instance.
    """
    return is_model(type(value))


def check_version() -> None:
    """Refuses a Pydantic schedr doesn't support.

    Raises:
        TypeModelError: For a version outside ``>=2.11,<3``, naming it.
    """
    pydantic = sys.modules.get("pydantic")
    version = str(getattr(pydantic, "VERSION", "0"))
    parts = tuple(int(p) for p in version.split(".")[:2] if p.isdigit())
    if not MIN_VERSION <= parts < (3,):
        raise TypeModelError(f"Pydantic {version} isn't supported for outputs: {_INSTALL}")


def schema(cls: type, side: Side) -> dict[str, Any]:
    """Pydantic's JSON Schema of a model, as an output or an input.

    Args:
        cls: The model class.
        side: ``"output"``: the serialization schema; ``"input"``: the validation schema.

    Returns:
        The schema, keyed by attribute names.

    Raises:
        TypeModelError: If Pydantic is unsupported, or can't make a schema of the model.
    """
    check_version()
    mode = "serialization" if side == "output" else "validation"
    try:
        made: dict[str, Any] = cls.model_json_schema(by_alias=False, mode=mode)  # type: ignore[attr-defined]
    except Exception as e:  # Pydantic's errors for types without a JSON Schema
        raise TypeModelError(
            f"`{cls.__name__}`: Pydantic can't describe it as JSON Schema ({e})"
        ) from None
    return _all_required(made) if side == "output" else made


def _all_required(node: Any) -> Any:
    """Every object's properties marked required.

    ``model_dump`` writes each field, defaults too, but Pydantic's serialization schema leaves
    fields with defaults out of ``required``.
    """
    if isinstance(node, list):
        return [_all_required(n) for n in node]
    if not isinstance(node, dict):
        return node
    copied: dict[str, Any] = {}
    for key, value in node.items():
        if key in {"properties", "$defs"} and isinstance(value, dict):
            # Maps of names to schemas: each value is a schema, the keys are names.
            copied[key] = {name: _all_required(sub) for name, sub in value.items()}
        else:
            copied[key] = _all_required(value)
    if isinstance(node.get("properties"), dict):
        copied["required"] = list(node["properties"])
    return copied


def dump(value: object) -> Any:
    """A model instance as stored: Pydantic's JSON, keyed by attribute names.

    Args:
        value: The model instance.

    Returns:
        JSON-compatible data.
    """
    return value.model_dump(mode="json", by_alias=False)  # type: ignore[attr-defined]


def validate(cls: type, data: Any) -> Any:
    """A model instance from stored data, by Pydantic's own rules.

    Args:
        cls: The receiving model class.
        data: The stored data.

    Returns:
        The instance.

    Raises:
        ValueError: Pydantic's ``ValidationError`` when the data doesn't fit.
    """
    return cls.model_validate(data, by_name=True, by_alias=False)  # type: ignore[attr-defined]


def parse_date(text: str, kind: object) -> Any:
    """A model's date text (``date-time``, ``date``, ``duration``) as a value of ``kind``.

    Pydantic's own parser reads it, as it would in the model: a date written without a time
    zone stays naive (decision 60). Pydantic is loaded whenever a model wrote the text.

    Args:
        text: The text Pydantic wrote.
        kind: ``datetime``, ``date`` or ``timedelta``.

    Returns:
        The value.

    Raises:
        ValueError: If Pydantic isn't loaded, or the text isn't a value of ``kind``.
    """
    pydantic = sys.modules.get("pydantic")
    if pydantic is None:
        raise ValueError(f"reading a model's date needs Pydantic: {_INSTALL}")
    return pydantic.TypeAdapter(kind).validate_python(text)
