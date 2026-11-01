"""A DAG's run parameters (``Dag(params=...)``, decisions 133-138), as DAG JSON.

The declared class becomes its type, a JSON Schema and its defaults. A dataclass's JSON
Schema is written here, in the JSON a trigger gives (a date as ``format: date``, a
``timedelta`` as ISO 8601 duration text, a tuple as ``prefixItems``); a Pydantic model's
is Pydantic's own. The master checks a trigger's values against it, then stores them: a
dataclass's as outputs are stored (tagged), a model's as the JSON its schema takes, so a task
decodes them as it decodes an output.
"""

import dataclasses
import json
from typing import Any

from schedr._model import (
    DagParams,
    DataclassType,
    DictType,
    ListType,
    LiteralType,
    ModelType,
    ScalarType,
    TupleType,
    TypedDictType,
    TypeDesc,
    UnionType,
)
from schedr._serialization import models
from schedr._serialization.annotations import TypeModelError, type_hints
from schedr._serialization.codec import CodecError, encode, json_form
from schedr._serialization.describe import describe

MAX_PARAMS_BYTES = 64 * 1024
"""At most this many bytes of a run's parameters as JSON, defaults included (decision 135)."""

_FORMATS = {"datetime": "date-time", "date": "date"}
"""A ``timedelta`` is plain text in the schema: JSON Schema's ``duration`` (RFC 3339) has no
fractional seconds; the master checks it as an ISO 8601 duration when storing it."""


def declare(cls: object, dag_id: str, *, scheduled: bool) -> DagParams:
    """The run parameters a DAG takes, as DAG JSON.

    Args:
        cls: What ``params=`` was given: a dataclass or a Pydantic model class.
        dag_id: The DAG, for messages.
        scheduled: Whether the DAG has a schedule (its runs then use the defaults).

    Returns:
        Their type, JSON Schema and defaults.

    Raises:
        TypeError: For anything but a dataclass or model class, a field of a type outputs
            can't hold, a scheduled DAG's field without a default, or defaults over 64 KiB.
    """
    where = f"DAG `{dag_id}`: params"
    if not isinstance(cls, type) or not (dataclasses.is_dataclass(cls) or models.is_model(cls)):
        raise TypeError(f"{where} must be a dataclass or a Pydantic model class, got {cls!r}")
    try:
        desc = describe(cls, "input")
    except TypeModelError as e:
        raise TypeError(f"{where}: {e}") from None
    if isinstance(desc, ModelType):
        schema = dict(desc.schema)
        defaults = _model_defaults(cls)
    else:
        assert isinstance(desc, DataclassType)
        defs: dict[str, Any] = {}
        try:
            schema = _schema(desc, defs)
        except TypeError as e:
            raise TypeError(f"{where}: {e}") from None
        defaults = _dataclass_defaults(cls, where)
        for name, value in defaults.items():
            schema["properties"][name]["default"] = value
        if defs:
            schema["$defs"] = defs
        schema["title"] = cls.__name__
    required = [n for n in schema.get("required", []) if n not in defaults]
    if scheduled and required:
        raise TypeError(
            f"{where}: `{cls.__name__}.{required[0]}` has no default, but the DAG has a "
            "trigger: its scheduled runs use the defaults; give every field a default"
        )
    size = len(json.dumps(defaults, separators=(",", ":")).encode())
    if size > MAX_PARAMS_BYTES:
        raise TypeError(
            f"{where}: the defaults are {size} bytes as JSON, over the limit of "
            f"{MAX_PARAMS_BYTES // 1024} KiB (they travel with every task)"
        )
    return DagParams(type_desc=desc, schema=schema, defaults=defaults)


def _dataclass_defaults(cls: type, where: str) -> dict[str, Any]:
    hints = type_hints(cls, f"`{cls.__name__}`")
    defaults: dict[str, Any] = {}
    for f in dataclasses.fields(cls):
        if f.default is not dataclasses.MISSING:
            value = f.default
        elif f.default_factory is not dataclasses.MISSING:
            value = f.default_factory()
        else:
            continue
        try:
            defaults[f.name] = json_form(encode(value, hints[f.name], f.name))
        except CodecError:
            # The codec's reason quotes the value, and the type may hold it (a `Literal`): it
            # may be a secret (decision 134).
            raise TypeError(
                f"{where}: the default of `{f.name}` doesn't fit its declared type"
            ) from None
    return defaults


def _model_defaults(cls: type) -> dict[str, Any]:
    """A model's defaults as Pydantic writes them, keyed by attribute names."""
    fields: dict[str, Any] = getattr(cls, "model_fields", {})
    names = {name for name, f in fields.items() if not f.is_required()}
    if not names:
        return {}
    built = cls.model_construct()  # type: ignore[attr-defined]
    dumped: dict[str, Any] = built.model_dump(
        # No warnings: one quotes the value (a mistyped default), which may be a secret; the
        # master checks each default against its field.
        mode="json",
        by_alias=False,
        include=names,
        warnings=False,
    )
    return dumped


def _schema(desc: TypeDesc, defs: dict[str, Any]) -> dict[str, Any]:  # noqa: PLR0911, PLR0912
    """A dataclass's part as standard JSON Schema; a nested model's ``$defs`` go to ``defs``."""
    match desc:
        case ScalarType(name="any"):
            return {}
        case ScalarType(name="none"):
            return {"type": "null"}
        case ScalarType(name="bool"):
            return {"type": "boolean"}
        case ScalarType(name="int"):
            return {"type": "integer"}
        case ScalarType(name="float"):
            return {"type": "number"}
        case ScalarType(name="str"):
            return {"type": "string"}
        case ScalarType(name=name) if name in _FORMATS:
            return {"type": "string", "format": _FORMATS[name]}
        case ScalarType(name="timedelta"):
            return {"type": "string"}
        case ScalarType(name=name):
            raise TypeError(f"`{name}` can't be a parameter")
        case ListType(item=item):
            return {"type": "array", "items": _schema(item, defs)}
        case TupleType(items=items):
            return {
                "type": "array",
                "prefixItems": [_schema(i, defs) for i in items],
                "minItems": len(items),
                "maxItems": len(items),
            }
        case DictType(value=value):
            return {"type": "object", "additionalProperties": _schema(value, defs)}
        case LiteralType(values=values):
            return {"enum": list(values)}
        case UnionType(variants=variants):
            return {"anyOf": [_schema(v, defs) for v in variants]}
        case TypedDictType(fields=fields) | DataclassType(fields=fields):
            return {
                "type": "object",
                "properties": {f.name: _field_schema(f.name, f.type, defs) for f in fields},
                "required": [f.name for f in fields if f.required],
                "additionalProperties": False,
            }
        case ModelType(name=name, schema=schema):
            return _hoisted(name, schema, defs)
    raise TypeError(f"`{desc}` can't be a parameter")


def _field_schema(name: str, desc: TypeDesc, defs: dict[str, Any]) -> dict[str, Any]:
    """A field's JSON Schema, a refusal naming the field."""
    try:
        return _schema(desc, defs)
    except TypeError as e:
        raise TypeError(f"field `{name}`: {e}") from None


def _hoisted(name: str, schema: Any, defs: dict[str, Any]) -> dict[str, Any]:
    """A nested model's schema, its ``$defs`` moved to the root's (its ``$ref``s point there)."""
    own = dict(schema)
    for key, value in own.pop("$defs", {}).items():
        if key in defs and defs[key] != value:
            raise TypeError(f"two different models are named `{key}` in the params ({name})")
        defs[key] = value
    return own
