"""Encoding values to stored JSON and decoding them back, guided by the annotation.

Encoding is strict: no coercion except ``int`` for ``float``; ``bool`` is never an ``int``.
``datetime``, ``date``, ``timedelta`` and ``tuple`` are stored **tagged**, so Rust can render
them without Python and a task annotated ``Any`` gets the real type back:

- ``{"$datetime": "2026-10-19T03:00:00+00:00"}`` (aware datetimes only),
- ``{"$date": "2026-10-19"}``,
- ``{"$timedelta": 3600000000}`` (whole microseconds),
- ``{"$tuple": [...]}``.

Dataclasses are stored as plain objects, without a class name; decoding follows the receiving
annotation (see PROJECT.md, Output serialization). Pydantic models are stored as Pydantic writes
them (``model_dump(mode="json")``) and read back with ``model_validate`` (decision 59).
"""

import dataclasses
import json
import math
from datetime import date, datetime, timedelta
from typing import Any, Literal, get_args, get_origin, is_typeddict

from schedr._serialization import models
from schedr._serialization.annotations import (
    MAPPING_ORIGINS,
    SEQUENCE_ORIGINS,
    TypeModelError,
    is_union,
    label,
    tuple_items,
    type_hints,
    unwrap,
)
from schedr.outcomes import Outcome

TAGS = frozenset({"$datetime", "$date", "$timedelta", "$tuple"})
"""The keys of tagged values."""


class CodecError(ValueError):
    """A value that doesn't fit its type, or can't be stored.

    Attributes:
        at: Where in the value, e.g. ``[3].created`` (empty for the value itself).
        reason: What is wrong.
    """

    def __init__(self, at: str, reason: str) -> None:
        """Records where and why.

        Args:
            at: Where in the value.
            reason: What is wrong.
        """
        super().__init__(f"at `{at or '.'}`: {reason}")
        self.at = at
        self.reason = reason


def _type_of(value: object) -> str:
    return type(value).__name__


# ------------------------------------------------------------------------------- encoding


def encode(value: object, annotation: object, at: str = "") -> Any:  # noqa: PLR0911, PLR0912
    """Encodes ``value`` as JSON-compatible data, checked against ``annotation``.

    Args:
        value: The value (e.g. a task's return value).
        annotation: Its declared type.
        at: Where in an enclosing value (for messages).

    Returns:
        JSON-compatible data with tagged extra types.

    Raises:
        CodecError: If the value doesn't fit the type or can't be stored.
    """
    annotation = unwrap(annotation)
    if annotation is Any:
        return encode_any(value, at)
    if isinstance(annotation, type) and issubclass(annotation, Outcome):
        if not isinstance(value, annotation):
            raise CodecError(at, f"expected a member of {annotation.__name__}, got {value!r}")
        return value.name
    if annotation is None:
        if value is not None:
            raise CodecError(at, f"expected None, got {_type_of(value)}")
        return None
    if is_union(annotation):
        return _encode_union(value, get_args(annotation), at)
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is Literal:
        if not any(value == a and type(value) is type(a) for a in args):
            raise CodecError(at, f"{value!r} is not one of {list(args)!r}")
        return value
    if annotation in (bool, int, float, str):
        return _encode_scalar(value, annotation, at)
    if annotation in (datetime, date, timedelta):
        return _encode_time(value, annotation, at)
    if origin in SEQUENCE_ORIGINS or annotation in SEQUENCE_ORIGINS:
        if not isinstance(value, list):
            raise CodecError(at, f"expected a list, got {_type_of(value)}")
        item = args[0] if args else Any
        return [encode(v, item, f"{at}[{i}]") for i, v in enumerate(value)]
    if origin is tuple:
        return _encode_tuple(value, args, at)
    if origin in MAPPING_ORIGINS or annotation in MAPPING_ORIGINS:
        return _encode_dict(value, args[1] if args else Any, at)
    if isinstance(annotation, type) and is_typeddict(annotation):
        return _encode_typed_dict(value, annotation, at)
    if models.is_model(annotation):
        if not isinstance(value, annotation):  # type: ignore[arg-type]
            raise CodecError(at, f"expected {label(annotation)}, got {_type_of(value)}")
        return models.dump(value)
    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        if not isinstance(value, annotation):
            raise CodecError(at, f"expected {annotation.__name__}, got {_type_of(value)}")
        return _encode_dataclass(value, annotation, at)
    raise TypeModelError(f"`{label(annotation)}` is not supported")


def _encode_scalar(value: object, annotation: object, at: str) -> Any:
    if annotation is bool:
        ok = isinstance(value, bool)
    elif annotation is int:
        ok = isinstance(value, int) and not isinstance(value, bool)
    elif annotation is float:
        ok = isinstance(value, int | float) and not isinstance(value, bool)
        if ok and not math.isfinite(value):  # type: ignore[arg-type]
            raise CodecError(at, f"{value} can't be stored (NaN and infinity aren't JSON)")
    else:
        ok = isinstance(value, str)
    if not ok:
        raise CodecError(at, f"expected {label(annotation)}, got {_type_of(value)}")
    return value


def _encode_time(value: object, annotation: object, at: str) -> Any:
    if annotation is datetime:
        if not isinstance(value, datetime):
            raise CodecError(at, f"expected datetime, got {_type_of(value)}")
        if value.tzinfo is None or value.utcoffset() is None:
            raise CodecError(at, "naive datetime (no time zone) can't be stored")
        return {"$datetime": value.isoformat()}
    if annotation is date:
        if not isinstance(value, date) or isinstance(value, datetime):
            raise CodecError(at, f"expected date, got {_type_of(value)}")
        return {"$date": value.isoformat()}
    if not isinstance(value, timedelta):
        raise CodecError(at, f"expected timedelta, got {_type_of(value)}")
    return {"$timedelta": (value.days * 86_400 + value.seconds) * 1_000_000 + value.microseconds}


def _encode_union(value: object, members: tuple[Any, ...], at: str) -> Any:
    for member in members:
        try:
            return encode(value, member, at)
        except CodecError:
            continue
    names = " | ".join(label(m) for m in members)
    raise CodecError(at, f"{_type_of(value)} matches none of {names}")


def _encode_tuple(value: object, args: tuple[Any, ...], at: str) -> Any:
    items = tuple_items(args)
    if not isinstance(value, tuple) or len(value) != len(items):
        raise CodecError(at, f"expected a tuple of {len(items)}, got {_type_of(value)}")
    return {
        "$tuple": [
            encode(v, t, f"{at}[{i}]") for i, (v, t) in enumerate(zip(value, items, strict=True))
        ]
    }


def _encode_dict(value: object, item: object, at: str) -> Any:
    if not isinstance(value, dict):
        raise CodecError(at, f"expected a dict, got {_type_of(value)}")
    result: dict[str, Any] = {}
    for key, v in value.items():
        if not isinstance(key, str):
            raise CodecError(at, f"dict keys must be str, got {_type_of(key)}")
        result[key] = encode(v, item, f"{at}[{key!r}]")
    return result


def _typed_dict_hints(value: object, cls: type, at: str) -> dict[str, Any]:
    """Checks a dict's keys against a TypedDict and returns the TypedDict's hints."""
    if not isinstance(value, dict):
        raise CodecError(at, f"expected {cls.__name__} (a dict), got {_type_of(value)}")
    hints = type_hints(cls, f"`{cls.__name__}`")
    required = set(getattr(cls, "__required_keys__", frozenset()))
    missing = sorted(required - value.keys())
    if missing:
        raise CodecError(at, f"{cls.__name__}: missing key {missing[0]!r}")
    unknown = sorted(set(value) - set(hints))
    if unknown:
        raise CodecError(at, f"{cls.__name__}: unknown key {unknown[0]!r}")
    return hints


def _encode_typed_dict(value: object, cls: type, at: str) -> Any:
    hints = _typed_dict_hints(value, cls, at)
    return {k: encode(v, hints[k], f"{at}[{k!r}]") for k, v in value.items()}  # type: ignore[attr-defined]


def _encode_dataclass(value: object, cls: type, at: str) -> Any:
    hints = type_hints(cls, f"`{cls.__name__}`")
    return {
        f.name: encode(getattr(value, f.name), hints[f.name], f"{at}.{f.name}")
        for f in dataclasses.fields(cls)
    }


def encode_any(value: object, at: str = "") -> Any:  # noqa: PLR0911
    """Encodes a value with no declared type (``Any``), by its run-time type.

    Args:
        value: The value.
        at: Where in an enclosing value.

    Returns:
        JSON-compatible data with tagged extra types; dataclasses become plain objects.

    Raises:
        CodecError: For a value that can't be stored (e.g. ``Decimal``, ``set``, NaN).
    """
    if value is None or isinstance(value, bool | str):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return _encode_scalar(value, float, at)
    if isinstance(value, datetime):
        return _encode_time(value, datetime, at)
    if isinstance(value, date):
        return _encode_time(value, date, at)
    if isinstance(value, timedelta):
        return _encode_time(value, timedelta, at)
    if isinstance(value, list):
        return [encode_any(v, f"{at}[{i}]") for i, v in enumerate(value)]
    if isinstance(value, tuple):
        return {"$tuple": [encode_any(v, f"{at}[{i}]") for i, v in enumerate(value)]}
    if isinstance(value, dict):
        return _encode_dict(value, Any, at)
    if models.is_model_value(value):
        return models.dump(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            f.name: encode_any(getattr(value, f.name), f"{at}.{f.name}")
            for f in dataclasses.fields(value)
        }
    raise CodecError(at, f"{_type_of(value)} is not supported")


def encoded_size(data: object) -> int:
    """The size of encoded data as stored: compact JSON, in UTF-8 bytes.

    Args:
        data: JSON-compatible data (from :func:`encode`).

    Returns:
        The size in bytes.
    """
    return len(json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode())


def check_size(data: object, limit: int) -> None:
    """Checks encoded data against an output size limit.

    Args:
        data: JSON-compatible data.
        limit: The limit in bytes.

    Raises:
        CodecError: If it is larger.
    """
    size = encoded_size(data)
    if size > limit:
        raise CodecError("", f"output is {size} bytes, over the limit of {limit} bytes")


# ------------------------------------------------------------------------------- decoding


def decode(data: Any, annotation: object, at: str = "") -> Any:  # noqa: PLR0911, PLR0912
    """Decodes stored JSON into the type ``annotation`` describes.

    Args:
        data: JSON-compatible data (as :func:`encode` produced it).
        annotation: The receiving type (the downstream parameter's annotation).
        at: Where in an enclosing value.

    Returns:
        The value; dataclasses are constructed with keyword arguments.

    Raises:
        CodecError: If the data doesn't fit the type (e.g. the class changed since).
    """
    annotation = unwrap(annotation)
    if annotation is Any:
        return decode_any(data, at)
    if isinstance(annotation, type) and issubclass(annotation, Outcome):
        if not isinstance(data, str) or data not in annotation.__members__:
            raise CodecError(at, f"`{data}` isn't one of {annotation.__name__}'s outcomes")
        return annotation[data]
    if annotation is None:
        if data is not None:
            raise CodecError(at, f"expected None, got {_type_of(data)}")
        return None
    if is_union(annotation):
        for member in get_args(annotation):
            try:
                return decode(data, member, at)
            except CodecError:
                continue
        raise CodecError(at, f"the stored value matches none of {label(annotation)}")
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is Literal:
        return encode(data, annotation, at)
    if annotation in (bool, int, float, str):
        value = _encode_scalar(data, annotation, at)
        return float(value) if annotation is float else value
    if annotation in (datetime, date, timedelta):
        return _decode_time(data, annotation, at)
    if origin in SEQUENCE_ORIGINS or annotation in SEQUENCE_ORIGINS:
        if not isinstance(data, list):
            raise CodecError(at, f"expected a list, got {_type_of(data)}")
        item = args[0] if args else Any
        return [decode(v, item, f"{at}[{i}]") for i, v in enumerate(data)]
    if origin is tuple:
        items = tuple_items(args)
        values = _tagged(data, "$tuple", at)
        if not isinstance(values, list) or len(values) != len(items):
            raise CodecError(at, f"expected a tuple of {len(items)}")
        return tuple(
            decode(v, t, f"{at}[{i}]") for i, (v, t) in enumerate(zip(values, items, strict=True))
        )
    if origin in MAPPING_ORIGINS or annotation in MAPPING_ORIGINS:
        if not isinstance(data, dict):
            raise CodecError(at, f"expected a dict, got {_type_of(data)}")
        item = args[1] if args else Any
        return {k: decode(v, item, f"{at}[{k!r}]") for k, v in data.items()}
    if isinstance(annotation, type) and is_typeddict(annotation):
        hints = _typed_dict_hints(data, annotation, at)
        return {k: decode(v, hints[k], f"{at}[{k!r}]") for k, v in data.items()}
    if models.is_model(annotation):
        assert isinstance(annotation, type)
        try:
            # schedr's tagged dates (from a dataclass or plain output) become values first.
            return models.validate(annotation, decode_any(data))
        except ValueError as e:  # Pydantic's ValidationError
            raise CodecError(at, f"{annotation.__name__}: {e}") from e
    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        return _decode_dataclass(data, annotation, at)
    raise TypeModelError(f"`{label(annotation)}` is not supported")


def _tagged(data: Any, tag: str, at: str) -> Any:
    if not isinstance(data, dict) or list(data) != [tag]:
        raise CodecError(at, f"expected a {tag} value, got {_type_of(data)}")
    return data[tag]


def _decode_time(data: Any, annotation: object, at: str) -> Any:
    if isinstance(data, str):
        # A model's date, as Pydantic wrote it (decision 60): read by Pydantic's own rules.
        try:
            return models.parse_date(data, annotation)
        except ValueError as e:
            raise CodecError(at, f"bad {label(annotation)} text {data!r}: {e}") from e
    if annotation is timedelta:
        micros = _tagged(data, "$timedelta", at)
        if not isinstance(micros, int) or isinstance(micros, bool):
            raise CodecError(at, "a $timedelta holds whole microseconds")
        return timedelta(microseconds=micros)
    tag = "$datetime" if annotation is datetime else "$date"
    text = _tagged(data, tag, at)
    try:
        return datetime.fromisoformat(text) if annotation is datetime else date.fromisoformat(text)
    except (TypeError, ValueError) as e:
        raise CodecError(at, f"bad {tag} value {text!r}") from e


def _decode_dataclass(data: Any, cls: type, at: str) -> Any:
    if not isinstance(data, dict):
        raise CodecError(at, f"expected {cls.__name__} (an object), got {_type_of(data)}")
    hints = type_hints(cls, f"`{cls.__name__}`")
    fields = {f.name: f for f in dataclasses.fields(cls)}
    unknown = sorted(set(data) - set(fields))
    if unknown:
        raise CodecError(at, f"{cls.__name__}: unknown field {unknown[0]!r} (the class changed?)")
    kwargs = {k: decode(v, hints[k], f"{at}.{k}") for k, v in data.items()}
    try:
        return cls(**kwargs)
    except TypeError as e:
        raise CodecError(at, f"{cls.__name__}: {e} (the class changed?)") from e


def decode_any(data: Any, at: str = "") -> Any:
    """Decodes stored JSON with no declared type: tagged values get their real types back.

    Args:
        data: JSON-compatible data.
        at: Where in an enclosing value.

    Returns:
        The value; objects (including stored dataclasses) are plain dicts.
    """
    if isinstance(data, list):
        return [decode_any(v, f"{at}[{i}]") for i, v in enumerate(data)]
    if isinstance(data, dict):
        if len(data) == 1 and next(iter(data)) in TAGS:
            tag = next(iter(data))
            if tag == "$tuple":
                return tuple(decode_any(v, f"{at}[{i}]") for i, v in enumerate(data[tag]))
            kind = {"$datetime": datetime, "$date": date, "$timedelta": timedelta}[tag]
            return _decode_time(data, kind, at)
        return {k: decode_any(v, f"{at}[{k!r}]") for k, v in data.items()}
    return data
