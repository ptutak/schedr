"""Whether an output fits a parameter, by JSON Schema, when a Pydantic model is involved.

Decision 59 (C): a model's type is Pydantic's JSON Schema of it. Comparing it with another
type is strict: an output fits only when **every value it can be is provably accepted** by
the parameter (59.1): a constraint, format, enum or required field the parameter has and the
output doesn't is refused, even if the values might happen to fit. What can't be compared
(``allOf``, ``not``, ``if``, differing patterns) fits only when identical (59.2). A model's
dates and schedr's ``datetime`` / ``date`` / ``timedelta`` fit each other, converted when
the task receives them (decision 60). schedr's own descriptors are turned into JSON Schema here, so
models, dataclasses and plain types meet in one comparison.
"""

import math
from collections.abc import Mapping
from typing import Any

from schedr._model import (
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

type Schema = Mapping[str, Any]

_ANNOTATIONS = frozenset(
    {
        "title",
        "description",
        "default",
        "examples",
        "deprecated",
        "readOnly",
        "writeOnly",
        "$comment",
        "discriminator",
    }
)
"""Keywords that don't restrict values: ignored when comparing."""

_COMPARED = frozenset(
    {
        "type",
        "enum",
        "const",
        "anyOf",
        "oneOf",
        "$ref",
        "$defs",
        "format",
        "minLength",
        "maxLength",
        "pattern",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "items",
        "prefixItems",
        "minItems",
        "maxItems",
        "uniqueItems",
        "properties",
        "required",
        "additionalProperties",
        "$schedr",
    }
)
"""Keywords the comparison understands; any other must be identical on both sides (59.2)."""

_TAGGED = frozenset({"datetime", "date", "timedelta", "process_result"})
"""schedr's own types stored in its own form (``{"$datetime": ...}``), not Pydantic's."""


def contains_model(desc: TypeDesc) -> bool:
    """Whether a descriptor is, or holds, a Pydantic model.

    Args:
        desc: The descriptor.

    Returns:
        True when the schema comparison applies to it.
    """
    match desc:
        case ModelType():
            return True
        case ListType(item=item) | DictType(value=item):
            return contains_model(item)
        case TupleType(items=items) | UnionType(variants=items):
            return any(contains_model(i) for i in items)
        case TypedDictType(fields=fields) | DataclassType(fields=fields):
            return any(contains_model(f.type) for f in fields)
    return False


def from_desc(desc: TypeDesc, *, output: bool) -> dict[str, Any]:  # noqa: PLR0911, PLR0912
    """A descriptor as JSON Schema, for the comparison.

    Args:
        desc: The descriptor.
        output: Whether it is an output's type (a dataclass then always has every field)
            or a parameter's (its fields with defaults are optional, unknown ones refused).

    Returns:
        The schema; schedr's tagged types become a marker only they match.
    """
    match desc:
        case ModelType(schema=schema):
            return dict(schema)
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
        case ScalarType(name=name):
            return {"$schedr": name}
        case ListType(item=item):
            return {"type": "array", "items": from_desc(item, output=output)}
        case TupleType(items=items):
            return {"$schedr": "tuple", "prefixItems": [from_desc(i, output=output) for i in items]}
        case DictType(value=value):
            return {"type": "object", "additionalProperties": from_desc(value, output=output)}
        case LiteralType(values=values):
            return {"enum": list(values)}
        case UnionType(variants=variants):
            return {"anyOf": [from_desc(v, output=output) for v in variants]}
        case TypedDictType(fields=fields) | DataclassType(fields=fields):
            return {
                "type": "object",
                "properties": {f.name: from_desc(f.type, output=output) for f in fields},
                "required": [f.name for f in fields if output or f.required],
                "additionalProperties": False,
            }


def part(desc: ModelType, step: str | int) -> ModelType | None:
    """The type of one step into a model's value (``out.url``, ``out.items[0]``).

    Args:
        desc: The model, or a part of one.
        step: A field name or a list index.

    Returns:
        The part's type (carrying the ``$defs`` its references need), or ``None`` if the
        schema has no such part. In a union, the step must exist in every member.
    """
    root = desc.schema
    found = _step(_resolve(root, root), step, root)
    if found is None:
        return None
    if "$defs" in root and "$defs" not in found:
        found = {**found, "$defs": root["$defs"]}
    return ModelType(
        f"{desc.name}.{step}" if isinstance(step, str) else f"{desc.name}[{step}]", found
    )


def _step(schema: Schema, step: str | int, root: Schema) -> Schema | None:
    members = schema.get("anyOf") or schema.get("oneOf")
    if members is not None:
        parts = [_step(_resolve(m, root), step, root) for m in members]
        if any(p is None for p in parts):
            return None
        return {"anyOf": parts}
    if isinstance(step, str):
        properties = schema.get("properties", {})
        if step in properties:
            found: Schema = properties[step]
            return found
        extra = schema.get("additionalProperties")
        return extra if isinstance(extra, Mapping) else None
    prefix = schema.get("prefixItems")
    if prefix is not None:
        return prefix[step] if step < len(prefix) else None
    items = schema.get("items")
    return items if isinstance(items, Mapping) else None


def fit_problem(source: TypeDesc, target: TypeDesc) -> str | None:
    """Why an output of type ``source`` doesn't provably fit a parameter of type ``target``.

    Args:
        source: The output's type (holding a model, or not).
        target: The parameter's type.

    Returns:
        ``None`` when it fits; otherwise the first reason, e.g. "`retries`: the parameter
        needs minimum 0; the output has none".
    """
    out = from_desc(source, output=True)
    in_ = from_desc(target, output=False)
    return _Compare(out, in_).fits(out, in_, "")


class _Compare:
    """One comparison: the two roots (for ``$ref``) and the pairs assumed to fit (recursion)."""

    def __init__(self, out_root: Schema, in_root: Schema) -> None:
        self.out_root = out_root
        self.in_root = in_root
        self.assumed: set[tuple[str, str]] = set()

    def fits(self, out: Schema, in_: Schema, at: str) -> str | None:
        """Why ``out`` doesn't fit ``in_`` at ``at`` (``None`` when it does)."""
        refs = (out.get("$ref", ""), in_.get("$ref", ""))
        if any(refs):
            if refs in self.assumed:
                return None  # recursion: this pair is being checked further up
            self.assumed.add(refs)
        out = _strip(_resolve(out, self.out_root))
        in_ = _strip(_resolve(in_, self.in_root))
        if not in_ or not out or out == in_:
            return None
        where = f"`{at}`: " if at else ""
        out_members = out.get("anyOf") or out.get("oneOf")
        if out_members is not None:
            # Every form the output may take must fit.
            problems = (self.fits(member, in_, at) for member in out_members)
            return next((p for p in problems if p is not None), None)
        in_members = in_.get("anyOf") or in_.get("oneOf")
        if in_members is not None:
            if any(self.fits(out, m, at) is None for m in in_members):
                return None
            return f"{where}{_show(out)} fits none of the parameter's {_show(in_)}"
        return self._kinds(out, in_, at, where)

    def _kinds(self, out: Schema, in_: Schema, at: str, where: str) -> str | None:
        """``fits`` past references and unions: tagged types, values, then typed schemas."""
        if "$schedr" in out or "$schedr" in in_:
            return self._tagged(out, in_, at, where)
        unknown = sorted((set(out) | set(in_)) - _COMPARED)
        if unknown:
            return (
                f"{where}`{unknown[0]}` can't be compared and the two sides differ "
                f"({_show(out)} against {_show(in_)}); make them the same type"
            )
        if "enum" in out or "const" in out:
            return self._values(out, in_, where)
        if "enum" in in_ or "const" in in_:
            return (
                f"{where}the parameter allows only {_values_of(in_)}; "
                f"the output may be any {_show(out)}"
            )
        return self._typed(out, in_, at, where)

    def _tagged(self, out: Schema, in_: Schema, at: str, where: str) -> str | None:
        """Schedr's own stored forms: the same kind, or a date of a model's (decision 60).

        A model's ``date-time``, ``date`` and ``duration`` text and schedr's ``datetime``,
        ``date`` and ``timedelta`` fit each other both ways: the runner converts the value
        when the task receives it. Tuples are compared element by element.
        """
        mine, theirs = out.get("$schedr"), in_.get("$schedr")
        if _same_date(out, in_):
            return None
        if mine != theirs:
            return (
                f"{where}{_show(out)} is stored differently from {_show(in_)} (schedr tags its "
                "own tuples and command results; Pydantic writes them as lists and objects): "
                "pass the model whole, or take the part as Pydantic writes it"
            )
        if mine != "tuple":
            return None
        out_items, in_items = out["prefixItems"], in_["prefixItems"]
        if len(out_items) != len(in_items):
            return f"{where}a tuple of {len(out_items)} doesn't fit a tuple of {len(in_items)}"
        for i, (o, n) in enumerate(zip(out_items, in_items, strict=True)):
            problem = self.fits(o, n, f"{at}[{i}]")
            if problem is not None:
                return problem
        return None

    def _values(self, out: Schema, in_: Schema, where: str) -> str | None:
        values = out["enum"] if "enum" in out else [out["const"]]
        if "enum" in in_ or "const" in in_:
            allowed = in_["enum"] if "enum" in in_ else [in_["const"]]
            missing = [v for v in values if not any(_same(v, a) for a in allowed)]
            return f"{where}{missing[0]!r} isn't one of {allowed!r}" if missing else None
        for value in values:
            problem = _value_problem(value, in_)
            if problem is not None:
                return f"{where}{value!r}: {problem}"
        return None

    def _typed(self, out: Schema, in_: Schema, at: str, where: str) -> str | None:
        out_types = _types(out)
        in_types = _types(in_)
        if out_types is None:
            return f"{where}can't tell the output's type ({_show(out)}) fits {_show(in_)}"
        if in_types is not None:
            for t in out_types:
                if t not in in_types and not (t == "integer" and "number" in in_types):
                    return (
                        f"{where}the output may be {_type_word(t)}; "
                        f"the parameter takes {_show(in_)}"
                    )
        for check in (_strings, _numbers):
            problem = check(out, in_)
            if problem is not None:
                return f"{where}{problem}"
        problem = self._arrays(out, in_, at, where)
        if problem is not None:
            return problem
        return self._objects(out, in_, at, where)

    def _arrays(self, out: Schema, in_: Schema, at: str, where: str) -> str | None:
        bounds = _bounds(out, in_, "minItems", "maxItems", "items")
        if bounds is not None:
            return f"{where}{bounds}"
        if in_.get("uniqueItems") and not out.get("uniqueItems"):
            return f"{where}the parameter needs unique items; the output doesn't promise them"
        in_prefix = in_.get("prefixItems")
        out_prefix = out.get("prefixItems")
        if in_prefix is not None:
            if out_prefix is None or len(out_prefix) != len(in_prefix):
                return (
                    f"{where}the parameter takes exactly {len(in_prefix)} item(s); "
                    "the output may have another number"
                )
            for i, (o, n) in enumerate(zip(out_prefix, in_prefix, strict=True)):
                problem = self.fits(o, n, f"{at}[{i}]")
                if problem is not None:
                    return problem
        in_items = in_.get("items")
        if isinstance(in_items, Mapping):
            out_items = [
                *(out_prefix or []),
                *([out["items"]] if isinstance(out.get("items"), Mapping) else []),
            ]
            for o in out_items:
                problem = self.fits(o, in_items, f"{at}[]")
                if problem is not None:
                    return problem
        return None

    def _objects(self, out: Schema, in_: Schema, at: str, where: str) -> str | None:
        """Objects: required fields, each field the parameter takes, and the extra ones."""
        out_props: Mapping[str, Schema] = out.get("properties", {})
        out_required = set(out.get("required", ()))
        for name in in_.get("required", ()):
            if name not in out_required:
                present = "may be missing" if name in out_props else "is missing"
                return f"{where}field `{name}` {present} in the output; the parameter needs it"
        return self._fields(out, in_, at) or self._extras(out, in_, at, where)

    def _fields(self, out: Schema, in_: Schema, at: str) -> str | None:
        """Each field the parameter declares, against the output's (or its extras')."""
        out_props: Mapping[str, Schema] = out.get("properties", {})
        out_extra = out.get("additionalProperties", False)  # an output writes only its fields
        for name, in_schema in in_.get("properties", {}).items():
            out_schema = out_props.get(name, out_extra if isinstance(out_extra, Mapping) else None)
            if out_schema is not None:
                problem = self.fits(out_schema, in_schema, f"{at}.{name}" if at else name)
                if problem is not None:
                    return problem
        return None

    def _extras(self, out: Schema, in_: Schema, at: str, where: str) -> str | None:
        """The output's fields the parameter doesn't declare, and its open-ended ones."""
        in_props: Mapping[str, Schema] = in_.get("properties", {})
        out_extra = out.get("additionalProperties", False)
        in_extra = in_.get("additionalProperties", True)  # a parameter ignores unknown ones
        for name, out_schema in out.get("properties", {}).items():
            if name in in_props:
                continue
            if in_extra is False:
                return f"{where}the output has field `{name}`, which the parameter doesn't take"
            if isinstance(in_extra, Mapping):
                problem = self.fits(out_schema, in_extra, f"{at}.{name}" if at else name)
                if problem is not None:
                    return problem
        if out_extra is False or in_extra is True:
            return None
        if in_extra is False:
            return f"{where}the output may have more fields; the parameter takes only its own"
        rest = out_extra if isinstance(out_extra, Mapping) else {}
        return self.fits(rest, in_extra, f"{at}[]")


_DATE_FORMATS = {"datetime": "date-time", "date": "date", "timedelta": "duration"}
"""schedr's date types and the JSON Schema format Pydantic writes each as (decision 60)."""


def _same_date(out: Schema, in_: Schema) -> bool:
    """Whether one side is schedr's date type and the other a model's text of that kind."""
    for tagged, text in ((out, in_), (in_, out)):
        fmt = _DATE_FORMATS.get(str(tagged.get("$schedr", "")))
        if fmt is not None and text.get("type") == "string" and text.get("format") == fmt:
            return True
    return False


def _resolve(schema: Schema, root: Schema) -> Schema:
    """Follows ``$ref`` (``#/$defs/Name``) to the definition, keeping sibling keywords."""
    seen: set[str] = set()
    while "$ref" in schema:
        ref = str(schema["$ref"])
        if ref in seen or not ref.startswith("#/"):
            break
        seen.add(ref)
        target: Any = root
        for key in ref[2:].split("/"):
            target = target.get(key, {}) if isinstance(target, Mapping) else {}
        schema = {**{k: v for k, v in schema.items() if k != "$ref"}, **target}
    return schema


def _strip(schema: Schema) -> dict[str, Any]:
    return {k: v for k, v in schema.items() if k not in _ANNOTATIONS and k != "$defs"}


def _types(schema: Schema) -> set[str] | None:
    declared = schema.get("type")
    if declared is None:
        return None
    return set(declared) if isinstance(declared, list) else {str(declared)}


def _type_word(name: str) -> str:
    return {
        "integer": "an int",
        "number": "a float",
        "string": "a str",
        "boolean": "a bool",
        "null": "None",
        "array": "a list",
        "object": "an object",
    }.get(name, name)


def _strings(out: Schema, in_: Schema) -> str | None:
    if "format" in in_ and out.get("format") != in_["format"]:
        has = f"format `{out['format']}`" if "format" in out else "none"
        return f"the parameter needs format `{in_['format']}`; the output has {has}"
    if "pattern" in in_ and out.get("pattern") != in_["pattern"]:
        has = f"`{out['pattern']}`" if "pattern" in out else "none"
        return (
            f"the parameter needs pattern `{in_['pattern']}`; the output has {has} "
            "(patterns must be identical)"
        )
    return _bounds(out, in_, "minLength", "maxLength", "length")


def _bounds(out: Schema, in_: Schema, low: str, high: str, what: str) -> str | None:
    if low in in_ and out.get(low, 0) < in_[low]:
        return f"the parameter needs {low} {in_[low]}; the output has {out.get(low, 'none')}"
    if high in in_ and (high not in out or out[high] > in_[high]):
        return f"the parameter needs {high} {in_[high]}; the output has {out.get(high, 'none')}"
    del what
    return None


def _numbers(out: Schema, in_: Schema) -> str | None:
    for side, plain, exclusive in (
        ("lower", "minimum", "exclusiveMinimum"),
        ("upper", "maximum", "exclusiveMaximum"),
    ):
        needed = _limit(in_, plain, exclusive)
        if needed is None:
            continue
        has = _limit(out, plain, exclusive)
        if has is None or not _within(has, needed, lower=side == "lower"):
            return (
                f"the parameter needs {_show_limit(needed, plain, exclusive)}; the output has "
                f"{_show_limit(has, plain, exclusive) if has else 'none'}"
            )
    step = in_.get("multipleOf")
    if step is not None:
        mine = out.get("multipleOf")
        if mine is None or not _is_multiple(mine, step):
            shown = mine if mine is not None else "none"
            return f"the parameter needs multipleOf {step}; the output has {shown}"
    return None


def _limit(schema: Schema, plain: str, exclusive: str) -> tuple[float, bool] | None:
    if exclusive in schema:
        return (float(schema[exclusive]), True)
    if plain in schema:
        return (float(schema[plain]), False)
    return None


def _within(has: tuple[float, bool], needed: tuple[float, bool], *, lower: bool) -> bool:
    (value, open_), (limit, needed_open) = has, needed
    if value != limit:
        return value > limit if lower else value < limit
    return open_ or not needed_open


def _show_limit(limit: tuple[float, bool], plain: str, exclusive: str) -> str:
    value, open_ = limit
    shown = int(value) if value.is_integer() else value
    return f"{exclusive if open_ else plain} {shown}"


def _is_multiple(mine: float, step: float) -> bool:
    ratio = mine / step
    return math.isclose(ratio, round(ratio))


def _value_problem(value: Any, in_: Schema) -> str | None:
    """Why a literal value doesn't fit ``in_`` (``None`` when it does)."""
    types = _types(in_)
    kind = _json_type(value)
    if types is not None and kind not in types and not (kind == "integer" and "number" in types):
        return f"the parameter takes {_show(in_)}"
    if isinstance(value, str):
        return _text_problem(value, in_)
    if isinstance(value, int | float) and not isinstance(value, bool):
        return _number_problem(value, in_)
    return None


def _text_problem(value: str, in_: Schema) -> str | None:
    if "format" in in_ or "pattern" in in_:
        return (
            "the parameter's format or pattern can't be checked on a literal; "
            "make them the same type"
        )
    if not in_.get("minLength", 0) <= len(value) <= in_.get("maxLength", math.inf):
        return "its length is outside the parameter's"
    return None


def _number_problem(value: float, in_: Schema) -> str | None:
    limits = (("minimum", "exclusiveMinimum", True), ("maximum", "exclusiveMaximum", False))
    for plain, exclusive, lower in limits:
        needed = _limit(in_, plain, exclusive)
        if needed is not None and not _within((float(value), False), needed, lower=lower):
            return f"outside the parameter's {_show_limit(needed, plain, exclusive)}"
    if "multipleOf" in in_ and not _is_multiple(value, in_["multipleOf"]):
        return f"not a multiple of {in_['multipleOf']}"
    return None


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"


def _same(a: Any, b: Any) -> bool:
    return a == b and type(a) is type(b)


def _values_of(schema: Schema) -> str:
    return repr(schema["enum"] if "enum" in schema else [schema["const"]])


def _show(schema: Schema) -> str:
    """A short form of a schema for messages: its type and the keywords that restrict it."""
    if "$schedr" in schema:
        return f"schedr's `{schema['$schedr']}`"
    if "type" in schema:
        rest = {
            k: v
            for k, v in schema.items()
            if k
            not in {
                "type",
                "properties",
                "required",
                "items",
                "$defs",
                "additionalProperties",
                "prefixItems",
            }
            and k not in _ANNOTATIONS
        }
        extra = ", ".join(f"{k} {v}" for k, v in rest.items())
        return f"`{schema['type']}`" + (f" ({extra})" if extra else "")
    if "enum" in schema or "const" in schema:
        return f"one of {_values_of(schema)}"
    members = schema.get("anyOf") or schema.get("oneOf")
    if members is not None:
        return " | ".join(_show(m) for m in members)
    if "$ref" in schema:
        return f"`{str(schema['$ref']).rsplit('/', 1)[-1]}`"
    return "a value of another kind"
