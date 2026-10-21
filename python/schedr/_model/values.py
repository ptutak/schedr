"""References, templates and task arguments in DAG JSON (``schedr-model``, ``value.rs``)."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from schedr._model.ids import validate_task_id


class DagCtxField(StrEnum):
    """A field of the run's context: ``dag_ctx.run.*`` and ``dag_ctx.dag_id``."""

    RUN_ID = "run_id"
    RUN_KIND = "run_kind"
    PLANNED_FOR = "planned_for"
    SINCE = "since"
    DAG_ID = "dag_id"


@dataclass(frozen=True, slots=True)
class OutputRef:
    """An upstream task's output, or a part of it.

    Attributes:
        task: The upstream task's ID.
        path: Field names and list / tuple indexes into the output; empty for all of it.
    """

    task: str
    path: Sequence[str | int] = ()

    def __post_init__(self) -> None:
        """Validates the task ID and the path.

        Raises:
            IdError: If the task ID is invalid.
            ValueError: If a path index is negative or not an ``int``/``str``.
        """
        validate_task_id(self.task)
        for step in self.path:
            if isinstance(step, bool) or not isinstance(step, str | int):
                raise ValueError(f"path step {step!r} must be a field name or an index")
            if isinstance(step, int) and step < 0:
                raise ValueError(f"path index {step} must not be negative")

    def to_json(self) -> dict[str, Any]:
        """The reference as DAG JSON.

        Returns:
            ``{"kind": "output", "task": ..., "path": [...]}`` (``path`` only if not empty).
        """
        result: dict[str, Any] = {"kind": "output", "task": self.task}
        if self.path:
            result["path"] = list(self.path)
        return result


@dataclass(frozen=True, slots=True)
class DagCtxRef:
    """A field of the run's context.

    Attributes:
        field: Which field.
    """

    field: DagCtxField

    def to_json(self) -> dict[str, Any]:
        """The reference as DAG JSON.

        Returns:
            ``{"kind": "dag_ctx", "field": ...}``.
        """
        return {"kind": "dag_ctx", "field": self.field.value}


type Reference = OutputRef | DagCtxRef
"""Any reference."""


@dataclass(frozen=True, slots=True)
class TemplateRef:
    """A reference inside a template, with its optional format spec.

    Attributes:
        reference: What is filled in.
        format: The t-string format spec (``raw``, ``%Y-%m-%d``, ...), if any.
    """

    reference: Reference
    format: str | None = None

    def to_json(self) -> dict[str, Any]:
        """The template part as DAG JSON.

        Returns:
            ``{"ref": {...reference, "format": ...}}`` (``format`` only if set).
        """
        inner = self.reference.to_json()
        if self.format is not None:
            inner["format"] = self.format
        return {"ref": inner}


type TemplatePart = str | TemplateRef
"""Literal text or a reference."""


@dataclass(frozen=True, slots=True)
class Template:
    """A t-string as data: its parts in order.

    Attributes:
        parts: Literal text and references.
    """

    parts: Sequence[TemplatePart] = field(default_factory=tuple)

    def references(self) -> list[Reference]:
        """The references in this template, in order.

        Returns:
            Every reference of the template's parts.
        """
        return [p.reference for p in self.parts if isinstance(p, TemplateRef)]

    def to_json(self) -> list[dict[str, Any]]:
        """The template as DAG JSON.

        Returns:
            A list of ``{"text": ...}`` and ``{"ref": ...}`` parts.
        """
        return [{"text": p} if isinstance(p, str) else p.to_json() for p in self.parts]


@dataclass(frozen=True, slots=True)
class LiteralValue:
    """A JSON value as encoded by the API (with tagged extra types), passed as is.

    Attributes:
        value: The JSON-compatible value.
    """

    value: Any

    def references(self) -> list[Reference]:
        """A literal has no references.

        Returns:
            An empty list.
        """
        return []

    def to_json(self) -> dict[str, Any]:
        """The value as DAG JSON.

        Returns:
            ``{"literal": ...}``.
        """
        return {"literal": self.value}


@dataclass(frozen=True, slots=True)
class RefValue:
    """A reference passed with its type.

    Attributes:
        reference: The reference.
    """

    reference: Reference

    def references(self) -> list[Reference]:
        """The reference itself.

        Returns:
            A one-element list.
        """
        return [self.reference]

    def to_json(self) -> dict[str, Any]:
        """The value as DAG JSON.

        Returns:
            ``{"ref": ...}``.
        """
        return {"ref": self.reference.to_json()}


@dataclass(frozen=True, slots=True)
class TemplateValue:
    """A t-string, rendered to a string at run time.

    Attributes:
        template: The template.
    """

    template: Template

    def references(self) -> list[Reference]:
        """The template's references.

        Returns:
            Its references, in order.
        """
        return self.template.references()

    def to_json(self) -> dict[str, Any]:
        """The value as DAG JSON.

        Returns:
            ``{"template": [...]}``.
        """
        return {"template": self.template.to_json()}


@dataclass(frozen=True, slots=True)
class ListValue:
    """A list whose elements may contain references.

    Attributes:
        items: The elements.
    """

    items: Sequence[Value]

    def references(self) -> list[Reference]:
        """The elements' references, in order.

        Returns:
            Every reference at any depth.
        """
        return [r for item in self.items for r in item.references()]

    def to_json(self) -> dict[str, Any]:
        """The value as DAG JSON.

        Returns:
            ``{"list": [...]}``.
        """
        return {"list": [item.to_json() for item in self.items]}


@dataclass(frozen=True, slots=True)
class MapValue:
    """A dictionary (string keys) whose values may contain references.

    Attributes:
        entries: The entries.
    """

    entries: Mapping[str, Value]

    def references(self) -> list[Reference]:
        """The values' references, by key order.

        Returns:
            Every reference at any depth, ordered by key (as stored).
        """
        return [r for key in sorted(self.entries) for r in self.entries[key].references()]

    def to_json(self) -> dict[str, Any]:
        """The value as DAG JSON.

        Returns:
            ``{"map": {...}}`` with keys sorted (as the Rust side stores them).
        """
        return {"map": {key: self.entries[key].to_json() for key in sorted(self.entries)}}


type Value = LiteralValue | RefValue | TemplateValue | ListValue | MapValue
"""Any task argument value."""
