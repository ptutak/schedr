"""References, templates and task arguments in DAG JSON (``schedr-model``, ``value.rs``)."""

import keyword
import re
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
        on: The outcomes it was narrowed to (``.on(...)``); empty for any. On another
            outcome, or a skipped upstream task, it stands for ``None``.
    """

    task: str
    path: Sequence[str | int] = ()
    on: Sequence[str] = ()

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
            ``{"kind": "output", "task": ..., "path": [...], "on": [...]}`` (``path`` and
            ``on`` only if not empty).
        """
        result: dict[str, Any] = {"kind": "output", "task": self.task}
        if self.path:
            result["path"] = list(self.path)
        if self.on:
            result["on"] = list(self.on)
        return result


@dataclass(frozen=True, slots=True)
class OutcomeRef:
    """An upstream task's outcome itself (``.outcome``), as its name.

    Attributes:
        task: The upstream task's ID.
    """

    task: str

    def __post_init__(self) -> None:
        """Validates the task ID.

        Raises:
            IdError: If it is invalid.
        """
        validate_task_id(self.task)

    def to_json(self) -> dict[str, Any]:
        """The reference as DAG JSON.

        Returns:
            ``{"kind": "outcome", "task": ...}``.
        """
        return {"kind": "outcome", "task": self.task}


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


@dataclass(frozen=True, slots=True)
class ParamRef:
    """The run's parameters, or a part of them (``dag_ctx.params.region``, decision 138).

    Attributes:
        path: Field names and list / tuple indexes into the parameters; empty for all of them.
    """

    path: Sequence[str | int] = ()

    def to_json(self) -> dict[str, Any]:
        """The reference as DAG JSON.

        Returns:
            ``{"kind": "param", "path": [...]}`` (``path`` only if not empty).
        """
        result: dict[str, Any] = {"kind": "param"}
        if self.path:
            result["path"] = list(self.path)
        return result


_STORAGE_NAME = re.compile(r"[A-Za-z0-9_-]+")


def check_storage_name(name: str) -> str | None:
    """Why ``name`` can't be a secret storage's name (its ``[secret_storage.<name>]`` key).

    Args:
        name: The name.

    Returns:
        The reason, or ``None`` for a good name.
    """
    if not name:
        return "the storage's name is empty"
    if not _STORAGE_NAME.fullmatch(name):
        return f"storage `{name}`: a name has only letters, digits, `_` and `-`"
    if name == "default":
        return "`default` names the default storage's setting, not a storage"
    return None


def storage_label(storage: str | None) -> str:
    """A storage as written in Python.

    ``secrets``, ``secrets.other``, or ``secrets.storage('vault-2')`` for a name that can't
    be an attribute: not a Python name, a keyword, ``_`` first, or ``storage`` or ``value``
    (the methods).

    Args:
        storage: Its name; ``None`` for the default one.

    Returns:
        The label.
    """
    if storage is None:
        return "secrets"
    dotted = (
        storage.isidentifier()
        and not keyword.iskeyword(storage)
        and not storage.startswith("_")
        and storage not in {"storage", "value"}
    )
    return f"secrets.{storage}" if dotted else f"secrets.storage({storage!r})"


def check_secret_path(path: str, what: str) -> str | None:
    """Why ``path`` can't be a secret's path (or a prefix without its last ``/``).

    Args:
        path: The path.
        what: ``path`` or ``prefix``, for the message.

    Returns:
        The reason, or ``None`` for a good path.
    """
    if not path:
        return f"the {what} is empty"
    if "#" in path:
        return f"a {what} contains no `#` (it separates the key)"
    if "\0" in path:
        return f"a {what} contains no NUL"
    if any(not part for part in path.split("/")):
        return f"a {what} has no empty part (no leading `/`, no `//`)"
    return None


@dataclass(frozen=True, slots=True)
class SecretRef:
    """A secret in a secret storage: ``path#key``, or ``path`` for the whole secret.

    Attributes:
        path: The secret's path in the storage (``etl/db``).
        key: The key in it (``password``); ``None`` for the whole secret.
        storage: The storage; ``None`` for the default one.
        default: What a reference reads when the secret or its key is missing
            (``.get(key, default)``); a key's only.
    """

    path: str
    key: str | None = None
    storage: str | None = None
    default: str | None = None

    def spec(self) -> str:
        """``path#key``, or ``path`` for the whole secret.

        Returns:
            The spec.
        """
        return self.path if self.key is None else f"{self.path}#{self.key}"

    def label(self) -> str:
        """As written in Python (decision 176).

        ``secrets['etl/db']['password']``, ``secrets.other['etl/db']``,
        ``secrets['etl/db'].get('password', '')``.

        Returns:
            The label.
        """
        whole = f"{storage_label(self.storage)}[{self.path!r}]"
        if self.key is None:
            return whole
        if self.default is not None:
            return f"{whole}.get({self.key!r}, {self.default!r})"
        return f"{whole}[{self.key!r}]"

    def __post_init__(self) -> None:
        """Validates the storage name, the path and the key (as the master does).

        Raises:
            ValueError: For a bad name, path or key.
        """
        reason: str | None = None
        if self.storage is not None:
            reason = check_storage_name(self.storage)
        if reason is None and len(self.path) > 1 and self.path.endswith("/"):
            reason = "a path doesn't end with `/` (a prefix is `SecretPrefix(...)`)"
        if reason is None:
            reason = check_secret_path(self.path, "path")
        if reason is None and self.key is not None:
            if not self.key:
                reason = "the key after `#` is empty"
            elif "#" in self.key or "\0" in self.key:
                reason = "a key contains neither `#` nor NUL"
        if reason is None and self.key is None and self.default is not None:
            reason = "a default is a key's (`.get(key, default)`)"
        if reason is not None:
            raise ValueError(f"`{self.label()}`: {reason}")

    def to_json(self) -> dict[str, Any]:
        """The reference as DAG JSON.

        Returns:
            ``{"kind": "secret", "path": ..., "key": ..., "storage": ..., "default": ...}``
            (``key``, ``storage`` and ``default`` only if set).
        """
        result: dict[str, Any] = {"kind": "secret", "path": self.path}
        if self.key is not None:
            result["key"] = self.key
        if self.storage is not None:
            result["storage"] = self.storage
        if self.default is not None:
            result["default"] = self.default
        return result


@dataclass(frozen=True, slots=True)
class SecretPrefixDecl:
    """Every secret under a prefix, declared in ``secrets=`` (``SecretPrefix(...)``).

    Attributes:
        prefix: The prefix, ending in ``/``.
        storage: The storage; ``None`` for the default one.
    """

    prefix: str
    storage: str | None = None

    def label(self) -> str:
        """As written in Python.

        Returns:
            ``SecretPrefix('connections/')``, with ``storage=`` when it names one.
        """
        if self.storage is None:
            return f"SecretPrefix({self.prefix!r})"
        return f"SecretPrefix({self.prefix!r}, storage={self.storage!r})"

    def __post_init__(self) -> None:
        """Validates the storage name and the prefix.

        Raises:
            ValueError: For a bad name or prefix.
        """
        reason: str | None = None
        if self.storage is not None:
            reason = check_storage_name(self.storage)
        if reason is None:
            if not self.prefix.endswith("/"):
                reason = "a prefix ends with `/`"
            else:
                reason = check_secret_path(self.prefix[:-1], "prefix")
        if reason is not None:
            raise ValueError(f"`{self.label()}`: {reason}")

    def to_json(self) -> dict[str, Any]:
        """The declaration as DAG JSON.

        Returns:
            ``{"kind": "prefix", "prefix": ..., "storage": ...}`` (``storage`` only if set).
        """
        result: dict[str, Any] = {"kind": "prefix", "prefix": self.prefix}
        if self.storage is not None:
            result["storage"] = self.storage
        return result


def covers(declared: SecretDecl, ref: SecretRef) -> bool:
    """Whether a declaration in ``secrets=`` lets a task read ``ref`` (as the agent decides).

    Args:
        declared: The declaration.
        ref: The secret read.

    Returns:
        ``True`` for the same secret (or a key of a whole one declared), or one under a
        declared prefix, in the same storage.
    """
    if declared.storage != ref.storage:
        return False
    if isinstance(declared, SecretPrefixDecl):
        return ref.path.startswith(declared.prefix)
    return declared.path == ref.path and declared.key in {None, ref.key}


type SecretDecl = SecretRef | SecretPrefixDecl
"""A secret a task declares in ``secrets=``: one (or a whole one), or a prefix."""


type Reference = OutputRef | OutcomeRef | DagCtxRef | ParamRef | SecretRef
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
