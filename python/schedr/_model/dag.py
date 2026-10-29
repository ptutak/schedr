"""A DAG in DAG JSON: what the runner sends after parsing a DAG file.

Mirrors ``schedr-model`` (``dag.rs``).
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from schedr._model.ids import IdError, validate_dag_id, validate_task_id
from schedr._model.tasks import Task
from schedr._model.timestamps import format_optional_timestamp
from schedr._model.trigger import NoTrigger, Trigger
from schedr._model.version import SchedrVersion


@dataclass(frozen=True, slots=True)
class DagLocation:
    """Where a DAG is defined: a DAG root and a file path relative to it.

    Attributes:
        root: The root's name.
        path: The file's path relative to the root, with ``/`` separators.
    """

    root: str
    path: str

    def to_json(self) -> dict[str, Any]:
        """The location as DAG JSON.

        Returns:
            ``{"root": ..., "path": ...}``.
        """
        return {"root": self.root, "path": self.path}


class EdgeKind(StrEnum):
    """Why one task depends on another."""

    DATA = "data"
    """The downstream task takes the upstream task's output."""

    ORDER = "order"
    """An order-only dependency (``>>``)."""

    SELECTION = "selection"
    """From a ``@branch`` task: taken when the branch chose the downstream task."""


@dataclass(frozen=True, slots=True)
class Edge:
    """A dependency: ``to`` runs after ``from_``.

    Attributes:
        from_: The upstream task's ID.
        to: The downstream task's ID.
        kind: Why the dependency exists.
        on: The upstream outcomes it is taken on (``.on(...)``); empty for any.
    """

    from_: str
    to: str
    kind: EdgeKind
    on: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validates both task IDs.

        Raises:
            IdError: If either is invalid.
        """
        validate_task_id(self.from_)
        validate_task_id(self.to)

    def to_json(self) -> dict[str, Any]:
        """The edge as DAG JSON.

        Returns:
            ``{"from": ..., "to": ..., "kind": ..., "on": [...]}`` (``on`` only if not empty).
        """
        result: dict[str, Any] = {"from": self.from_, "to": self.to, "kind": self.kind.value}
        if self.on:
            result["on"] = list(self.on)
        return result


@dataclass(frozen=True, slots=True)
class Group:
    """A group of tasks (decision 64): a name for some of a DAG's tasks, for the UI and API.

    Attributes:
        id: The group's ID (``load``, ``load.eu`` when nested).
        parent: The group it is nested in, if any.
    """

    id: str
    parent: str | None = None

    def to_json(self) -> dict[str, Any]:
        """The group as DAG JSON.

        Returns:
            ``{"id": ...}``, with ``"parent"`` when nested.
        """
        result: dict[str, Any] = {"id": self.id}
        if self.parent is not None:
            result["parent"] = self.parent
        return result


@dataclass(frozen=True, slots=True)
class Dag:
    """A DAG: its identity, trigger, tasks and dependencies.

    Attributes:
        id: The DAG's ID.
        location: Where it is defined.
        tasks: The tasks, in declaration order.
        edges: The dependencies between them.
        trigger: What starts its runs (besides people and other DAGs).
        initial_since: ``since`` of the run on the DAG's first fire time.
        max_active_runs: At most this many runs at a time; ``None`` for no limit.
        description: Shown in the UI.
        tags: Shown in the UI and used for filtering.
        produced_by: The schedr version producing this JSON (the installed one by default).
        groups: The task groups, outer before inner.
    """

    id: str
    location: DagLocation
    tasks: Sequence[Task] = ()
    edges: Sequence[Edge] = ()
    trigger: Trigger = field(default_factory=NoTrigger)
    initial_since: datetime | None = None
    max_active_runs: int | None = 1
    description: str | None = None
    tags: Sequence[str] = ()
    produced_by: SchedrVersion = field(default_factory=SchedrVersion.current)
    groups: Sequence[Group] = ()

    def __post_init__(self) -> None:
        """Validates the DAG ID, unique task IDs, edges between known tasks and the limits.

        Raises:
            IdError: If the DAG ID is invalid, or two tasks have the same ID.
            ValueError: If an edge names an unknown task or ``max_active_runs`` isn't
                positive.
        """
        validate_dag_id(self.id)
        seen: set[str] = set()
        for task in self.tasks:
            if task.id in seen:
                raise IdError(f"task ID `{task.id}` is defined twice in DAG `{self.id}`")
            seen.add(task.id)
        for edge in self.edges:
            for end in (edge.from_, edge.to):
                if end not in seen:
                    raise ValueError(f"edge {edge.from_} -> {edge.to}: no task `{end}`")
        if self.max_active_runs is not None and self.max_active_runs <= 0:
            raise ValueError(f"max_active_runs must be positive, got {self.max_active_runs}")

    def to_json(self) -> dict[str, Any]:
        """The DAG as DAG JSON.

        Returns:
            The document the master stores; ``description`` and ``tags`` only when set.

        Raises:
            ValueError: If a timestamp is naive.
        """
        result: dict[str, Any] = {
            "produced_by": str(self.produced_by),
            "id": self.id,
            "location": self.location.to_json(),
        }
        if self.description is not None:
            result["description"] = self.description
        if self.tags:
            result["tags"] = list(self.tags)
        result |= {
            "trigger": self.trigger.to_json(),
            "initial_since": format_optional_timestamp(self.initial_since),
            "max_active_runs": self.max_active_runs,
            "tasks": [t.to_json() for t in self.tasks],
            "edges": [e.to_json() for e in self.edges],
        }
        if self.groups:
            result["groups"] = [g.to_json() for g in self.groups]
        return result
