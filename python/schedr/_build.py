"""Internal machinery shared by ``Dag``, ``@task`` and the operators.

A ``with Dag(...)`` block pushes a :class:`DagBuilder`; tasks created inside it add nodes and
edges to it. Arguments are turned into DAG JSON values here, recording a data dependency for
every upstream output they reference.
"""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

from schedr._model import (
    Edge,
    EdgeKind,
    IdError,
    ListValue,
    LiteralValue,
    MapValue,
    Reference,
    RefValue,
    ScalarType,
    Task,
    TaskKind,
    TaskSettings,
    Template,
    TemplateRef,
    TypeDesc,
    Value,
    validate_task_id,
)


class HasReference(Protocol):
    """Something that stands for data known only at run time (an output, ``dag_ctx``)."""

    def _schedr_reference(self) -> Reference:
        """The reference in DAG JSON."""
        ...


def is_reference(value: object) -> bool:
    """Whether ``value`` stands for run-time data.

    Args:
        value: Any object.

    Returns:
        True for outputs and ``dag_ctx`` references.
    """
    return callable(getattr(value, "_schedr_reference", None))


@dataclass(frozen=True, slots=True)
class TaskOptions:
    """Settings given on a task or in ``TaskDefaults``; ``None`` means "not set here".

    Attributes:
        retries: How many times a failed attempt is retried.
        retry_delay: The delay before each retry.
        timeout: The attempt's timeout.
        queue: The queue the task goes to.
        max_output_size: The output size limit in bytes.
    """

    retries: int | None = None
    retry_delay: timedelta | None = None
    timeout: timedelta | None = None
    queue: str | None = None
    max_output_size: int | None = None

    def over(self, base: TaskOptions) -> TaskOptions:
        """These options, falling back to ``base`` where not set.

        Args:
            base: The options that apply where these don't set a value.

        Returns:
            The combined options.
        """
        return TaskOptions(
            retries=base.retries if self.retries is None else self.retries,
            retry_delay=base.retry_delay if self.retry_delay is None else self.retry_delay,
            timeout=base.timeout if self.timeout is None else self.timeout,
            queue=base.queue if self.queue is None else self.queue,
            max_output_size=(
                base.max_output_size if self.max_output_size is None else self.max_output_size
            ),
        )

    def settings(self) -> TaskSettings:
        """The resolved settings, with the built-in defaults where nothing is set.

        Returns:
            The task's settings in DAG JSON.

        Raises:
            ValueError: If a duration isn't a whole number of milliseconds or is out of range.
        """
        defaults = TaskSettings()
        return TaskSettings(
            retries=defaults.retries if self.retries is None else self.retries,
            retry_delay_ms=(
                defaults.retry_delay_ms
                if self.retry_delay is None
                else to_ms(self.retry_delay, "retry_delay")
            ),
            timeout_ms=None if self.timeout is None else to_ms(self.timeout, "timeout"),
            queue=defaults.queue if self.queue is None else self.queue,
            max_output_size=(
                defaults.max_output_size if self.max_output_size is None else self.max_output_size
            ),
        )


def to_ms(duration: timedelta, name: str) -> int:
    """A duration in whole milliseconds.

    Args:
        duration: The duration.
        name: The argument's name, for messages.

    Returns:
        The duration in milliseconds.

    Raises:
        TypeError: If it isn't a ``timedelta``.
        ValueError: If it is negative or has a fraction of a millisecond.
    """
    if not isinstance(duration, timedelta):
        raise TypeError(f"{name} must be a timedelta, got {type(duration).__name__}")
    micros = (duration.days * 86_400 + duration.seconds) * 1_000_000 + duration.microseconds
    if micros < 0:
        raise ValueError(f"{name} must not be negative, got {duration}")
    if micros % 1000:
        raise ValueError(f"{name} must be a whole number of milliseconds, got {duration}")
    return micros // 1000


class DagBuilder:
    """The tasks and edges of the DAG being defined."""

    def __init__(self, dag_id: str, defaults: TaskOptions) -> None:
        """Starts an empty DAG.

        Args:
            dag_id: The DAG's ID, for messages.
            defaults: The DAG's task defaults.
        """
        self.dag_id = dag_id
        self.defaults = defaults
        self.tasks: dict[str, Task] = {}
        self.edges: list[Edge] = []

    def add_task(
        self, task_id: str, kind: TaskKind, output_type: TypeDesc, options: TaskOptions
    ) -> None:
        """Adds a task with its settings resolved against the DAG's defaults.

        Args:
            task_id: The task's ID.
            kind: Its kind.
            output_type: The type of its output.
            options: Its own options.

        Raises:
            IdError: If a task with this ID already exists, or the ID is invalid.
        """
        validate_task_id(task_id)
        if task_id in self.tasks:
            raise IdError(
                f"task ID `{task_id}` is defined twice in DAG `{self.dag_id}`; give one a "
                "different ID (`task_id=` for operators, `.override(task_id=...)` for @task)"
            )
        settings = options.over(self.defaults).settings()
        self.tasks[task_id] = Task(task_id, kind, output_type, settings)

    def add_edge(self, from_: str, to: str, kind: EdgeKind) -> None:
        """Adds a dependency, once.

        Args:
            from_: The upstream task's ID.
            to: The downstream task's ID.
            kind: Why the dependency exists.

        Raises:
            ValueError: For a task depending on itself or on a task of another DAG.
        """
        for end in (from_, to):
            if end not in self.tasks:
                raise ValueError(f"task `{end}` is not part of DAG `{self.dag_id}`")
        if from_ == to:
            raise ValueError(f"task `{to}` can't depend on itself")
        edge = Edge(from_, to, kind)
        if edge not in self.edges:
            self.edges.append(edge)

    def find_cycle(self) -> list[str] | None:
        """A dependency cycle, if there is one.

        Returns:
            The task IDs around a cycle (first repeated at the end), or ``None``.
        """
        downstream: dict[str, list[str]] = {t: [] for t in self.tasks}
        for edge in self.edges:
            downstream[edge.from_].append(edge.to)
        state: dict[str, int] = {}
        path: list[str] = []

        def visit(task: str) -> list[str] | None:
            state[task] = 1
            path.append(task)
            for nxt in downstream[task]:
                if state.get(nxt) == 1:
                    return [*path[path.index(nxt) :], nxt]
                if nxt not in state and (cycle := visit(nxt)) is not None:
                    return cycle
            path.pop()
            state[task] = 2
            return None

        for task in self.tasks:
            if task not in state and (cycle := visit(task)) is not None:
                return cycle
        return None


_ACTIVE: list[DagBuilder] = []


def push(builder: DagBuilder) -> None:
    """Makes ``builder`` the DAG being defined.

    Args:
        builder: The new DAG's builder.

    Raises:
        RuntimeError: If another DAG is being defined (DAGs can't be nested).
    """
    if _ACTIVE:
        raise RuntimeError(
            f"`with Dag({builder.dag_id!r})` inside `with Dag({_ACTIVE[-1].dag_id!r})`: "
            "DAGs can't be nested"
        )
    _ACTIVE.append(builder)


def pop() -> None:
    """Ends the DAG being defined."""
    _ACTIVE.pop()


def active() -> DagBuilder | None:
    """The DAG being defined, if any.

    Returns:
        Its builder, or ``None`` outside ``with Dag(...)``.
    """
    return _ACTIVE[-1] if _ACTIVE else None


def require_active(what: str) -> DagBuilder:
    """The DAG being defined.

    Args:
        what: What is being created, for the message.

    Returns:
        Its builder.

    Raises:
        RuntimeError: Outside ``with Dag(...)``.
    """
    builder = active()
    if builder is None:
        raise RuntimeError(f"{what} must be created inside `with Dag(...)`")
    return builder


def _literal(value: object, where: str) -> object:
    """Checks a literal value (JSON types only, until tagged types arrive in 1.2)."""
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{where}: NaN and infinity can't be stored")
        return value
    raise TypeError(
        f"{where}: values of type {type(value).__name__} can't be passed yet "
        "(supported: None, bool, int, float, str, and lists and dicts of them)"
    )


def to_value(value: object, where: str, upstream: list[str]) -> Value:
    """Converts an argument to a DAG JSON value.

    Args:
        value: The argument as written in the DAG file.
        where: The argument's name, for messages.
        upstream: Receives the IDs of the upstream tasks whose outputs ``value`` references.

    Returns:
        The value, keeping references visible inside lists and dicts.

    Raises:
        TypeError: For a value that can't be stored, or a dict with non-string keys.
        ValueError: For NaN or infinity.
    """
    if is_reference(value):
        reference: Reference = value._schedr_reference()  # type: ignore[attr-defined]
        task = getattr(reference, "task", None)
        if task is not None:
            upstream.append(task)
        return RefValue(reference)
    if isinstance(value, Mapping):
        items: dict[str, Value] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{where}: dictionary keys must be str, got {type(key).__name__}")
            items[key] = to_value(item, f"{where}[{key!r}]", upstream)
        if all(isinstance(v, LiteralValue) for v in items.values()):
            return LiteralValue({k: v.value for k, v in items.items()})  # type: ignore[union-attr]
        return MapValue(items)
    if isinstance(value, list | tuple):
        elements = [to_value(item, f"{where}[{i}]", upstream) for i, item in enumerate(value)]
        if all(isinstance(e, LiteralValue) for e in elements):
            return LiteralValue([e.value for e in elements])  # type: ignore[union-attr]
        return ListValue(elements)
    return LiteralValue(_literal(value, where))


def to_template(value: object, where: str, upstream: list[str]) -> Template:
    """Converts one command-line argument or environment value to a template.

    Args:
        value: A string, or a reference (rendered at run time).
        where: The argument's name, for messages.
        upstream: Receives the IDs of the upstream tasks it references.

    Returns:
        The template.

    Raises:
        TypeError: For anything but a string or a reference.
    """
    if isinstance(value, str):
        return Template([value])
    if is_reference(value):
        reference: Reference = value._schedr_reference()  # type: ignore[attr-defined]
        task = getattr(reference, "task", None)
        if task is not None:
            upstream.append(task)
        return Template([TemplateRef(reference)])
    raise TypeError(f"{where}: expected a string or a reference, got {type(value).__name__}")


def task_ids_of(operand: object, what: str) -> list[str]:
    """The task IDs a ``>>`` operand stands for.

    Args:
        operand: A task, an output (its task), or a list of them.
        what: The operator, for messages.

    Returns:
        The task IDs.

    Raises:
        TypeError: For anything else.
    """
    items: Sequence[object] = operand if isinstance(operand, list | tuple) else [operand]
    ids: list[str] = []
    for item in items:
        task_id = getattr(item, "_schedr_task_id", None)
        if not isinstance(task_id, str):
            raise TypeError(f"`{what}` needs tasks or outputs, got {type(item).__name__}")
        ids.append(task_id)
    return ids


def order(upstream: object, downstream: object, what: str) -> None:
    """Records order-only dependencies: every downstream task runs after every upstream one.

    Args:
        upstream: Tasks, outputs, or lists of them.
        downstream: Tasks, outputs, or lists of them.
        what: The operator, for messages.
    """
    builder = require_active(f"`{what}`")
    for up in task_ids_of(upstream, what):
        for down in task_ids_of(downstream, what):
            builder.add_edge(up, down, EdgeKind.ORDER)


def any_type() -> TypeDesc:
    """The output type recorded until return annotations are converted (milestone 1.2).

    Returns:
        ``Any``.
    """
    return ScalarType("any")
