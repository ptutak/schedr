"""Internal machinery shared by ``Dag``, ``@task`` and the operators.

A ``with Dag(...)`` block pushes a :class:`DagBuilder`; tasks created inside it add nodes and
edges to it. Arguments are turned into DAG JSON values here, recording a data dependency for
every upstream output they reference.
"""

import dataclasses
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

from schedr._model import (
    DagCtxField,
    DagCtxRef,
    Edge,
    EdgeKind,
    IdError,
    ListValue,
    LiteralValue,
    MapValue,
    OutputRef,
    Reference,
    RefValue,
    ScalarType,
    Task,
    TaskKind,
    TaskSettings,
    Template,
    TemplateRef,
    TemplateValue,
    TypeDesc,
    UnionType,
    Value,
    validate_task_id,
)
from schedr._serialization import CodecError, encode_any, follow_path, type_name
from schedr._templates import is_tstring
from schedr._templates import to_template as tstring_to_template


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


def _note_upstream(reference: Reference, upstream: list[str]) -> None:
    """Records the upstream task of an output reference."""
    if isinstance(reference, OutputRef):
        upstream.append(reference.task)


def _hides_tstring(value: object) -> bool:
    """Whether a container that isn't searched (a set, a dataclass, ...) holds a t-string."""
    if isinstance(value, set | frozenset):
        return any(is_tstring(v) or _hides_tstring(v) for v in value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return any(
            is_tstring(getattr(value, f.name)) or _hides_tstring(getattr(value, f.name))
            for f in dataclasses.fields(value)
        )
    return False


def templates_in(value: Value) -> list[Template]:
    """Every template inside a value, at any depth.

    Args:
        value: A DAG JSON value.

    Returns:
        The templates, in order.
    """
    if isinstance(value, TemplateValue):
        return [value.template]
    if isinstance(value, ListValue):
        return [t for item in value.items for t in templates_in(item)]
    if isinstance(value, MapValue):
        return [t for key in sorted(value.entries) for t in templates_in(value.entries[key])]
    return []


def to_value(value: object, where: str, upstream: list[str]) -> Value:  # noqa: PLR0911
    """Converts an argument to a DAG JSON value.

    Args:
        value: The argument as written in the DAG file.
        where: The argument's name, for messages.
        upstream: Receives the IDs of the upstream tasks whose outputs ``value`` references.

    Returns:
        The value, keeping references visible inside lists and dicts.

    Raises:
        TypeError: For a value that can't be stored (e.g. ``set``, ``Decimal``, NaN), or a
            dict with non-string keys.
    """
    if is_reference(value):
        reference: Reference = value._schedr_reference()  # type: ignore[attr-defined]
        _note_upstream(reference, upstream)
        return RefValue(reference)
    if is_tstring(value):
        template = tstring_to_template(value, where, shell=False)  # type: ignore[arg-type]
        for ref in template.references():
            _note_upstream(ref, upstream)
        return TemplateValue(template)
    if isinstance(value, Mapping):
        items: dict[str, Value] = {}
        for key, item in value.items():
            if is_tstring(key):
                raise TypeError(f"{where}: a t-string can't be a dictionary key")
            if not isinstance(key, str):
                raise TypeError(f"{where}: dictionary keys must be str, got {type(key).__name__}")
            items[key] = to_value(item, f"{where}[{key!r}]", upstream)
        if all(isinstance(v, LiteralValue) for v in items.values()):
            return LiteralValue({k: v.value for k, v in items.items()})  # type: ignore[union-attr]
        return MapValue(items)
    if isinstance(value, list | tuple):
        elements = [to_value(item, f"{where}[{i}]", upstream) for i, item in enumerate(value)]
        if all(isinstance(e, LiteralValue) for e in elements):
            plain = [e.value for e in elements]  # type: ignore[union-attr]
            return LiteralValue({"$tuple": plain} if isinstance(value, tuple) else plain)
        return ListValue(elements)
    if _hides_tstring(value):
        raise TypeError(
            f"{where}: a t-string inside a {type(value).__name__} isn't filled in (only lists, "
            "tuples and dict values are searched); convert it, e.g. a set to a list"
        )
    try:
        return LiteralValue(encode_any(value))
    except CodecError as e:
        raise TypeError(f"{where}: {e.reason}") from None


def to_template(value: object, where: str, upstream: list[str]) -> Template:
    """Converts one command-line argument or environment value to a template.

    Args:
        value: A string, a t-string, or a reference (rendered at run time).
        where: The argument's name, for messages.
        upstream: Receives the IDs of the upstream tasks it references.

    Returns:
        The template.

    Raises:
        TypeError: For anything but a string, a t-string or a reference.
    """
    if isinstance(value, str):
        return Template([value])
    if is_reference(value):
        reference: Reference = value._schedr_reference()  # type: ignore[attr-defined]
        _note_upstream(reference, upstream)
        return Template([TemplateRef(reference)])
    if is_tstring(value):
        template = tstring_to_template(value, where, shell=False)  # type: ignore[arg-type]
        for ref in template.references():
            _note_upstream(ref, upstream)
        return template
    raise TypeError(
        f"{where}: expected a string, a t-string or a reference, got {type(value).__name__}"
    )


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


_DAG_CTX_TYPES: dict[DagCtxField, TypeDesc] = {
    DagCtxField.RUN_ID: ScalarType("str"),
    DagCtxField.RUN_KIND: ScalarType("str"),
    DagCtxField.PLANNED_FOR: ScalarType("datetime"),
    DagCtxField.SINCE: ScalarType("datetime"),
    DagCtxField.DAG_ID: ScalarType("str"),
}


def reference_label(reference: Reference) -> str:
    """A reference as written in the DAG file, for messages.

    Args:
        reference: The reference.

    Returns:
        E.g. ``extract.rows[0]`` or ``dag_ctx.run.planned_for``.
    """
    if isinstance(reference, DagCtxRef):
        name = reference.field.value
        return "dag_ctx.dag_id" if name == "dag_id" else f"dag_ctx.run.{name.removeprefix('run_')}"
    assert isinstance(reference, OutputRef)
    path = "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in reference.path)
    return f"{reference.task}{path}"


_RENDERABLE = frozenset({"any", "str", "int", "float", "bool", "datetime", "date", "timedelta"})


def reference_type(builder: DagBuilder, reference: Reference, where: str) -> TypeDesc:
    """The type of the data a reference stands for.

    Args:
        builder: The DAG being defined (for upstream output types).
        reference: The reference.
        where: Where it is used, for messages.

    Returns:
        Its type.

    Raises:
        TypeError: If the path doesn't exist in the upstream output's type.
        ValueError: If the upstream task belongs to another DAG.
    """
    if isinstance(reference, DagCtxRef):
        return _DAG_CTX_TYPES[reference.field]
    assert isinstance(reference, OutputRef)
    if reference.task not in builder.tasks:
        raise ValueError(f"task `{reference.task}` is not part of DAG `{builder.dag_id}`")
    produced = builder.tasks[reference.task].output_type
    found = follow_path(produced, tuple(reference.path))
    if found is None:
        raise TypeError(
            f"{where}: `{reference_label(reference)}` doesn't exist: the output of "
            f"`{reference.task}` is `{type_name(produced)}`"
        )
    return found


def is_renderable(desc: TypeDesc) -> bool:
    """Whether values of a type can be rendered into a command line or a template.

    Args:
        desc: The type.

    Returns:
        True for strings, numbers, booleans, dates, times, durations and ``Any`` (and unions
        of them); ``None``, lists, dicts and classes can't be rendered.
    """
    if isinstance(desc, UnionType):
        return all(is_renderable(v) for v in desc.variants)
    return isinstance(desc, ScalarType) and desc.name in _RENDERABLE


def _is_date_like(desc: TypeDesc) -> bool:
    if isinstance(desc, UnionType):
        return all(_is_date_like(v) for v in desc.variants)
    return isinstance(desc, ScalarType) and desc.name in ("datetime", "date", "any")


def check_template_references(builder: DagBuilder, template: Template, where: str) -> None:
    """Checks that every reference in a template exists and can be rendered.

    Args:
        builder: The DAG being defined.
        template: The template.
        where: Where it is used, for messages.

    Raises:
        TypeError: For a missing path, a type that can't be rendered (e.g. ``None`` or a
            list), or a strftime spec on something that isn't a date or a time.
    """
    for part in template.parts:
        if not isinstance(part, TemplateRef):
            continue
        desc = reference_type(builder, part.reference, where)
        if not is_renderable(desc):
            raise TypeError(
                f"{where}: a value of type `{type_name(desc)}` can't be rendered into text; "
                "pass a part of it (e.g. a field), or convert it in a task"
            )
        if part.format and "%" in part.format and not _is_date_like(desc):
            raise TypeError(
                f"{where}: `:{part.format}` formats dates and times, but "
                f"`{reference_label(part.reference)}` is `{type_name(desc)}`"
            )
