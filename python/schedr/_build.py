"""Internal machinery shared by ``Dag``, ``@task`` and the operators.

A ``with Dag(...)`` block pushes a :class:`DagBuilder`; tasks created inside it add nodes and
edges to it. Arguments are turned into DAG JSON values here, recording a data dependency for
every upstream output they reference.
"""

import dataclasses
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from types import FrameType
from typing import Any, Protocol, get_args, get_origin, is_typeddict

from schedr._model import (
    DagCtxField,
    DagCtxRef,
    DictType,
    Edge,
    EdgeKind,
    Expand,
    IdError,
    ListType,
    ListValue,
    LiteralValue,
    MapValue,
    ModelType,
    OutcomeRef,
    OutputRef,
    PythonKind,
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
from schedr._serialization import (
    CodecError,
    decode,
    encode_any,
    follow_path,
    is_assignable,
    schemas,
    type_name,
)
from schedr._serialization.annotations import (
    MAPPING_ORIGINS,
    SEQUENCE_ORIGINS,
    is_union,
    tuple_items,
    type_hints,
    unwrap,
)
from schedr._serialization.models import is_model
from schedr._templates import is_tstring
from schedr._templates import to_template as tstring_to_template
from schedr.outcomes import DEFAULT, OutcomeSpec, RunIf

type Upstream = tuple[str, tuple[str, ...]]
"""An upstream task a value references, with the outcomes its edge is taken on (``()``: any)."""


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
        preload: A Python task forks from the preloaded fork server.
        fresh_process: A Python task is always a new process.
        run_if: Its trigger rule.
    """

    retries: int | None = None
    retry_delay: timedelta | None = None
    timeout: timedelta | None = None
    queue: str | None = None
    max_output_size: int | None = None
    preload: bool | None = None
    fresh_process: bool | None = None
    run_if: RunIf | None = None

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
            preload=base.preload if self.preload is None else self.preload,
            fresh_process=base.fresh_process if self.fresh_process is None else self.fresh_process,
            run_if=base.run_if if self.run_if is None else self.run_if,
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
            preload=defaults.preload if self.preload is None else self.preload,
            fresh_process=(
                defaults.fresh_process if self.fresh_process is None else self.fresh_process
            ),
            run_if=defaults.run_if if self.run_if is None else self.run_if.value,
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


@dataclass
class GroupScope:
    """A task group being defined, or defined (``with group(...)``; decision 56A).

    Attributes:
        id: Its ID: its name, prefixed by its parents' (``load.eu``).
        parent: The group it is nested in.
        options: The options its tasks get unless they set their own (its parents' under
            its own).
        members: The tasks in it, nested groups' included, in definition order.
    """

    id: str
    parent: str | None
    options: TaskOptions
    members: list[str] = field(default_factory=list)


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
        self.functions: dict[str, Callable[..., Any]] = {}
        self.outcomes: dict[str, OutcomeSpec] = {}
        self.outcome_types: dict[str, dict[str, TypeDesc]] = {}
        # Arguments that take a reference, checked once the DAG is complete (check_nones).
        self.none_checks: list[tuple[str, str, OutputRef | OutcomeRef, TypeDesc]] = []
        # Mapped tasks (`.expand()`): their output is the list of their instances' outputs.
        self.mapped: set[str] = set()
        # Task groups, outer before inner, and the ones open now (innermost last).
        self.groups: dict[str, GroupScope] = {}
        self.open_groups: list[GroupScope] = []
        # Dependencies on groups, made once the DAG is complete (`make_group_dependencies`),
        # each with where it was written (`file:line`, for its errors).
        self.group_dependencies: list[tuple[object, object, str, str]] = []

    def qualified(self, task_id: str) -> str:
        """A task's ID inside the open groups: their prefix, then ``task_id``.

        Args:
            task_id: The task's own ID.

        Returns:
            E.g. ``load.eu.extract`` inside ``with group("load"): with group("eu"):``.
        """
        return f"{self.open_groups[-1].id}.{task_id}" if self.open_groups else task_id

    def open_group(self, name: str, options: TaskOptions) -> GroupScope:
        """Starts a group inside the open ones.

        Args:
            name: Its own name (one ID segment).
            options: Its own options (``defaults=`` with ``run_if=`` over them).

        Returns:
            The group, open.

        Raises:
            IdError: For an invalid name, one used twice in a parent, or one that is a task's
                ID.
        """
        if not isinstance(name, str) or "." in name:
            raise IdError(f"group name {name!r} must be one ID part, without `.`")
        group_id = self.qualified(name)
        validate_task_id(group_id)
        if group_id in self.groups:
            raise IdError(f"group `{group_id}` is defined twice in DAG `{self.dag_id}`")
        if group_id in self.tasks:
            raise IdError(f"group `{group_id}` has the ID of a task of DAG `{self.dag_id}`")
        parent = self.open_groups[-1] if self.open_groups else None
        inherited = parent.options if parent is not None else TaskOptions()
        scope = GroupScope(group_id, parent.id if parent else None, options.over(inherited))
        self.groups[group_id] = scope
        self.open_groups.append(scope)
        return scope

    def close_group(self, scope: GroupScope) -> None:
        """Ends a group's block, and those of the groups opened in it and left open.

        A block whose error was caught in the DAG file ends too: tasks created from here on
        aren't theirs.

        Args:
            scope: The group.
        """
        while scope in self.open_groups:
            self.open_groups.pop()

    def ends(self, scope: GroupScope, *, upstream: bool) -> list[str]:
        """A group's last tasks (``upstream``) or first tasks, from the DAG's edges now.

        Args:
            scope: The group.
            upstream: Its last tasks (none of its tasks after them) or its first ones.

        Returns:
            Their IDs, in definition order.

        Raises:
            ValueError: For a group whose tasks form a cycle (so none is first or last).
        """
        members = set(scope.members)
        inside = [e for e in self.edges if e.from_ in members and e.to in members]
        if upstream:
            found = [t for t in scope.members if not any(e.from_ == t for e in inside)]
        else:
            found = [t for t in scope.members if not any(e.to == t for e in inside)]
        if not found and members:
            cycle = self.find_cycle(within=members) or []
            raise ValueError(
                f"group `{scope.id}`'s tasks form a cycle: {' -> '.join(cycle)}, so none is "
                f"its {'last' if upstream else 'first'}"
            )
        return found

    def make_group_dependencies(self) -> None:
        """Makes the dependencies on groups (decision 56A), once every edge is known.

        A group stands for its last tasks upstream and its first tasks downstream, as the
        whole DAG's edges make them, wherever those edges were written. So a dependency that
        may add an edge inside a group is made before any that reads that group's ends,
        whatever the order they were written in (written order otherwise). The edges from a
        group's last tasks are plain order edges, a ``@branch``'s too (decision 94).

        Raises:
            ValueError: For a dependency on a group without tasks, one making a task depend on
                itself, or dependencies that each need the other's edges first.
        """
        written = list(self.group_dependencies)
        self.group_dependencies = []
        reads = [_groups_in(up) | _groups_in(down) for up, down, _, _ in written]
        # The groups a dependency may add an edge inside: one holding a task it may start
        # from and one it may go to (a group operand may stand for any of its tasks).
        writes = []
        for up, down, _, _ in written:
            starts, finishes = set(task_ids_in(up)), set(task_ids_in(down))
            writes.append(
                {
                    g.id
                    for g in self.groups.values()
                    if starts & set(g.members) and finishes & set(g.members)
                }
            )
        before = {
            n: {m for m in range(len(written)) if m != n and writes[m] & reads[n]}
            for n in range(len(written))
        }
        done: list[int] = []
        while len(done) < len(written):
            ready = [n for n in range(len(written)) if n not in done and before[n] <= set(done)]
            if not ready:
                stuck = [
                    f"`{written[n][2]}` at {written[n][3]}"
                    for n in range(len(written))
                    if n not in done
                ]
                raise ValueError(
                    "dependencies on groups each need the other's edges first "
                    f"({', '.join(stuck)}): write the tasks' own dependencies instead"
                )
            done.append(ready[0])
        for n in done:
            upstream, downstream, what, where = written[n]
            try:
                self._make_group_dependency(upstream, downstream, what)
            except ValueError as e:
                # Made after the line that wrote it: the message says where that was.
                raise ValueError(f"{where}: {e}") from None

    def _make_group_dependency(self, upstream: object, downstream: object, what: str) -> None:
        ups: Sequence[object] = upstream if isinstance(upstream, list | tuple) else [upstream]
        downs = task_ids_of(downstream, what, upstream=False)
        for item in ups:
            on = tuple(getattr(item, "_schedr_on", ()))
            group_end = isinstance(getattr(item, "_schedr_group", None), GroupScope)
            for up in task_ids_of(item, what):
                for down in downs:
                    self.add_edge(up, down, EdgeKind.ORDER, on, plain=group_end)

    def add_task(
        self,
        task_id: str,
        kind: TaskKind,
        output_type: TypeDesc,
        options: TaskOptions,
        outcomes: OutcomeSpec = DEFAULT,
        outcome_types: Mapping[str, TypeDesc] | None = None,
        expand: Expand | None = None,
    ) -> None:
        """Adds a task with its settings resolved against its groups' and the DAG's defaults.

        Args:
            task_id: The task's ID, qualified by its groups (see ``qualified``).
            kind: Its kind.
            output_type: The type of its output.
            options: Its own options.
            outcomes: Its outcomes.
            outcome_types: For an outcome union, each outcome's type (what ``.on()`` narrows
                to).
            expand: What a mapped task's instances vary over (``.expand()``).

        Raises:
            IdError: If a task with this ID already exists, or the ID is invalid.
        """
        validate_task_id(task_id)
        if task_id in self.tasks:
            raise IdError(
                f"task ID `{task_id}` is defined twice in DAG `{self.dag_id}`; give one a "
                "different ID (`task_id=` for operators, `.override(task_id=...)` for @task)"
            )
        if task_id in self.groups:
            raise IdError(f"task ID `{task_id}` is a group's ID in DAG `{self.dag_id}`")
        scope = self.open_groups[-1] if self.open_groups else None
        if scope is not None:
            options = options.over(scope.options)
            for group in self.open_groups:
                group.members.append(task_id)
        settings = options.over(self.defaults).settings()
        self.tasks[task_id] = Task(
            task_id,
            kind,
            output_type,
            settings,
            outcomes.declared(),
            group=scope.id if scope is not None else None,
            expand=expand,
        )
        if expand is not None:
            self.mapped.add(task_id)
        self.outcomes[task_id] = outcomes
        self.outcome_types[task_id] = dict(outcome_types or {})

    def add_edge(
        self, from_: str, to: str, kind: EdgeKind, on: tuple[str, ...] = (), *, plain: bool = False
    ) -> None:
        """Adds a dependency, once for each outcome condition.

        Args:
            from_: The upstream task's ID.
            to: The downstream task's ID.
            kind: Why the dependency exists.
            on: The upstream outcomes it is taken on; empty for any.
            plain: Kept as ``kind`` even from a ``@branch`` (made by a dependency on a group
                that ends with it, decision 94).

        Raises:
            ValueError: For a task depending on itself or on a task of another DAG.
        """
        for end in (from_, to):
            if end not in self.tasks:
                raise ValueError(f"task `{end}` is not part of DAG `{self.dag_id}`")
        if from_ == to:
            raise ValueError(f"task `{to}` can't depend on itself")
        task = self.tasks[from_]
        if isinstance(task.kind, PythonKind) and task.kind.branch and not plain:
            # Every edge out of a branch is taken only when it chose its target.
            kind = EdgeKind.SELECTION
        edge = Edge(from_, to, kind, on)
        if edge not in self.edges:
            self.edges.append(edge)

    def narrows(self, task_id: str, on: Sequence[str]) -> bool:
        """Whether ``.on(...)`` with the outcomes ``on`` leaves out some outcome of a task.

        Naming every outcome (``.on(Rows, Missing)`` for ``Rows | Missing``) is taken whatever
        the task ends with, like no condition at all.

        Args:
            task_id: The upstream task.
            on: The outcomes named; empty for no condition.

        Returns:
            True when an outcome it may end with isn't named.
        """
        if not on:
            return False
        spec = self.outcomes.get(task_id)
        return spec is None or not set(spec.names) <= set(on)

    def may_skip(self) -> set[str]:
        """The tasks that may be skipped, by their own rule over edges that may not be taken.

        An edge may not be taken when it has a condition (an outcome, a branch's choice) or its
        upstream task may be skipped. Under ``RunIf.NONE_FAILED`` a task is skipped only when
        every edge is, under ``RunIf.ALL_SUCCEEDED`` when any one is.

        Returns:
            Their IDs.
        """
        skippable: set[str] = set()
        changed = True
        while changed:
            changed = False
            for task_id, task in self.tasks.items():
                incoming = [e for e in self.edges if e.to == task_id]
                if task_id in skippable or not incoming:
                    continue
                not_taken = [
                    self.narrows(e.from_, e.on)
                    or e.kind == EdgeKind.SELECTION
                    or e.from_ in skippable
                    for e in incoming
                ]
                every = task.settings.run_if == RunIf.NONE_FAILED
                if all(not_taken) if every else any(not_taken):
                    skippable.add(task_id)
                    changed = True
        return skippable

    def check_nones(self) -> None:
        """Checks, once the DAG is complete, that every parameter that may get ``None`` allows it.

        Under ``RunIf.NONE_FAILED`` a reference whose edge isn't taken (narrowed with
        ``.on()`` to an outcome that didn't happen, or to a task that was skipped) passes
        ``None``, as does an outcome reference to a task that was skipped, at any depth of an
        argument; that happens only when the task runs anyway, through another edge (a
        different upstream task or condition). A task with no such edge is skipped instead.

        Raises:
            TypeError: For a parameter that may get ``None`` and doesn't allow it.
        """
        skippable = self.may_skip()
        partial = self.instances_may_skip()
        for task_id, where, reference, expected in self.none_checks:
            if task_id not in self.tasks:
                continue  # Its definition failed (the error was caught in the DAG block).
            if isinstance(reference, OutputRef) and reference.task in partial:
                _check_mapped_none(reference, expected, where)
            if self.tasks[task_id].settings.run_if != RunIf.NONE_FAILED:
                continue
            on = tuple(reference.on) if isinstance(reference, OutputRef) else ()
            if not (self.narrows(reference.task, on) or reference.task in skippable):
                continue
            own = (reference.task, on)
            if not any(e.to == task_id and (e.from_, e.on) != own for e in self.edges):
                continue
            if not is_assignable(ScalarType("none"), expected):
                raise TypeError(
                    f"{where}: `{reference_label(reference)}` is None when its edge isn't "
                    f"taken and the task runs through another; allow None "
                    f"(`{type_name(expected)} | None`), or give the task "
                    "run_if=RunIf.ALL_SUCCEEDED (skipped instead)"
                )

    def instances_may_skip(self) -> set[str]:
        """The mapped tasks some of whose instances may be skipped while others run.

        An instance is skipped when its element's edge isn't taken: it maps element by element
        over a narrowed mapped output (``.expand(r=results.on(Rows))``), or over a mapped task
        whose instances may be skipped.

        Returns:
            Their IDs.
        """
        found: set[str] = set()
        changed = True
        while changed:
            changed = False
            for task_id in self.mapped - found:
                expand = self.tasks[task_id].expand
                assert expand is not None
                for reference in expand.references():
                    if not isinstance(reference, OutputRef) or reference.task not in self.mapped:
                        continue
                    if self.narrows(reference.task, reference.on) or reference.task in found:
                        found.add(task_id)
                        changed = True
                        break
        return found

    def find_cycle(self, within: set[str] | None = None) -> list[str] | None:
        """A dependency cycle, if there is one.

        Args:
            within: Only among these tasks (a group's), not the whole DAG.

        Returns:
            The task IDs around a cycle (first repeated at the end), or ``None``.
        """
        tasks = [t for t in self.tasks if within is None or t in within]
        downstream: dict[str, list[str]] = {t: [] for t in tasks}
        for edge in self.edges:
            if edge.from_ in downstream and edge.to in downstream:
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

        for task in tasks:
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


def note_upstream(reference: Reference, upstream: list[Upstream]) -> None:
    """Records the upstream task of an output or outcome reference, with its condition.

    Args:
        reference: The reference.
        upstream: Receives ``(task, on)``.
    """
    if isinstance(reference, OutputRef):
        upstream.append((reference.task, tuple(reference.on)))
    elif isinstance(reference, OutcomeRef):
        upstream.append((reference.task, ()))


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


def typed_references(value: Value, expected: TypeDesc) -> list[tuple[Reference, TypeDesc]]:
    """The references inside a value, each with the type expected there.

    Args:
        value: A DAG JSON value.
        expected: The type it is passed as.

    Returns:
        The references at any depth, in order, with the type of the place each one fills (an
        element's type from its container's; ``Any`` where that can't be told).
    """
    match value:
        case RefValue(reference=reference):
            return [(reference, expected)]
        case ListValue(items=items):
            return [
                found
                for index, item in enumerate(items)
                for found in typed_references(item, _element_type(expected, index, "a list"))
            ]
        case MapValue(entries=entries):
            return [
                found
                for key in sorted(entries)
                for found in typed_references(entries[key], _element_type(expected, key, "a dict"))
            ]
    return []


def _element_type(container: TypeDesc, step: str | int, written: str) -> TypeDesc:
    """The type of one element of a container type (of each form a union allows).

    Raises:
        TypeError: When the type has no such element: the container written (``written``)
            can't be a value of it, e.g. a list where a model is expected.
    """
    variants = container.variants if isinstance(container, UnionType) else (container,)
    found = [t for v in variants if (t := follow_path(v, (step,))) is not None]
    if not found:
        raise TypeError(f"expects `{type_name(container)}`, but gets {written}")
    return found[0] if len(found) == 1 else UnionType(tuple(found))


def check_literals(value: Value, annotation: object, where: str) -> None:
    """Checks the literals inside a list or dict that also holds references (B27).

    A container of literals only is encoded against the parameter's type when it is
    converted; one mixing in references keeps its literals as they are, so each is checked
    here against its element's annotation, as stored data is when a task decodes it.

    Args:
        value: The converted argument.
        annotation: The parameter's annotation.
        where: The argument, for messages.

    Raises:
        TypeError: For a literal that isn't a value of its element's type.
    """
    match value:
        case ListValue(items=items):
            children: list[tuple[str | int, Value]] = list(enumerate(items))
        case MapValue(entries=entries):
            children = sorted(entries.items())
        case _:
            return
    for step, child in children:
        element = _element_annotation(annotation, step)
        if isinstance(child, LiteralValue):
            if element is not Any:
                try:
                    decode(child.value, element)
                except CodecError as e:
                    raise TypeError(f"{where}[{step!r}]: {e.reason}") from None
        else:
            check_literals(child, element, f"{where}[{step!r}]")


def _element_annotation(annotation: object, step: str | int) -> object:  # noqa: PLR0911
    """The annotation of one element of a container annotation (``Any`` when unclear)."""
    annotation = unwrap(annotation)
    if annotation is Any or is_union(annotation):
        return Any
    origin, args = get_origin(annotation), get_args(annotation)
    if isinstance(step, int):
        if origin in SEQUENCE_ORIGINS:
            return args[0] if args else Any
        if origin is tuple:
            items = tuple_items(args)
            return items[step] if step < len(items) else Any
        return Any
    if origin in MAPPING_ORIGINS:
        return args[1] if len(args) == 2 else Any  # noqa: PLR2004
    if is_model(annotation):
        field = getattr(annotation, "model_fields", {}).get(step)
        return field.annotation if field is not None else Any
    if isinstance(annotation, type) and (
        dataclasses.is_dataclass(annotation) or is_typeddict(annotation)
    ):
        return type_hints(annotation, f"`{annotation.__name__}`").get(step, Any)
    return Any


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


def to_value(value: object, where: str, upstream: list[Upstream]) -> Value:  # noqa: PLR0911
    """Converts an argument to a DAG JSON value.

    Args:
        value: The argument as written in the DAG file.
        where: The argument's name, for messages.
        upstream: Receives the upstream tasks whose outputs ``value`` references.

    Returns:
        The value, keeping references visible inside lists and dicts.

    Raises:
        TypeError: For a value that can't be stored (e.g. ``set``, ``Decimal``, NaN), or a
            dict with non-string keys.
    """
    if is_reference(value):
        reference: Reference = value._schedr_reference()  # type: ignore[attr-defined]
        note_upstream(reference, upstream)
        return RefValue(reference)
    if is_tstring(value):
        template = tstring_to_template(value, where)  # type: ignore[arg-type]
        for ref in template.references():
            note_upstream(ref, upstream)
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


def to_template(value: object, where: str, upstream: list[Upstream]) -> Template:
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
        note_upstream(reference, upstream)
        return Template([TemplateRef(reference)])
    if is_tstring(value):
        template = tstring_to_template(value, where)  # type: ignore[arg-type]
        for ref in template.references():
            note_upstream(ref, upstream)
        return template
    raise TypeError(
        f"{where}: expected a string, a t-string or a reference, got {type(value).__name__}"
    )


def task_ids_of(operand: object, what: str, *, upstream: bool = True) -> list[str]:
    """The task IDs a ``>>`` operand stands for.

    Args:
        operand: A task, an output (its task), a group, or a list of them.
        what: The operator, for messages.
        upstream: Which side the operand is on: a group stands for its leaves upstream and
            its roots downstream (decision 56A).

    Returns:
        The task IDs.

    Raises:
        TypeError: For anything else.
        ValueError: For a group without tasks.
    """
    items: Sequence[object] = operand if isinstance(operand, list | tuple) else [operand]
    ids: list[str] = []
    for item in items:
        scope = getattr(item, "_schedr_group", None)
        if isinstance(scope, GroupScope):
            ends = require_active(f"`{what}`").ends(scope, upstream=upstream)
            if not ends:
                raise ValueError(
                    f"`{what}`: group `{scope.id}` has no tasks, so nothing can depend on it"
                )
            ids.extend(ends)
            continue
        task_id = getattr(item, "_schedr_task_id", None)
        if not isinstance(task_id, str):
            raise TypeError(f"`{what}` needs tasks or outputs, got {type(item).__name__}")
        ids.append(task_id)
    return ids


_PACKAGE = Path(__file__).resolve().parent


def _written_at() -> str:
    """Where the DAG file's statement being run is.

    Returns:
        `file:line` of the first caller outside schedr, or `?`.
    """
    frame: FrameType | None = sys._getframe(1)  # the cheap way to the caller's line
    while frame is not None and Path(frame.f_code.co_filename).resolve().is_relative_to(_PACKAGE):
        frame = frame.f_back
    if frame is None:
        return "?"
    return f"{Path(frame.f_code.co_filename).name}:{frame.f_lineno}"


def _check_operand(operand: object, what: str) -> None:
    """Refuses a ``>>`` operand that is no task, output or group, where it is written."""
    items: Sequence[object] = operand if isinstance(operand, list | tuple) else [operand]
    for item in items:
        if isinstance(getattr(item, "_schedr_group", None), GroupScope):
            continue
        if not isinstance(getattr(item, "_schedr_task_id", None), str):
            raise TypeError(f"`{what}` needs tasks or outputs, got {type(item).__name__}")


def _groups_in(operand: object) -> set[str]:
    """The IDs of the groups among a ``>>`` operand."""
    items: Sequence[object] = operand if isinstance(operand, list | tuple) else [operand]
    return {
        scope.id
        for item in items
        if isinstance(scope := getattr(item, "_schedr_group", None), GroupScope)
    }


def task_ids_in(operand: object) -> list[str]:
    """Every task a ``>>`` operand involves: a group's members, a task's ID."""
    items: Sequence[object] = operand if isinstance(operand, list | tuple) else [operand]
    ids: list[str] = []
    for item in items:
        scope = getattr(item, "_schedr_group", None)
        if isinstance(scope, GroupScope):
            ids.extend(scope.members)
        elif isinstance(task_id := getattr(item, "_schedr_task_id", None), str):
            ids.append(task_id)
    return ids


def _has_group(*operands: object) -> bool:
    """Whether a group is among ``>>``'s operands."""
    for operand in operands:
        items: Sequence[object] = operand if isinstance(operand, list | tuple) else [operand]
        if any(isinstance(getattr(i, "_schedr_group", None), GroupScope) for i in items):
            return True
    return False


def order(upstream: object, downstream: object, what: str) -> None:
    """Records order-only dependencies: every downstream task runs after every upstream one.

    A group stands for its last tasks upstream and its first tasks downstream: a dependency
    on one is made when the DAG is complete, from all its edges (decision 56A).

    Args:
        upstream: Tasks, outputs (``f.on(X)`` too: then on those outcomes), groups, or lists
            of them.
        downstream: Tasks, outputs, groups, or lists of them.
        what: The operator, for messages.
    """
    builder = require_active(f"`{what}`")
    if _has_group(upstream, downstream):
        for operand in (upstream, downstream):
            _check_operand(operand, what)
        builder.group_dependencies.append((upstream, downstream, what, _written_at()))
        return
    ups: Sequence[object] = upstream if isinstance(upstream, list | tuple) else [upstream]
    downs = task_ids_of(downstream, what, upstream=False)
    for item in ups:
        on = tuple(getattr(item, "_schedr_on", ()))
        for up in task_ids_of(item, what):
            for down in downs:
                builder.add_edge(up, down, EdgeKind.ORDER, on)


_DAG_CTX_TYPES: dict[DagCtxField, TypeDesc] = {
    DagCtxField.RUN_ID: ScalarType("str"),
    DagCtxField.RUN_KIND: ScalarType("str"),
    DagCtxField.PLANNED_FOR: ScalarType("datetime"),
    DagCtxField.SINCE: ScalarType("datetime"),
    DagCtxField.DAG_ID: ScalarType("str"),
}


def may_have_field(desc: TypeDesc, name: str) -> bool:
    """Whether data of a type may have a field or key ``name``.

    It may when its type declares one, or is a dict or ``Any`` whose keys aren't known.

    Args:
        desc: The type.
        name: The field or key.

    Returns:
        True if it may.
    """
    if isinstance(desc, ScalarType):
        return desc.name == "any"
    if isinstance(desc, DictType):
        return True
    if isinstance(desc, UnionType):
        return any(may_have_field(v, name) for v in desc.variants)
    if isinstance(desc, ModelType):
        return schemas.part(desc, name) is not None
    fields = getattr(desc, "fields", None)
    return fields is not None and any(f.name == name for f in fields)


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
    if isinstance(reference, OutcomeRef):
        return f"{reference.task}.outcome"
    assert isinstance(reference, OutputRef)
    on = f".on({', '.join(reference.on)})" if reference.on else ""
    path = "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in reference.path)
    return f"{reference.task}{on}{path}"


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
    if reference.task not in builder.tasks:
        raise ValueError(f"task `{reference.task}` is not part of DAG `{builder.dag_id}`")
    if isinstance(reference, OutcomeRef):
        # The outcome's name, decoded into the task's enum by the receiving parameter (an
        # Outcome enum is described as `str`).
        return ScalarType("str")
    assert isinstance(reference, OutputRef)
    produced = builder.tasks[reference.task].output_type
    by_outcome = builder.outcome_types.get(reference.task, {})
    if reference.on and by_outcome:
        narrowed = [by_outcome[name] for name in reference.on]
        produced = narrowed[0] if len(narrowed) == 1 else UnionType(tuple(narrowed))
    if reference.task in builder.mapped:
        return _mapped_type(reference, produced, where)
    if by_outcome and reference.path and not reference.on:
        raise TypeError(
            f"{where}: `{reference_label(reference)}`: the output of `{reference.task}` is one of "
            f"its outcomes ({', '.join(by_outcome)}); narrow it first, e.g. "
            f"`{reference.task}.on({next(iter(by_outcome))}).{reference.path[0]}`"
        )
    found = follow_path(produced, tuple(reference.path))
    if found is None:
        raise TypeError(
            f"{where}: `{reference_label(reference)}` doesn't exist: the output of "
            f"`{reference.task}` is `{type_name(produced)}`"
        )
    return found


def _check_mapped_none(reference: OutputRef, expected: TypeDesc, where: str) -> None:
    """Refuses a parameter that can't take ``None`` for a skipped instance's element.

    Raises:
        TypeError: When the element type (or the indexed element) doesn't allow ``None``.
    """
    element = follow_path(expected, (0,)) if not reference.path else expected
    if element is None or is_assignable(ScalarType("none"), element):
        return
    raise TypeError(
        f"{where}: some instances of the mapped task `{reference.task}` may be skipped, and "
        f"`{reference_label(reference)}` has None for them; allow None "
        f"(`{type_name(element)} | None`{' in the list' if not reference.path else ''})"
    )


def _mapped_type(reference: OutputRef, element: TypeDesc, where: str) -> TypeDesc:
    """The type a reference to a mapped task's output stands for.

    The list of its instances' outputs (each ``element``), or a part of one by index
    (``results[0].rows``); a field of every element (``results.rows``) is refused.
    """
    path = tuple(reference.path)
    if path and isinstance(path[0], str):
        raise TypeError(
            f"{where}: `{reference_label(reference)}`: `{reference.task}` is mapped, so its "
            "output is a list (one element per instance); index it (`"
            f"{reference.task}[0].{path[0]}`), or gather the field in a task"
        )
    found = follow_path(ListType(element), path)
    if found is None:
        raise TypeError(
            f"{where}: `{reference_label(reference)}` doesn't exist: the output of "
            f"`{reference.task}` is `list[{type_name(element)}]`"
        )
    return found


def is_renderable(desc: TypeDesc) -> bool:
    """Whether values of a type can be rendered into a command line or a template.

    Args:
        desc: The type.

    Returns:
        True for strings, numbers, booleans, dates, times, durations and ``Any`` (and unions
        of them), and a model's field holding text, a number or a boolean (a model's date is
        its text: a format spec such as ``%Y`` doesn't apply to it); ``None``, lists, dicts
        and classes can't be rendered.
    """
    if isinstance(desc, UnionType):
        return all(is_renderable(v) for v in desc.variants)
    if isinstance(desc, ModelType):
        # A model's field as Pydantic writes it: text, a number or a boolean.
        return schemas.is_scalar(desc)
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
