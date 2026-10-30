"""The run's context, ``dag_ctx``, and the running task's own, ``task_ctx``.

``dag_ctx`` is the current run's information.

In the DAG body, ``dag_ctx.run.planned_for`` is a **reference**, filled in at run time; it
can be passed to tasks and commands, but not used as a value in the DAG file. Inside a
running task (while the runner calls its function) the same expression gives the **value**
(see PROJECT.md, Run parameters and the DAG context). Anywhere else it fails ("must be run
inside a schedr task"); tests set values with ``schedr.testing.task_context(...)``.
``dag_ctx.params`` comes with milestone M4.
"""

import contextlib
from collections.abc import Callable, Iterator
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime

from schedr import _build
from schedr._model import DagCtxField, DagCtxRef, Reference

_VALUE_IN_DAG_BODY = (
    "a reference can't be used as a value in the DAG file (it is known only at run time); "
    "pass it to a task, or decide in a task"
)

_OUTSIDE = (
    "`dag_ctx` must be run inside a schedr task (it gives references inside `with Dag(...)` "
    "and values while a task runs); in tests, set values with "
    "`schedr.testing.task_context(...)`"
)


@dataclass(frozen=True, slots=True)
class DagCtxReference:
    """A field of the run's context, as a reference in the DAG body.

    Attributes:
        field: Which field.
    """

    field: DagCtxField

    def _schedr_reference(self) -> Reference:
        return DagCtxRef(self.field)

    def __bool__(self) -> bool:
        """Refuses to be used as a value.

        Raises:
            TypeError: Always.
        """
        raise TypeError(_VALUE_IN_DAG_BODY)

    def __str__(self) -> str:
        """Refuses to be formatted into a plain string (use a t-string or pass it directly).

        Raises:
            TypeError: Always.
        """
        raise TypeError(_VALUE_IN_DAG_BODY)


@dataclass(frozen=True, slots=True)
class RunValues:
    """The run's context as values, inside a running task.

    Attributes:
        dag_id: The DAG's ID.
        run_id: The run's ID.
        kind: The run's kind.
        planned_for: The moment the run is for.
        since: The start of the run's window.
    """

    dag_id: str
    run_id: str
    kind: str
    planned_for: datetime
    since: datetime


@dataclass(frozen=True, slots=True)
class TaskValues:
    """The running task's own context (decision 84).

    Attributes:
        task_id: The task's ID.
        try_number: The attempt's try number, from 1.
        map_index: Which instance of a mapped task, from 0; ``None`` for an unmapped task.
    """

    task_id: str
    try_number: int
    map_index: int | None


_RUNNING: ContextVar[RunValues | None] = ContextVar("schedr_running_task", default=None)
_TASK: ContextVar[TaskValues | None] = ContextVar("schedr_running_task_own", default=None)


@contextlib.contextmanager
def running(values: RunValues, task: TaskValues | None = None) -> Iterator[None]:
    """Makes ``dag_ctx`` give values inside the block, and ``task_ctx`` the task's own.

    The runner calls a task's function inside it; references stand for values there.

    Args:
        values: The run's context.
        task: The task's own context.

    Yields:
        Nothing.
    """
    token = _RUNNING.set(values)
    task_token = _TASK.set(task)
    try:
        yield
    finally:
        _TASK.reset(task_token)
        _RUNNING.reset(token)


def _current[V](field: DagCtxField, value: Callable[[RunValues], V]) -> V:
    """A field's value inside a running task, its reference in a DAG body, else an error.

    Type checkers read it as the value (``V``) in both places, as they read a task call.
    """
    values = _RUNNING.get()
    if values is not None:
        return value(values)
    if _build.active() is None:
        raise RuntimeError(_OUTSIDE)
    return DagCtxReference(field)  # type: ignore[return-value]


class RunContext:
    """``dag_ctx.run``: the run's information.

    Each field is a reference in the DAG body and a value inside a running task; type
    checkers read it as the value in both (as they read a task call), so it passes to a task
    taking that type. Reading a field anywhere else raises ``RuntimeError``.
    """

    @property
    def id(self) -> str:
        """The run's ID (a UUIDv7)."""
        return _current(DagCtxField.RUN_ID, lambda v: v.run_id)

    @property
    def kind(self) -> str:
        """The run's kind: ``scheduled``, ``manual``, ``backfill``, ``chained`` or ``event``."""
        return _current(DagCtxField.RUN_KIND, lambda v: v.kind)

    @property
    def planned_for(self) -> datetime:
        """The moment the run is for (a fire time for scheduled runs)."""
        return _current(DagCtxField.PLANNED_FOR, lambda v: v.planned_for)

    @property
    def since(self) -> datetime:
        """The start of the run's window, which is ``(since, planned_for]``."""
        return _current(DagCtxField.SINCE, lambda v: v.since)


class DagContext:
    """``dag_ctx``: the current run's context."""

    @property
    def run(self) -> RunContext:
        """The run's information."""
        return RunContext()

    @property
    def dag_id(self) -> str:
        """The DAG's ID."""
        return _current(DagCtxField.DAG_ID, lambda v: v.dag_id)

    @property
    def params(self) -> None:
        """The run's parameters (milestone M4).

        Raises:
            NotImplementedError: Always, for now.
        """
        raise NotImplementedError("dag_ctx.params comes with run parameters (milestone M4)")


dag_ctx = DagContext()
"""The current run's context: ``dag_ctx.run.planned_for``, ``dag_ctx.run.since``, ..."""

_TASK_OUTSIDE = (
    "`task_ctx` is the running task's own context: read it inside a task's function while it "
    "runs (it has no references for the DAG body); in tests, set it with "
    "`schedr.testing.task_context(...)`"
)


def _task() -> TaskValues:
    values = _TASK.get()
    if values is None:
        raise RuntimeError(_TASK_OUTSIDE)
    return values


class TaskContext:
    """``task_ctx``: the running task's own context (decision 84), values only.

    Reading it outside a running task (in the DAG body too) raises ``RuntimeError``.
    """

    @property
    def task_id(self) -> str:
        """The task's ID."""
        return _task().task_id

    @property
    def try_number(self) -> int:
        """The attempt's try number, from 1 (retries and requeues count)."""
        return _task().try_number

    @property
    def map_index(self) -> int | None:
        """Which instance of a mapped task this is, from 0; ``None`` for an unmapped task."""
        return _task().map_index


task_ctx = TaskContext()
"""The running task's own context: ``task_ctx.map_index``, ``task_ctx.try_number``, ..."""
