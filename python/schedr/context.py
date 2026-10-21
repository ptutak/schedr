"""The DAG context, ``dag_ctx``: the current run's information.

In the DAG body, ``dag_ctx.run.planned_for`` is a **reference**, filled in at run time; it
can be passed to tasks and commands, but not used as a value in the DAG file. Inside a
running task (while the runner calls its function) the same expression gives the **value**
(see PROJECT.md, Run parameters and the DAG context). ``dag_ctx.params`` comes with
milestone M4.
"""

import contextlib
from collections.abc import Iterator
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from schedr._model import DagCtxField, DagCtxRef, Reference

_VALUE_IN_DAG_BODY = (
    "a reference can't be used as a value in the DAG file (it is known only at run time); "
    "pass it to a task, or decide in a task"
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


_RUNNING: ContextVar[RunValues | None] = ContextVar("schedr_running_task", default=None)


@contextlib.contextmanager
def running(values: RunValues) -> Iterator[None]:
    """Makes ``dag_ctx`` give values instead of references inside the block (the runner).

    Args:
        values: The run's context.

    Yields:
        Nothing.
    """
    token = _RUNNING.set(values)
    try:
        yield
    finally:
        _RUNNING.reset(token)


class RunContext:
    """``dag_ctx.run``: the run's information.

    Each field is a reference in the DAG body and a value inside a running task; for type
    checkers it is ``Any`` (as ``dag_ctx.params`` fields are), since the same expression is
    used in both places.
    """

    @property
    def id(self) -> Any:
        """The run's ID (a UUIDv7)."""
        values = _RUNNING.get()
        return DagCtxReference(DagCtxField.RUN_ID) if values is None else values.run_id

    @property
    def kind(self) -> Any:
        """The run's kind: ``scheduled``, ``manual``, ``backfill`` or ``triggered``."""
        values = _RUNNING.get()
        return DagCtxReference(DagCtxField.RUN_KIND) if values is None else values.kind

    @property
    def planned_for(self) -> Any:
        """The moment the run is for (a fire time for scheduled runs)."""
        values = _RUNNING.get()
        return DagCtxReference(DagCtxField.PLANNED_FOR) if values is None else values.planned_for

    @property
    def since(self) -> Any:
        """The start of the run's window, which is ``(since, planned_for]``."""
        values = _RUNNING.get()
        return DagCtxReference(DagCtxField.SINCE) if values is None else values.since


class DagContext:
    """``dag_ctx``: the current run's context."""

    @property
    def run(self) -> RunContext:
        """The run's information."""
        return RunContext()

    @property
    def dag_id(self) -> Any:
        """The DAG's ID."""
        values = _RUNNING.get()
        return DagCtxReference(DagCtxField.DAG_ID) if values is None else values.dag_id

    @property
    def params(self) -> None:
        """The run's parameters (milestone M4).

        Raises:
            NotImplementedError: Always, for now.
        """
        raise NotImplementedError("dag_ctx.params comes with run parameters (milestone M4)")


dag_ctx = DagContext()
"""The current run's context: ``dag_ctx.run.planned_for``, ``dag_ctx.run.since``, ..."""
