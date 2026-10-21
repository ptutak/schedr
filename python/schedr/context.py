"""The DAG context, ``dag_ctx``: the current run's information.

In the DAG body, ``dag_ctx.run.planned_for`` is a **reference**, filled in at run time; it
can be passed to tasks and commands, but not used as a value in the DAG file (see
PROJECT.md, Run parameters and the DAG context). Reading the values inside a running task
comes with the runner (milestone 1.7); ``dag_ctx.params`` with milestone M4.
"""

from dataclasses import dataclass

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


class RunContext:
    """``dag_ctx.run``: the run's information."""

    @property
    def id(self) -> DagCtxReference:
        """The run's ID (a UUIDv7)."""
        return DagCtxReference(DagCtxField.RUN_ID)

    @property
    def kind(self) -> DagCtxReference:
        """The run's kind: ``scheduled``, ``manual``, ``backfill`` or ``triggered``."""
        return DagCtxReference(DagCtxField.RUN_KIND)

    @property
    def planned_for(self) -> DagCtxReference:
        """The moment the run is for (a fire time for scheduled runs)."""
        return DagCtxReference(DagCtxField.PLANNED_FOR)

    @property
    def since(self) -> DagCtxReference:
        """The start of the run's window, which is ``(since, planned_for]``."""
        return DagCtxReference(DagCtxField.SINCE)


class DagContext:
    """``dag_ctx``: the current run's context."""

    @property
    def run(self) -> RunContext:
        """The run's information."""
        return RunContext()

    @property
    def dag_id(self) -> DagCtxReference:
        """The DAG's ID."""
        return DagCtxReference(DagCtxField.DAG_ID)

    @property
    def params(self) -> None:
        """The run's parameters (milestone M4).

        Raises:
            NotImplementedError: Always, for now.
        """
        raise NotImplementedError("dag_ctx.params comes with run parameters (milestone M4)")


dag_ctx = DagContext()
"""The current run's context: ``dag_ctx.run.planned_for``, ``dag_ctx.run.since``, ..."""
