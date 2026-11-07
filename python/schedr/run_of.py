"""Waiting for a run of another DAG: ``RunOf`` and ``wait_for`` (decision 353)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

from schedr import _build
from schedr import outcomes as _outcomes
from schedr._model import RunOfTrigger, ScalarType, WaitForKind, validate_dag_id, validate_task_id
from schedr._serialization.describe import describe
from schedr.tasks import Output
from schedr.triggers import Trigger

if TYPE_CHECKING:
    from schedr.outcomes import Waited

MAX_LAG = timedelta(days=3650)
"""The longest ``lag``, either way: 10 years (the master's ``MAX_LAG_MS``)."""


@dataclass(frozen=True, slots=True)
class RunOf(Trigger):
    """Which run of another DAG to wait for, or, as a DAG's trigger, which runs start one of it.

    ``RunOf("orders")`` is the next run of ``orders`` to end; ``RunOf("orders", task="load",
    lag=timedelta(hours=6))`` is the ``load`` of the run planned six hours before this one. As
    ``Dag("report", trigger=RunOf("orders", lag=timedelta(hours=6)))``, each run of ``orders``
    that succeeds starts one of ``report``, planned six hours after it (without ``lag``, for the
    moment it is seen); only those ending while ``report`` is live count, none caught up.

    Attributes:
        dag: The DAG's ID.
        task: Its task; ``None``: the whole run.
        lag: How long before this run's ``planned_for`` that run was planned for; ``None``: the
            next run to end after the step starts.
    """

    dag: str
    task: str | None = None
    lag: timedelta | None = None

    def __post_init__(self) -> None:
        """Checks the IDs and the lag.

        Raises:
            ValueError: For a lag that isn't whole milliseconds, or over 10 years.
        """
        validate_dag_id(self.dag)
        if self.task is not None:
            validate_task_id(self.task)
        if self.lag is not None and self.lag % timedelta(milliseconds=1):
            raise ValueError(f"RunOf: `lag` is whole milliseconds, got {self.lag}")
        if self.lag is not None and abs(self.lag) > MAX_LAG:
            raise ValueError(f"RunOf: `lag` is at most 10 years either way, got {self.lag}")

    @property
    def lag_ms(self) -> int | None:
        """The lag in milliseconds, as stored."""
        return None if self.lag is None else self.lag // timedelta(milliseconds=1)

    def to_model(self) -> RunOfTrigger:
        """The trigger in DAG JSON.

        Returns:
            Its model.

        Raises:
            ValueError: For a negative lag: a run can't be planned before the one starting it.
        """
        if self.lag is not None and self.lag < timedelta(0):
            raise ValueError(f"RunOf as a trigger: `lag` can't be negative, got {self.lag}")
        return RunOfTrigger(self.dag, self.task, self.lag_ms)


def _free_id(builder: _build.DagBuilder, run_of: RunOf) -> str:
    name = f"wait_for_{run_of.dag}" if run_of.task is None else f"wait_for_{run_of.task}"
    name = "".join(c if c.isalnum() or c in "_-" else "_" for c in name)
    task_id = builder.qualified(name)
    number = 1
    while task_id in builder.tasks:
        number += 1
        task_id = builder.qualified(f"{name}_{number}")
    return task_id


def wait_for(
    run_of: RunOf, *, max_wait: timedelta | None = None, task_id: str | None = None
) -> Waited[str]:
    """A step the master ends when that run, or its task, has ended: no process, no slot.

    Its value is that run's ID: ``build(wait_for(RunOf("orders")).on(str))``. It ends ``Failed``
    when the run or task didn't succeed: route it with ``.on(Failed)``, or the step fails and its
    run too. After ``max_wait`` with no such run it ends ``TimedOut``, as any wait step.

    Args:
        run_of: Which run, and which task of it.
        max_wait: How long it waits; the master's ``[sensors] default_max_wait`` if not given.
        task_id: Its ID; ``wait_for_<task or DAG>``, numbered from the second on, if not given.

    Returns:
        The step's output, read as its value, ``Failed`` or ``TimedOut``.

    Raises:
        TypeError: For what isn't a ``RunOf``.
    """
    if not isinstance(run_of, RunOf):
        raise TypeError(f"wait_for: give it a RunOf, got {run_of!r}")
    builder = _build.require_active("wait_for")
    node = builder.qualified(task_id) if task_id is not None else _free_id(builder, run_of)
    spec = _outcomes.OutcomeSpec(
        names=(_outcomes.SUCCESS_NAME, _outcomes.FAILED_NAME, _outcomes.TIMED_OUT_NAME),
        timed_out=True,
        value=str,
        failed=True,
    )
    builder.add_task(
        node,
        WaitForKind(run_of.dag, run_of.task, run_of.lag_ms),
        describe(str | _outcomes.Failed | _outcomes.TimedOut),
        _build.TaskOptions(max_wait=max_wait),
        outcomes=spec,
        outcome_types={
            _outcomes.SUCCESS_NAME: ScalarType("str"),
            _outcomes.FAILED_NAME: describe(_outcomes.Failed),
            _outcomes.TIMED_OUT_NAME: describe(_outcomes.TimedOut),
        },
    )
    return Output(node, dag=builder)  # type: ignore[return-value]
