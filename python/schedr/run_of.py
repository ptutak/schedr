"""Waiting for a run of another DAG: ``RunOf`` and ``wait_for`` (decision 353)."""

from __future__ import annotations

import inspect
import typing
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from schedr import _build
from schedr import outcomes as _outcomes
from schedr._model import RunOfTrigger, ScalarType, WaitForKind, validate_dag_id, validate_task_id
from schedr._serialization.describe import describe
from schedr.tasks import Output
from schedr.triggers import Trigger

if TYPE_CHECKING:
    from collections.abc import Callable

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


@dataclass(frozen=True, slots=True)
class RunResult[T = None]:
    """The run of another DAG an ``@sensor(wait_for=RunOf(...))`` is called for (decision 353).

    Its function's first parameter: ``async def check(run: RunResult[Orders]) -> Report |
    NotYet``. It is called once that run (or its task) has ended, whatever its state; returning
    ``NOT_YET`` waits for a later matching run (with ``lag``, a newer one for the same moment,
    as when it is run again), until ``max_wait``.

    Attributes:
        run_id: The run's ID.
        dag_id: Its DAG.
        planned_for: Its ``planned_for``.
        state: How it ended: ``success``, ``failed``, ``cancelled``, ...; still ``running`` when
            only its task (``RunOf(task=...)``) is what was waited for.
        ended_at: When it ended, if it has.
        task_id: With ``RunOf(task=...)``, that task.
        task_state: Its state: ``success``, ``failed``, ...; ``None`` if the run ended without it.
        outcome: Its outcome (``SUCCESS``, or an outcome class's name), if it succeeded.
        output: Its output, as ``T``, if it succeeded (a mapped task's, the list of its
            elements'), masked as the API masks it: secret fields and URL passwords ``****``.
    """

    run_id: str
    dag_id: str
    planned_for: datetime
    state: str
    ended_at: datetime | None = None
    task_id: str | None = None
    task_state: str | None = None
    outcome: str | None = None
    output: T | None = None


def run_parameter(fn: Callable[..., object], where: str) -> str:
    """The name of a ``wait_for=`` function's first parameter, the run's: ``RunResult[T]``.

    Args:
        fn: An ``@sensor(wait_for=...)``'s or ``@trigger(wait_for=...)``'s function.
        where: Who it is, for messages.

    Returns:
        The parameter's name.

    Raises:
        TypeError: If it has none, or it isn't annotated ``RunResult``.
    """
    parameters = list(inspect.signature(fn).parameters.values())
    if not parameters or parameters[0].kind not in {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    }:
        raise TypeError(
            f"{where}: with `wait_for=`, its first parameter is the run it waits for: "
            "`run: RunResult[...]`"
        )
    first = parameters[0]
    annotation = typing.get_type_hints(fn).get(first.name)
    if annotation is not RunResult and typing.get_origin(annotation) is not RunResult:
        raise TypeError(
            f"{where}: with `wait_for=`, its first parameter `{first.name}` is the run it waits "
            f"for: annotate it `RunResult[...]`, got `{annotation}`"
        )
    return first.name


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


def call_limit_ms(timeout: timedelta, where: str) -> int:
    """A ``wait_for=`` function's ``timeout`` for one call, in milliseconds (decision 353).

    Returns:
        The milliseconds.

    Raises:
        TypeError: Unless it is a ``timedelta`` above zero, in whole milliseconds.
    """
    if not isinstance(timeout, timedelta) or timeout <= timedelta(0):
        raise TypeError(f"{where}: `timeout` is a timedelta above zero, got {timeout!r}")
    if timeout % timedelta(milliseconds=1):
        raise TypeError(f"{where}: `timeout` is whole milliseconds, got {timeout}")
    return timeout // timedelta(milliseconds=1)
