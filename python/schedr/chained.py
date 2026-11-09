"""``TriggerDagOperator`` starts a run of another DAG; ``Chained`` lets it (decisions 357-365, 398).

The master carries it out, with no process and no worker slot: it creates a ``chained`` run of
the target, linked to this run, and with ``wait=True`` (the default) waits for it to end.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, overload

from schedr import _build
from schedr import outcomes as _outcomes
from schedr._model import (
    ChainedTrigger,
    EdgeKind,
    Expand,
    ExpandKwargs,
    ExpandProduct,
    LiteralValue,
    ScalarType,
    TriggerDagKind,
    TypeDesc,
    Value,
    validate_dag_id,
)
from schedr._serialization.describe import describe
from schedr.operators import Operator
from schedr.outcomes import DefaultOutcome, Done, Failed, RunIf, TimedOut
from schedr.tasks import Output, TaskFunction, refuse_param_mapping
from schedr.triggers import Trigger


@dataclass(frozen=True, slots=True)
class Chained(Trigger):
    """``Dag(trigger=Chained())``: only other DAGs' ``TriggerDagOperator`` tasks start its runs.

    A DAG's runs come one way (decision 398): its own trigger, or other DAGs'
    TriggerDagOperators, which start only a ``Chained`` DAG. People can still start it by hand,
    as any DAG. Paused, it refuses them: the operator fails, and its retries apply.

    Attributes:
        by: The DAG IDs whose operators may start it; any DAG's if not given. Not empty.
    """

    by: Sequence[str] | None = None

    def __post_init__(self) -> None:
        """Checks the DAG IDs.

        Raises:
            TypeError: For a ``by`` that is a single string, or IDs that aren't strings.
            ValueError: For an empty ``by`` or an invalid DAG ID.
        """
        if self.by is None:
            return
        if isinstance(self.by, str):
            raise TypeError(f"Chained(by=...) is a list of DAG IDs, got the string {self.by!r}")
        if not self.by:
            raise ValueError("Chained(by=[]) names no DAG to start it; Chained() is any DAG")
        for dag_id in self.by:
            if not isinstance(dag_id, str):
                raise TypeError(f"Chained(by=...) holds DAG IDs, got {type(dag_id).__name__}")
            validate_dag_id(dag_id)

    def to_model(self) -> ChainedTrigger:
        """The trigger in DAG JSON.

        Returns:
            The DAGs that may start it.
        """
        return ChainedTrigger(None if self.by is None else tuple(self.by))


@dataclass(frozen=True, slots=True)
class _Fixed:
    """A ``TriggerDagOperator``'s settings as given (fixed, when it is mapped)."""

    dag: str
    params: Mapping[str, object] | None
    wait: bool
    planned_for: datetime | object | None
    max_wait: timedelta | None
    retries: int | None
    retry_delay: timedelta | None
    run_if: RunIf | None


type _ExpandOf = Callable[[_build.DagBuilder, str, list[_build.Upstream]], Expand]
"""What a mapped one maps over, converted once its ID and DAG are known."""


class TriggerDagOperator(Operator):
    """Starts a run of another DAG: ``TriggerDagOperator("load", dag="dwh_load")``.

    The target is a DAG with ``trigger=Chained()``, or ``Chained(by=[...])`` naming this DAG
    (decision 398); any other fails the task, not retried, and a paused one fails it, retried.
    The run is ``chained`` to this one (its ``triggered_by``), planned for this run's
    ``planned_for`` unless ``planned_for`` says otherwise, queued with a manual run's priority.
    The task's value is the run's ID, a ``str``.

    With ``wait=True`` (the default) the task ends when the run has: its value when the run
    succeeded; ``Failed`` (its state and ID) when it failed, was cancelled or skipped, which the
    DAG routes with ``.on(Failed)`` or the task fails; ``TimedOut`` after ``max_wait``, the run
    left going. Cancelling this run cancels a run it waits for. A retry starts a new run only
    after the previous one failed. With ``wait=False`` it ends once the run is created.

    It holds no worker and doesn't count towards the DAG's limits (decision 380). A target that
    is missing, in an ID conflict or paused fails the task, with its retries (decision 379);
    parameters the target refuses, or a target that isn't ``Chained`` for this DAG, fail it
    without. DAGs may start each other in a cycle (decision 399). Read the target run's output
    with ``@sensor(wait_for=RunOf(...))`` or the API.

    Args:
        task_id: The task's ID.
        dag: The target DAG's ID.
        params: The run's parameters, over the target's defaults: literals, upstream outputs or
            ``dag_ctx`` references, checked against the target's schema when the task runs.
        wait: Whether the task waits for the run to end.
        planned_for: The run's ``planned_for``: an aware ``datetime`` or a reference
            (``dag_ctx.run.planned_for``); this run's if not given. Airflow's ``logical_date``.
        max_wait: How long it waits (with ``wait=True``); the master's ``[sensors]
            default_max_wait`` if not given.
        retries: How many times a failed task is tried again.
        retry_delay: How long it waits before trying again.
        run_if: When it runs, from its upstream edges.

    Raises:
        RuntimeError: Outside ``with Dag(...)``.
        TypeError: For a target that isn't a DAG ID, parameters that aren't a dict with string
            keys, or a ``max_wait`` with ``wait=False``.
    """

    def __init__(
        self,
        task_id: str,
        *,
        dag: str,
        params: Mapping[str, object] | None = None,
        wait: bool = True,
        planned_for: datetime | object | None = None,
        max_wait: timedelta | None = None,
        retries: int | None = None,
        retry_delay: timedelta | None = None,
        run_if: RunIf | None = None,
    ) -> None:
        """Records the task in the DAG being defined."""
        builder = _build.require_active("TriggerDagOperator")
        fixed = _Fixed(dag, params, wait, planned_for, max_wait, retries, retry_delay, run_if)
        self._record(builder, builder.qualified(task_id), fixed, None)

    @classmethod
    def partial(
        cls,
        task_id: str,
        *,
        dag: str,
        params: Mapping[str, object] | None = None,
        wait: bool = True,
        planned_for: datetime | object | None = None,
        max_wait: timedelta | None = None,
        retries: int | None = None,
        retry_delay: timedelta | None = None,
        run_if: RunIf | None = None,
    ) -> PartialTriggerDag:
        """Fixes the settings of a mapped one: ``.partial("fan", dag="region").expand(params=...)``.

        Mapped (decision 407), it starts one run per element of what ``.expand()`` or
        ``.expand_kwargs()`` maps over: ``params`` or ``planned_for``, the others fixed here.

        Args:
            task_id: The task's ID.
            dag: The target DAG's ID.
            params: The runs' parameters, if not mapped.
            wait: Whether each instance waits for its run to end.
            planned_for: The runs' ``planned_for``, if not mapped.
            max_wait: How long each instance waits.
            retries: How many times a failed instance is tried again.
            retry_delay: How long it waits before trying again.
            run_if: When it runs, from its upstream edges.

        Returns:
            The operator with them fixed, to ``.expand(...)``.
        """
        fixed = _Fixed(dag, params, wait, planned_for, max_wait, retries, retry_delay, run_if)
        return PartialTriggerDag(task_id, fixed)

    def _record(
        self,
        builder: _build.DagBuilder,
        task_id: str,
        fixed: _Fixed,
        expand: _ExpandOf | None,
    ) -> None:
        """Records the task: its settings and, mapped, what it maps over."""
        dag, params, wait, planned_for = fixed.dag, fixed.params, fixed.wait, fixed.planned_for
        max_wait = fixed.max_wait
        try:
            validate_dag_id(dag)
        except ValueError as error:
            raise TypeError(f"TriggerDagOperator `{task_id}`: `dag`: {error}") from None
        if max_wait is not None and not wait:
            raise TypeError(
                f"TriggerDagOperator `{task_id}`: `max_wait` is for wait=True; with wait=False "
                "it ends once the run is created"
            )
        if params is not None and not isinstance(params, Mapping):
            raise TypeError(
                f"TriggerDagOperator `{task_id}`: `params` is a dict, got {type(params).__name__}"
            )
        upstream: list[_build.Upstream] = []
        values: dict[str, Value] = {}
        for key, value in (params or {}).items():
            if not isinstance(key, str):
                raise TypeError(f"TriggerDagOperator `{task_id}`: `params` keys are strings")
            values[key] = _build.to_value(value, f"params[{key!r}]", upstream)
        planned = None
        if planned_for is not None:
            planned = _build.to_value(planned_for, "planned_for", upstream)
            if isinstance(planned, LiteralValue) and not isinstance(planned_for, datetime):
                raise TypeError(
                    f"TriggerDagOperator `{task_id}`: `planned_for` is a datetime or a "
                    f"reference, got {planned_for!r}"
                )
        self._task_id = task_id
        self._dag = builder
        self._wait = wait
        names = [_outcomes.SUCCESS_NAME]
        types: dict[str, TypeDesc] = {_outcomes.SUCCESS_NAME: ScalarType("str")}
        if wait:
            names += [_outcomes.FAILED_NAME, _outcomes.TIMED_OUT_NAME]
            types[_outcomes.FAILED_NAME] = describe(Failed)
            types[_outcomes.TIMED_OUT_NAME] = describe(TimedOut)
        spec = _outcomes.OutcomeSpec(names=tuple(names), timed_out=wait, value=str, failed=wait)
        builder.add_task(
            task_id,
            TriggerDagKind(dag, values, planned, wait),
            describe(str | Failed | TimedOut) if wait else ScalarType("str"),
            _build.TaskOptions(
                max_wait=max_wait,
                retries=fixed.retries,
                retry_delay=fixed.retry_delay,
                run_if=fixed.run_if,
            ),
            outcomes=spec,
            outcome_types=types,
            expand=expand(builder, task_id, upstream) if expand is not None else None,
        )
        for up, on in dict.fromkeys(upstream):
            builder.add_edge(up, task_id, EdgeKind.DATA, on)

    @property
    def output(self) -> str:
        """The started run's ID: a reference, read by type checkers as its value.

        With ``wait=True``, only when the run succeeded (``.on(str)``): a task taking it is
        skipped on ``Failed`` or ``TimedOut`` unless something takes those.
        """
        if self._wait:
            return self.on(str)
        # Its only value is the ID: the reference stands for it, as `CommandOperator.output`.
        return Output[str](self._task_id, dag=self._dag)  # type: ignore[return-value]

    @overload
    def on[U](self, kind: type[U], /) -> U: ...
    @overload
    def on[U, V](self, kind: type[U], other: type[V], /) -> U | V: ...
    @overload
    def on(self, kind: None, /) -> Done: ...
    @overload
    def on(self, kind: DefaultOutcome, /) -> str: ...
    def on(self, *kinds: object) -> object:
        """Depends on it only for these outcomes: ``.on(str)``, ``.on(Failed)``, ``.on(TimedOut)``.

        Args:
            *kinds: ``str`` (the run succeeded), ``Failed``, ``TimedOut``, or ``None`` (any).

        Returns:
            The narrowed reference (see ``Output.on``).
        """
        builder = _build.require_active("`.on()`")
        _build.check_same_dag(self._dag, self._task_id)
        spec = builder.outcomes.get(self._task_id, _outcomes.DEFAULT)
        names = _outcomes.names_for(spec, kinds, self._task_id)
        return Output[object](self._task_id, on=names, dag=self._dag)


_MAPPED = ("params", "planned_for")
"""What a mapped ``TriggerDagOperator`` maps over (decision 407)."""

# Each mapped argument's element type, as the type model describes it (`Any`: a dict of values
# the target's schema checks when the run is created).
_ELEMENT: dict[str, object] = {"params": dict[str, Any], "planned_for": datetime}
_LIST: dict[str, object] = {"params": list[dict[str, Any]], "planned_for": list[datetime]}


class PartialTriggerDag:
    """A ``TriggerDagOperator`` with its settings fixed, to map (``TriggerDagOperator.partial``)."""

    def __init__(self, task_id: str, fixed: _Fixed) -> None:
        """Remembers the task's ID and settings.

        Args:
            task_id: The task's ID.
            fixed: Its settings.
        """
        self._task_id = task_id
        self._fixed = fixed

    def expand(self, **mapped: object) -> MappedTriggerDagOperator:
        """One run per element: ``.expand(params=regions)``, ``.expand(planned_for=days)``.

        Each list is a literal or an upstream output; several are combined as their cartesian
        product, the first varying slowest.

        Args:
            **mapped: ``params`` (a list of dicts), ``planned_for`` (a list of aware datetimes),
                or both.

        Returns:
            The mapped task; its output is the list of the runs' IDs.

        Raises:
            RuntimeError: Outside ``with Dag(...)``.
            TypeError: For nothing mapped, another argument, or one also fixed.
        """
        where = f"TriggerDagOperator `{self._task_id}`"
        if not mapped:
            raise TypeError(f"{where}: `.expand()` needs `params=` or `planned_for=`")
        for name in mapped:
            self._check_name(where, name, "`.expand()`")

        def over(
            builder: _build.DagBuilder, task_id: str, upstream: list[_build.Upstream]
        ) -> Expand:
            converted = {
                name: TaskFunction._argument(
                    builder,
                    (task_id, values),
                    _LIST[name],
                    f"{where}, mapped argument `{name}`",
                    upstream,
                    elementwise=True,
                )
                for name, values in mapped.items()
            }
            for name, value in converted.items():
                refuse_param_mapping(value, f"{where}, mapped argument `{name}`")
            return ExpandProduct(converted)

        return self._mapped(over)

    def expand_kwargs(self, values: object) -> MappedTriggerDagOperator:
        """One run per dict of ``values``: ``[{"params": {...}, "planned_for": ...}, ...]``.

        Args:
            values: A list of dicts naming ``params``, ``planned_for`` or both (a literal, or an
                upstream output that is one).

        Returns:
            The mapped task; its output is the list of the runs' IDs.

        Raises:
            RuntimeError: Outside ``with Dag(...)``.
            TypeError: For an element written here that isn't a dict, or names another
                argument or one also fixed.
        """
        where = f"TriggerDagOperator `{self._task_id}`"
        for index, element in enumerate(values if isinstance(values, list | tuple) else ()):
            if _build.is_reference(element):
                continue
            spot = f"{where}: `.expand_kwargs()` element {index}"
            if not isinstance(element, Mapping):
                raise TypeError(
                    f"{spot}: every element must be a dict, not `{type(element).__name__}`"
                )
            for name in element:
                self._check_name(spot, name, "`.expand_kwargs()`")

        def over(
            builder: _build.DagBuilder, task_id: str, upstream: list[_build.Upstream]
        ) -> Expand:
            for index, element in enumerate(values if isinstance(values, list | tuple) else ()):
                for name, item in element.items() if isinstance(element, Mapping) else ():
                    TaskFunction._argument(
                        builder,
                        (task_id, item),
                        _ELEMENT[name],
                        f"{where}, `.expand_kwargs()` element {index}, argument `{name}`",
                        [],
                    )
            converted = TaskFunction._argument(
                builder,
                (task_id, values),
                list[dict[str, Any]],
                f"{where}, `.expand_kwargs()`",
                upstream,
                elementwise=True,
            )
            refuse_param_mapping(converted, f"{where}, `.expand_kwargs()`")
            return ExpandKwargs(converted)

        return self._mapped(over)

    def _check_name(self, where: str, name: object, how: str) -> None:
        """Refuses mapping an argument other than ``params`` and ``planned_for``, or a fixed one."""
        if name not in _MAPPED:
            raise TypeError(f"{where}: {how} maps `params` or `planned_for`, not `{name}`")
        given = self._fixed.params if name == "params" else self._fixed.planned_for
        if given is not None:
            raise TypeError(f"{where}: `{name}` is both fixed (`.partial()`) and mapped ({how})")

    def _mapped(self, over: _ExpandOf) -> MappedTriggerDagOperator:
        """Records the mapped task."""
        builder = _build.require_active(f"`{self._task_id}.expand()`")
        operator = MappedTriggerDagOperator.__new__(MappedTriggerDagOperator)
        operator._record(builder, builder.qualified(self._task_id), self._fixed, over)
        return operator


class MappedTriggerDagOperator(TriggerDagOperator):
    """A mapped ``TriggerDagOperator`` (decision 407): one run per element.

    Each instance's outcome is its own. Made by ``TriggerDagOperator.partial(...).expand(...)``.
    """

    @property
    def output(self) -> list[str | Failed | TimedOut]:  # type: ignore[override]  # a list
        """Each instance's value, in the order of the elements: a reference.

        Its run's ID; with ``wait=True``, its ``Failed`` or ``TimedOut`` instead when it ended
        so (each instance has its own outcome, so the whole list isn't narrowed: filter it in a
        task). With ``wait=False`` every element is a run's ID.
        """
        # The reference stands for the list of the instances' outputs.
        return Output[list[str | Failed | TimedOut]](self._task_id, dag=self._dag)  # type: ignore[return-value]
