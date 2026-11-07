"""``Dag``, the context manager that defines a DAG, and ``StepDefaults``.

Tasks created inside ``with Dag(...)`` belong to it. When the block ends, the DAG is checked
(no cycles) and recorded for the runner (see PROJECT.md, Native API).
"""

import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from types import FrameType, TracebackType
from typing import Self

from schedr import _build, _params
from schedr._model import Dag as DagModel
from schedr._model import (
    DagCtxField,
    DagCtxRef,
    Group,
    NoTrigger,
    RunOfTrigger,
    parse_moment,
    registry,
    validate_dag_id,
)
from schedr._serialization.describe import describe
from schedr.event_triggers import EventTrigger
from schedr.outcomes import RunIf
from schedr.runner.sources import IMPORTING
from schedr.triggers import Cron, Every, Once, Trigger


@dataclass(frozen=True, slots=True)
class StepDefaults:
    """Defaults for the steps of a DAG or group, its tasks and wait steps; a step's own win.

    Attributes:
        retries: How many times a failed attempt is retried.
        retry_delay: The delay before each retry.
        timeout: A task's attempt's timeout; a wait step has ``max_wait`` instead.
        max_wait: How long a wait step waits (default: ``[sensors] default_max_wait``); not
            for tasks.
        queue: The queue steps go to.
        max_output_size: The output size limit in bytes (e.g. ``8 * MiB``).
        preload: Python tasks fork from the preloaded fork server (``False``: only from the
            bare one, for code that misbehaves next to the preloaded libraries).
        fresh_process: Python tasks are always a new process, never forked (for code that
            misbehaves after a fork).
        run_if: When tasks run, from their upstream edges (default ``RunIf.NONE_FAILED``).
        max_active: At most this many instances of each task running at once, across all
            runs.
        max_active_per_run: At most this many instances of each task running at once within
            one run (for mapped tasks).
        cleanup_timeout: How long one try of a task's cleanup may run (default: the worker's
            ``cleanup_timeout``, 5 minutes).
        cleanup_retries: How many times a failed cleanup is retried (default 3).
        cleanup_retry_delay: The delay before each cleanup retry (default 30 seconds).
        cleanup_anywhere: Whether the cleanup may run on another worker when this one is lost
            (default no: only you know whether it depends on where the task ran).
    """

    retries: int | None = None
    retry_delay: timedelta | None = None
    timeout: timedelta | None = None
    max_wait: timedelta | None = None
    queue: str | None = None
    max_output_size: int | None = None
    preload: bool | None = None
    fresh_process: bool | None = None
    run_if: RunIf | None = None
    max_active: int | None = None
    max_active_per_run: int | None = None
    cleanup_timeout: timedelta | None = None
    cleanup_retries: int | None = None
    cleanup_retry_delay: timedelta | None = None
    cleanup_anywhere: bool | None = None

    def options(self) -> _build.TaskOptions:
        """These defaults as task options.

        Returns:
            The options.
        """
        return _build.TaskOptions(
            retries=self.retries,
            retry_delay=self.retry_delay,
            timeout=self.timeout,
            max_wait=self.max_wait,
            queue=self.queue,
            max_output_size=self.max_output_size,
            preload=self.preload,
            fresh_process=self.fresh_process,
            run_if=self.run_if,
            max_active=self.max_active,
            max_active_per_run=self.max_active_per_run,
            cleanup_timeout=self.cleanup_timeout,
            cleanup_retries=self.cleanup_retries,
            cleanup_retry_delay=self.cleanup_retry_delay,
            cleanup_anywhere=self.cleanup_anywhere,
        )


def _moment(value: datetime | date | str | None, tz: str, name: str) -> datetime | None:
    """Reads ``initial_since``: naive values are in ``tz``."""
    return None if value is None else parse_moment(value, tz, name)


class Dag:
    """Defines a DAG: ``with Dag("sync", trigger=Cron("0 3 * * *")): ...``.

    Args:
        dag_id: The DAG's ID (required, explicit).
        trigger: What starts its runs: ``Cron``, ``Every`` or ``Once``; ``None`` for manual
            runs only. ``catchup``, ``start`` and ``end`` are set on the trigger.
        initial_since: ``since`` of the run on the DAG's first fire time (or of its first
            run without one). A date or a datetime without a time zone is read in the
            trigger's time zone (UTC for ``Every`` and no trigger).
        max_active_runs: At most this many runs at a time (default 1; ``None``: no limit).
        max_active_tasks: At most this many of its tasks running at once, across all its
            runs (default ``None``: no limit); each instance of a mapped task counts.
        defaults: Defaults for every task.
        description: Shown in the UI.
        tags: Shown in the UI and used for filtering.
        params: The run parameters it takes: a dataclass or a Pydantic model class. With a
            trigger, every field needs a default (scheduled runs use them). Read them with
            ``dag_ctx.params.<field>`` in the DAG body (a reference) and in a task.

    Raises:
        IdError: If the DAG ID is invalid.
        TypeError: For a wrong argument type, e.g. a string trigger.
        ValueError: For a bad date or a non-positive ``max_active_runs`` or
            ``max_active_tasks``.
    """

    def __init__(
        self,
        dag_id: str,
        *,
        trigger: Trigger | None = None,
        initial_since: datetime | date | str | None = None,
        max_active_runs: int | None = 1,
        max_active_tasks: int | None = None,
        defaults: StepDefaults | None = None,
        description: str | None = None,
        tags: Sequence[str] = (),
        params: type | None = None,
    ) -> None:
        """Records the DAG's settings; tasks are added inside the ``with`` block."""
        self.dag_id = validate_dag_id(dag_id)
        if trigger is not None and not isinstance(trigger, Trigger):
            raise TypeError(
                "trigger must be Cron(...), Every(...), Once(...), a @trigger function called "
                "with its arguments, or None, "
                f"got {type(trigger).__name__}"
            )
        if isinstance(tags, str):
            raise TypeError("tags must be a list of strings, not a string")
        self.trigger = trigger
        tz = trigger.tz if isinstance(trigger, Cron | Once) else "UTC"
        self.initial_since = _moment(initial_since, tz, "initial_since")
        if max_active_runs is not None and max_active_runs <= 0:
            raise ValueError(f"max_active_runs must be positive or None, got {max_active_runs}")
        self.max_active_runs = max_active_runs
        if max_active_tasks is not None and max_active_tasks <= 0:
            raise ValueError(f"max_active_tasks must be positive or None, got {max_active_tasks}")
        self.max_active_tasks = max_active_tasks
        self.defaults = defaults or StepDefaults()
        self.description = description
        self.tags = tuple(tags)
        self.params = params
        self._params = (
            None
            if params is None
            else _params.declare(params, self.dag_id, scheduled=trigger is not None)
        )
        self._filename = sys._getframe(1).f_code.co_filename
        self._location = registry.location_for(self._filename)
        self._builder: _build.DagBuilder | None = None
        self.model: DagModel | None = None

    def __enter__(self) -> Self:
        """Starts defining the DAG's tasks.

        Returns:
            The DAG.

        Raises:
            RuntimeError: If another DAG is being defined.
        """
        self._locate_in_module()
        self._builder = _build.DagBuilder(self.dag_id, self.defaults.options())
        if isinstance(self.trigger, EventTrigger):
            self._builder.event_data = self.trigger.function.data_type()
        elif self.trigger is not None and isinstance(self.trigger.to_model(), RunOfTrigger):
            # The run that started it: `{"run_id", "dag_id", "planned_for"}` (decision 353).
            self._builder.event_data = describe(dict[str, str])
        if self._params is not None:
            self._builder.params_class = self.params
            self._builder.params_type = self._params.type_desc
        _build.push(self._builder)
        return self

    def _locate_in_module(self) -> None:
        """Gives the DAG to the module whose top level enters it, while one of a DAG root runs.

        A function in a helper can then build it (decision 329, B170), and a module imported by
        another still owns its own DAGs (B110).
        """
        if IMPORTING.get() is None:
            return
        frame: FrameType | None = sys._getframe(2)
        while frame is not None and frame.f_code.co_name != "<module>":
            frame = frame.f_back
        if frame is None:
            return
        spec = frame.f_globals.get("__spec__")
        origin = getattr(spec, "origin", None) or frame.f_globals.get("__file__")
        if isinstance(origin, str):
            self._filename = origin
            self._location = registry.location_for(origin)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Ends the definition: checks the DAG and records it for the runner.

        Nothing is recorded when the block raised.

        Raises:
            ValueError: If the dependencies form a cycle, or one on a group can't be made.
        """
        builder = self._builder
        try:
            # While the DAG is still the active one: a group's ends are read through it.
            if exc_type is None and builder is not None:
                resources = builder.finish_resources()
                builder.make_group_dependencies()
                builder.wait_for_cleanups(resources)
        finally:
            _build.pop()
        if exc_type is not None or builder is None:
            return
        cycle = builder.find_cycle()
        if cycle is not None:
            raise ValueError(f"DAG `{self.dag_id}` has a dependency cycle: {' -> '.join(cycle)}")
        builder.check_nones()
        self._check_since(builder)
        self._check_event(builder)
        trigger = NoTrigger() if self.trigger is None else self.trigger.to_model()
        if isinstance(trigger, RunOfTrigger) and trigger.dag == self.dag_id:
            raise ValueError(
                f"DAG `{self.dag_id}`: its trigger is a run of itself, which would start it "
                "for ever"
            )
        self.model = DagModel(
            id=self.dag_id,
            location=self._location,
            tasks=list(builder.tasks.values()),
            edges=list(builder.edges),
            trigger=trigger,
            initial_since=self.initial_since,
            max_active_runs=self.max_active_runs,
            max_active_tasks=self.max_active_tasks,
            description=self.description,
            tags=self.tags,
            groups=[Group(g.id, g.parent) for g in builder.groups.values()],
            params=self._params,
            resources=resources,
        )
        registry.register(
            self.model, builder.functions, self._filename, self.params, builder.cleanups
        )

    def _check_event(self, builder: _build.DagBuilder) -> None:
        """Checks what an event trigger asks of the DAG.

        A task reads ``dag_ctx.event`` only where events start the runs; a trigger's function
        has its place among them, so its name can't be a task's.

        Raises:
            ValueError: For a task using it elsewhere, or named as the trigger's function.
        """
        if isinstance(self.trigger, EventTrigger):
            name = self.trigger.function.task_id
            if name in builder.tasks:
                raise ValueError(
                    f"DAG `{self.dag_id}`: task `{name}` has the name of its trigger's function"
                )
            builder.functions[name] = self.trigger.function.fn
            return
        if self.trigger is not None and isinstance(self.trigger.to_model(), RunOfTrigger):
            return  # its runs bring the run that started them as their event
        fields = {DagCtxField.EVENT_KEY, DagCtxField.EVENT_DATA, DagCtxField.EVENT_AT}
        for task in builder.tasks.values():
            if {DagCtxRef(field) for field in fields} & set(task.references()):
                raise ValueError(
                    f"DAG `{self.dag_id}`: task `{task.id}` uses dag_ctx.event, but no event "
                    "trigger starts its runs"
                )

    def _check_since(self, builder: _build.DagBuilder) -> None:
        if self.initial_since is not None or isinstance(self.trigger, Cron | Every | EventTrigger):
            return
        since = DagCtxRef(DagCtxField.SINCE)
        for task in builder.tasks.values():
            if since in task.references():
                what = "no trigger" if self.trigger is None else "a Once trigger"
                raise ValueError(
                    f"DAG `{self.dag_id}` has {what}, so its first run has no previous fire "
                    f"time, but task `{task.id}` uses dag_ctx.run.since: set initial_since="
                )
