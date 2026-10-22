"""``Dag``, the context manager that defines a DAG, and ``TaskDefaults``.

Tasks created inside ``with Dag(...)`` belong to it. When the block ends, the DAG is checked
(no cycles) and recorded for the runner (see PROJECT.md, Native API).
"""

import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from types import TracebackType
from typing import Self

from schedr import _build
from schedr._model import Dag as DagModel
from schedr._model import (
    DagCtxField,
    DagCtxRef,
    NoTrigger,
    parse_moment,
    registry,
    validate_dag_id,
)
from schedr.triggers import Cron, Every, Once, Trigger


@dataclass(frozen=True, slots=True)
class TaskDefaults:
    """Defaults for every task of a DAG; a task's own arguments win.

    Attributes:
        retries: How many times a failed attempt is retried.
        retry_delay: The delay before each retry.
        timeout: The attempt's timeout.
        queue: The queue tasks go to.
        max_output_size: The output size limit in bytes (e.g. ``8 * MiB``).
    """

    retries: int | None = None
    retry_delay: timedelta | None = None
    timeout: timedelta | None = None
    queue: str | None = None
    max_output_size: int | None = None

    def options(self) -> _build.TaskOptions:
        """These defaults as task options.

        Returns:
            The options.
        """
        return _build.TaskOptions(
            retries=self.retries,
            retry_delay=self.retry_delay,
            timeout=self.timeout,
            queue=self.queue,
            max_output_size=self.max_output_size,
        )


def _moment(value: datetime | date | str | None, tz: str, name: str) -> datetime | None:
    """Reads ``start`` / ``end`` / ``initial_since``: naive values are in ``tz``."""
    return None if value is None else parse_moment(value, tz, name)


class Dag:
    """Defines a DAG: ``with Dag("sync", trigger=Cron("0 3 * * *")): ...``.

    Args:
        dag_id: The DAG's ID (required, explicit).
        trigger: What starts its runs: ``Cron``, ``Every`` or ``Once``; ``None`` for manual
            runs only. ``catchup`` is set on the trigger.
        start: The first scheduled run is the first fire time at or after it. A date or a
            datetime without a time zone is read in the trigger's time zone (UTC for
            ``Every`` and no trigger).
        end: The last scheduled run is the last fire time at or before it (same zone rule).
        initial_since: ``since`` of the run on the DAG's first fire time (same zone rule).
        max_active_runs: At most this many runs at a time (default 1; ``None``: no limit).
        defaults: Defaults for every task.
        description: Shown in the UI.
        tags: Shown in the UI and used for filtering.

    Raises:
        IdError: If the DAG ID is invalid.
        TypeError: For a wrong argument type, e.g. a string trigger.
        ValueError: For a bad date or a non-positive ``max_active_runs``.
    """

    def __init__(
        self,
        dag_id: str,
        *,
        trigger: Trigger | None = None,
        start: datetime | date | str | None = None,
        end: datetime | date | str | None = None,
        initial_since: datetime | date | str | None = None,
        max_active_runs: int | None = 1,
        defaults: TaskDefaults | None = None,
        description: str | None = None,
        tags: Sequence[str] = (),
    ) -> None:
        """Records the DAG's settings; tasks are added inside the ``with`` block."""
        self.dag_id = validate_dag_id(dag_id)
        if trigger is not None and not isinstance(trigger, Trigger):
            raise TypeError(
                "trigger must be Cron(...), Every(...), Once(...) or None, "
                f"got {type(trigger).__name__}"
            )
        if isinstance(tags, str):
            raise TypeError("tags must be a list of strings, not a string")
        self.trigger = trigger
        tz = trigger.tz if isinstance(trigger, Cron | Once) else "UTC"
        self.start = _moment(start, tz, "start")
        self.end = _moment(end, tz, "end")
        self.initial_since = _moment(initial_since, tz, "initial_since")
        if max_active_runs is not None and max_active_runs <= 0:
            raise ValueError(f"max_active_runs must be positive or None, got {max_active_runs}")
        self.max_active_runs = max_active_runs
        self.defaults = defaults or TaskDefaults()
        self.description = description
        self.tags = tuple(tags)
        self._location = registry.location_for(sys._getframe(1).f_code.co_filename)
        self._builder: _build.DagBuilder | None = None
        self.model: DagModel | None = None

    def __enter__(self) -> Self:
        """Starts defining the DAG's tasks.

        Returns:
            The DAG.

        Raises:
            RuntimeError: If another DAG is being defined.
        """
        self._builder = _build.DagBuilder(self.dag_id, self.defaults.options())
        _build.push(self._builder)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Ends the definition: checks the DAG and records it for the runner.

        Nothing is recorded when the block raised.

        Raises:
            ValueError: If the dependencies form a cycle.
        """
        _build.pop()
        builder = self._builder
        if exc_type is not None or builder is None:
            return
        cycle = builder.find_cycle()
        if cycle is not None:
            raise ValueError(f"DAG `{self.dag_id}` has a dependency cycle: {' -> '.join(cycle)}")
        self._check_since(builder)
        self.model = DagModel(
            id=self.dag_id,
            location=self._location,
            tasks=list(builder.tasks.values()),
            edges=list(builder.edges),
            trigger=NoTrigger() if self.trigger is None else self.trigger.to_model(),
            start=self.start,
            end=self.end,
            initial_since=self.initial_since,
            max_active_runs=self.max_active_runs,
            description=self.description,
            tags=self.tags,
        )
        registry.register(self.model, builder.functions)

    def _check_since(self, builder: _build.DagBuilder) -> None:
        if self.initial_since is not None or isinstance(self.trigger, Cron | Every):
            return
        since = DagCtxRef(DagCtxField.SINCE)
        for task in builder.tasks.values():
            if since in task.references():
                what = "no trigger" if self.trigger is None else "a Once trigger"
                raise ValueError(
                    f"DAG `{self.dag_id}` has {what}, so its first run has no previous fire "
                    f"time, but task `{task.id}` uses dag_ctx.run.since: set initial_since="
                )
