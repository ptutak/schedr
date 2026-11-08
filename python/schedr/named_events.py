"""Events posted through the API: ``EventNamed`` (decision 354).

An outside system posts ``POST /api/v1/events`` (or ``schedr events post``) with a name, a key
and data. ``Dag("ship", trigger=EventNamed("payment-completed"))`` starts a run per key posted
while it is live; ``wait_for(EventNamed("payment-completed", key=dag_ctx.params.order))`` in a
run waits for the first of that key posted since the run was created.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

from schedr import _build
from schedr import outcomes as _outcomes
from schedr._model import EdgeKind, EventNamedTrigger, WaitForEventKind, validate_dag_id
from schedr._model.values import LiteralValue
from schedr._serialization.describe import describe
from schedr.event_triggers import MAX_KEY_BYTES, Event, acts_on_display
from schedr.tasks import Output
from schedr.triggers import Trigger
from schedr.types import Json

if TYPE_CHECKING:
    from schedr.outcomes import Waited


@dataclass(frozen=True, slots=True)
class EventNamed(Trigger):
    """Events of one name posted through the API; with ``key``, the one of that key.

    As a DAG's trigger, ``Dag("ship", trigger=EventNamed("payment-completed"))``: each event
    posted while the DAG is live starts a run, one per key (a key seen before starts none), with
    the event as ``dag_ctx.event``, planned for when it happened. In a run,
    ``wait_for(EventNamed("payment-completed", key=dag_ctx.params.order))`` waits for the first
    event of that name and key posted since the run was created (one posted before the step
    began waiting counts).

    Attributes:
        name: The events' name, written as a DAG ID is.
        key: Which key ``wait_for`` waits for: a string, a run parameter, ``dag_ctx.run.id``,
            ``dag_ctx.dag_id``, ``dag_ctx.event.key`` or an upstream task's output; ``None``:
            any. A trigger takes none: every key starts a run.
    """

    name: str
    key: str | None = None

    def __post_init__(self) -> None:
        """Checks the name, and a literal key.

        Raises:
            TypeError: For a key that is neither a string nor a reference, or one that can't
                be an event's (empty, over 1024 bytes, control characters).
        """
        validate_dag_id(self.name)
        key: object = self.key
        if key is None or _build.is_reference(key):
            return
        if not isinstance(key, str):
            raise TypeError(
                f"EventNamed `{self.name}`: `key` is a string or a reference (a run parameter, "
                f"dag_ctx.run.id, an upstream output), got {key!r}"
            )
        control = acts_on_display(key)
        if not key or len(key.encode()) > MAX_KEY_BYTES or control:
            raise TypeError(
                f"EventNamed `{self.name}`: an event's key has 1 to {MAX_KEY_BYTES} bytes and no "
                f"control, line-separator or bidirectional characters, got {key[:40]!r}"
            )

    def to_model(self) -> EventNamedTrigger:
        """The trigger in DAG JSON.

        Returns:
            Its model.

        Raises:
            ValueError: For a key: a trigger starts a run for every key.
        """
        if self.key is not None:
            raise ValueError(
                f"EventNamed `{self.name}` as a trigger takes no `key`: each key posted starts "
                "a run (a key seen before starts none)"
            )
        return EventNamedTrigger(self.name)


def _free_id(builder: _build.DagBuilder, name: str) -> str:
    base = "".join(c if c.isalnum() or c in "_-" else "_" for c in f"wait_for_{name}")
    task_id = builder.qualified(base)
    number = 1
    while task_id in builder.tasks:
        number += 1
        task_id = builder.qualified(f"{base}_{number}")
    return task_id


def wait_for_event(
    event: EventNamed, *, max_wait: timedelta | None = None, task_id: str | None = None
) -> Waited[Event[Json]]:
    """``wait_for(EventNamed(...))``: a step the master ends when the event comes.

    Args:
        event: Which event, and its key.
        max_wait: How long it waits; the master's ``[sensors] default_max_wait`` if not given.
        task_id: Its ID; ``wait_for_<name>``, numbered from the second on, if not given.

    Returns:
        The step's output: the event, read as its value, or ``TimedOut``.
    """
    builder = _build.require_active("wait_for")
    node = builder.qualified(task_id) if task_id is not None else _free_id(builder, event.name)
    upstream: list[_build.Upstream] = []
    key = None if event.key is None else _build.to_value(event.key, "EventNamed key", upstream)
    if isinstance(key, LiteralValue) and not isinstance(key.value, str):
        raise TypeError(f"EventNamed `{event.name}`: `key` is a string, got {key.value!r}")
    spec = _outcomes.OutcomeSpec(
        names=(_outcomes.SUCCESS_NAME, _outcomes.TIMED_OUT_NAME),
        timed_out=True,
        value=Event[Json],
    )
    builder.add_task(
        node,
        WaitForEventKind(event.name, key),
        describe(Event[Json] | _outcomes.TimedOut),
        _build.TaskOptions(max_wait=max_wait),
        outcomes=spec,
        outcome_types={
            _outcomes.SUCCESS_NAME: describe(Event[Json]),
            _outcomes.TIMED_OUT_NAME: describe(_outcomes.TimedOut),
        },
    )
    for up, on in dict.fromkeys(upstream):
        builder.add_edge(up, node, EdgeKind.DATA, on)
    return Output(node, dag=builder)  # type: ignore[return-value]
