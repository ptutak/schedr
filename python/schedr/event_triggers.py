"""Event triggers: ``@trigger`` and ``@trigger.poke``, whose events start a DAG's runs.

A trigger belongs to its DAG, not to a run: ``Dag("ingest", trigger=new_files(prefix="s3://in/"))``.
It runs in a worker's watcher host for as long as the DAG is live and hands schedr events. Each new
key starts a run, a key seen before starts none, and the last key acted on comes back as ``after``
whenever the trigger starts again.
"""

import dataclasses
import inspect
import typing
import unicodedata
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Concatenate, Self, Unpack, get_args, get_origin, overload

from schedr import _secrets
from schedr._build import to_value
from schedr._model import TypeDesc
from schedr._model.tasks import SensorKind, SensorMode, SensorWaitFor, Task, TaskSettings
from schedr._model.trigger import EventTrigger as EventTriggerModel
from schedr._secrets import SecretsArg
from schedr._serialization import describe
from schedr.connections import ConnectionsArg, declared_connections
from schedr.run_of import RunOf, RunResult, call_limit_ms, run_parameter
from schedr.triggers import UNSET, Moment, Trigger, _Unset, _window

MAX_KEY_BYTES = 1024
"""The longest key an event may have: it is stored with every event."""

AFTER = "after"
"""The parameter every trigger function takes first: the last key acted on, or ``None``."""


@dataclass(frozen=True, slots=True)
class Event[T = None]:
    """What a trigger found: a new ``key`` starts a run, with its ``data``.

    Attributes:
        key: Identifies it in its DAG: a key seen before starts no run. Required, but for an
            ``@trigger(wait_for=...)``'s, where it is that run's ID unless given.
        data: What the run gets as ``dag_ctx.event.data``.
        at: When it happened, if you know: the run is planned for this moment (else for when
            schedr received it). Give it time-zone aware.
    """

    key: str = ""
    # `T` is `None` unless it is given.
    data: T = None  # type: ignore[assignment]  # pyright: ignore[reportAssignmentType]
    at: datetime | None = None

    def __post_init__(self) -> None:
        """Checks what can't be stored.

        Raises:
            TypeError: For a key that isn't a non-empty string, or a naive ``at``.
        """
        if not isinstance(self.key, str):
            raise TypeError(f"an event's key must be a non-empty string, got {self.key!r}")
        control = any(unicodedata.category(c) == "Cc" for c in self.key)
        if len(self.key.encode()) > MAX_KEY_BYTES or control:
            raise TypeError(
                f"an event's key has at most {MAX_KEY_BYTES} bytes and no control characters, got "
                f"{self.key[:40]!r}..."
            )
        if self.at is not None and self.at.tzinfo is None:
            raise TypeError(f"event `{self.key}`: `at` must be time-zone aware, got {self.at}")


class TriggerOptions(typing.TypedDict, total=False):
    """What ``@trigger`` and ``@trigger.poke`` take besides the function."""

    secrets: SecretsArg
    connections: ConnectionsArg


def event_data_hint(annotation: object, where: str) -> object:
    """The type of a trigger's events' data, from its function's return annotation.

    Args:
        annotation: ``AsyncIterator[Event[T]]`` or ``list[Event[T]]``.
        where: Who it is, for messages.

    Returns:
        ``T``; ``None`` for a plain ``Event``.

    Raises:
        TypeError: For an annotation saying something else.
    """
    args = get_args(annotation)
    event = args[0] if len(args) == 1 else None
    if event is Event:
        return None
    if get_origin(event) is Event:
        return get_args(event)[0]
    raise TypeError(
        f"{where}: annotate it with what it gives, `AsyncIterator[Event[T]]` for `@trigger` and "
        f"`list[Event[T]]` for `@trigger.poke`, got `{annotation}`"
    )


@dataclass(frozen=True, slots=True)
class EventTrigger(Trigger):
    """A trigger function called with its arguments: what ``Dag(trigger=...)`` takes.

    Attributes:
        function: The trigger function.
        arguments: Its literal arguments.
        queue: The queue of the workers that may run it.
        restart_if_idle: Started again from its bookmark when no event came for this long.
        start: It runs from this moment on.
        end: It stops at this moment.
    """

    function: TriggerFunction[..., Any]
    arguments: Mapping[str, object] = field(default_factory=dict)
    queue: str | None = None
    restart_if_idle: timedelta | None = None
    start: Moment = None
    end: Moment = None

    def __post_init__(self) -> None:
        """Checks the options.

        Raises:
            ValueError: For a restart time that isn't more than zero, or ``start`` after ``end``.
        """
        if self.restart_if_idle is not None and self.restart_if_idle <= timedelta(0):
            raise ValueError(f"restart_if_idle must be more than zero, got {self.restart_if_idle}")
        _window(self.start, self.end, "UTC")

    def override(
        self,
        *,
        queue: str | _Unset | None = UNSET,
        restart_if_idle: timedelta | _Unset | None = UNSET,
        start: Moment | _Unset = UNSET,
        end: Moment | _Unset = UNSET,
    ) -> Self:
        """The same trigger with some options changed.

        Args:
            queue: The queue of the workers that may run it.
            restart_if_idle: Started again from its bookmark when no event came for this long.
            start: It runs from this moment on.
            end: It stops at this moment.

        Returns:
            A new trigger.

        Raises:
            ValueError: For a value the class rejects.
        """
        return self._changed(
            {"queue": queue, "restart_if_idle": restart_if_idle, "start": start, "end": end}
        )

    def _changed(self, changes: dict[str, object]) -> Self:
        given = {name: value for name, value in changes.items() if value is not UNSET}
        return dataclasses.replace(self, **given)  # type: ignore[arg-type]

    def to_model(self) -> EventTriggerModel:
        """The trigger in DAG JSON.

        Returns:
            Its function as a task of kind ``sensor``, and its options.
        """
        first, last = _window(self.start, self.end, "UTC")
        idle = self.restart_if_idle
        return EventTriggerModel(
            task=self.function.task_model(self.arguments, self.queue),
            restart_if_idle_ms=None if idle is None else round(idle / timedelta(milliseconds=1)),
            start=first,
            end=last,
            run_retries=self.function.run_retries,
        )


def event_for_run[T](event: Event[T], run: RunResult[Any], lag: timedelta | None) -> Event[T]:
    """What an ``@trigger(wait_for=...)``'s event is, as its function returned it for ``run``.

    Its key is the run's ID unless it gives one; its moment the run's ``planned_for`` plus the
    ``RunOf``'s ``lag``, unless it gives one (decision 353).

    Returns:
        The event, filled in.
    """
    at = event.at
    if at is None and lag is not None:
        at = run.planned_for + lag
    return dataclasses.replace(event, key=event.key or run.run_id, at=at)


DEFAULT_CALL_TIMEOUT = timedelta(minutes=10)
"""How long an ``@trigger(wait_for=...)``'s call may take unless it says (decision 353)."""


class TriggerFunction[**P, R]:
    """A function decorated with ``@trigger`` or ``@trigger.poke``.

    Called with its arguments, without ``after``, it is what ``Dag(trigger=...)`` takes.
    """

    def __init__(
        self,
        fn: Callable[..., object],
        every: timedelta | None,
        options: TriggerOptions,
        wait_for: RunOf | None = None,
        run_retries: int | None = None,
        timeout: timedelta | None = None,
    ) -> None:
        """Wraps ``fn`` as a trigger.

        Args:
            fn: The function.
            every: How often to call it (``@trigger.poke``); ``None``: it yields events.
            options: What it declares.
            wait_for: ``@trigger(wait_for=RunOf(...))``: called once per run of that DAG that
                ends, with it as its first parameter (decision 353).
            run_retries: With ``wait_for``, how many times a run it raised on is tried again
                before it is passed; ``None``: tried until ``schedr triggers skip`` passes it.
            timeout: With ``wait_for``, how long one call may take, 10 minutes by default; a call
                over it is cancelled, failing as a raise does.

        Raises:
            TypeError: If ``fn`` can't be a trigger.
        """
        self.fn = fn
        self.task_id: str = getattr(fn, "__name__", "trigger")
        self._every = every
        self.wait_for = wait_for
        self.run_retries = run_retries
        # With `wait_for=`: the parameter the run is given as, and how long a call may take.
        self.run_param = ""
        self.timeout_ms: int | None = None
        where = f"@trigger `{self.task_id}`"
        if wait_for is None and timeout is not None:
            raise TypeError(f"{where}: `timeout` is for a trigger with `wait_for=`")
        if wait_for is not None:
            self.timeout_ms = call_limit_ms(
                DEFAULT_CALL_TIMEOUT if timeout is None else timeout, where
            )
            self._init_waiting(fn, where, options)
            return
        if run_retries is not None:
            raise TypeError(f"{where}: `run_retries` is for a trigger with `wait_for=`")
        if every is None and not inspect.isasyncgenfunction(fn):
            raise TypeError(
                f"{where} must be an async generator (`async def` with `yield`): it shares its "
                "event loop with others, and yields events for as long as its DAG is live"
            )
        if every is not None and not inspect.iscoroutinefunction(fn):
            raise TypeError(
                f"@trigger.poke `{self.task_id}` must be `async def`, returning its events: it "
                "shares its event loop with others"
            )
        if every is not None and every <= timedelta(0):
            raise TypeError(f"{where}: `every` must be more than zero, got {every}")
        self._signature = inspect.signature(fn)
        if next(iter(self._signature.parameters), None) != AFTER:
            raise TypeError(
                f"{where}: its first parameter is `{AFTER}: str | None`, the last key acted on"
            )
        self._secrets = _secrets.declared(options.get("secrets") or (), where)
        self._connections = declared_connections(options.get("connections"), where) or ()
        self._data_hint = event_data_hint(typing.get_type_hints(fn).get("return"), where)

    def _init_waiting(self, fn: Callable[..., object], where: str, options: TriggerOptions) -> None:
        """As ``__init__``, for ``wait_for=``: the run first, ``-> Event[T] | None``."""
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(
                f"{where}: with `wait_for=`, it is `async def`, called once per run and returning "
                "an `Event` or `None`"
            )
        if self.run_retries is not None and self.run_retries < 0:
            raise TypeError(f"{where}: `run_retries` is at least 0, got {self.run_retries}")
        lag = self.wait_for.lag if self.wait_for is not None else None
        if lag is not None and lag < timedelta(0):
            raise TypeError(
                f"{where}: its RunOf's `lag` can't be negative: a run can't be planned before "
                "the one starting it"
            )
        self.run_param = run_parameter(fn, where)
        signature = inspect.signature(fn)
        self._signature = signature.replace(parameters=list(signature.parameters.values())[1:])
        self._secrets = _secrets.declared(options.get("secrets") or (), where)
        self._connections = declared_connections(options.get("connections"), where) or ()
        returned = typing.get_type_hints(fn).get("return")
        kept = [a for a in get_args(returned) if a is not type(None)]
        event = kept[0] if len(kept) == 1 and type(None) in get_args(returned) else None
        if event is not Event and get_origin(event) is not Event:
            raise TypeError(
                f"{where}: with `wait_for=`, annotate it `-> Event[T] | None`, got `{returned}`"
            )
        self._data_hint = None if event is Event else get_args(event)[0]

    def __call__(self, *args: object, **kwargs: object) -> EventTrigger:
        """The trigger with its arguments, for ``Dag(trigger=...)``.

        Returns:
            What ``Dag(trigger=...)`` takes.

        Raises:
            TypeError: For arguments its function doesn't take.
        """
        where = f"@trigger `{self.task_id}`"
        if self.wait_for is not None:
            try:
                bound = self._signature.bind(*args, **kwargs)
            except TypeError as e:
                raise TypeError(f"{where}: {e}") from e
            return EventTrigger(self, dict(bound.arguments))
        if AFTER in kwargs:
            raise TypeError(f"{where}: schedr fills in `{AFTER}`")
        try:
            bound = self._signature.bind(None, *args, **kwargs)
        except TypeError as e:
            raise TypeError(f"{where}: {e}") from e
        arguments = {name: value for name, value in bound.arguments.items() if name != AFTER}
        return EventTrigger(self, arguments)

    @property
    def every(self) -> timedelta | None:
        """How often schedr calls it, for ``@trigger.poke``."""
        return self._every

    @property
    def every_ms(self) -> int:
        """``every`` in milliseconds; 0 for a generator."""
        return 0 if self._every is None else round(self._every / timedelta(milliseconds=1))

    def data_type(self) -> TypeDesc:
        """The type of its events' data.

        Returns:
            What ``dag_ctx.event.data`` is.
        """
        return describe(self._data_hint)

    def task_model(self, arguments: Mapping[str, object], queue: str | None) -> Task:
        """Its function as a task of kind ``sensor``, which isn't in the graph.

        Args:
            arguments: Its literal arguments.
            queue: Its queue; ``None``: the default one.

        Returns:
            The task.

        Raises:
            TypeError: For an argument that isn't a literal.
        """
        values = {}
        for name, value in arguments.items():
            upstream: list[Any] = []
            values[name] = to_value(value, name, upstream)
            if upstream or values[name].references():
                raise TypeError(
                    f"@trigger `{self.task_id}`: `{name}` must be a literal, as no run exists yet"
                )
        every = self._every
        wait_for = self.wait_for
        kind = SensorKind(
            values,
            SensorMode.LOOP if every is None else SensorMode.POKE,
            None if every is None else round(every / timedelta(milliseconds=1)),
            wait_for=None
            if wait_for is None
            else SensorWaitFor(
                wait_for.dag, wait_for.task, wait_for.lag_ms, self.run_param, self.timeout_ms
            ),
        )
        settings = TaskSettings(timeout_ms=None)
        if queue is not None:
            settings = dataclasses.replace(settings, queue=queue)
        return Task(
            self.task_id,
            kind,
            self.data_type(),
            settings,
            secrets=self._secrets,
            connections=self._connections,
        )


class _TriggerDecorator(typing.Protocol):
    """What ``@trigger(...)`` gives."""

    def __call__[T, **P](
        self, fn: Callable[P, AsyncIterator[Event[T]]], /
    ) -> TriggerFunction[..., T]: ...


class _WaitForDecorator(typing.Protocol):
    """What ``@trigger(wait_for=...)`` gives: the run is its first parameter, never the DAG's."""

    def __call__[T, D, **P](
        self,
        fn: Callable[Concatenate[RunResult[T], P], Coroutine[object, object, Event[D] | None]],
        /,
    ) -> TriggerFunction[P, D]: ...


class _PokeDecorator(typing.Protocol):
    """What ``@trigger.poke(...)`` gives."""

    def __call__[T, **P](
        self, fn: Callable[P, Coroutine[object, object, Sequence[Event[T]]]], /
    ) -> TriggerFunction[..., T]: ...


class _Trigger:
    """``@trigger`` and ``@trigger.poke``."""

    @overload
    def __call__[T, **P](
        self, fn: Callable[P, AsyncIterator[Event[T]]], /
    ) -> TriggerFunction[..., T]: ...
    @overload
    def __call__(
        self,
        /,
        *,
        wait_for: RunOf,
        run_retries: int | None = None,
        timeout: timedelta | None = None,
        **options: Unpack[TriggerOptions],
    ) -> _WaitForDecorator: ...
    @overload
    def __call__(self, /, **options: Unpack[TriggerOptions]) -> _TriggerDecorator: ...
    def __call__(
        self,
        fn: Callable[..., object] | None = None,
        /,
        *,
        wait_for: RunOf | None = None,
        run_retries: int | None = None,
        timeout: timedelta | None = None,
        **options: Unpack[TriggerOptions],
    ) -> object:
        """Makes an async generator a trigger: ``-> AsyncIterator[Event[T]]``, yielding events.

        It takes ``after: str | None`` first, the last key acted on: go on from there. It never
        ends; if it returns, or raises, it is an error on its status. Read secrets and open
        connections as a task does, declaring them here.

        With ``wait_for=RunOf(...)`` it is an ``async def`` instead, called once for each run of
        that DAG (or its task) that ends while this DAG is live, whatever its state, with it as
        its first parameter, ``run: RunResult[T]``: ``-> Event[D] | None``, ``None`` starting no
        run. The event's key is that run's ID unless it gives one. One that raises is tried
        again on the same run; ``run_retries`` passes it after so many tries, and ``schedr
        triggers skip <dag>`` passes it by hand (decision 353).

        Args:
            fn: The function (``@trigger`` without arguments).
            wait_for: The runs it is called for.
            run_retries: With ``wait_for``, how many times a run it raised on is tried again
                before it is passed; ``None``: until ``schedr triggers skip``.
            timeout: With ``wait_for``, how long one call may take, 10 minutes by default; one
                over it is cancelled and fails as a raise does. A call blocking the event loop
                (``time.sleep``) is ended by the host's 30-second rule instead.
            **options: ``secrets`` and ``connections``, as for ``@task``.

        Returns:
            The trigger, or a decorator making one.
        """
        if wait_for is not None and not isinstance(wait_for, RunOf):
            raise TypeError(f"@trigger: `wait_for` takes a RunOf, got {wait_for!r}")

        def made(function: Callable[..., object]) -> object:
            return TriggerFunction(function, None, options, wait_for, run_retries, timeout)

        return made if fn is None else made(fn)

    def poke(self, *, every: timedelta, **options: Unpack[TriggerOptions]) -> _PokeDecorator:
        """Makes an ``async def`` function a trigger that schedr calls every ``every``.

        It returns the events it found, oldest first: ``-> list[Event[T]]``. An empty list leaves
        no trace. Each call is limited to ``every``, and calls never overlap.

        Args:
            every: How often to call it.
            **options: As for ``@trigger``.

        Returns:
            A decorator making the trigger.
        """

        def made(function: Callable[..., object]) -> object:
            return TriggerFunction(function, every, options)

        return made  # type: ignore[return-value]  # the protocol says what it keeps


trigger = _Trigger()
"""``@trigger`` and ``@trigger.poke``."""
