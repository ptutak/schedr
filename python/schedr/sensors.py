"""Wait steps: ``@sensor`` and ``@sensor.poke``.

A wait step is a task that waits for one thing belonging to its run and ends, with a value or
with the ``TimedOut`` outcome (waiting in vain is an expected problem, not a failure).
"""

from __future__ import annotations

import functools
import types
import typing
from dataclasses import replace
from datetime import timedelta
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Final,
    Self,
    Union,
    Unpack,
    cast,
    get_args,
    get_origin,
    overload,
)

from schedr import _build
from schedr import outcomes as _outcomes
from schedr._model import SensorKind, SensorMode, SensorWaitFor
from schedr._secrets import SecretsArg
from schedr._wait_rules import refused_in
from schedr.connections import ConnectionsArg
from schedr.event_triggers import Event
from schedr.named_events import EventNamed
from schedr.run_of import RunOf, RunResult, call_limit_ms, run_parameter
from schedr.tasks import AsyncTaskFunction, Output, TaskFunction, is_async, task

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping, Sequence

    from schedr._model import TaskKind, TypeDesc, Value


class NotYet(Enum):
    """The type of ``NOT_YET``: what ``@sensor.poke`` returns while it has nothing."""

    NOT_YET = "not_yet"


NOT_YET: Final = NotYet.NOT_YET
"""What a ``@sensor.poke`` function returns until it has its value (so ``None`` is a value); an
``@sensor(wait_for=...)`` returns it for a run that isn't the one, to wait for a later one."""

NOT_YET_OUTCOME: Final = "NotYet"
"""The outcome an ``@sensor(wait_for=...)``'s attempt ends with when it returned ``NOT_YET``:
the master then waits for a later matching run (its ``NOT_YET_OUTCOME``)."""


class SensorOptions(typing.TypedDict, total=False):
    """``@task``'s options that a wait step has too (decision 323)."""

    task_id: str | None
    retries: int | None
    retry_delay: timedelta | None
    queue: str | None
    max_output_size: int | None
    run_if: _outcomes.RunIf | None
    max_active: int | None
    max_active_per_run: int | None
    secrets: SecretsArg
    connections: ConnectionsArg
    uses: Sequence[object] | None
    max_wait: timedelta | None
    """How long it waits before ending with ``TimedOut``, from its first start, across retries;
    default: ``max_wait`` of ``StepDefaults``, then ``[sensors] default_max_wait``."""


def output_hint(annotation: object) -> object:
    """A wait step's return annotation without ``NotYet``: the type of its output.

    Args:
        annotation: The annotation as written.

    Returns:
        ``Path`` for ``Path | NotYet``; the annotation itself if ``NotYet`` isn't in it.
    """
    resolved = _outcomes.resolve_alias(annotation)
    if not _has_not_yet(resolved):
        return annotation
    kept = [member for member in get_args(resolved) if member is not NotYet]
    return functools.reduce(lambda a, b: a | b, kept)


def _has_not_yet(annotation: object) -> bool:
    resolved = _outcomes.resolve_alias(annotation)
    return get_origin(resolved) in {Union, types.UnionType} and NotYet in get_args(resolved)


def _milliseconds(value: timedelta, name: str, where: str) -> int:
    if value <= timedelta(0):
        raise TypeError(f"{where}: `{name}` must be more than zero, got {value}")
    return round(value / timedelta(milliseconds=1))


class SensorFunction[**P, R, A = R](AsyncTaskFunction[P, R, A]):
    """A function decorated with ``@sensor`` or ``@sensor.poke``: a wait step.

    In a DAG it is a task like any other, whose call reads as ``Waited[...]``: its value or
    ``TimedOut``, which a task taking it routes with ``.on(...)`` or expects in its parameter.
    A plain ``>>`` runs when it has its value (decision 324).
    """

    def __init__(
        self,
        fn: Callable[P, R],
        task_id: str,
        options: _build.TaskOptions,
        every: timedelta | None,
        wait_for: RunOf | EventNamed | None = None,
        call_timeout_ms: int | None = None,
    ) -> None:
        """Wraps ``fn`` as a wait step.

        Args:
            fn: The ``async def`` function.
            task_id: The task's ID.
            options: The task's own options, with how long it waits.
            every: How often to call it (``@sensor.poke``); ``None``: called once.
            wait_for: ``@sensor(wait_for=RunOf(...))``: called once that run has ended, with it
                as its first parameter (decision 353); ``EventNamed(...)``: once such an event
                is posted, with it (decision 354).
            call_timeout_ms: With ``wait_for``, how long one call may take; ``None``: what is
                left of ``max_wait``.

        Raises:
            TypeError: If ``fn`` can't be a wait step.
        """
        where = f"@sensor `{task_id}`"
        super().__init__(fn, task_id, options)
        self._wait_for = wait_for
        self._call_timeout_ms = call_timeout_ms
        self._run_param: str | None = None
        # An `EventNamed`'s key, as stored, while the step is being added (see `_record`).
        self._event_key: Value | None = None
        self._key_upstream: list[_build.Upstream] = []
        if wait_for is not None:
            given = Event if isinstance(wait_for, EventNamed) else RunResult
            self._run_param = run_parameter(fn, where, given)
            # The DAG never gives it: the master does, with the run (see `_kind`).
            rest = list(self._signature.parameters.values())[1:]
            self._signature = self._signature.replace(parameters=rest)
        if not is_async(fn):
            raise TypeError(
                f"{where} is a plain `def`: a wait step shares its event loop with others, so it "
                "must be `async def`"
            )
        self._every = every
        self._every_ms = None if every is None else _milliseconds(every, "every", where)
        if options.max_wait is not None:
            _milliseconds(options.max_wait, "max_wait", where)

    def _record(
        self,
        builder: _build.DagBuilder,
        fixed: dict[str, object],
        expand: tuple[str, object] | None,
    ) -> Output[Any]:
        if expand is not None:
            # Each instance could end its own way, and `TimedOut` has no element-wise routing yet.
            raise TypeError(f"@sensor `{self._task_id}`: a wait step can't be mapped for now")
        # Here, not at the decorator: the file has defined what it refers to by now.
        refused = refused_in(self._fn)
        if refused is not None:
            raise TypeError(
                f"@sensor `{self._task_id}`: `{refused.what}` ({refused.where}) {refused.why}"
            )
        wait_for = self._wait_for
        if not isinstance(wait_for, EventNamed) or wait_for.key is None:
            return super()._record(builder, fixed, expand)
        # Its key may come from upstream: an edge from that task, as for an argument.
        self._key_upstream = []
        self._event_key = _build.to_value(wait_for.key, "EventNamed key", self._key_upstream)
        try:
            return super()._record(builder, fixed, expand)
        finally:
            self._event_key = None
            self._key_upstream = []

    def _kind_upstream(self) -> list[_build.Upstream]:
        return list(self._key_upstream)

    def _output_hint(self, hints: Mapping[str, object], where: str) -> object:
        annotation = hints["return"]
        if self._wait_for is not None:
            # NOT_YET waits for a later run: never its output.
            return output_hint(annotation)
        if self._every is None:
            if _has_not_yet(annotation):
                raise TypeError(
                    f"{where}: a @sensor waits by awaiting until it has its value, called once: "
                    "`NOT_YET` is for @sensor.poke (called again) or @sensor(wait_for=...) (given "
                    f"a later run); annotate it without `NotYet`, got `{annotation}`"
                )
            return annotation
        if not _has_not_yet(annotation):
            raise TypeError(
                f"{where}: a @sensor.poke function returns its value or `NOT_YET`: annotate it "
                f"`-> ... | NotYet`, got `{annotation}`"
            )
        return output_hint(annotation)

    def _outcome_spec(self, annotation: object, where: str) -> _outcomes.OutcomeSpec:
        spec = _outcomes.from_return_type(annotation, where)
        return _outcomes.with_timed_out(spec, annotation, where)

    def _produced(self, annotation: object) -> object:
        return Union[annotation, _outcomes.TimedOut]  # noqa: UP007 (made at run time)

    def _outcome_types(
        self,
        spec: _outcomes.OutcomeSpec,
        annotation: object,
        describe: Callable[[object], TypeDesc],
    ) -> dict[str, TypeDesc]:
        if spec.is_union:
            return super()._outcome_types(spec, annotation, describe)
        return {
            _outcomes.SUCCESS_NAME: describe(annotation),
            _outcomes.TIMED_OUT_NAME: describe(_outcomes.TimedOut),
        }

    def _kind(self, arguments: Mapping[str, Value]) -> TaskKind:
        mode = SensorMode.LOOP if self._every is None else SensorMode.POKE
        wait_for = self._wait_for
        waits = (
            None
            if wait_for is None or self._run_param is None
            else SensorWaitFor(
                None,
                None,
                None,
                self._run_param,
                self._call_timeout_ms,
                event_name=wait_for.name,
                event_key=self._event_key,
            )
            if isinstance(wait_for, EventNamed)
            else SensorWaitFor(
                wait_for.dag,
                wait_for.task,
                wait_for.lag_ms,
                self._run_param,
                self._call_timeout_ms,
            )
        )
        # `max_wait` comes with the options, from the defaults too: see `add_task`.
        return SensorKind(arguments, mode, self._every_ms, wait_for=waits)

    def _again(
        self,
        task_id: str,
        options: _build.TaskOptions,
        cleanup: Callable[..., object] | None,
    ) -> Self:
        if cleanup is not None:
            raise TypeError(_no_cleanup(task_id))
        if options.timeout is not None:
            raise TypeError(
                f"@sensor `{task_id}`: `timeout` limits a task's work; a wait step has `max_wait`"
            )
        made = SensorFunction(
            self._fn, task_id, options, self._every, self._wait_for, self._call_timeout_ms
        )
        return cast("Self", made)


def _no_cleanup(task_id: str) -> str:
    return (
        f"@sensor `{task_id}`: a wait step has no `cleanup=` for now; use `finally` in the "
        "function (decision 323)"
    )


def _made[**P, R, A](
    fn: Callable[P, object],
    every: timedelta | None,
    options: SensorOptions,
    wait_for: RunOf | EventNamed | None = None,
    timeout: timedelta | None = None,
) -> SensorFunction[P, R, A]:
    max_wait = options.pop("max_wait", None)
    where = f"@sensor `{getattr(fn, '__name__', '')}`"
    call_timeout_ms = None if timeout is None else call_limit_ms(timeout, where)
    if wait_for is not None and not isinstance(wait_for, RunOf | EventNamed):
        raise TypeError(f"@sensor: `wait_for` takes a RunOf or an EventNamed, got {wait_for!r}")
    for name in options:
        if name not in SensorOptions.__annotations__:
            raise TypeError(
                _no_cleanup(getattr(fn, "__name__", ""))
                if name.startswith("cleanup")
                # The user's, option A: a poke is called on a timer, never per run.
                else "@sensor.poke: `wait_for=` is for `@sensor`, called once per matching run; "
                "a poke is called on a timer, and can read a run from the API itself"
                if name == "wait_for"
                else "@sensor: `timeout` limits a task's work; a wait step has `max_wait`"
                if name == "timeout"
                else f"@sensor: `{name}` isn't an option of a wait step"
            )
    # `@task` checks the options as it does for any task.
    decorate = cast("Callable[..., Callable[..., TaskFunction[..., object]]]", task)
    plain = decorate(**options)(fn)
    made = replace(plain._options, max_wait=max_wait)
    return SensorFunction(
        cast("Callable[P, R]", fn), plain.task_id, made, every, wait_for, call_timeout_ms
    )


class _SensorDecorator(typing.Protocol):
    """What ``@sensor(...)`` gives."""

    @overload
    def __call__[**P](
        self, fn: Callable[P, Coroutine[object, object, None]], /
    ) -> SensorFunction[P, _outcomes.Waited[None], None]: ...
    @overload
    def __call__[**P, R](
        self, fn: Callable[P, Coroutine[object, object, R]], /
    ) -> SensorFunction[P, _outcomes.Waited[R], R]: ...


class _WaitForDecorator(typing.Protocol):
    """What ``@sensor(wait_for=...)`` gives: the run is its first parameter, never the DAG's."""

    def __call__[**P, T, R](
        self, fn: Callable[Concatenate[RunResult[T], P], Coroutine[object, object, R | NotYet]], /
    ) -> SensorFunction[P, _outcomes.Waited[R], R | NotYet]: ...


class _WaitForEventDecorator(typing.Protocol):
    """What ``@sensor(wait_for=EventNamed(...))`` gives: the event is its first parameter."""

    def __call__[**P, T, R](
        self, fn: Callable[Concatenate[Event[T], P], Coroutine[object, object, R | NotYet]], /
    ) -> SensorFunction[P, _outcomes.Waited[R], R | NotYet]: ...


class _PokeDecorator(typing.Protocol):
    """What ``@sensor.poke(...)`` gives: its function's value is what isn't ``NOT_YET``."""

    def __call__[**P, R](
        self, fn: Callable[P, Coroutine[object, object, R | NotYet]], /
    ) -> SensorFunction[P, _outcomes.Waited[R], R | NotYet]: ...


class _Sensor:
    """``@sensor`` and ``@sensor.poke``."""

    @overload
    def __call__[**P](
        self, fn: Callable[P, Coroutine[object, object, None]], /
    ) -> SensorFunction[P, _outcomes.Waited[None], None]: ...
    @overload
    def __call__[**P, R](
        self, fn: Callable[P, Coroutine[object, object, R]], /
    ) -> SensorFunction[P, _outcomes.Waited[R], R]: ...
    @overload
    def __call__(
        self,
        /,
        *,
        wait_for: RunOf,
        timeout: timedelta | None = None,
        **options: Unpack[SensorOptions],
    ) -> _WaitForDecorator: ...
    @overload
    def __call__(
        self,
        /,
        *,
        wait_for: EventNamed,
        timeout: timedelta | None = None,
        **options: Unpack[SensorOptions],
    ) -> _WaitForEventDecorator: ...
    @overload
    def __call__(self, /, **options: Unpack[SensorOptions]) -> _SensorDecorator: ...
    def __call__(
        self,
        fn: Callable[..., object] | None = None,
        /,
        *,
        wait_for: RunOf | EventNamed | None = None,
        timeout: timedelta | None = None,
        **options: Unpack[SensorOptions],
    ) -> object:
        """Makes an ``async def`` function a wait step, which you write the waiting of.

        With ``wait_for=RunOf(...)`` the master waits, with no call and no slot, until a matching
        run of that DAG (or its task) has ended, whatever its state, then calls the function once
        with it as its first parameter, ``run: RunResult[T]``. Returning ``NOT_YET`` waits for a
        later matching run (with ``lag``, a newer one for the same moment, as when it is run
        again), until ``max_wait`` (decision 353).

        With ``wait_for=EventNamed(...)`` it is called the same way once an event of that name
        (and key) is posted since its run was created, ``event: Event[T]``; ``NOT_YET`` waits for
        a later one (decision 354).

        Args:
            fn: The function (``@sensor`` without arguments).
            wait_for: The run, or the event, it is called for.
            timeout: With ``wait_for``, how long one call may take, below what is left of
                ``max_wait``: one over it is cancelled and fails the attempt, whose retries get
                the same run (``TimedOut`` stays for ``max_wait``). ``None``: no limit of its own.
            **options: ``max_wait``, how long it waits before ending with the ``TimedOut``
                outcome; the others as for ``@task``, but for ``cleanup=`` (use ``finally``).

        Returns:
            The wait step, or a decorator making one.

        Raises:
            TypeError: For a ``timeout`` without ``wait_for``.
        """
        if timeout is not None and wait_for is None:
            raise TypeError(
                "@sensor: `timeout` limits a task's work; a wait step has `max_wait` (with "
                "`wait_for=`, `timeout` limits one call)"
            )

        def made(function: Callable[..., object]) -> object:
            return _made(function, None, options.copy(), wait_for, timeout)

        return made if fn is None else made(fn)

    def poke(self, *, every: timedelta, **options: Unpack[SensorOptions]) -> _PokeDecorator:
        """Makes an ``async def`` function a wait step that schedr calls every ``every``.

        It returns its value, or ``NOT_YET`` until it has one: ``-> Path | NotYet``. Each call is
        limited to ``every``, and calls never overlap: one that raises, or runs out its time, is
        a failure with the usual retries.

        Args:
            every: How often to call it.
            **options: As for ``@sensor``.

        Returns:
            A decorator making the wait step.
        """

        def made(function: Callable[..., object]) -> object:
            return _made(function, every, options.copy())

        return cast("_PokeDecorator", made)


sensor: Final = _Sensor()
"""``@sensor`` and ``@sensor.poke``: wait steps."""
