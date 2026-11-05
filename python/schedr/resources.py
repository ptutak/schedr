"""Resources: one thing of two parts, a setup and its cleanup (decision 259).

A task is a set of instructions, and may have no cleanup at all. A resource is by definition
created and undone: both parts always exist, as tasks of the DAG, the cleanup running once every
user is done. One without ``@cleanup`` has an empty cleanup (decision 260).
"""

from __future__ import annotations

import inspect
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from string.templatelib import Interpolation
from string.templatelib import Template as TString
from typing import Any, Self, TypedDict, Unpack, overload

from schedr import _build, _secrets
from schedr import outcomes as _outcomes
from schedr._model import EdgeKind, EmptyKind, ResourceRole, ScalarType, SecretDecl
from schedr._model.tasks import DEFAULT_CLEANUP_RETRIES, DEFAULT_CLEANUP_RETRY_DELAY_MS
from schedr._secrets import SecretsArg
from schedr._serialization.annotations import type_hints, unwrap
from schedr._serialization.describe import describe
from schedr._templates import is_tstring
from schedr.connections import Connection, ConnectionsArg, declared_connections
from schedr.operators import CommandSettings, Operator, add_command
from schedr.outcomes import RunIf
from schedr.tasks import Output, TaskFunction, task

NO_VALUE = (None, type(None))
"""What a setup without a value returns."""


class ResourceOptions(TypedDict, total=False):
    """``@resource``'s options: as for ``@task``."""

    task_id: str | None
    retries: int | None
    retry_delay: timedelta | None
    timeout: timedelta | None
    queue: str | None
    max_output_size: int | None
    preload: bool | None
    fresh_process: bool | None
    run_if: _outcomes.RunIf | None
    max_active: int | None
    max_active_per_run: int | None
    secrets: SecretsArg
    connections: ConnectionsArg
    cleanup: Callable[..., object] | None
    cleanup_timeout: timedelta | None
    cleanup_retries: int | None
    cleanup_retry_delay: timedelta | None
    cleanup_anywhere: bool | None
    uses: Sequence[object] | None


class CleanupOptions(TypedDict, total=False):
    """``.cleanup()``'s options.

    Attributes:
        timeout: How long one try may run (default: the worker's
            ``resource_cleanup_timeout``, 30 minutes), for each try: with retries, a hung one
            holds its run up to that times the tries. It isn't cancelled with its run;
            ``schedr tasks cancel`` stops it.
        retries: How many times it is tried again.
        retry_delay: The delay before each try again.
        queue: Its queue; the setup's if not given.
        secrets: Secrets it declares, besides its setup's.
        connections: Connections it declares, besides its setup's.
        blocking: Whether what follows the resource's group waits for this cleanup (the
            default). With ``False`` it goes on, and isn't stopped if the cleanup fails; the
            run still fails.
        unschedulable_timeout: How long a user may wait for a worker (``unschedulable``)
            before it fails, so this cleanup can run; no limit if not given.
    """

    timeout: timedelta | None
    retries: int | None
    retry_delay: timedelta | None
    queue: str | None
    secrets: SecretsArg
    connections: ConnectionsArg
    blocking: bool
    unschedulable_timeout: timedelta | None


type _WithValue[T] = Callable[[Callable[[T], object]], Callable[[T], object]]
type _WithoutValue = Callable[[Callable[[], object]], Callable[[], object]]


@dataclass(frozen=True, slots=True)
class _Command:
    """What ``cleanup_command`` declared: run as given, its value references resolved."""

    command: str | TString | Sequence[object]
    env: Mapping[str, object] | None


@dataclass(frozen=True, slots=True)
class _Cleanup:
    """What ``.cleanup`` or ``.cleanup_command`` declared."""

    run: Callable[..., object] | _Command
    origin: tuple[object, object]
    timeout: timedelta | None
    retries: int | None
    retry_delay: timedelta | None
    queue: str | None
    secrets: tuple[SecretDecl, ...]
    connections: tuple[str, ...]
    blocking: bool
    unschedulable_timeout: timedelta | None


def _origin(fn: Callable[..., object]) -> tuple[object, object]:
    """Where a function was defined, the same when its file is imported again."""
    return getattr(fn, "__module__", None), getattr(fn, "__qualname__", None)


def _call_site(depth: int) -> tuple[object, object]:
    """Where a ``cleanup_command`` call is, the same when its file is imported again."""
    frame = sys._getframe(depth + 1)
    return frame.f_code.co_filename, frame.f_lineno


class _Value:
    """A resource's value, or a part of it, in a ``cleanup_command``.

    It becomes a reference to its setup's output once the DAG names the setup.
    """

    __slots__ = ("_declared", "_path")

    def __init__(self, declared: _Declared, path: tuple[str | int, ...] = ()) -> None:
        """References ``declared``'s value at ``path``."""
        self._declared = declared
        self._path = path

    def __getattr__(self, name: str) -> _Value:
        """References a field of the value.

        Raises:
            AttributeError: For names starting with ``_``.
        """
        if name.startswith("_"):
            raise AttributeError(name)
        return _Value(self._declared, (*self._path, name))

    def __getitem__(self, key: str | int) -> _Value:
        """References a key or an index of the value."""
        return _Value(self._declared, (*self._path, key))

    def resolved(self, builder: _build.DagBuilder, where: str) -> Output[object]:
        """The reference to the setup's output in ``builder``.

        Raises:
            TypeError: For a resource without a value, or one not set up in the DAG.
        """
        if not self._declared.has_value:
            raise TypeError(f"{where}: resource `{self._declared.name}` has no value")
        for setup, known in builder.resources.items():
            if known is self._declared:
                return Output(setup, self._path, dag=builder)
        raise TypeError(f"{where}: resource `{self._declared.name}` isn't set up in this DAG")


def _resolved(value: object, builder: _build.DagBuilder, where: str) -> object:
    """``value`` with each resource value in it a reference to its setup's output."""
    if isinstance(value, _Value):
        return value.resolved(builder, where)
    if is_tstring(value):
        parts: list[str | Interpolation[object]] = [
            Interpolation(
                _resolved(part.value, builder, where),
                part.expression,
                part.conversion,
                part.format_spec,
            )
            if isinstance(part, Interpolation)
            else part
            for part in value  # type: ignore[attr-defined]
        ]
        return TString(*parts)
    if isinstance(value, str):
        return value
    if isinstance(value, Sequence):
        return [_resolved(item, builder, where) for item in value]
    return value


class _Declared:
    """A resource as declared: what its setup and all its overrides share."""

    def __init__(self, name: str, *, has_value: bool) -> None:
        """Starts a resource with an empty cleanup.

        Args:
            name: Its name, for messages.
            has_value: Whether its setup returns a value, for the cleanup to take.
        """
        self.name = name
        self.has_value = has_value
        self.cleanup: _Cleanup | None = None

    @property
    def blocking(self) -> bool:
        """Whether what follows the resource's group waits for its cleanup."""
        return self.cleanup is None or self.cleanup.blocking

    @property
    def unschedulable_timeout(self) -> timedelta | None:
        """How long a user may wait for a worker before it fails (decision 273)."""
        return None if self.cleanup is None else self.cleanup.unschedulable_timeout

    @property
    def value(self) -> _Value:
        """The resource's value, for a ``cleanup_command``."""
        return _Value(self)

    def attach[F: Callable[..., object]](self, fn: F, options: CleanupOptions) -> F:
        """Gives the resource its cleanup, one wherever it is imported (decision 267).

        Args:
            fn: The cleanup.
            options: Its options.

        Returns:
            The function as it is.
        """
        self._declare(fn, _origin(fn), options)
        return fn

    def attach_command(
        self,
        command: _Command,
        options: CleanupOptions,
        origin: tuple[object, object],
    ) -> None:
        """Gives the resource a command as its cleanup (decision 271).

        Args:
            command: The command.
            options: Its options.
            origin: Where it is declared.
        """
        if not isinstance(command.command, str | TString | Sequence) or (
            isinstance(command.command, Sequence)
            and not isinstance(command.command, str)
            and not command.command
        ):
            raise TypeError(
                f"resource `{self.name}`: `cleanup_command` takes a string, a t-string or a "
                "non-empty list"
            )
        self._declare(command, origin, options)

    def _declare(
        self,
        run: Callable[..., object] | _Command,
        origin: tuple[object, object],
        options: CleanupOptions,
    ) -> None:
        """Records the cleanup.

        Raises:
            TypeError: For a second cleanup.
        """
        # The same declaration again is its file imported again: a resource from a helper
        # module outlives the import of the DAG file that gives it its cleanup.
        if self.cleanup is not None and self.cleanup.origin != origin:
            raise TypeError(f"resource `{self.name}` has a cleanup already")
        where = f"resource `{self.name}`"
        secrets = options.get("secrets")
        self.cleanup = _Cleanup(
            run,
            origin,
            options.get("timeout"),
            options.get("retries"),
            options.get("retry_delay"),
            options.get("queue"),
            () if secrets is None else tuple(_secrets.declared(secrets, where)),
            declared_connections(options.get("connections"), where) or (),
            options.get("blocking", True),
            options.get("unschedulable_timeout"),
        )

    def decorator(
        self, fn: Callable[..., object] | None, options: CleanupOptions
    ) -> Callable[..., object]:
        """What ``.cleanup`` gives, used bare or with options.

        Args:
            fn: The cleanup, when used bare.
            options: Its options.

        Returns:
            The function, or a decorator taking it.
        """
        if fn is not None:
            return self.attach(fn, options)
        return lambda function: self.attach(function, options)

    def finish(self, builder: _build.DagBuilder, setup: str) -> str:
        """Adds its cleanup to the DAG.

        Args:
            builder: The DAG.
            setup: Its setup task.

        Returns:
            The cleanup task's ID.

        Raises:
            TypeError: For a cleanup whose parameters don't fit the resource.
        """
        task_id = f"{setup}.cleanup"
        declared = self.cleanup
        if declared is None:
            builder.add_task(task_id, EmptyKind(), ScalarType("none"), _build.TaskOptions())
            builder.add_edge(setup, task_id, EdgeKind.REQUIREMENT, plain=True)
            return task_id
        made = builder.tasks[setup]
        options = _build.TaskOptions(
            retries=DEFAULT_CLEANUP_RETRIES if declared.retries is None else declared.retries,
            retry_delay=(
                timedelta(milliseconds=DEFAULT_CLEANUP_RETRY_DELAY_MS)
                if declared.retry_delay is None
                else declared.retry_delay
            ),
            timeout=declared.timeout,
            queue=made.settings.queue if declared.queue is None else declared.queue,
            # Forgotten, it would surface only when the cleanup runs (decision 226).
            secrets=tuple(dict.fromkeys((*made.secrets, *declared.secrets))),
            connections=tuple(dict.fromkeys((*made.connections, *declared.connections))),
        )
        if isinstance(declared.run, _Command):
            where = f"task `{task_id}`"
            env = declared.run.env
            add_command(
                builder,
                task_id,
                _resolved(declared.run.command, builder, where),  # type: ignore[arg-type]
                CommandSettings(
                    env=None
                    if env is None
                    else {name: _resolved(v, builder, where) for name, v in env.items()},
                    options=options,
                    resource=ResourceRole(setup),
                ),
            )
            if not any(
                e.from_ == setup and e.to == task_id and e.kind == EdgeKind.DATA
                for e in builder.edges
            ):
                builder.add_edge(setup, task_id, EdgeKind.REQUIREMENT, plain=True)
            return task_id
        parameters = list(inspect.signature(declared.run).parameters)
        wanted = 1 if self.has_value else 0
        if len(parameters) != wanted:
            what = "the resource's value" if self.has_value else "nothing: it has no value"
            raise TypeError(f"resource `{setup}`: its cleanup takes {what}, got {parameters}")
        function = TaskFunction(declared.run, task_id, options)
        fixed: dict[str, object] = {name: Output[Any](setup, dag=builder) for name in parameters}
        function._record(builder, fixed, None)
        if not self.has_value:
            builder.add_edge(setup, task_id, EdgeKind.REQUIREMENT, plain=True)
        return task_id

    def command_cleanup(
        self,
        command: str | TString | Sequence[object],
        env: Mapping[str, object] | None,
        options: CleanupOptions,
    ) -> None:
        """What ``.cleanup_command`` does, called from the resource object's method."""
        self.attach_command(_Command(command, env), options, _call_site(2))


class ResourceFunction[**P, T](TaskFunction[P, T]):
    """A function decorated with ``@resource``: the setup, called like a task."""

    def __init__(
        self,
        fn: Callable[P, T],
        task_id: str,
        options: _build.TaskOptions,
        cleanup: Callable[..., object] | None = None,
        declared: _Declared | None = None,
    ) -> None:
        """Wraps ``fn`` as a setup.

        Args:
            fn: The function.
            task_id: The task's ID.
            options: The task's own options.
            cleanup: Its per-task ``cleanup=``: for a setup failing halfway.
            declared: The resource, for an override of a setup.
        """
        super().__init__(fn, task_id, options, cleanup=cleanup)
        hints = type_hints(fn, f"resource `{task_id}`")
        self._schedr_resource = declared or _Declared(
            task_id, has_value=unwrap(hints["return"]) not in NO_VALUE
        )

    @overload
    def cleanup(
        self: ResourceFunction[P, None], fn: Callable[[], object], /
    ) -> Callable[[], object]: ...
    @overload
    def cleanup(self, fn: Callable[[T], object], /) -> Callable[[T], object]: ...
    @overload
    def cleanup(
        self: ResourceFunction[P, None], /, **options: Unpack[CleanupOptions]
    ) -> _WithoutValue: ...
    @overload
    def cleanup(self, /, **options: Unpack[CleanupOptions]) -> _WithValue[T]: ...
    def cleanup(
        self, fn: Callable[..., object] | None = None, /, **options: Unpack[CleanupOptions]
    ) -> Callable[..., object]:
        """Marks a function as the resource's cleanup: it gets what the setup returned.

        Used bare (``@cluster.cleanup``) or with options (``@cluster.cleanup(retries=1)``).

        Args:
            fn: The function, when used bare.
            **options: See ``CleanupOptions``.

        Returns:
            The function as it is, or a decorator keeping it so.

        Raises:
            TypeError: For a second cleanup.
        """
        return self._schedr_resource.decorator(fn, options)

    @property
    def value(self) -> T:
        """The resource's value, for its ``cleanup_command``: ``t"{cluster.value.cluster_id}"``.

        Type checkers read it as the value; it is a reference, resolved when the DAG sets the
        resource up.
        """
        return self._schedr_resource.value  # type: ignore[return-value]

    def cleanup_command(
        self,
        command: str | TString | Sequence[object],
        /,
        *,
        env: Mapping[str, object] | None = None,
        **options: Unpack[CleanupOptions],
    ) -> None:
        """Gives the resource a command as its cleanup (decision 271).

        It reads the value with ``schedr task-ctx resource get``, or takes it in its command
        line or ``env`` as ``CommandOperator`` takes outputs: ``t"{cluster.value.cluster_id}"``.

        Args:
            command: A shell string, or a list of arguments.
            env: Extra environment variables for the process.
            **options: See ``CleanupOptions``.

        Raises:
            TypeError: For a second cleanup, or a command of the wrong type.
        """
        self._schedr_resource.command_cleanup(command, env, options)

    def _again(
        self,
        task_id: str,
        options: _build.TaskOptions,
        cleanup: Callable[..., object] | None,
    ) -> Self:
        return type(self)(self._fn, task_id, options, cleanup, self._schedr_resource)

    def _record(
        self,
        builder: _build.DagBuilder,
        fixed: dict[str, object],
        expand: tuple[str, object] | None,
    ) -> Output[Any]:
        task_id = builder.qualified(self._task_id)
        where = f"resource `{task_id}`"
        if expand is not None:
            raise TypeError(f"{where}: a setup can't be mapped (`.expand()`)")
        if _outcomes.from_return_type(type_hints(self._fn, where)["return"], where).is_union:
            raise TypeError(f"{where}: a setup can't return an outcome union")
        builder.refuse_second_setup(task_id, self._schedr_resource)
        output = super()._record(builder, fixed, None)
        builder.add_resource(task_id, self._schedr_resource)
        return output


class Resource:
    """A resource without setup code: ``tmp = Resource("tmp-prefix")``.

    The master completes its setup, with no process. It has no value: its cleanup takes no
    parameter, and its users name it with ``uses=``.

    Args:
        name: Its name: the ID of its setup.

    Raises:
        RuntimeError: Outside ``with Dag(...)``.
    """

    def __init__(self, name: str) -> None:
        """Adds its setup to the DAG being defined."""
        builder = _build.require_active("Resource")
        task_id = builder.qualified(name)
        self.__name__ = task_id
        self._schedr_resource = _Declared(task_id, has_value=False)
        builder.add_task(task_id, EmptyKind(), ScalarType("none"), _build.TaskOptions())
        builder.add_resource(task_id, self._schedr_resource)

    @overload
    def cleanup(self, fn: Callable[[], object], /) -> Callable[[], object]: ...
    @overload
    def cleanup(self, /, **options: Unpack[CleanupOptions]) -> _WithoutValue: ...
    def cleanup(
        self, fn: Callable[..., object] | None = None, /, **options: Unpack[CleanupOptions]
    ) -> Callable[..., object]:
        """Marks a function as the resource's cleanup; it takes no parameter.

        Used bare (``@tmp.cleanup``) or with options (``@tmp.cleanup(retries=1)``).

        Args:
            fn: The function, when used bare.
            **options: See ``CleanupOptions``.

        Returns:
            The function as it is, or a decorator keeping it so.

        Raises:
            TypeError: For a second cleanup.
        """
        return self._schedr_resource.decorator(fn, options)

    def cleanup_command(
        self,
        command: str | TString | Sequence[object],
        /,
        *,
        env: Mapping[str, object] | None = None,
        **options: Unpack[CleanupOptions],
    ) -> None:
        """Gives the resource a command as its cleanup (decision 271).

        It takes no value: the resource has none.

        Args:
            command: A shell string, or a list of arguments.
            env: Extra environment variables for the process.
            **options: See ``CleanupOptions``.

        Raises:
            TypeError: For a second cleanup, or a command of the wrong type.
        """
        self._schedr_resource.command_cleanup(command, env, options)


class CommandResourceOptions(TypedDict, total=False):
    """``CommandResource``'s options besides its value: as for ``CommandOperator``."""

    env: Mapping[str, object] | None
    retries: int | None
    retry_delay: timedelta | None
    timeout: timedelta | None
    queue: str | None
    max_output_size: int | None
    run_if: RunIf | None
    max_active: int | None
    secrets: SecretsArg
    connections: ConnectionsArg | Mapping[str, Connection[object] | str]
    cleanup: str | TString | Sequence[object] | None
    cleanup_timeout: timedelta | None
    cleanup_retries: int | None
    cleanup_retry_delay: timedelta | None
    cleanup_anywhere: bool | None
    cleanup_handles: Sequence[str] | None
    uses: Sequence[object] | None


class CommandResource[T = None](Operator):
    """A resource whose setup is a command (decision 271).

    The command reports the value with ``schedr task-ctx resource set``, checked against
    ``value=`` at once. The setup succeeds only if it exits with 0 and, with ``value=``,
    reported the value; its output is that value, not its ``ProcessResult``. Its users take
    ``cluster.value`` (or ``uses=[cluster]``); a command can take a field as
    ``t"{cluster.cluster_id}"``.

    Args:
        name: Its name: the ID of its setup.
        command: A shell string, or a list of arguments (as for ``CommandOperator``).
        value: The type of the value it reports; none: the resource has no value. A union
            or a ``Literal`` needs the type given twice for mypy:
            ``CommandResource[int | None]("n", ..., value=int | None)``.
        env: Extra environment variables for the process.
        retries: How many times a failed attempt is retried.
        retry_delay: The delay before each retry.
        timeout: The attempt's timeout.
        queue: The queue the task goes to.
        max_output_size: The output size limit in bytes (per stream).
        run_if: When it runs, from its upstream edges.
        max_active: At most this many running at once, across all runs.
        secrets: The secrets it reads with ``schedr task-ctx secret get``.
        connections: The connections it reads, as for ``CommandOperator``.
        cleanup: A per-task cleanup: for a setup failing halfway (``cleanup=``).
        cleanup_timeout: How long one try of that cleanup may run.
        cleanup_retries: How many times it is retried.
        cleanup_retry_delay: The delay before each retry.
        cleanup_anywhere: Whether it may run on another worker when this one is lost.
        cleanup_handles: The handle types the attempt may register.
        uses: The resources it needs.

    Raises:
        RuntimeError: Outside ``with Dag(...)``.
        TypeError: For a value type that can't be stored, or a command of the wrong type.
    """

    @overload
    def __init__(
        self,
        name: str,
        command: str | TString | Sequence[object],
        *,
        value: type[T],
        **options: Unpack[CommandResourceOptions],
    ) -> None: ...
    @overload
    def __init__(
        self,
        name: str,
        command: str | TString | Sequence[object],
        *,
        value: object = None,
        **options: Unpack[CommandResourceOptions],
    ) -> None: ...
    def __init__(
        self,
        name: str,
        command: str | TString | Sequence[object],
        *,
        value: object = None,
        **options: Unpack[CommandResourceOptions],
    ) -> None:
        """Adds its setup to the DAG being defined."""
        builder = _build.require_active("CommandResource")
        task_id = builder.qualified(name)
        where = f"resource `{task_id}`"
        try:
            output_type = describe(value)
        except TypeError as e:
            raise TypeError(f"{where}: `value={value!r}`: {e}") from None
        self._schedr_resource = _Declared(
            task_id, has_value=value is not None and unwrap(value) not in NO_VALUE
        )
        builder.refuse_second_setup(task_id, self._schedr_resource)
        uses = options.get("uses")
        self._task_id = add_command(
            builder,
            task_id,
            command,
            CommandSettings(
                env=options.get("env"),
                options=_build.TaskOptions(
                    options.get("retries"),
                    options.get("retry_delay"),
                    options.get("timeout"),
                    options.get("queue"),
                    options.get("max_output_size"),
                    run_if=options.get("run_if"),
                    max_active=options.get("max_active"),
                    cleanup_timeout=options.get("cleanup_timeout"),
                    cleanup_retries=options.get("cleanup_retries"),
                    cleanup_retry_delay=options.get("cleanup_retry_delay"),
                    cleanup_anywhere=options.get("cleanup_anywhere"),
                    uses=None if uses is None else tuple(uses),
                ),
                secrets=options.get("secrets"),
                connections=options.get("connections"),
                cleanup=options.get("cleanup"),
                cleanup_handles=options.get("cleanup_handles"),
                output_type=output_type,
                resource=ResourceRole(),
            ),
        )
        self._dag = builder
        builder.add_resource(task_id, self._schedr_resource)

    @property
    def value(self) -> T:
        """The value it reports: a reference, read by type checkers as the value."""
        return Output[T](self._task_id, dag=self._dag)  # type: ignore[return-value]

    def __getattr__(self, name: str) -> Output[object]:
        """References a field of the value, as ``Output`` does: ``t"{cluster.cluster_id}"``.

        Raises:
            AttributeError: For names starting with ``_``.
        """
        if name.startswith("_"):
            raise AttributeError(name)
        return Output(self._task_id, (name,), dag=self._dag)

    @overload
    def cleanup(
        self: CommandResource[None], fn: Callable[[], object], /
    ) -> Callable[[], object]: ...
    @overload
    def cleanup(self, fn: Callable[[T], object], /) -> Callable[[T], object]: ...
    @overload
    def cleanup(
        self: CommandResource[None], /, **options: Unpack[CleanupOptions]
    ) -> _WithoutValue: ...
    @overload
    def cleanup(self, /, **options: Unpack[CleanupOptions]) -> _WithValue[T]: ...
    def cleanup(
        self, fn: Callable[..., object] | None = None, /, **options: Unpack[CleanupOptions]
    ) -> Callable[..., object]:
        """Marks a function as the resource's cleanup: it gets the reported value.

        Args:
            fn: The function, when used bare.
            **options: See ``CleanupOptions``.

        Returns:
            The function as it is, or a decorator keeping it so.

        Raises:
            TypeError: For a second cleanup.
        """
        return self._schedr_resource.decorator(fn, options)

    def cleanup_command(
        self,
        command: str | TString | Sequence[object],
        /,
        *,
        env: Mapping[str, object] | None = None,
        **options: Unpack[CleanupOptions],
    ) -> None:
        """Gives the resource a command as its cleanup (decision 271).

        It reads the value with ``schedr task-ctx resource get``, or takes it in its command
        line or ``env``: ``t"{cluster.cluster_id}"``.

        Args:
            command: A shell string, or a list of arguments.
            env: Extra environment variables for the process.
            **options: See ``CleanupOptions``.

        Raises:
            TypeError: For a second cleanup, or a command of the wrong type.
        """
        self._schedr_resource.command_cleanup(command, env, options)


@overload
def resource[**P, T](fn: Callable[P, T], /) -> ResourceFunction[P, T]: ...
@overload
def resource[**P, T](
    **options: Unpack[ResourceOptions],
) -> Callable[[Callable[P, T]], ResourceFunction[P, T]]: ...
def resource[**P, T](
    fn: Callable[P, T] | None = None, /, **options: Unpack[ResourceOptions]
) -> ResourceFunction[P, T] | Callable[[Callable[P, T]], ResourceFunction[P, T]]:
    """Marks a function as a resource's setup: what it returns is the resource.

    Args:
        fn: The function (``@resource`` without arguments).
        **options: As for ``@task``.

    Returns:
        The setup, called like a task, or a decorator making one.

    Raises:
        TypeError: If the function can't be a task.
    """

    def setup(function: Callable[P, T]) -> ResourceFunction[P, T]:
        made: TaskFunction[P, T] = task(**options)(function)
        return ResourceFunction(
            function,
            made.task_id,
            made._options,
            made._cleanup,
        )

    return setup if fn is None else setup(fn)
