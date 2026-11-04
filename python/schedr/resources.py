"""Resources: one thing of two parts, a setup and its cleanup (decision 259).

A task is a set of instructions, and may have no cleanup at all. A resource is by definition
created and undone: both parts always exist, as tasks of the DAG, the cleanup running once every
user is done. One without ``@cleanup`` has an empty cleanup (decision 260).
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Self, TypedDict, Unpack, overload

from schedr import _build, _secrets
from schedr import outcomes as _outcomes
from schedr._model import EdgeKind, EmptyKind, ScalarType, SecretDecl
from schedr._model.tasks import DEFAULT_CLEANUP_RETRIES, DEFAULT_CLEANUP_RETRY_DELAY_MS
from schedr._secrets import SecretsArg
from schedr._serialization.annotations import type_hints, unwrap
from schedr.connections import ConnectionsArg, declared_connections
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
        timeout: How long one try may run.
        retries: How many times it is tried again.
        retry_delay: The delay before each try again.
        queue: Its queue; the setup's if not given.
        secrets: Secrets it declares, besides its setup's.
        connections: Connections it declares, besides its setup's.
        blocking: Whether what follows the resource's group waits for this cleanup (the
            default). With ``False`` it goes on, and isn't stopped if the cleanup fails; the
            run still fails.
    """

    timeout: timedelta | None
    retries: int | None
    retry_delay: timedelta | None
    queue: str | None
    secrets: SecretsArg
    connections: ConnectionsArg
    blocking: bool


type _WithValue[T] = Callable[[Callable[[T], object]], Callable[[T], object]]
type _WithoutValue = Callable[[Callable[[], object]], Callable[[], object]]


@dataclass(frozen=True, slots=True)
class _Cleanup:
    """What ``@cleanup`` declared."""

    fn: Callable[..., object]
    timeout: timedelta | None
    retries: int | None
    retry_delay: timedelta | None
    queue: str | None
    secrets: tuple[SecretDecl, ...]
    connections: tuple[str, ...]
    blocking: bool


def _origin(fn: Callable[..., object]) -> tuple[object, object]:
    """Where a function was defined, the same when its file is imported again."""
    return getattr(fn, "__module__", None), getattr(fn, "__qualname__", None)


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

    def attach[F: Callable[..., object]](self, fn: F, options: CleanupOptions) -> F:
        """Gives the resource its cleanup, one wherever it is imported (decision 267).

        Args:
            fn: The cleanup.
            options: Its options.

        Returns:
            The function as it is.

        Raises:
            TypeError: For a second cleanup.
        """
        # The same function again is its file imported again: a resource from a helper module
        # outlives the import of the DAG file that gives it its cleanup.
        if self.cleanup is not None and _origin(self.cleanup.fn) != _origin(fn):
            raise TypeError(f"resource `{self.name}` has a cleanup already")
        where = f"resource `{self.name}`"
        secrets = options.get("secrets")
        self.cleanup = _Cleanup(
            fn,
            options.get("timeout"),
            options.get("retries"),
            options.get("retry_delay"),
            options.get("queue"),
            () if secrets is None else tuple(_secrets.declared(secrets, where)),
            declared_connections(options.get("connections"), where) or (),
            options.get("blocking", True),
        )
        return fn

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
        parameters = list(inspect.signature(declared.fn).parameters)
        wanted = 1 if self.has_value else 0
        if len(parameters) != wanted:
            what = "the resource's value" if self.has_value else "nothing: it has no value"
            raise TypeError(f"resource `{setup}`: its cleanup takes {what}, got {parameters}")
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
        function = TaskFunction(declared.fn, task_id, options)
        fixed: dict[str, object] = {name: Output[Any](setup, dag=builder) for name in parameters}
        function._record(builder, fixed, None)
        if not self.has_value:
            builder.add_edge(setup, task_id, EdgeKind.REQUIREMENT, plain=True)
        return task_id


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
