"""Connections (PROJECT.md, Connections; decisions 53 and 174): what a task connects to.

A connection is a generator function, as with :func:`contextlib.contextmanager`, whose
parameters are its shape; ``@connection(...)`` gives their values, which may be secrets, so
each environment supplies its own::

    @connection(uri=t"{secrets['mongo/default']['uri']}", database="orders")
    def mongodb_default(uri: str, database: str) -> Iterator[Database]:
        client = MongoClient(uri)
        try:
            yield client[database]
        finally:
            client.close()

    @task(connections=[mongodb_default])
    def count() -> int:
        with mongodb_default() as db:
            return db.orders.count_documents({})

A task opens only the connections it declares (decision 179). The worker's agent renders
each before the task starts (fetching its secrets, masked in the task's log); entering it, the
function runs up to ``yield``, and leaving runs the rest. The parser records every connection
of a DAG folder (its name and values, never a secret's value): the agent renders only what
was recorded. Its values are secrets and plain values (decision 180).
"""

import contextlib
import dataclasses
import functools
import inspect
import json
import typing
from collections.abc import AsyncIterator, Callable, Collection, Iterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass
from string.templatelib import Template
from types import TracebackType
from typing import Any, Final, TypeVar, cast

from schedr import _build, _secrets
from schedr._model import (
    LiteralValue,
    ScalarType,
    TemplateRef,
    TemplateValue,
    Value,
    validate_task_id,
)
from schedr._model import Template as TemplateData
from schedr._model.dag import DagLocation
from schedr._model.registry import LOCAL_ROOT, parse_root_active, root_location
from schedr._serialization import (
    CodecError,
    TypeModelError,
    assignment_problem,
    decode,
    describe,
    encode,
    is_assignable,
    type_name,
)
from schedr._serialization.annotations import label, type_hints
from schedr.context import ConnectionOpener, current_task
from schedr.context import declared_connections as _declared_in_test

__all__ = [
    "HTTP",
    "SQL",
    "SSH",
    "AsyncConnection",
    "Connection",
    "ConnectionBase",
    "ConnectionDeniedError",
    "ConnectionNotFoundError",
    "DbApiConnection",
    "Generic",
    "Mongo",
    "TwoWayConnection",
    "connection",
]

MAX_CONNECTION_SIZE = 1024 * 1024
"""The most a connection's recorded values may take, as JSON (``schedr-model``'s own)."""


class ConnectionNotFoundError(LookupError):
    """A connection that can't be opened: not recorded by the parser, or no double for it."""


class ConnectionDeniedError(LookupError):
    """A connection the task opens without declaring it in ``connections=`` (decision 179)."""


@dataclass(frozen=True, slots=True)
class ConnectionDef:
    """A connection as the parser records it (never a secret's value).

    Attributes:
        name: Its name: the function's, or a typed connection's first argument.
        location: Where it is defined.
        values: Its parameters' values, as DAG JSON values.
        kind: A typed connection's type (``"http"``, ``"ssh"``, ...); ``None`` for a function.
    """

    name: str
    location: DagLocation
    values: Mapping[str, Value]
    kind: str | None = None

    def to_json(self) -> dict[str, Any]:
        """The connection as the runner reports it.

        Returns:
            ``{"name", "params"}`` (and ``"type"`` for a typed one), each parameter's value as
            DAG JSON.
        """
        recorded: dict[str, Any] = {
            "name": self.name,
            "params": {name: value.to_json() for name, value in self.values.items()},
        }
        if self.kind is not None:
            recorded["type"] = self.kind
        return recorded


_DEFINED: list[ConnectionDef] = []
"""The connections defined while a parse root is active, until taken."""


def take_connections() -> list[ConnectionDef]:
    """The connections defined since the last call, in definition order; forgets them.

    Returns:
        The connections.
    """
    defined = list(_DEFINED)
    _DEFINED.clear()
    return defined


type Checker[T] = Callable[[T, Mapping[str, object]], object]
"""A connection's check: given its client and its values, raises when it doesn't work."""


# Declared, not inferred: `check`'s self-typed signature would make an inferred `T`
# invariant (mypy), and `Connection[httpx.Client]` must be a `Connection[object]`.
T_co = TypeVar("T_co", covariant=True)
A_co = TypeVar("A_co", covariant=True)


class ConnectionBase(typing.Generic[T_co]):  # noqa: UP046  # declared covariant (see `check`)
    """What :class:`Connection` and :class:`AsyncConnection` share.

    Attributes:
        name: Its name: the function's, or a typed connection's first argument.
        kind: A typed connection's type (``"http"``, ...); ``None`` for a ``@connection``.
    """

    def __init__(
        self,
        function: Callable[..., object],
        values: Mapping[str, Value],
        *,
        name: str | None = None,
        kind: str | None = None,
        checker: Checker[T_co] | None = None,
    ) -> None:
        """Wraps ``function``, given its values (already checked).

        Args:
            function: The generator function.
            values: Its parameters' values, as DAG JSON values.
            name: Its name (a typed connection's); else the function's.
            kind: A typed connection's type.
            checker: Its check (a typed connection's own); else none until ``@name.check``.
        """
        self.name: Final[str] = name or function.__name__
        self.kind: Final = kind
        self._function: Final = function
        self._values = dict(values)
        self._hints = type_hints(function, f"connection `{self.name}`")
        self._others = _keywords_hint(function, self._hints)
        # Stored taking `object`: a `T_co` in a mutable attribute's callable would make
        # `Connection` invariant (`Connection[httpx.Client]` no `Connection[object]` in
        # `connections=`); it is only ever called with what this connection yields.
        self._checker = cast("Checker[object] | None", checker)
        if name is None:
            # Not called through what it wraps: only its name and docstring are taken.
            functools.update_wrapper(cast("Callable[..., object]", self), function)

    def check[U, R](self: ConnectionBase[U], function: Callable[[U], R]) -> Callable[[U], R]:
        """Gives the connection a check (``@name.check``), replacing a typed one's own.

        ``schedr connections test NAME`` runs it on a worker: it gets the client and raises
        when the connection doesn't work (a Mongo ``ping``, a ``SELECT 1``).

        Args:
            function: The check, given what the connection yields: a type checker refuses
                one taking something else.

        Returns:
            ``function``, unchanged.
        """
        # `self` typed with a `U` of its own: `T_co` in a parameter's callable is allowed for
        # a covariant variable, but not in what it returns (a callable taking `T_co`).
        given = cast("Callable[[object], object]", function)
        self._checker = lambda client, _values: given(client)
        return function

    def _the_checker(self) -> Checker[object]:
        """Its check.

        Raises:
            ValueError: When it has none.
        """
        if self._checker is None:
            raise ValueError(
                f"connection `{self.name}` has no check: give it one with `@{self.name}.check`"
            )
        return self._checker

    def run_test(self) -> None:
        """Opens the connection and runs its check, or opens and closes it without one.

        What ``schedr connections test NAME`` runs on a worker (decision 198).
        """
        raise NotImplementedError

    def _opener(self) -> ConnectionOpener:
        """What opens it, in a running task (or a test's ``task_context``).

        Raises:
            ConnectionDeniedError: When a test's task doesn't declare it.
            RuntimeError: Outside a running task.
            ConnectionNotFoundError: When there are no connections here.
        """
        task = current_task()
        declared = _declared_in_test()
        if declared is not None and self.name not in declared:
            raise ConnectionDeniedError(
                f"connection `{self.name}` isn't declared in the task's `connections=`"
            )
        if task is None:
            raise RuntimeError(
                f"`{self.name}()` opens the connection inside a running task (or "
                f"`schedr.testing.task_context`); to call it with values of your own, "
                f"`{self.name}.fn(...)`"
            )
        if task.connections is None:
            raise ConnectionNotFoundError(
                f"connection `{self.name}`: no connections here (in a test, give a double: "
                f"`task_context(connections={{{self.name!r}: ...}})`)"
            )
        return task.connections

    def _call(self, stored: Mapping[str, object]) -> object:
        """The function as a context manager, called with its rendered values."""
        raise NotImplementedError

    def _decoded(self, stored: Mapping[str, object]) -> dict[str, object]:
        """Its rendered values (stored JSON), decoded by type."""
        return {
            name: decode(value, self._hints.get(name, self._others), name)
            for name, value in stored.items()
        }

    def _enter(self, stored: Mapping[str, object]) -> AbstractContextManager[object]:
        """The function called with its rendered values (stored JSON, decoded by type)."""
        # Async too: `AsyncConnection` enters what this gives with `async with`.
        return cast("AbstractContextManager[object]", self._call(stored))

    @contextlib.asynccontextmanager
    async def _aopened(
        self, enter: Callable[[Mapping[str, object]], AbstractContextManager[object]]
    ) -> AsyncIterator[T_co]:
        import asyncio  # noqa: PLC0415  # slow to import; most tasks don't need it

        # Its values come from the agent: a wait the event loop mustn't be blocked by.
        opened = await asyncio.to_thread(self._opener(), self.name, enter)
        # The function's own, or a test's double (`nullcontext`: both protocols).
        async with cast("AbstractAsyncContextManager[T_co]", opened) as client:
            yield client

    def __repr__(self) -> str:
        """The connection, by name."""
        return f"<connection {self.name}>"


class Connection(ConnectionBase[T_co]):
    """A connection: ``with name() as client:`` in a task (see the module's description).

    Attributes:
        fn: The function as a context manager, for calling it yourself (in a test) with values
            of your own: ``with mongodb_default.fn(uri=..., database=...) as db:``.
    """

    def __init__(
        self,
        function: Callable[..., Iterator[T_co]],
        values: Mapping[str, Value],
        *,
        name: str | None = None,
        kind: str | None = None,
        checker: Checker[T_co] | None = None,
    ) -> None:
        """Wraps ``function``, given its values (already checked).

        Args:
            function: The generator function.
            values: Its parameters' values, as DAG JSON values.
            name: Its name (a typed connection's); else the function's.
            kind: A typed connection's type.
            checker: Its check (a typed connection's own); else none until ``@name.check``.
        """
        super().__init__(function, values, name=name, kind=kind, checker=checker)
        self.fn: Final[Callable[..., AbstractContextManager[T_co]]] = contextlib.contextmanager(
            function
        )

    def run_check(self) -> None:
        """Opens the connection and runs its check, in a running task."""
        checker = self._the_checker()
        given: dict[str, Mapping[str, object]] = {}

        def entered(stored: Mapping[str, object]) -> AbstractContextManager[object]:
            given["values"] = stored
            return self._enter(stored)

        with self._opened(entered) as client:
            checker(client, given.get("values", {}))

    def run_test(self) -> None:
        """Opens the connection and runs its check, or opens and closes it without one.

        What ``schedr connections test NAME`` runs on a worker (decision 198).
        """
        if self._checker is not None:
            self.run_check()
            return
        with self():
            pass

    def __call__(self) -> AbstractContextManager[T_co]:
        """Opens the connection, in a running task.

        Returns:
            The context manager yielding what the function yields.
        """
        return self._opened(self._enter)

    @contextlib.contextmanager
    def _opened(
        self, enter: Callable[[Mapping[str, object]], AbstractContextManager[object]]
    ) -> Iterator[T_co]:
        with self._opener()(self.name, enter) as client:
            yield client  # type: ignore[misc]  # the opener yields what `_enter` yields

    def _call(self, stored: Mapping[str, object]) -> object:
        return self.fn(**self._decoded(stored))


class AsyncConnection(ConnectionBase[T_co]):
    """An ``async def`` connection: ``async with name() as client:`` in an ``async def`` task.

    Attributes:
        fn: The function as an async context manager, for calling it yourself (in a test) with
            values of your own: ``async with mongodb_default.fn(uri=...) as db:``.
    """

    def __init__(self, function: Callable[..., AsyncIterator[T_co]], values: Mapping[str, Value]):
        """Wraps ``function``, given its values (already checked).

        Args:
            function: The async generator function.
            values: Its parameters' values, as DAG JSON values.
        """
        super().__init__(function, values)
        self.fn: Final[Callable[..., AbstractAsyncContextManager[T_co]]] = (
            contextlib.asynccontextmanager(function)
        )

    def __call__(self) -> AbstractAsyncContextManager[T_co]:
        """Opens the connection, in a running task.

        Returns:
            The async context manager yielding what the function yields.
        """
        return self._aopened(self._enter)

    def _call(self, stored: Mapping[str, object]) -> object:
        return self.fn(**self._decoded(stored))

    async def _tested(self) -> None:
        checker = self._checker
        given: dict[str, Mapping[str, object]] = {}

        def entered(stored: Mapping[str, object]) -> AbstractContextManager[object]:
            given["values"] = stored
            return self._enter(stored)

        async with self._aopened(entered) as client:
            if checker is not None:
                checked = checker(client, given.get("values", {}))
                if inspect.isawaitable(checked):
                    await checked

    def run_test(self) -> None:
        """Opens the connection and runs its check, or opens and closes it without one.

        What ``schedr connections test NAME`` runs on a worker (decision 198); the check may be
        ``async def``. As a task's coroutine: a timeout's stop signal lets its ``finally`` run.
        """
        # Slow to import; most tasks don't need it.
        from schedr.runner.coroutines import run_coroutine  # noqa: PLC0415

        run_coroutine(self._tested())


class _Opening[S, A](AbstractContextManager[S], AbstractAsyncContextManager[A]):
    """What opening a typed connection gives: either client, by how it is entered (decision 53)."""

    def __init__(
        self,
        sync: Callable[[], AbstractContextManager[S]],
        asynchronous: Callable[[], AbstractAsyncContextManager[A]],
    ) -> None:
        self._sync = sync
        self._asynchronous = asynchronous
        self._entered: AbstractContextManager[S] | None = None
        self._aentered: AbstractAsyncContextManager[A] | None = None

    def __enter__(self) -> S:
        self._entered = self._sync()
        return self._entered.__enter__()

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        trace: TracebackType | None,
    ) -> bool | None:
        assert self._entered is not None
        return self._entered.__exit__(kind, error, trace)

    async def __aenter__(self) -> A:
        self._aentered = self._asynchronous()
        return await self._aentered.__aenter__()

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        trace: TracebackType | None,
    ) -> bool | None:
        assert self._aentered is not None
        return await self._aentered.__aexit__(kind, error, trace)


class TwoWayConnection(Connection[T_co], typing.Generic[T_co, A_co]):  # noqa: UP046  # covariant
    """A typed connection: ``with`` gives its sync client, ``async with`` its async one.

    Attributes:
        afn: The async function as an async context manager, as ``fn`` is the sync one.
    """

    def __init__(
        self,
        function: Callable[..., Iterator[T_co]],
        async_function: Callable[..., AsyncIterator[A_co]],
        values: Mapping[str, Value],
        *,
        name: str,
        kind: str,
        checker: Checker[T_co],
    ) -> None:
        """Wraps both functions, which take the same values (already checked).

        Args:
            function: The sync generator function.
            async_function: The async one.
            values: Their parameters' values, as DAG JSON values.
            name: Its name.
            kind: Its type.
            checker: Its check, run with the sync client.
        """
        super().__init__(function, values, name=name, kind=kind, checker=checker)
        self.afn: Final[Callable[..., AbstractAsyncContextManager[A_co]]] = (
            contextlib.asynccontextmanager(async_function)
        )

    def __call__(self) -> _Opening[T_co, A_co]:
        """Opens the connection, in a running task.

        Returns:
            What ``with`` or ``async with`` enters.
        """
        return _Opening(
            lambda: self._opened(self._enter),
            # What `_aenter` enters yields the async client, not the `T_co` `_aopened` says.
            lambda: cast("AbstractAsyncContextManager[A_co]", self._aopened(self._aenter)),
        )

    def _aenter(self, stored: Mapping[str, object]) -> AbstractContextManager[object]:
        # Entered with `async with`, by `_aopened`.
        return cast("AbstractContextManager[object]", self.afn(**self._decoded(stored)))


class _ConnectionDecorator(typing.Protocol):
    """``@connection(...)``: what it makes depends on the function it is given."""

    @typing.overload
    def __call__[T](self, function: Callable[..., AsyncIterator[T]], /) -> AsyncConnection[T]: ...
    @typing.overload
    def __call__[T](self, function: Callable[..., Iterator[T]], /) -> Connection[T]: ...


def connection(**values: object) -> _ConnectionDecorator:
    """Makes a generator function a connection, with its parameters' values.

    An ``async def`` generator gives a connection for ``async with``.

    Args:
        **values: A value for each of its parameters (those with defaults may be left out):
            plain values, secrets (``secrets['mongo/default']['uri']``), and t-strings of
            them. A run's values (``dag_ctx``) go to the task's own arguments instead.

    Returns:
        The decorator.
    """

    def define[T](
        function: Callable[..., Iterator[T]] | Callable[..., AsyncIterator[T]],
    ) -> Connection[T] | AsyncConnection[T]:
        name = getattr(function, "__name__", "")
        is_async = inspect.isasyncgenfunction(function)
        if not is_async and not inspect.isgeneratorfunction(function):
            raise TypeError(
                f"connection `{name}`: a connection is a generator function that yields what "
                "a task gets (as with contextlib.contextmanager)"
            )
        converted = defined_values(
            function, values, name, None, inspect.getsourcefile(function) or ""
        )
        if is_async:
            return AsyncConnection(cast("Callable[..., AsyncIterator[T]]", function), converted)
        return Connection(cast("Callable[..., Iterator[T]]", function), converted)

    return cast("_ConnectionDecorator", define)  # its overloads say which function gives which


def defined_values(
    function: Callable[..., object],
    values: Mapping[str, object],
    name: str,
    kind: str | None,
    file: str,
    urls: Collection[str] = (),
) -> dict[str, Value]:
    """A connection's values, checked against ``function``'s parameters, and recorded.

    Args:
        function: The function its values are given to.
        values: The values given.
        name: Its name.
        kind: A typed connection's type, else ``None``.
        file: The file defining it (recorded when it is in the parse root).
        urls: The values that are URLs: what a t-string fills into them is percent-encoded
            (decision 188), so a secret is only the text it stands for.

    Returns:
        The values as DAG JSON values, defaults left out included.

    Raises:
        ValueError: For a name that isn't one, or values over 1 MiB as JSON.
    """
    where = f"connection `{name}`"
    try:
        validate_task_id(name)
    except ValueError as e:
        raise ValueError(f"{where}: {e}") from None
    # `Generic` takes keys of one's own, through its `**values`.
    converted = _check_values(function, values, where, any_keys=kind == "generic")
    for key in urls:
        value = converted.get(key)
        if isinstance(value, TemplateValue):
            converted[key] = TemplateValue(
                TemplateData(
                    [
                        dataclasses.replace(part, encode="url")
                        if isinstance(part, TemplateRef)
                        else part
                        for part in value.template.parts
                    ]
                )
            )
    definition = ConnectionDef(name, DagLocation(LOCAL_ROOT, ""), converted, kind)
    size = len(json.dumps(definition.to_json(), ensure_ascii=False, separators=(",", ":")).encode())
    if size > MAX_CONNECTION_SIZE:
        raise ValueError(
            f"{where}: its values take {size} bytes, over the 1 MiB a connection may hold "
            "(they travel with each task opening it); give bigger data to the task"
        )
    if parse_root_active():
        location = root_location(file)
        if location is not None:
            _DEFINED.append(ConnectionDef(name, location, converted, kind))
    return converted


type ConnectionsArg = Sequence[ConnectionBase[object] | str] | None
"""What ``connections=`` takes: ``@connection`` functions, or their names."""


def declared_connections(given: ConnectionsArg, where: str) -> tuple[str, ...] | None:
    """``connections=`` as names, each checked.

    Args:
        given: The connections, or their names; ``None``: not set.
        where: The task, for messages.

    Returns:
        The names, each once, in order; ``None`` when not set.

    Raises:
        TypeError: For something else than a connection or a name.
        ValueError: For a name that isn't one.
    """
    if given is None:
        return None
    if isinstance(given, str | ConnectionBase):
        given = [given]
    names: list[str] = []
    for item in given:
        if isinstance(item, ConnectionBase):
            name = item.name
        elif isinstance(item, str):
            try:
                name = validate_task_id(item)
            except ValueError as e:
                raise ValueError(f"{where}: connections=: {e}") from None
        else:
            raise TypeError(
                f"{where}: connections= takes `@connection` functions or their names, "
                f"not {type(item).__name__}"
            )
        if name not in names:
            names.append(name)
    return tuple(names)


def _check_values(
    function: Callable[..., object],
    values: Mapping[str, object],
    where: str,
    *,
    any_keys: bool = False,
) -> dict[str, Value]:
    """The values as DAG JSON values, each checked against its parameter's type."""
    parameters = inspect.signature(function).parameters
    hints = type_hints(function, where)
    for parameter in parameters.values():
        if parameter.kind is parameter.VAR_KEYWORD and any_keys:
            continue  # `Generic`: keys of one's own
        if parameter.kind in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD):
            raise TypeError(f"{where}: `{parameter}`: a connection's parameters are named")
        if parameter.kind is parameter.POSITIONAL_ONLY:
            raise TypeError(
                f"{where}: `{parameter.name}` is positional-only; its value is given by name"
            )
    unknown = [] if any_keys else sorted(set(values) - set(parameters))
    if unknown:
        raise TypeError(f"{where}: no parameter {', '.join(f'`{u}`' for u in unknown)}")
    missing = [
        name
        for name, parameter in parameters.items()
        if name not in values
        and parameter.default is parameter.empty
        and parameter.kind is not parameter.VAR_KEYWORD
    ]
    if missing:
        raise TypeError(f"{where}: no value for {', '.join(f'`{m}`' for m in missing)}")
    # A key without a parameter of its own (`Generic`'s) takes the `**values` one's type.
    others = _keywords_hint(function, hints)
    converted = {
        name: _value(value, hints.get(name, others), f"{where}, `{name}`")
        for name, value in values.items()
    }
    # A default left out is recorded too: a command reads it (`task-ctx connection`), and a
    # secret or a t-string is filled in only when recorded (the function is called with the
    # recorded values). A plain default that isn't a DAG JSON value is Python's alone.
    for name, parameter in parameters.items():
        if name in values or parameter.default is parameter.empty:
            continue  # given, or no default (a `**values` parameter has none)
        at = f"{where}, `{name}` (its default)"
        if isinstance(parameter.default, _secrets.Secret | Template):
            converted[name] = _value(parameter.default, hints.get(name, object), at)
            continue
        with contextlib.suppress(TypeError, ValueError, TypeModelError):
            converted[name] = _value(parameter.default, hints.get(name, object), at)
    return converted


def _keywords_hint(function: Callable[..., object], hints: Mapping[str, object]) -> object:
    """The type of ``function``'s ``**`` parameter, for keys without one of their own."""
    for parameter in inspect.signature(function).parameters.values():
        if parameter.kind is parameter.VAR_KEYWORD:
            return hints.get(parameter.name, object)
    return object


def _value(value: object, annotation: object, where: str) -> Value:
    """One value as a DAG JSON value, checked against ``annotation``."""
    upstream: list[_build.Upstream] = []
    converted = _build.to_value(value, where, upstream)
    try:
        expected = describe(annotation, "input")
    except TypeModelError as e:
        raise TypeModelError(f"{where}: parameter type `{label(annotation)}`: {e}") from None
    for template in _build.templates_in(converted):
        _build.check_template_references(None, template, where)
    for spot, place in _build.typed_templates(converted, expected, where):
        if not is_assignable(ScalarType("str"), place):
            raise TypeError(f"{spot}: expects `{type_name(place)}`, but a t-string gives `str`")
    for reference, at in _build.typed_references(converted, expected):
        produced = _build.reference_type(None, reference, where)
        problem = assignment_problem(produced, at)
        if problem is not None:
            reason = f": {problem}" if problem else ""
            raise TypeError(
                f"{where}: expects `{type_name(at)}`, but "
                f"`{_build.reference_label(reference)}` is `{type_name(produced)}`{reason}"
            )
    _build.check_literals(converted, annotation, where)
    _build.check_fits(None, converted, annotation, where)
    if isinstance(converted, LiteralValue):
        try:
            converted = LiteralValue(encode(value, annotation))
        except CodecError as e:
            raise TypeError(f"{where}: {e.reason}") from None
    return converted


# The built-in types, defined on top of the above (decision 174).
from schedr.connection_types import (  # noqa: E402  # they import this module's names
    HTTP,
    SQL,
    SSH,
    DbApiConnection,
    Generic,
    Mongo,
)
