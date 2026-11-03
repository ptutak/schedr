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
import functools
import inspect
import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from string.templatelib import Template
from typing import Any, Final

from schedr import _build, _secrets
from schedr._model import (
    LiteralValue,
    ScalarType,
    Value,
    validate_task_id,
)
from schedr._model.dag import DagLocation
from schedr._model.registry import parse_root_active, root_location
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
from schedr.context import current_task
from schedr.context import declared_connections as _declared_in_test

__all__ = ["Connection", "ConnectionDeniedError", "ConnectionNotFoundError", "connection"]

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
        name: Its name, the function's.
        location: Where it is defined.
        values: Its parameters' values, as DAG JSON values.
    """

    name: str
    location: DagLocation
    values: Mapping[str, Value]

    def to_json(self) -> dict[str, Any]:
        """The connection as the runner reports it.

        Returns:
            ``{"name", "params"}``, each parameter's value as DAG JSON.
        """
        return {
            "name": self.name,
            "params": {name: value.to_json() for name, value in self.values.items()},
        }


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


class Connection[T]:
    """A connection: ``with name() as client:`` in a task (see the module's description).

    Attributes:
        name: Its name, the function's.
        fn: The function as a context manager, for calling it yourself (in a test) with values
            of your own: ``with mongodb_default.fn(uri=..., database=...) as db:``.
    """

    def __init__(self, function: Callable[..., Iterator[T]], values: Mapping[str, Value]) -> None:
        """Wraps ``function``, given its values (already checked).

        Args:
            function: The generator function.
            values: Its parameters' values, as DAG JSON values.
        """
        self.name: str = function.__name__
        self._function: Final = function
        self._values = dict(values)
        self._hints = type_hints(function, f"connection `{self.name}`")
        self.fn: Final[Callable[..., AbstractContextManager[T]]] = contextlib.contextmanager(
            function
        )
        functools.update_wrapper(self, function)

    def __call__(self) -> AbstractContextManager[T]:
        """Opens the connection, in a running task.

        Returns:
            The context manager yielding what the function yields.
        """
        return self._open()

    @contextlib.contextmanager
    def _open(self) -> Iterator[T]:
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
        with task.connections(self.name, self._enter) as client:
            yield client  # type: ignore[misc]  # the opener yields what `_enter` yields

    def _enter(self, stored: Mapping[str, object]) -> AbstractContextManager[object]:
        """The function called with its rendered values (stored JSON, decoded by type)."""
        values = {
            name: decode(value, self._hints.get(name, object), name)
            for name, value in stored.items()
        }
        return self.fn(**values)

    def __repr__(self) -> str:
        """The connection, by name."""
        return f"<connection {self.name}>"


def connection[T](
    **values: object,
) -> Callable[[Callable[..., Iterator[T]]], Connection[T]]:
    """Makes a generator function a connection, with its parameters' values.

    Args:
        **values: A value for each of its parameters (those with defaults may be left out):
            plain values, secrets (``secrets['mongo/default']['uri']``), and t-strings of
            them. A run's values (``dag_ctx``) go to the task's own arguments instead.

    Returns:
        The decorator.
    """

    def define(function: Callable[..., Iterator[T]]) -> Connection[T]:
        name = getattr(function, "__name__", "")
        where = f"connection `{name}`"
        if not inspect.isgeneratorfunction(function):
            raise TypeError(
                f"{where}: a connection is a generator function that yields what a task gets "
                "(as with contextlib.contextmanager)"
            )
        try:
            validate_task_id(name)
        except ValueError as e:
            raise ValueError(f"{where}: {e}") from None
        converted = _check_values(function, values, where)
        recorded = json.dumps(
            {"name": name, "params": {k: v.to_json() for k, v in converted.items()}},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        if len(recorded.encode()) > MAX_CONNECTION_SIZE:
            raise ValueError(
                f"{where}: its values take {len(recorded.encode())} bytes, over the 1 MiB a "
                "connection may hold (they travel with each task opening it); give bigger "
                "data to the task"
            )
        defined = Connection(function, converted)
        if parse_root_active():
            location = root_location(inspect.getsourcefile(function) or "")
            if location is not None:
                _DEFINED.append(ConnectionDef(name, location, converted))
        return defined

    return define


type ConnectionsArg = Sequence[Connection[object] | str] | None
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
    if isinstance(given, str | Connection):
        given = [given]
    names: list[str] = []
    for item in given:
        if isinstance(item, Connection):
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
    function: Callable[..., object], values: Mapping[str, object], where: str
) -> dict[str, Value]:
    """The values as DAG JSON values, each checked against its parameter's type."""
    parameters = inspect.signature(function).parameters
    hints = type_hints(function, where)
    for parameter in parameters.values():
        if parameter.kind in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD):
            raise TypeError(f"{where}: `{parameter}`: a connection's parameters are named")
        if parameter.kind is parameter.POSITIONAL_ONLY:
            raise TypeError(
                f"{where}: `{parameter.name}` is positional-only; its value is given by name"
            )
    unknown = sorted(set(values) - set(parameters))
    if unknown:
        raise TypeError(f"{where}: no parameter {', '.join(f'`{u}`' for u in unknown)}")
    missing = [
        name
        for name, parameter in parameters.items()
        if name not in values and parameter.default is parameter.empty
    ]
    if missing:
        raise TypeError(f"{where}: no value for {', '.join(f'`{m}`' for m in missing)}")
    converted = {
        name: _value(value, hints.get(name, object), f"{where}, `{name}`")
        for name, value in values.items()
    }
    # A default left out is recorded too: a command reads it (`task-ctx connection`), and a
    # secret or a t-string is filled in only when recorded (the function is called with the
    # recorded values). A plain default that isn't a DAG JSON value is Python's alone.
    for name, parameter in parameters.items():
        if name in values or parameter.default is parameter.empty:
            continue
        at = f"{where}, `{name}` (its default)"
        if isinstance(parameter.default, _secrets.Secret | Template):
            converted[name] = _value(parameter.default, hints.get(name, object), at)
            continue
        with contextlib.suppress(TypeError, ValueError, TypeModelError):
            converted[name] = _value(parameter.default, hints.get(name, object), at)
    return converted


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
