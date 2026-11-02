"""The run's context, ``dag_ctx``, and the running task's own, ``task_ctx``.

``dag_ctx`` is the current run's information.

In the DAG body, ``dag_ctx.run.planned_for`` is a **reference**, filled in at run time; it
can be passed to tasks and commands, but not used as a value in the DAG file. Inside a
running task (while the runner calls its function) the same expression gives the **value**
(see PROJECT.md, Run parameters and the DAG context). Anywhere else it fails ("must be run
inside a schedr task"); tests set values with ``schedr.testing.task_context(...)``.
``dag_ctx.params.<field>`` is the same for the run's parameters (``Dag(params=...)``), and
``dag_ctx.params(ReportParams)`` gives them typed by the class.
"""

import contextlib
from collections.abc import Callable, Iterator, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from schedr import _build
from schedr._model import DagCtxField, DagCtxRef, ParamRef, Reference, SecretDecl, SecretRef
from schedr._secrets import parse_secret
from schedr._serialization.codec import CodecError, decode, decode_any

_VALUE_IN_DAG_BODY = (
    "a reference can't be used as a value in the DAG file (it is known only at run time); "
    "pass it to a task, or decide in a task"
)

_OUTSIDE = (
    "`dag_ctx` must be run inside a schedr task (it gives references inside `with Dag(...)` "
    "and values while a task runs); in tests, set values with "
    "`schedr.testing.task_context(...)`"
)


@dataclass(frozen=True, slots=True)
class DagCtxReference:
    """A field of the run's context, as a reference in the DAG body.

    Attributes:
        field: Which field.
    """

    field: DagCtxField

    def _schedr_reference(self) -> Reference:
        return DagCtxRef(self.field)

    def __bool__(self) -> bool:
        """Refuses to be used as a value.

        Raises:
            TypeError: Always.
        """
        raise TypeError(_VALUE_IN_DAG_BODY)

    def __str__(self) -> str:
        """Refuses to be formatted into a plain string (use a t-string or pass it directly).

        Raises:
            TypeError: Always.
        """
        raise TypeError(_VALUE_IN_DAG_BODY)

    def __eq__(self, other: object) -> bool:
        """Refuses to be compared: the value is known only at run time (B108).

        Raises:
            TypeError: Always.
        """
        raise TypeError(_VALUE_IN_DAG_BODY)

    def __ne__(self, other: object) -> bool:
        """Refuses to be compared, as ``==``.

        Raises:
            TypeError: Always.
        """
        raise TypeError(_VALUE_IN_DAG_BODY)

    __hash__ = object.__hash__


@dataclass(frozen=True, slots=True)
class RunValues:
    """The run's context as values, inside a running task.

    Attributes:
        dag_id: The DAG's ID.
        run_id: The run's ID.
        kind: The run's kind.
        planned_for: The moment the run is for.
        since: The start of the run's window.
        params: The run's parameters, as stored (decision 138).
        params_class: The class the DAG declared them with; ``None``: it takes none.
        from_script: Read by a plain script (``schedr.script``), which doesn't know the
            declared class: a field is decoded by its stored form, and ``dag_ctx.params(Cls)``
            decodes into ``Cls`` strictly without comparing it with the declaration.
    """

    dag_id: str
    run_id: str
    kind: str
    planned_for: datetime
    since: datetime
    params: Mapping[str, Any] = field(default_factory=dict)
    params_class: type | None = None
    from_script: bool = False


class SecretError(Exception):
    """A secret the task reads can't be read: not declared, not found, or its storage down.

    Attributes:
        kind: ``secret_denied``, ``secret_not_found`` or ``secret_unavailable``. Raised out
            of the task, the attempt fails with this kind (only ``secret_unavailable`` is
            retried).
    """

    def __init__(self, kind: str, message: str) -> None:
        """An error of ``kind``.

        Args:
            kind: The error kind.
            message: What went wrong, naming the secret (never a value).
        """
        super().__init__(message)
        self.kind = kind


type SecretReader = Callable[[SecretRef], str | dict[str, str]]
"""Reads one secret: a key's value, or a whole secret's keys and values."""


@dataclass(frozen=True, slots=True)
class TaskValues:
    """The running task's own context (decision 84).

    Attributes:
        task_id: The task's ID.
        try_number: The attempt's try number, from 1.
        map_index: Which instance of a mapped task, from 0; ``None`` for an unmapped task.
        secrets: Reads a secret the task declares (``task_ctx.secret(...)``); ``None``: it
            can read none.
    """

    task_id: str
    try_number: int
    map_index: int | None
    secrets: SecretReader | None = None


_RUNNING: ContextVar[RunValues | None] = ContextVar("schedr_running_task", default=None)
_TASK: ContextVar[TaskValues | None] = ContextVar("schedr_running_task_own", default=None)

_DECLARED: ContextVar[tuple[SecretDecl, ...] | None] = ContextVar(
    "schedr_declared_secrets", default=None
)
"""The ``secrets=`` of a ``@task`` function called outside a DAG, while it runs (tests)."""


@contextlib.contextmanager
def declaring(declared: tuple[SecretDecl, ...]) -> Iterator[None]:
    """Makes ``declared`` the secrets a test's ``task_ctx.secret`` may read, in the block.

    Args:
        declared: The ``@task`` function's ``secrets=``.

    Yields:
        Nothing.
    """
    token = _DECLARED.set(declared)
    try:
        yield
    finally:
        _DECLARED.reset(token)


def declared_secrets() -> tuple[SecretDecl, ...] | None:
    """The ``secrets=`` of the ``@task`` function a test is calling; ``None`` outside one.

    Returns:
        The declarations, or ``None``.
    """
    return _DECLARED.get()


_script_values: Callable[[], RunValues] | None = None
"""Loads the run's context in a plain script; set by importing ``schedr.script``."""


def use_script_values(load: Callable[[], RunValues] | None) -> None:
    """Makes ``dag_ctx`` give ``load()``'s values outside a running task and a DAG body.

    ``schedr.script`` calls it when imported; ``None`` stops it (tests).

    Args:
        load: Gives the run's context (loaded once, on first use), or raises
            ``RuntimeError`` outside a task.
    """
    global _script_values  # noqa: PLW0603 - one switch, set by importing schedr.script
    _script_values = load


def _outside() -> RunValues:
    """The script's values, or the error of being outside a task."""
    if _script_values is None:
        raise RuntimeError(_OUTSIDE)
    return _script_values()


@contextlib.contextmanager
def running(values: RunValues, task: TaskValues | None = None) -> Iterator[None]:
    """Makes ``dag_ctx`` give values inside the block, and ``task_ctx`` the task's own.

    The runner calls a task's function inside it; references stand for values there.

    Args:
        values: The run's context.
        task: The task's own context.

    Yields:
        Nothing.
    """
    token = _RUNNING.set(values)
    task_token = _TASK.set(task)
    try:
        yield
    finally:
        _TASK.reset(task_token)
        _RUNNING.reset(token)


def _current[V](field: DagCtxField, value: Callable[[RunValues], V]) -> V:
    """A field's value inside a running task, its reference in a DAG body, else an error.

    Type checkers read it as the value (``V``) in both places, as they read a task call.
    """
    values = _RUNNING.get()
    if values is not None:
        return value(values)
    if _build.active() is None:
        return value(_outside())
    return DagCtxReference(field)  # type: ignore[return-value]


class RunContext:
    """``dag_ctx.run``: the run's information.

    Each field is a reference in the DAG body and a value inside a running task; type
    checkers read it as the value in both (as they read a task call), so it passes to a task
    taking that type. Reading a field anywhere else raises ``RuntimeError``.
    """

    @property
    def id(self) -> str:
        """The run's ID (a UUIDv7)."""
        return _current(DagCtxField.RUN_ID, lambda v: v.run_id)

    @property
    def kind(self) -> str:
        """The run's kind: ``scheduled``, ``manual``, ``backfill``, ``chained`` or ``event``."""
        return _current(DagCtxField.RUN_KIND, lambda v: v.kind)

    @property
    def planned_for(self) -> datetime:
        """The moment the run is for (a fire time for scheduled runs)."""
        return _current(DagCtxField.PLANNED_FOR, lambda v: v.planned_for)

    @property
    def since(self) -> datetime:
        """The start of the run's window, which is ``(since, planned_for]``."""
        return _current(DagCtxField.SINCE, lambda v: v.since)


class DagContext:
    """``dag_ctx``: the current run's context."""

    @property
    def run(self) -> RunContext:
        """The run's information."""
        return RunContext()

    @property
    def dag_id(self) -> str:
        """The DAG's ID."""
        return _current(DagCtxField.DAG_ID, lambda v: v.dag_id)

    @property
    def params(self) -> Params:
        """The run's parameters: ``dag_ctx.params.region``, or ``dag_ctx.params(Cls)``.

        A reference in the DAG body (its path checked against the declared class), the
        values inside a running task.

        Raises:
            RuntimeError: Outside a DAG body and a running task.
            TypeError: In a DAG body that declares no params.
        """
        values = _RUNNING.get()
        if values is not None:
            return ParamsValues(values)
        builder = _build.active()
        if builder is None:
            return ParamsValues(_outside())
        _build.params_type(builder, (), f"DAG `{builder.dag_id}`")
        return ParamsReference(builder, ())


class Params:
    """``dag_ctx.params``: a reference in the DAG body, the values in a running task.

    Statically a field is untyped (``Any``), as an output's path is (PROJECT.md, Run
    parameters); ``dag_ctx.params(ReportParams)`` is typed by the class passed.
    """

    def __getattr__(self, name: str) -> Any:
        """A field of the parameters."""
        raise NotImplementedError

    def __getitem__(self, key: str | int) -> Any:
        """A key or an index of the parameters."""
        raise NotImplementedError

    def __call__[P](self, cls: type[P]) -> P:
        """The parameters as an instance of ``cls``, the class the DAG declared.

        Args:
            cls: The declared class.

        Returns:
            The instance (a reference to all of them in the DAG body).
        """
        raise NotImplementedError


def _check_class(declared: type | None, cls: object) -> None:
    if declared is None:
        raise TypeError("the DAG declares no params (`Dag(params=...)`)")
    if cls is not declared:
        name = getattr(cls, "__name__", repr(cls))
        raise TypeError(
            f"`dag_ctx.params({name})`: the DAG's params are `{declared.__name__}`; pass that class"
        )


class ParamsReference(Params):
    """The run's parameters, or a part of them, as a reference in the DAG body."""

    def __init__(self, builder: _build.DagBuilder, path: tuple[str | int, ...]) -> None:
        """A reference to the part at ``path``, checked against the declared class.

        Args:
            builder: The DAG being defined.
            path: Field names and indexes.
        """
        self._builder = builder
        self._path = path
        if path:
            _build.params_type(builder, path, f"DAG `{builder.dag_id}`")

    def _schedr_reference(self) -> Reference:
        return ParamRef(self._path)

    def __getattr__(self, name: str) -> Any:
        """A field, as a reference.

        Raises:
            AttributeError: For a dunder or private name (copy, pickle and type checks ask).
        """
        if name.startswith("_"):
            raise AttributeError(name)
        return ParamsReference(self._builder, (*self._path, name))

    def __getitem__(self, key: str | int) -> Any:
        """A key or an index, as a reference.

        Raises:
            TypeError: For a key that is neither text nor a whole number.
        """
        if isinstance(key, bool) or not isinstance(key, str | int):
            raise TypeError(f"`dag_ctx.params[{key!r}]`: a key is text or a whole number")
        return ParamsReference(self._builder, (*self._path, key))

    def __call__[P](self, cls: type[P]) -> P:
        """All of the parameters, typed by the declared class.

        Raises:
            TypeError: For another class, or a part of the parameters.
        """
        if self._path:
            raise TypeError(f"`{_build.reference_label(ParamRef(self._path))}` isn't callable")
        _check_class(self._builder.params_class, cls)
        return self  # type: ignore[return-value]  # a reference, read as the value

    def __bool__(self) -> bool:
        """Refuses to be used as a value.

        Raises:
            TypeError: Always.
        """
        raise TypeError(_VALUE_IN_DAG_BODY)

    def __str__(self) -> str:
        """Refuses to be formatted into a plain string.

        Raises:
            TypeError: Always.
        """
        raise TypeError(_VALUE_IN_DAG_BODY)

    def __eq__(self, other: object) -> bool:
        """Refuses to be compared (B108).

        Raises:
            TypeError: Always.
        """
        raise TypeError(_VALUE_IN_DAG_BODY)

    def __ne__(self, other: object) -> bool:
        """Refuses to be compared, as ``==``.

        Raises:
            TypeError: Always.
        """
        raise TypeError(_VALUE_IN_DAG_BODY)

    def __iter__(self) -> None:
        """Refuses to be iterated: its length is known only at run time.

        Raises:
            TypeError: Always.
        """
        raise TypeError(_VALUE_IN_DAG_BODY)

    __hash__ = object.__hash__


class ParamsValues(Params):
    """The run's parameters inside a running task, decoded into the declared class."""

    def __init__(self, values: RunValues) -> None:
        """The values of the running task's run.

        Args:
            values: The run's context.
        """
        self._values = values
        self._decoded: object = None

    def _instance(self) -> object:
        if self._decoded is None:
            if self._values.from_script:
                self._decoded = decode_any(self._values.params, "dag_ctx.params")
            else:
                self._decoded = self(_declared(self._values))
        return self._decoded

    def __getattr__(self, name: str) -> Any:
        """A field's value.

        Raises:
            AttributeError: For a private name, or a field the class doesn't have.
        """
        if name.startswith("_"):
            raise AttributeError(name)
        instance = self._instance()
        if isinstance(instance, dict):  # a script's: decoded by the stored form
            if name not in instance:
                raise AttributeError(f"the run's params have no field `{name}`")
            return instance[name]
        return getattr(instance, name)

    def __getitem__(self, key: str | int) -> Any:
        """A field's value by name.

        Raises:
            TypeError: For an index: the parameters are an object.
        """
        if not isinstance(key, str):
            raise TypeError("dag_ctx.params is indexed by field name")
        if self._values.from_script:
            return self.__getattr__(key)
        return getattr(self._instance(), key)

    def __call__[P](self, cls: type[P]) -> P:
        """The parameters as an instance of ``cls``, decoded strictly.

        Raises:
            TypeError: For another class than the declared one, or values that don't fit
                it.
        """
        if not self._values.from_script:
            _check_class(self._values.params_class, cls)
        try:
            decoded: P = decode(self._values.params, cls, "dag_ctx.params")
        except CodecError as e:
            raise TypeError(f"the run's params don't fit `{cls.__name__}`: {e}") from None
        return decoded


def _declared(values: RunValues) -> type:
    if values.params_class is None:
        raise TypeError("the DAG declares no params (`Dag(params=...)`)")
    return values.params_class


dag_ctx = DagContext()
"""The current run's context: ``dag_ctx.run.planned_for``, ``dag_ctx.run.since``, ..."""

_TASK_OUTSIDE = (
    "`task_ctx` is the running task's own context: read it inside a task's function while it "
    "runs (it has no references for the DAG body); in tests, set it with "
    "`schedr.testing.task_context(...)`"
)


def _task() -> TaskValues:
    values = _TASK.get()
    if values is None:
        raise RuntimeError(_TASK_OUTSIDE)
    return values


class TaskContext:
    """``task_ctx``: the running task's own context (decision 84), values only.

    Reading it outside a running task (in the DAG body too) raises ``RuntimeError``.
    """

    @property
    def task_id(self) -> str:
        """The task's ID."""
        return _task().task_id

    @property
    def try_number(self) -> int:
        """The attempt's try number, from 1 (retries and requeues count)."""
        return _task().try_number

    @property
    def map_index(self) -> int | None:
        """Which instance of a mapped task this is, from 0; ``None`` for an unmapped task."""
        return _task().map_index

    def secret(self, name: str, *, storage: str | None = None) -> Any:
        """A secret the task declares in ``secrets=``, read now.

        One declared by name was fetched before the task started; one under a declared
        ``SecretPrefix`` is fetched now. Its value is masked in the task's log and output.

        Args:
            name: ``path#key``, or ``path`` for the whole secret.
            storage: The storage; ``None`` for the default one.

        Returns:
            A key's value (``str``), or a whole secret's keys and values
            (``dict[str, str]``).

        Raises:
            SecretError: When it can't be read (not declared, not found, the storage down).
            ValueError: For a bad name.
        """
        ref = parse_secret(name, storage)
        reader = _task().secrets
        if reader is None:
            raise SecretError(
                "secret_denied", f"`{ref.label()}` isn't declared in the task's `secrets=`"
            )
        return reader(ref)


task_ctx = TaskContext()
"""The running task's own context: ``task_ctx.map_index``, ``task_ctx.try_number``, ..."""
