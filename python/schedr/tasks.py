"""``@task``: Python tasks, and ``Output``, the reference a task call returns.

Inside ``with Dag(...)``, calling a ``@task`` function records a task and returns an
``Output``: passing it to another task (as an argument, at any depth in lists and dicts)
creates the dependency. ``>>`` adds order-only dependencies. Outside a DAG, a ``@task``
function is the plain function, so it can be unit-tested directly (see PROJECT.md, Native
API and Testing DAGs).
"""

import annotationlib
import functools
import inspect
import itertools
import typing
from collections.abc import Callable, Coroutine, Mapping, Sequence
from datetime import timedelta
from string.templatelib import Template
from typing import TYPE_CHECKING, Any, NoReturn, Protocol, Self, cast, overload

from schedr import _build, _secrets
from schedr import outcomes as _outcomes
from schedr._model import (
    DagCtxRef,
    EdgeKind,
    Expand,
    ExpandKwargs,
    ExpandProduct,
    LiteralValue,
    OutcomeRef,
    OutputRef,
    ParamRef,
    PythonKind,
    Reference,
    RefValue,
    ScalarType,
    SecretRef,
    TaskKind,
    TypeDesc,
    Value,
)
from schedr._secrets import SecretsArg
from schedr._serialization import (
    CodecError,
    TypeModelError,
    assignment_problem,
    describe,
    encode,
    is_assignable,
    type_name,
)
from schedr._serialization.annotations import label, type_hints
from schedr.connections import ConnectionsArg, declared_connections
from schedr.context import HANDLES_PARAMETER, declaring, handle_classes
from schedr.outcomes import Done, Outcome

_VALUE_IN_DAG_BODY = (
    "an output can't be used as a value in the DAG file (it exists only at run time); "
    "pass it to a task, or decide in a task"
)


class Mapped[T](list[T]):
    """A mapped task's output, as type checkers read it: the list of its instances' outputs.

    At run time it is a reference (an ``Output``), as any task's output in the DAG body.
    Index it (``results[0]``), pass it whole to a task taking a list (a gather), or map over
    it element by element (``check.expand(n=results)``), narrowing each element with
    ``.on(...)``: ``count.expand(rows=results.on(Rows))``.
    """

    if TYPE_CHECKING:

        @overload
        def on[U](self, kind: type[U], /) -> list[U]: ...
        @overload
        def on[U, V](self, kind: type[U], other: type[V], /) -> list[U | V]: ...
        @overload
        def on(self, kind: None, /) -> list[Done]: ...
        @overload
        def on[E: Outcome](self: Mapped[E], member: E, /, *members: E) -> list[Done]: ...
        def on(self, *kinds: object) -> object:
            """Narrows each element to some of the task's outcomes (in ``.expand()``).

            Classes of an outcome union, ``None``, or members of an ``Outcome`` enum.
            """
            ...

        def __rshift__(self, other: object, /) -> object:
            """``self >> other``: ``other`` runs after every instance."""
            ...

        def __rrshift__(self, other: object, /) -> Self:
            """``[a, b] >> self``: every instance runs after the tasks in the list."""
            ...


class Output[T]:
    """The output of a task, or a part of it, as a reference in the DAG body.

    ``out.rows`` (a field), ``out["key"]`` (a key) and ``out[0]`` (an index) reference parts;
    the master extracts them when it builds the downstream task's inputs.
    """

    __slots__ = ("_dag", "_on", "_path", "_task")

    def __init__(
        self,
        task_id: str,
        path: tuple[str | int, ...] = (),
        on: tuple[str, ...] = (),
        *,
        dag: _build.DagBuilder | None = None,
    ) -> None:
        """References ``task_id``'s output at ``path``.

        Args:
            task_id: The task that produces the output.
            path: Fields, keys and indexes into it.
            on: The outcomes it is narrowed to (``.on(...)``); empty for any.
            dag: The DAG the task belongs to (the one being defined if not given).
        """
        self._task = task_id
        self._path = path
        self._on = on
        self._dag = dag or _build.active()

    @property
    def _schedr_dag_id(self) -> str | None:
        """The DAG the task belongs to, checked when a branch returns it (B126)."""
        return self._dag.dag_id if self._dag is not None else None

    @property
    def _schedr_task_id(self) -> str:
        _build.check_same_dag(self._dag, self._task)
        return self._task

    @property
    def _schedr_on(self) -> tuple[str, ...]:
        return self._on

    def _schedr_reference(self) -> Reference:
        _build.check_same_dag(self._dag, self._task)
        return OutputRef(self._task, self._path, self._on)

    @overload
    def on[U](self, kind: type[U], /) -> Output[U]: ...

    @overload
    def on[U, V](self, kind: type[U], other: type[V], /) -> Output[U | V]: ...

    @overload
    def on(self, kind: None, /) -> Output[None]: ...

    @overload
    def on(self, *members: _outcomes.Outcome) -> Output[T]: ...

    def on(self, *kinds: object) -> Output[object]:
        """Depends on the task only when it ends with one of these outcomes.

        The output is narrowed to them: ``load(fetch().on(Rows))``,
        ``check.on(Check.CHANGED) >> notify``.

        An edge whose outcome didn't happen isn't taken: under ``RunIf.NONE_FAILED`` the
        downstream task gets ``None`` for it (its parameter must allow ``None``), under
        ``RunIf.ALL_SUCCEEDED`` it is skipped.

        Args:
            *kinds: Classes (or ``None``) of the task's outcome union, or members of its
                ``Outcome`` enum.

        Returns:
            The narrowed reference.

        Raises:
            TypeError: For an outcome the task doesn't have, or ``.on()`` on a part of the
                output (narrow first: ``f.on(Rows).rows``).
        """
        if self._path or self._on:
            raise TypeError(
                f"{self!r}.on(): narrow the whole output first, e.g. `{self._task}.on(...)`"
            )
        builder = _build.require_active("`.on()`")
        _build.check_same_dag(self._dag, self._task)
        task = builder.tasks.get(self._task)
        if task is not None and isinstance(task.kind, PythonKind) and task.kind.branch:
            raise TypeError(
                f"`{self._task}.on()`: a @branch task's edges are taken when it chose their "
                "task, not on outcomes; return an outcome from a @task to route with .on()"
            )
        spec = builder.outcomes.get(self._task, _outcomes.DEFAULT)
        return Output(self._task, on=_outcomes.names_for(spec, kinds, self._task), dag=self._dag)

    @property
    def outcome(self) -> _OutcomeOutput:
        """The task's outcome itself as data: pass it to a task taking its ``Outcome`` enum.

        Raises:
            TypeError: On a task whose outcomes are a union of classes (the value's class
                already says which), on a wait step, or on a part of the output.
        """
        if self._path or self._on:
            raise TypeError(f"{self!r}.outcome: take it from the whole output")
        builder = _build.require_active("`.outcome`")
        _build.check_same_dag(self._dag, self._task)
        spec = builder.outcomes.get(self._task, _outcomes.DEFAULT)
        if _outcomes.TIMED_OUT_NAME in spec.names:
            raise TypeError(f"`{self._task}.outcome`: a wait step routes with `.on(TimedOut)`")
        if spec.codes and spec.names != _outcomes.DEFAULT.names:
            raise TypeError(
                f"`{self._task}.outcome`: its `schedr.toml` declares {', '.join(spec.names)}; "
                "to read it as data, give the task `outcomes=` an `Outcome` enum"
            )
        if spec.is_union:
            raise TypeError(
                f"`{self._task}.outcome`: its outcomes are a union of classes; the value's "
                f"class says which, so pass `{self._task}` (or narrow with `.on(...)`)"
            )
        if _build.may_have_field(builder.tasks[self._task].output_type, "outcome"):
            raise TypeError(
                f"`{self._task}.outcome` is the task's outcome, but its output may also have "
                f'a field `outcome`: write `{self._task}["outcome"]` for the field'
            )
        return _OutcomeOutput(self._task, self._dag)

    def __getattr__(self, name: str) -> Output[object]:
        """References a field of the output.

        Args:
            name: The field's name.

        Returns:
            The reference to the field.

        Raises:
            AttributeError: For names starting with ``_``.
        """
        if name.startswith("_"):
            raise AttributeError(name)
        return Output(self._task, (*self._path, name), self._on, dag=self._dag)

    def __getitem__(self, key: str | int) -> Output[object]:
        """References a key or an index of the output.

        Args:
            key: A dictionary key or a list / tuple index.

        Returns:
            The reference to the part.

        Raises:
            TypeError: For a key that is neither a string nor an integer.
            ValueError: For a negative index.
        """
        if isinstance(key, bool) or not isinstance(key, str | int):
            raise TypeError(f"output keys are str or int, got {type(key).__name__}")
        if isinstance(key, int) and key < 0:
            raise ValueError(f"output index {key}: negative indexes aren't supported")
        return Output(self._task, (*self._path, key), self._on, dag=self._dag)

    def __rshift__(self, other: object) -> object:
        """``self >> other``: ``other`` runs after this task.

        Returns:
            ``other``, so dependencies can be chained.
        """
        _build.order(self, other, ">>")
        return other

    def __rrshift__(self, other: object) -> Output[T]:
        """``[a, b] >> self``: this task runs after the tasks in the list.

        Returns:
            This output.
        """
        _build.order(other, self, ">>")
        return self

    def __lshift__(self, other: object) -> object:
        """``self << other``: this task runs after ``other``.

        Returns:
            ``other``.
        """
        _build.order(other, self, "<<")
        return other

    def __rlshift__(self, other: object) -> Output[T]:
        """``[a, b] << self``: the tasks in the list run after this task.

        Returns:
            This output.
        """
        _build.order(self, other, "<<")
        return self

    def __bool__(self) -> bool:
        """Refuses to be used as a value.

        Raises:
            TypeError: Always.
        """
        raise TypeError(_VALUE_IN_DAG_BODY)

    def __iter__(self) -> NoReturn:
        """Refuses to be iterated (map over outputs with ``.expand()``, milestone M4).

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
        """Refuses to be compared: the value exists only at run time (B108).

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

    def __repr__(self) -> str:
        """A readable form, e.g. ``Output(extract.rows[0])``."""
        on = f".on({', '.join(self._on)})" if self._on else ""
        path = "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in self._path)
        return f"Output({self._task}{on}{path})"


class _OutcomeOutput:
    """A task's outcome as data (``task.outcome``), a reference in the DAG body."""

    __slots__ = ("_dag", "_task")

    def __init__(self, task_id: str, dag: _build.DagBuilder | None) -> None:
        self._task = task_id
        self._dag = dag

    @property
    def _schedr_dag_id(self) -> str | None:
        """The DAG the task belongs to, checked when a branch returns it (B126)."""
        return self._dag.dag_id if self._dag is not None else None

    @property
    def _schedr_task_id(self) -> str:
        _build.check_same_dag(self._dag, self._task)
        return self._task

    def _schedr_reference(self) -> Reference:
        _build.check_same_dag(self._dag, self._task)
        return OutcomeRef(self._task)

    def __bool__(self) -> bool:
        """Refuses to be used as a value (B108).

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
        """Refuses to be compared: the value exists only at run time (B108).

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

    def __repr__(self) -> str:
        """A readable form, e.g. ``Output(check.outcome)``."""
        return f"Output({self._task}.outcome)"


_STARS: dict[inspect._ParameterKind, str] = {
    inspect.Parameter.VAR_POSITIONAL: "*",
    inspect.Parameter.VAR_KEYWORD: "**",
}


def _check_function(fn: Callable[..., Any]) -> inspect.Signature:
    """Checks that ``fn`` can be a task and returns its signature."""
    if not inspect.isfunction(fn):
        raise TypeError(f"@task needs a function, got {type(fn).__name__}")
    if inspect.isasyncgenfunction(fn):
        raise TypeError(
            f"@task `{fn.__name__}` is an async generator: a function that yields events is a "
            "trigger (@trigger, milestone M4), not a task"
        )
    annotations = annotationlib.get_annotations(fn, format=annotationlib.Format.FORWARDREF)
    if "return" not in annotations:
        raise TypeError(
            f"@task `{fn.__name__}` must annotate its return type (`-> None` for no output, "
            "`-> Any` to skip the parse-time check)"
        )
    signature = inspect.signature(fn, annotation_format=annotationlib.Format.FORWARDREF)
    for parameter in signature.parameters.values():
        stars = _STARS.get(parameter.kind)
        if stars is not None:
            raise TypeError(
                f"@task `{fn.__name__}`: `{stars}{parameter.name}` isn't supported; arguments "
                "are stored by name, so declare each parameter"
            )
        if parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
            raise TypeError(
                f"@task `{fn.__name__}`: `{parameter.name}` is positional-only (before `/`); "
                "arguments are stored and passed by name, so remove the `/`"
            )
    return signature


def _check_cleanup_function(cleanup: Callable[..., object], task_id: str) -> None:
    """Checks that ``cleanup`` is a plain function taking its arguments by name."""
    where = f"task `{task_id}`: cleanup="
    if not inspect.isfunction(cleanup):
        raise TypeError(f"{where} needs a function, got {type(cleanup).__name__}")
    if inspect.isgeneratorfunction(cleanup) or inspect.isasyncgenfunction(cleanup):
        raise TypeError(f"{where} `{cleanup.__name__}` must not be a generator: it wouldn't run")
    signature = inspect.signature(cleanup, annotation_format=annotationlib.Format.FORWARDREF)
    for parameter in signature.parameters.values():
        if parameter.kind in _STARS or parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
            raise TypeError(
                f"{where} `{cleanup.__name__}`: `{parameter.name}`: arguments are passed by "
                "name, so declare each parameter"
            )


def _cleanup_handles(
    cleanup: Callable[..., object],
    task_parameters: Mapping[str, object],
    task_hints: Mapping[str, object],
    where: str,
) -> tuple[str, ...]:
    """The handle types a cleanup takes; its parameters checked against the task's.

    Args:
        cleanup: The cleanup function.
        task_parameters: The task's parameters, by name.
        task_hints: The task's resolved annotations.
        where: The task, for messages.

    Returns:
        The class names of its ``handles``; empty when it takes none.

    Raises:
        TypeError: For a parameter that isn't the task's, or isn't of the same type.
    """
    name = cleanup.__name__
    hints = type_hints(cleanup, f"{where}: cleanup `{name}`")
    handles: tuple[str, ...] = ()
    for parameter in inspect.signature(
        cleanup, annotation_format=annotationlib.Format.FORWARDREF
    ).parameters.values():
        if parameter.name == HANDLES_PARAMETER:
            try:
                classes = handle_classes(hints.get(parameter.name))
            except TypeError as e:
                raise TypeError(f"{where}: cleanup `{name}`: {e}") from None
            handles = tuple(cls.__name__ for cls in classes)
        elif parameter.name not in task_parameters:
            if parameter.default is inspect.Parameter.empty:
                raise TypeError(
                    f"{where}: cleanup `{name}`: `{parameter.name}` isn't a parameter of the "
                    "task: a cleanup takes the task's parameters and `handles`"
                )
        elif (
            parameter.name in hints
            and parameter.name in task_hints
            and hints[parameter.name] != task_hints[parameter.name]
        ):
            raise TypeError(
                f"{where}: cleanup `{name}`: `{parameter.name}` is "
                f"`{label(hints[parameter.name])}`, the task's is "
                f"`{label(task_hints[parameter.name])}`"
            )
    return handles


class TaskFunction[**P, R]:
    """A function decorated with ``@task``.

    Calling it inside ``with Dag(...)`` records a task and returns its ``Output``; outside, it
    calls the function.
    """

    def __init__(
        self,
        fn: Callable[P, R],
        task_id: str,
        options: _build.TaskOptions,
        branch: bool = False,
        cleanup: Callable[..., object] | None = None,
    ) -> None:
        """Wraps ``fn`` as a task.

        Args:
            fn: The function.
            task_id: The task's ID.
            options: The task's own options.
            branch: A ``@branch`` task (its output: the downstream tasks it chose).
            cleanup: Its ``cleanup=`` function.

        Raises:
            TypeError: If ``fn`` can't be a task (see ``@task``), or ``cleanup`` a cleanup.
        """
        self._signature = _check_function(fn)
        self._cleanup = cleanup
        if cleanup is not None:
            _check_cleanup_function(cleanup, task_id)
        self._fn = fn
        self._task_id = task_id
        self._options = options
        self._branch = branch
        functools.update_wrapper(self, fn)

    @property
    def task_id(self) -> str:
        """The ID of the task a call creates."""
        return self._task_id

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Records the task (inside a DAG) or calls the function (outside).

        Type checkers read the call as the function's value (``R``), so it passes to a task
        taking ``R`` and its fields are typed; ``@task`` makes ``R`` ``Done`` for a function
        returning ``None``, which ``>>`` orders. Inside a DAG it is an ``Output`` all the same:
        a reference.

        Returns:
            The task's output, a reference; outside a DAG, the function's return value.

        Raises:
            TypeError: For arguments that don't match the function's parameters, or values
                that can't be stored.
        """
        builder = _build.active()
        if builder is None:
            # In a test's `task_context`, a read it doesn't declare is refused, as on a worker.
            with declaring(self._options.secrets or (), self._options.connections or ()):
                called = self._fn(*args, **kwargs)
            if inspect.iscoroutine(called):
                # To `await` in a test (decision 49): the body runs then, still declaring.
                return cast("R", self._declaring_while(called))
            return called
        task_id = builder.qualified(self._task_id)
        try:
            bound = self._signature.bind(*args, **kwargs)
        except TypeError as e:
            raise TypeError(f"task `{task_id}`: {e}") from None
        return self._record(builder, dict(bound.arguments), None)  # type: ignore[return-value]

    async def _declaring_while[T](self, called: Coroutine[object, object, T]) -> T:
        """Runs an ``async def`` task's coroutine with what the task declares in force."""
        with declaring(self._options.secrets or (), self._options.connections or ()):
            return await called

    def partial(self, **fixed: object) -> PartialTask[P, R]:
        """Fixes some arguments of a mapped task: ``load.partial(bucket="raw").expand(...)``.

        Args:
            **fixed: Arguments every instance gets.

        Returns:
            The task with them fixed, to ``.expand(...)``.

        Raises:
            TypeError: For a name that isn't a parameter.
        """
        unknown = sorted(set(fixed) - set(self._signature.parameters))
        if unknown:
            raise TypeError(f"task `{self._task_id}`: `.partial()`: no parameter `{unknown[0]}`")
        return PartialTask(self, fixed)

    def expand(self, **mapped: object) -> Mapped[R]:
        """A mapped task: one instance per element of the lists given (their cartesian product).

        ``load.expand(path=files)`` runs ``load`` once per element of ``files`` (a literal list
        or an upstream output that is a list, a mapped task's included, element by element).
        Type checkers read it as the list of the instances' outputs, in the order of the
        elements.

        Args:
            **mapped: Each mapped parameter's list.

        Returns:
            The list of the instances' outputs (a reference in a DAG body).
        """
        return PartialTask(self, {}).expand(**mapped)

    def expand_kwargs(self, values: object) -> Mapped[R]:
        """A mapped task: one instance per dict of ``values``, its arguments.

        Args:
            values: A list of dicts (a literal, or an upstream output that is one).

        Returns:
            The list of the instances' outputs (a reference in a DAG body).
        """
        return PartialTask(self, {}).expand_kwargs(values)

    def _with_secret_defaults(
        self, fixed: dict[str, object], expand: tuple[str, object] | None
    ) -> dict[str, object]:
        """``fixed`` with each parameter defaulting to a secret or a t-string that no call gives.

        The runner calls the function with the recorded arguments only: a default left out
        would reach it as the placeholder or the ``Template``, never filled in.
        """
        mapped = expand[1] if expand is not None and expand[0] == "product" else {}
        given = dict(fixed)
        for name, parameter in self._signature.parameters.items():
            if (
                isinstance(parameter.default, _secrets.Secret | Template)
                and name not in given
                and not (isinstance(mapped, Mapping) and name in mapped)
            ):
                given[name] = parameter.default
        return given

    def _record(
        self,
        builder: _build.DagBuilder,
        fixed: dict[str, object],
        expand: tuple[str, object] | None,
    ) -> Output[Any]:
        """Records the task: its fixed arguments and, for a mapped task, what it maps over.

        Args:
            builder: The DAG being defined.
            fixed: The arguments every instance gets, by parameter name.
            expand: ``("product", {name: list})`` or ``("kwargs", list_of_dicts)``.

        Returns:
            The reference to its output (the list of its instances' outputs when mapped).
        """
        # Inside groups, the ID carries their prefix (decision 56A).
        task_id = builder.qualified(self._task_id)
        where = f"task `{task_id}`"
        if expand is not None and self._branch:
            raise TypeError(f"{where}: a @branch task can't be mapped (`.expand()`)")
        hints = type_hints(self._fn, where)
        annotation = self._output_hint(hints, where)
        spec = self._outcome_spec(annotation, where)
        try:
            # An Outcome enum task's output is None: the member is its outcome.
            output_type = describe(
                list[str]
                if self._branch
                else None
                if spec.enum is not _outcomes.DefaultOutcome
                else self._produced(annotation)
            )
            outcome_types = self._outcome_types(spec, annotation, describe)
        except TypeModelError as e:
            raise TypeModelError(f"{where}: return type `{label(annotation)}`: {e}") from None
        upstream: list[_build.Upstream] = list(self._kind_upstream())
        fixed = self._with_secret_defaults(fixed, expand)
        # The None checks its arguments queue are its own: dropped if it isn't added (an ID
        # already taken would otherwise give them to the task that has it).
        checks = len(builder.none_checks)
        try:
            arguments = {
                name: self._argument(
                    builder,
                    (task_id, value),
                    hints.get(name, Any),
                    f"{where}, argument `{name}`",
                    upstream,
                )
                for name, value in fixed.items()
            }
            _refuse_whole_mapped(builder, arguments, where)
            mapped = (
                None if expand is None else self._mapped(builder, task_id, hints, expand, upstream)
            )
            if expand is None:
                _build.hint_unmapped_per_run(self._options, task_id, stacklevel=3)
            wanted = None
            if (cleanup := self._cleanup) is not None:
                parameters = self._signature.parameters
                handles = _cleanup_handles(cleanup, parameters, hints, where)
                wanted = _build.CleanupWanted(None, handles)
            builder.add_task(
                task_id,
                self._kind(arguments),
                output_type,
                self._options,
                spec,
                outcome_types,
                mapped,
                wanted,
            )
        except BaseException:
            del builder.none_checks[checks:]
            raise
        builder.functions[task_id] = self._fn
        if self._cleanup is not None:
            builder.cleanups[task_id] = self._cleanup
        for up, on in dict.fromkeys(upstream):
            builder.add_edge(up, task_id, EdgeKind.DATA, on)
        # A reference, which type checkers read as the value (see above).
        return Output[R](task_id, dag=builder)

    def _output_hint(self, hints: Mapping[str, object], where: str) -> object:
        """The type of its output, from its function's type hints."""
        del where
        return hints["return"]

    def _produced(self, annotation: object) -> object:
        """What it can store: what its function returns, for a plain task."""
        return annotation

    def _outcome_spec(self, annotation: object, where: str) -> _outcomes.OutcomeSpec:
        """Its outcomes: a branch's output is the IDs of the tasks it chose, its outcome SUCCESS."""
        return _outcomes.DEFAULT if self._branch else _outcomes.from_return_type(annotation, where)

    def _outcome_types(
        self,
        spec: _outcomes.OutcomeSpec,
        annotation: object,
        describe: Callable[[object], TypeDesc],
    ) -> dict[str, TypeDesc]:
        """What ``.on()`` narrows to, by outcome: each class of an outcome union."""
        del annotation
        if not spec.is_union:
            return {}
        return {name: describe(cls) for name, cls in zip(spec.names, spec.classes, strict=True)}

    def _kind(self, arguments: Mapping[str, Value]) -> TaskKind:
        """Its kind in DAG JSON."""
        return PythonKind(arguments, branch=self._branch)

    def _kind_upstream(self) -> list[_build.Upstream]:
        """The upstream tasks its kind reads besides its arguments: none for a plain task."""
        return []

    def _mapped(
        self,
        builder: _build.DagBuilder,
        task_id: str,
        hints: dict[str, Any],
        expand: tuple[str, object],
        upstream: list[_build.Upstream],
    ) -> Expand:
        """What a mapped task varies over, each list checked against its parameter's type."""
        where = f"task `{task_id}`"
        kind, value = expand
        if kind == "kwargs":
            # A dict written in the DAG: each argument against its parameter's type.
            for index, element in enumerate(value if isinstance(value, list | tuple) else ()):
                if not isinstance(element, Mapping):
                    continue
                for name, item in element.items():
                    self._argument(
                        builder,
                        (task_id, item),
                        hints.get(name, Any),
                        f"{where}, `.expand_kwargs()` element {index}, argument `{name}`",
                        upstream,
                    )
            converted = self._argument(
                builder,
                (task_id, value),
                list[dict[str, Any]],
                f"{where}, `.expand_kwargs()`",
                upstream,
                elementwise=True,
            )
            _refuse_param_mapping(converted, f"{where}, `.expand_kwargs()`")
            return ExpandKwargs(converted)
        assert isinstance(value, dict)
        over = {
            name: self._argument(
                builder,
                (task_id, values),
                list[hints.get(name, Any)],  # type: ignore[misc]
                f"{where}, mapped argument `{name}`",
                upstream,
                elementwise=True,
            )
            for name, values in value.items()
        }
        for name, converted in over.items():
            _refuse_param_mapping(converted, f"{where}, mapped argument `{name}`")
        return ExpandProduct(over)

    @staticmethod
    def _argument(
        builder: _build.DagBuilder,
        task_value: tuple[str, object],
        annotation: object,
        where: str,
        upstream: list[_build.Upstream],
        *,
        elementwise: bool = False,
    ) -> Value:
        """Converts one argument of a task, checking it against its parameter's type.

        A reference is also queued for the ``None`` check, made once the DAG is complete
        (see ``DagBuilder.check_nones``), unless it is what a mapped task maps over as a whole
        (``elementwise``): narrowed there, a mapped output skips the instances whose element
        didn't match instead.
        """
        task_id, value = task_value
        try:
            expected = describe(annotation, "input")
        except TypeModelError as e:
            raise TypeModelError(f"{where}: parameter type `{label(annotation)}`: {e}") from None
        converted = _build.to_value(value, where, upstream)
        for template in _build.templates_in(converted):
            _build.check_template_references(builder, template, where)
        try:
            templates = _build.typed_templates(converted, expected, where)
        except TypeError as e:
            raise TypeError(f"{where}: {e}") from None
        for spot, place in templates:
            if not is_assignable(ScalarType("str"), place):
                raise TypeError(f"{spot}: expects `{type_name(place)}`, but a t-string gives `str`")
        # Each reference, at any depth, against the type of the place it fills; the literals
        # beside them against theirs.
        try:
            typed = _build.typed_references(converted, expected)
        except TypeError as e:
            raise TypeError(f"{where}: {e}") from None
        _build.check_literals(converted, annotation, where)
        for reference, at in typed:
            produced = _build.reference_type(builder, reference, where)
            problem = assignment_problem(produced, at)
            if problem is not None:
                reason = f": {problem}" if problem else ""
                raise TypeError(
                    f"{where}: expects `{type_name(at)}`, but "
                    f"`{_build.reference_label(reference)}` is `{type_name(produced)}`{reason}"
                )
            # Whether it may get None is known once the DAG is complete.
            whole = elementwise and isinstance(converted, RefValue)
            if isinstance(reference, OutputRef | OutcomeRef) and not whole:
                builder.none_checks.append((task_id, where, reference, at))
        # The whole value against one form of the type (B111, B112).
        _build.check_fits(builder, converted, annotation, where)
        if isinstance(converted, LiteralValue):
            try:
                converted = LiteralValue(encode(value, annotation))
            except CodecError as e:
                raise TypeError(f"{where}: {e.reason}") from None
        return converted

    def override(  # noqa: PLR0913 (the task's options, by keyword)
        self,
        *,
        task_id: str | None = None,
        retries: int | None = None,
        retry_delay: timedelta | None = None,
        timeout: timedelta | None = None,
        queue: str | None = None,
        max_output_size: int | None = None,
        preload: bool | None = None,
        fresh_process: bool | None = None,
        run_if: _outcomes.RunIf | None = None,
        max_active: int | None = None,
        max_active_per_run: int | None = None,
        secrets: SecretsArg = None,
        connections: ConnectionsArg = None,
        cleanup: Callable[..., object] | None = None,
        cleanup_timeout: timedelta | None = None,
        cleanup_retries: int | None = None,
        cleanup_retry_delay: timedelta | None = None,
        cleanup_anywhere: bool | None = None,
        uses: Sequence[object] | None = None,
        max_wait: timedelta | None = None,
    ) -> Self:
        """The same function as another task, e.g. ``load.override(task_id="load_eu")(...)``.

        Args:
            task_id: A different task ID (needed to use the function twice in one DAG).
            retries: Overrides the retries.
            retry_delay: Overrides the retry delay.
            timeout: Overrides the timeout.
            queue: Overrides the queue.
            max_output_size: Overrides the output size limit.
            preload: Overrides whether it forks from the preloaded fork server.
            fresh_process: Overrides whether it is always a new process.
            run_if: Overrides its trigger rule.
            max_active: Overrides its limit across runs.
            max_active_per_run: Overrides its limit within a run.
            secrets: Overrides the secrets it declares.
            connections: Overrides the connections it declares.
            cleanup: Overrides its cleanup.
            cleanup_timeout: Overrides how long one try of its cleanup may run.
            cleanup_retries: Overrides how many times a failed cleanup is retried.
            cleanup_retry_delay: Overrides the delay before each cleanup retry.
            cleanup_anywhere: Overrides whether the cleanup may run on another worker.
            uses: Overrides the resources it needs.
            max_wait: Overrides how long a wait step waits; a wait step's only.

        Returns:
            A new task function; this one is unchanged.
        """
        changes = _build.TaskOptions(
            retries,
            retry_delay,
            timeout,
            queue,
            max_output_size,
            preload,
            fresh_process,
            run_if,
            max_active=max_active,
            max_active_per_run=max_active_per_run,
            secrets=None
            if secrets is None
            else _secrets.declared(secrets, f"task `{task_id or self._task_id}`"),
            connections=declared_connections(connections, f"task `{task_id or self._task_id}`"),
            cleanup_timeout=cleanup_timeout,
            cleanup_retries=cleanup_retries,
            cleanup_retry_delay=cleanup_retry_delay,
            cleanup_anywhere=cleanup_anywhere,
            uses=None if uses is None else tuple(uses),
            max_wait=max_wait,
        )
        return self._again(
            task_id or self._task_id,
            changes.over(self._options),
            self._cleanup if cleanup is None else cleanup,
        )

    def _again(
        self,
        task_id: str,
        options: _build.TaskOptions,
        cleanup: Callable[..., object] | None,
    ) -> Self:
        """The same function with other settings (``.override()``)."""
        if options.max_wait is not None:
            raise TypeError(f"task `{task_id}`: only a wait step has `max_wait`")
        # A subclass gives its own: this is the case of a plain task.
        return typing.cast("Self", _made(self._fn, task_id, options, self._branch, cleanup))


class AsyncTaskFunction[**P, R, A = R](TaskFunction[P, R]):
    """A function decorated with ``@task`` that is ``async def``.

    A call reads, to a type checker, as ``R``, so ``load(extract())`` checks in a DAG; in a test,
    ``await task.fn(...)`` is typed as the coroutine it is, of ``A``, what the function returns
    (decision 319). They differ for ``-> None``, whose call in a DAG reads as ``Done``.
    """

    def fn(self, *args: P.args, **kwargs: P.kwargs) -> Coroutine[object, object, A]:
        """Calls the function, to ``await`` in a test.

        Args:
            *args: Its arguments.
            **kwargs: Its arguments.

        Returns:
            Its coroutine; what the task declares is in force while it runs.
        """
        # `_fn` is typed as what `@task` read `R` from: an `async def` gives this at run time.
        called = cast("Coroutine[object, object, A]", self._fn(*args, **kwargs))
        return self._declaring_while(called)


def is_async(fn: Callable[..., object]) -> bool:
    """Whether calling ``fn`` gives a coroutine, seeing through a ``functools.wraps`` decorator."""
    return inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(inspect.unwrap(fn))


def _made[**P, R](
    fn: Callable[P, R],
    task_id: str,
    options: _build.TaskOptions,
    branch: bool = False,
    cleanup: Callable[..., object] | None = None,
) -> TaskFunction[P, R]:
    """``fn`` as a task function, of the class its kind has."""
    kind = AsyncTaskFunction if is_async(fn) else TaskFunction
    return kind(fn, task_id, options, branch, cleanup)


class PartialTask[**P, R]:
    """A ``@task`` function with some arguments fixed: ``load.partial(bucket="raw")``."""

    def __init__(self, function: TaskFunction[P, R], fixed: dict[str, object]) -> None:
        """Remembers the function and the fixed arguments.

        Args:
            function: The task function.
            fixed: The arguments every instance gets.
        """
        self._function = function
        self._fixed = fixed

    def expand(self, **mapped: object) -> Mapped[R]:
        """See :meth:`TaskFunction.expand`.

        Args:
            **mapped: Each mapped parameter's list.

        Returns:
            The list of the instances' outputs (a reference in a DAG body).

        Raises:
            RuntimeError: Outside ``with Dag(...)``.
            TypeError: For no mapped argument, an unknown or fixed one, or a required
                parameter left without a value.
        """
        name = self._function.task_id
        if not mapped:
            raise TypeError(f"task `{name}`: `.expand()` needs at least one mapped argument")
        parameters = self._function._signature.parameters
        for key in mapped:
            if key not in parameters:
                raise TypeError(f"task `{name}`: `.expand()`: no parameter `{key}`")
            if key in self._fixed:
                raise TypeError(
                    f"task `{name}`: `{key}` is both fixed (`.partial()`) and mapped (`.expand()`)"
                )
        try:
            self._function._signature.bind(**self._fixed, **dict.fromkeys(mapped))
        except TypeError as e:
            raise TypeError(f"task `{name}`: {e}") from None
        builder = _build.require_active(f"`{name}.expand()`")
        recorded = self._function._record(builder, dict(self._fixed), ("product", mapped))
        return recorded  # type: ignore[return-value]

    def expand_kwargs(self, values: object) -> Mapped[R]:
        """See :meth:`TaskFunction.expand_kwargs`.

        Args:
            values: A list of dicts (a literal, or an upstream output that is one).

        Returns:
            The list of the instances' outputs (a reference in a DAG body).

        Raises:
            RuntimeError: Outside ``with Dag(...)``.
            TypeError: For a list written in the DAG with an element that isn't a dict, or a
                dict naming an unknown or fixed parameter or leaving a required one without a
                value.
        """
        name = self._function.task_id
        if isinstance(values, list | tuple):
            # Each dict written here is one instance's arguments: checked as `.expand()` checks
            # its names (an upstream output's dicts are checked by its type).
            for index, element in enumerate(values):
                if _build.is_reference(element):
                    continue
                where = f"task `{name}`: `.expand_kwargs()` element {index}"
                if not isinstance(element, Mapping):
                    raise TypeError(
                        f"{where}: every element must be a dict of arguments, "
                        f"not `{type(element).__name__}`"
                    )
                self._check_kwargs(where, element)
        builder = _build.require_active(f"`{name}.expand_kwargs()`")
        recorded = self._function._record(builder, dict(self._fixed), ("kwargs", values))
        return recorded  # type: ignore[return-value]

    def _check_kwargs(self, where: str, element: Mapping[object, object]) -> None:
        """Checks one instance's dict of arguments against the function and the fixed ones."""
        parameters = self._function._signature.parameters
        for key in element:
            if key not in parameters:
                raise TypeError(f"{where}: no parameter `{key}`")
            if key in self._fixed:
                raise TypeError(
                    f"{where}: `{key}` is both fixed (`.partial()`) and mapped (`.expand_kwargs()`)"
                )
        try:
            self._function._signature.bind(**self._fixed, **dict.fromkeys(map(str, element)))
        except TypeError as e:
            raise TypeError(f"{where}: {e}") from None


def _refuse_param_mapping(converted: Value, where: str) -> None:
    """Refuses mapping over the run's parameters: the number of instances would depend on them.

    Raises:
        TypeError: When the whole list is a ``dag_ctx.params`` reference.
    """
    if isinstance(converted, RefValue) and isinstance(converted.reference, ParamRef):
        raise TypeError(
            f"{where}: a task can't be mapped over the run's params (the number of instances "
            "can't depend on them); map over a task's output instead"
        )


def _refuse_whole_mapped(
    builder: _build.DagBuilder, arguments: dict[str, Value], where: str
) -> None:
    """Refuses ``.on(...)`` and ``.outcome`` on a mapped task's whole output as an argument.

    Each instance has its own outcome: narrowing the list has no single meaning (filter it and
    lose the alignment with map indexes, or replace the others with ``None``?), so it is only
    allowed element by element, inside ``.expand()``.
    """
    for name, value in arguments.items():
        for reference in value.references():
            if (
                isinstance(reference, DagCtxRef | ParamRef | SecretRef)
                or reference.task not in builder.mapped
            ):
                continue
            if isinstance(reference, OutcomeRef) or reference.on:
                what = "`.outcome`" if isinstance(reference, OutcomeRef) else "`.on(...)`"
                raise TypeError(
                    f"{where}, argument `{name}`: {what} on the mapped task `{reference.task}`'s "
                    "whole output: each instance has its own outcome; narrow element by element "
                    "inside `.expand()`, or take the whole list and filter it in a task"
                )


class TaskDecorator:
    """What ``@task(...)`` with arguments gives: makes a function a task, as ``@task`` does."""

    def __init__(
        self,
        task_id: str | None,
        options: _build.TaskOptions,
        cleanup: Callable[..., object] | None = None,
    ) -> None:
        """Remembers the task's own ID, options and cleanup.

        Args:
            task_id: The task's ID; the function's name when ``None``.
            options: The task's own options.
            cleanup: Its ``cleanup=`` function.
        """
        self._task_id = task_id
        self._options = options
        self._cleanup = cleanup

    @overload
    def __call__[**Q](
        self, fn: Callable[Q, Coroutine[object, object, None]], /
    ) -> AsyncTaskFunction[Q, Done, None]: ...
    @overload
    def __call__[**Q, S](
        self, fn: Callable[Q, Coroutine[object, object, S]], /
    ) -> AsyncTaskFunction[Q, S]: ...
    @overload
    def __call__[**Q](self, fn: Callable[Q, None], /) -> TaskFunction[Q, Done]: ...
    @overload
    def __call__[**Q, S](self, fn: Callable[Q, S], /) -> TaskFunction[Q, S]: ...
    def __call__(self, fn: Callable[..., object], /) -> object:
        """Makes ``fn`` a task (a function returning ``None`` gives ``Done`` when called).

        Args:
            fn: The function.

        Returns:
            The task function.
        """
        name: str = getattr(fn, "__name__", "")
        return _made(fn, self._task_id or name, self._options, cleanup=self._cleanup)


@overload
def task[**P](
    fn: Callable[P, Coroutine[object, object, None]], /
) -> AsyncTaskFunction[P, Done, None]: ...


@overload
def task[**P, R](fn: Callable[P, Coroutine[object, object, R]], /) -> AsyncTaskFunction[P, R]: ...


@overload
def task[**P](fn: Callable[P, None], /) -> TaskFunction[P, Done]: ...


@overload
def task[**P, R](fn: Callable[P, R], /) -> TaskFunction[P, R]: ...


@overload
def task(
    *,
    task_id: str | None = None,
    retries: int | None = None,
    retry_delay: timedelta | None = None,
    timeout: timedelta | None = None,
    queue: str | None = None,
    max_output_size: int | None = None,
    preload: bool | None = None,
    fresh_process: bool | None = None,
    run_if: _outcomes.RunIf | None = None,
    max_active: int | None = None,
    max_active_per_run: int | None = None,
    secrets: SecretsArg = None,
    connections: ConnectionsArg = None,
    cleanup: Callable[..., object] | None = None,
    cleanup_timeout: timedelta | None = None,
    cleanup_retries: int | None = None,
    cleanup_retry_delay: timedelta | None = None,
    cleanup_anywhere: bool | None = None,
    uses: Sequence[object] | None = None,
) -> TaskDecorator: ...


def task(  # noqa: PLR0913 (the task's options, by keyword)
    fn: Callable[..., object] | None = None,
    /,
    *,
    task_id: str | None = None,
    retries: int | None = None,
    retry_delay: timedelta | None = None,
    timeout: timedelta | None = None,
    queue: str | None = None,
    max_output_size: int | None = None,
    preload: bool | None = None,
    fresh_process: bool | None = None,
    run_if: _outcomes.RunIf | None = None,
    max_active: int | None = None,
    max_active_per_run: int | None = None,
    secrets: SecretsArg = None,
    connections: ConnectionsArg = None,
    cleanup: Callable[..., object] | None = None,
    cleanup_timeout: timedelta | None = None,
    cleanup_retries: int | None = None,
    cleanup_retry_delay: timedelta | None = None,
    cleanup_anywhere: bool | None = None,
    uses: Sequence[object] | None = None,
) -> object:
    """Makes a function a Python task: ``@task`` or ``@task(retries=2, ...)``.

    The function must annotate its return type and declare each parameter (no ``*args`` /
    ``**kwargs``). The task ID defaults to the function's name. Type checkers read a call of
    it in a DAG body as the function's value; a function returning ``None`` gives ``Done``.

    Args:
        fn: The function (when used as ``@task`` without parentheses).
        task_id: The task's ID; default: the function's name.
        retries: How many times a failed attempt is retried.
        retry_delay: The delay before each retry.
        timeout: The attempt's timeout.
        queue: The queue the task goes to.
        max_output_size: The output size limit in bytes.
        preload: Fork it from the preloaded fork server, on a worker that has one (default);
            ``False``: only from the bare one, for code that misbehaves next to the preloaded
            libraries.
        fresh_process: Always a new process, never forked, for code that misbehaves after a
            fork.
        run_if: When it runs, from its upstream edges: ``RunIf.NONE_FAILED`` (the default)
            or ``RunIf.ALL_SUCCEEDED``.
        max_active: At most this many of its instances running at once, across all runs
            (``None``: no limit); e.g. 1 for a task holding an exclusive external lock.
        max_active_per_run: At most this many of its instances running at once within one
            run (``None``: no limit), for a mapped task: ``.expand()`` over 1,000 files, 10 at
            a time.
        secrets: The secrets it reads on demand, with ``task_ctx.secret(...)``:
            ``secrets['path']['key']``, ``'path#key'``, ``'path'`` (every key) or
            ``SecretPrefix(...)`` (every secret under it, fetched when read). Secrets in its
            arguments need no declaring.
        connections: The connections it opens (``with mongodb_default() as db:``), each the
            ``@connection`` function or its name: only these open, each rendered before the
            task starts (a missing one or an unreadable secret fails it before it runs).
        cleanup: A function run after each attempt, whatever its end, like ``finally``, in
            the attempt's slot and before its retry or downstream tasks. It takes any of the
            task's parameters, and ``handles: list[A | B]``: the attempt's ``cleanup_handle``
            values. It must be idempotent; a cleanup that keeps failing fails the run.
        cleanup_timeout: How long one try of the cleanup may run (default: the worker's
            ``cleanup_timeout``, 5 minutes).
        cleanup_retries: How many times a failed cleanup is retried (default 3).
        cleanup_retry_delay: The delay before each cleanup retry (default 30 seconds).
        cleanup_anywhere: Whether the cleanup may run on another worker when this one is lost
            (default no: only you know whether it depends on where the task ran).
        uses: The resources it needs without taking their data (decision 259).

    Returns:
        The task function, or a decorator making one.

    Raises:
        TypeError: If the function can't be a task.
    """
    options = _build.TaskOptions(
        retries,
        retry_delay,
        timeout,
        queue,
        max_output_size,
        preload,
        fresh_process,
        run_if,
        max_active=max_active,
        max_active_per_run=max_active_per_run,
        secrets=None
        if secrets is None
        else _secrets.declared(secrets, f"task `{task_id}`" if task_id else "`@task(...)`"),
        connections=declared_connections(
            connections, f"task `{task_id}`" if task_id else "`@task(...)`"
        ),
        cleanup_timeout=cleanup_timeout,
        cleanup_retries=cleanup_retries,
        cleanup_retry_delay=cleanup_retry_delay,
        cleanup_anywhere=cleanup_anywhere,
        uses=None if uses is None else tuple(uses),
    )

    decorator = TaskDecorator(task_id, options, cleanup)
    return decorator if fn is None else decorator(fn)


def chain(*items: object) -> None:
    """Order-only dependencies along a sequence: ``chain(a, [b, c], d)`` is ``a >> [b, c] >> d``.

    Args:
        *items: Tasks, outputs, or lists of them.
    """
    for upstream, downstream in itertools.pairwise(items):
        _build.order(upstream, downstream, "chain()")


type Task = object
"""A task handle, as a ``@branch`` function returns: a task call or an operator.

Type checkers read a task call as its value (``Done``, ``Rows``, ...), so any object is
accepted here; a handle that isn't a direct downstream task fails the branch when it runs.
"""


class Chosen(Protocol):
    """What a call of a ``@branch`` task gives, for type checkers: what it chose, to order.

    ``targets(cfg) >> [to_s3, to_gcs]``: ``>>`` orders it, nothing can be passed.
    """

    def __rshift__(self, other: object, /) -> object:
        """``self >> other``: ``other`` runs after it (its edges, selection edges)."""
        ...

    def __rrshift__(self, other: object, /) -> Chosen:
        """``[a, b] >> self``: it runs after the tasks in the list."""
        ...

    def __lshift__(self, other: object, /) -> object:
        """``self << other``: it runs after ``other``."""
        ...

    def __rlshift__(self, other: object, /) -> Chosen:
        """``[a, b] << self``: the tasks in the list run after it."""
        ...


def on[U](output: object, kind: type[U], /) -> U:
    """``output.on(kind)`` for type checkers, when the class doesn't declare ``.on``.

    ``plain_load(on(fetch(), Plain))`` where ``Plain`` is a plain ``@dataclass`` outcome: it
    runs exactly as ``fetch().on(Plain)``, and type checkers read it as a ``Plain`` (see
    ``schedr.Data`` to make ``.on`` itself typed).

    Args:
        output: A task call (an ``Output`` in a DAG body).
        kind: One class of its outcome union.

    Returns:
        The narrowed reference.

    Raises:
        TypeError: If ``output`` isn't a task's output, or ``kind`` isn't one of its outcomes.
    """
    if not isinstance(output, Output):
        raise TypeError(f"on(): expects a task's output in a DAG body, got {output!r}")
    return output.on(kind)  # type: ignore[return-value]


@overload
def branch[**P](
    fn: Callable[P, Coroutine[object, object, list[Task] | Task]], /
) -> AsyncTaskFunction[P, Chosen, list[Task] | Task]: ...
@overload
def branch[**P](fn: Callable[P, list[Task] | Task], /) -> TaskFunction[P, Chosen]: ...
def branch(fn: Callable[..., object], /) -> object:
    """Makes a function a branch: it chooses which of its direct downstream tasks run.

    The function returns the chosen tasks' handles (one, or a list; ``[]`` for none, never
    ``None``), e.g. ``return [to_s3, to_local]`` with ``targets(cfg) >> [to_s3, to_gcs,
    to_local]``. Its output is their task IDs; each edge out of it is taken only when it chose
    that task.

    Args:
        fn: The function; annotate its return as ``list[Task]`` (or ``Task``).

    Returns:
        The task function.

    Raises:
        TypeError: If the function can't be a task.
    """
    name: str = getattr(fn, "__name__", "")
    # Its call in a DAG body is what it chose, ordered with `>>` (type checkers' view).
    return _made(fn, name, _build.TaskOptions(), branch=True)
