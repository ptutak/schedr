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
from collections.abc import Callable
from datetime import timedelta
from typing import Any, Protocol, overload

from schedr import _build
from schedr import outcomes as _outcomes
from schedr._model import (
    EdgeKind,
    LiteralValue,
    OutcomeRef,
    OutputRef,
    PythonKind,
    Reference,
    RefValue,
    ScalarType,
    TemplateValue,
    Value,
)
from schedr._serialization import (
    CodecError,
    TypeModelError,
    describe,
    encode,
    is_assignable,
    type_name,
)
from schedr._serialization.annotations import label, type_hints

_VALUE_IN_DAG_BODY = (
    "an output can't be used as a value in the DAG file (it exists only at run time); "
    "pass it to a task, or decide in a task"
)


class Output[T]:
    """The output of a task, or a part of it, as a reference in the DAG body.

    ``out.rows`` (a field), ``out["key"]`` (a key) and ``out[0]`` (an index) reference parts;
    the master extracts them when it builds the downstream task's inputs.
    """

    __slots__ = ("_on", "_path", "_task")

    def __init__(
        self, task_id: str, path: tuple[str | int, ...] = (), on: tuple[str, ...] = ()
    ) -> None:
        """References ``task_id``'s output at ``path``.

        Args:
            task_id: The task that produces the output.
            path: Fields, keys and indexes into it.
            on: The outcomes it is narrowed to (``.on(...)``); empty for any.
        """
        self._task = task_id
        self._path = path
        self._on = on

    @property
    def _schedr_task_id(self) -> str:
        return self._task

    @property
    def _schedr_on(self) -> tuple[str, ...]:
        return self._on

    def _schedr_reference(self) -> Reference:
        return OutputRef(self._task, self._path, self._on)

    @overload
    def on[U](self, kind: type[U], /) -> Output[U]: ...

    @overload
    def on[U, V](self, kind: type[U], other: type[V], /) -> Output[U | V]: ...

    @overload
    def on(self, kind: None, /) -> Output[None]: ...

    @overload
    def on(self, *members: _outcomes.Outcome) -> Output[T]: ...

    def on(self, *kinds: Any) -> Output[Any]:
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
        task = builder.tasks.get(self._task)
        if task is not None and isinstance(task.kind, PythonKind) and task.kind.branch:
            raise TypeError(
                f"`{self._task}.on()`: a @branch task's edges are taken when it chose their "
                "task, not on outcomes; return an outcome from a @task to route with .on()"
            )
        spec = builder.outcomes.get(self._task, _outcomes.DEFAULT)
        return Output(self._task, on=_outcomes.names_for(spec, kinds, self._task))

    @property
    def outcome(self) -> _OutcomeOutput:
        """The task's outcome itself as data: pass it to a task taking its ``Outcome`` enum.

        Raises:
            TypeError: On a task whose outcomes are a union of classes (the value's class
                already says which), or on a part of the output.
        """
        if self._path or self._on:
            raise TypeError(f"{self!r}.outcome: take it from the whole output")
        builder = _build.require_active("`.outcome`")
        if builder.outcomes.get(self._task, _outcomes.DEFAULT).is_union:
            raise TypeError(
                f"`{self._task}.outcome`: its outcomes are a union of classes; the value's "
                f"class says which, so pass `{self._task}` (or narrow with `.on(...)`)"
            )
        if _build.may_have_field(builder.tasks[self._task].output_type, "outcome"):
            raise TypeError(
                f"`{self._task}.outcome` is the task's outcome, but its output may also have "
                f'a field `outcome`: write `{self._task}["outcome"]` for the field'
            )
        return _OutcomeOutput(self._task)

    def __getattr__(self, name: str) -> Output[Any]:
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
        return Output(self._task, (*self._path, name), self._on)

    def __getitem__(self, key: str | int) -> Output[Any]:
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
        return Output(self._task, (*self._path, key), self._on)

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

    def __iter__(self) -> Any:
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

    def __repr__(self) -> str:
        """A readable form, e.g. ``Output(extract.rows[0])``."""
        on = f".on({', '.join(self._on)})" if self._on else ""
        path = "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in self._path)
        return f"Output({self._task}{on}{path})"


class _OutcomeOutput:
    """A task's outcome as data (``task.outcome``), a reference in the DAG body."""

    __slots__ = ("_task",)

    def __init__(self, task_id: str) -> None:
        self._task = task_id

    @property
    def _schedr_task_id(self) -> str:
        return self._task

    def _schedr_reference(self) -> Reference:
        return OutcomeRef(self._task)

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
    if inspect.iscoroutinefunction(fn):
        raise TypeError(
            f"@task `{fn.__name__}`: async tasks come with milestone M4; to wait for something, "
            "use a sensor (@sensor, milestone M4)"
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
    return signature


class TaskFunction[**P, R]:
    """A function decorated with ``@task``.

    Calling it inside ``with Dag(...)`` records a task and returns its ``Output``; outside, it
    calls the function.
    """

    def __init__(
        self, fn: Callable[P, R], task_id: str, options: _build.TaskOptions, branch: bool = False
    ) -> None:
        """Wraps ``fn`` as a task.

        Args:
            fn: The function.
            task_id: The task's ID.
            options: The task's own options.
            branch: A ``@branch`` task (its output: the downstream tasks it chose).

        Raises:
            TypeError: If ``fn`` can't be a task (see ``@task``).
        """
        self._signature = _check_function(fn)
        self._fn = fn
        self._task_id = task_id
        self._options = options
        self._branch = branch
        functools.update_wrapper(self, fn)

    @property
    def task_id(self) -> str:
        """The ID of the task a call creates."""
        return self._task_id

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Output[R]:
        """Records the task (inside a DAG) or calls the function (outside).

        Returns:
            The task's output, a reference; outside a DAG, the function's return value.

        Raises:
            TypeError: For arguments that don't match the function's parameters, or values
                that can't be stored.
        """
        builder = _build.active()
        if builder is None:
            return self._fn(*args, **kwargs)  # type: ignore[return-value]
        where = f"task `{self._task_id}`"
        try:
            bound = self._signature.bind(*args, **kwargs)
        except TypeError as e:
            raise TypeError(f"{where}: {e}") from None
        hints = type_hints(self._fn, where)
        # A branch's output is the IDs of the tasks it chose; its outcome SUCCESS.
        spec = (
            _outcomes.DEFAULT
            if self._branch
            else _outcomes.from_return_type(hints["return"], where)
        )
        try:
            # An Outcome enum task's output is None: the member is its outcome.
            output_type = describe(
                list[str]
                if self._branch
                else None
                if spec.enum is not _outcomes.DefaultOutcome
                else hints["return"]
            )
            outcome_types = (
                {name: describe(cls) for name, cls in zip(spec.names, spec.classes, strict=True)}
                if spec.is_union
                else {}
            )
        except TypeModelError as e:
            raise TypeModelError(f"{where}: return type `{label(hints['return'])}`: {e}") from None
        upstream: list[_build.Upstream] = []
        arguments = {
            name: self._argument(
                builder,
                (self._task_id, value),
                hints.get(name, Any),
                f"{where}, argument `{name}`",
                upstream,
            )
            for name, value in bound.arguments.items()
        }
        builder.add_task(
            self._task_id,
            PythonKind(arguments, branch=self._branch),
            output_type,
            self._options,
            spec,
            outcome_types,
        )
        builder.functions[self._task_id] = self._fn
        for task_id, on in dict.fromkeys(upstream):
            builder.add_edge(task_id, self._task_id, EdgeKind.DATA, on)
        return Output(self._task_id)

    @staticmethod
    def _argument(
        builder: _build.DagBuilder,
        task_value: tuple[str, object],
        annotation: object,
        where: str,
        upstream: list[_build.Upstream],
    ) -> Value:
        """Converts one argument of a task, checking it against its parameter's type.

        A reference is also queued for the ``None`` check, made once the DAG is complete
        (see ``DagBuilder.check_nones``).
        """
        task_id, value = task_value
        try:
            expected = describe(annotation)
        except TypeModelError as e:
            raise TypeModelError(f"{where}: parameter type `{label(annotation)}`: {e}") from None
        converted = _build.to_value(value, where, upstream)
        for reference in converted.references():
            _build.reference_type(builder, reference, where)
        for template in _build.templates_in(converted):
            _build.check_template_references(builder, template, where)
        if isinstance(converted, TemplateValue) and not is_assignable(ScalarType("str"), expected):
            raise TypeError(f"{where}: expects `{type_name(expected)}`, but a t-string gives `str`")
        # Whether a reference may get None is known once the DAG is complete.
        builder.none_checks.extend(
            (task_id, where, reference, at)
            for reference, at in _build.typed_references(converted, expected)
        )
        if isinstance(converted, RefValue):
            produced = _build.reference_type(builder, converted.reference, where)
            if not is_assignable(produced, expected):
                raise TypeError(
                    f"{where}: expects `{type_name(expected)}`, but "
                    f"`{_build.reference_label(converted.reference)}` is `{type_name(produced)}`"
                )
        elif isinstance(converted, LiteralValue):
            try:
                converted = LiteralValue(encode(value, annotation))
            except CodecError as e:
                raise TypeError(f"{where}: {e.reason}") from None
        return converted

    def override(
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
    ) -> TaskFunction[P, R]:
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

        Returns:
            A new task function; this one is unchanged.
        """
        changes = _build.TaskOptions(
            retries, retry_delay, timeout, queue, max_output_size, preload, fresh_process, run_if
        )
        return TaskFunction(
            self._fn, task_id or self._task_id, changes.over(self._options), self._branch
        )


@overload
def task[**P, R](fn: Callable[P, R], /) -> TaskFunction[P, R]: ...


@overload
def task[**P, R](
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
) -> Callable[[Callable[P, R]], TaskFunction[P, R]]: ...


def task[**P, R](
    fn: Callable[P, R] | None = None,
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
) -> TaskFunction[P, R] | Callable[[Callable[P, R]], TaskFunction[P, R]]:
    """Makes a function a Python task: ``@task`` or ``@task(retries=2, ...)``.

    The function must annotate its return type and declare each parameter (no ``*args`` /
    ``**kwargs``). The task ID defaults to the function's name.

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

    Returns:
        The task function, or a decorator making one.

    Raises:
        TypeError: If the function can't be a task.
    """
    options = _build.TaskOptions(
        retries, retry_delay, timeout, queue, max_output_size, preload, fresh_process, run_if
    )

    def decorate(function: Callable[P, R]) -> TaskFunction[P, R]:
        name: str = getattr(function, "__name__", "")
        return TaskFunction(function, task_id or name, options)

    return decorate if fn is None else decorate(fn)


def chain(*items: object) -> None:
    """Order-only dependencies along a sequence: ``chain(a, [b, c], d)`` is ``a >> [b, c] >> d``.

    Args:
        *items: Tasks, outputs, or lists of them.
    """
    for upstream, downstream in itertools.pairwise(items):
        _build.order(upstream, downstream, "chain()")


class Task(Protocol):
    """A task handle, as a ``@branch`` function returns: an output or an operator."""

    @property
    def _schedr_task_id(self) -> str: ...


def branch[**P](fn: Callable[P, Any], /) -> TaskFunction[P, list[str]]:
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
    return TaskFunction(fn, name, _build.TaskOptions(), branch=True)
