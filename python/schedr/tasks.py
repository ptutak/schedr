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
from typing import Any, overload

from schedr import _build
from schedr._model import (
    EdgeKind,
    LiteralValue,
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

    __slots__ = ("_path", "_task")

    def __init__(self, task_id: str, path: tuple[str | int, ...] = ()) -> None:
        """References ``task_id``'s output at ``path``.

        Args:
            task_id: The task that produces the output.
            path: Fields, keys and indexes into it.
        """
        self._task = task_id
        self._path = path

    @property
    def _schedr_task_id(self) -> str:
        return self._task

    def _schedr_reference(self) -> Reference:
        return OutputRef(self._task, self._path)

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
        return Output(self._task, (*self._path, name))

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
        return Output(self._task, (*self._path, key))

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
        path = "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in self._path)
        return f"Output({self._task}{path})"


_STARS: dict[inspect._ParameterKind, str] = {
    inspect.Parameter.VAR_POSITIONAL: "*",
    inspect.Parameter.VAR_KEYWORD: "**",
}


def _check_function(fn: Callable[..., Any]) -> inspect.Signature:
    """Checks that ``fn`` can be a task and returns its signature."""
    if not inspect.isfunction(fn):
        raise TypeError(f"@task needs a function, got {type(fn).__name__}")
    if inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn):
        raise TypeError(f"@task `{fn.__name__}`: async functions aren't supported")
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

    def __init__(self, fn: Callable[P, R], task_id: str, options: _build.TaskOptions) -> None:
        """Wraps ``fn`` as a task.

        Args:
            fn: The function.
            task_id: The task's ID.
            options: The task's own options.

        Raises:
            TypeError: If ``fn`` can't be a task (see ``@task``).
        """
        self._signature = _check_function(fn)
        self._fn = fn
        self._task_id = task_id
        self._options = options
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
        try:
            output_type = describe(hints["return"])
        except TypeModelError as e:
            raise TypeModelError(f"{where}: return type `{label(hints['return'])}`: {e}") from None
        upstream: list[str] = []
        arguments = {
            name: self._argument(
                builder, value, hints.get(name, Any), f"{where}, argument `{name}`", upstream
            )
            for name, value in bound.arguments.items()
        }
        builder.add_task(self._task_id, PythonKind(arguments), output_type, self._options)
        builder.functions[self._task_id] = self._fn
        for task_id in dict.fromkeys(upstream):
            builder.add_edge(task_id, self._task_id, EdgeKind.DATA)
        return Output(self._task_id)

    @staticmethod
    def _argument(
        builder: _build.DagBuilder,
        value: object,
        annotation: object,
        where: str,
        upstream: list[str],
    ) -> Value:
        """Converts one argument, checking it against its parameter's type."""
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
    ) -> TaskFunction[P, R]:
        """The same function as another task, e.g. ``load.override(task_id="load_eu")(...)``.

        Args:
            task_id: A different task ID (needed to use the function twice in one DAG).
            retries: Overrides the retries.
            retry_delay: Overrides the retry delay.
            timeout: Overrides the timeout.
            queue: Overrides the queue.
            max_output_size: Overrides the output size limit.

        Returns:
            A new task function; this one is unchanged.
        """
        changes = _build.TaskOptions(retries, retry_delay, timeout, queue, max_output_size)
        return TaskFunction(self._fn, task_id or self._task_id, changes.over(self._options))


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

    Returns:
        The task function, or a decorator making one.

    Raises:
        TypeError: If the function can't be a task.
    """
    options = _build.TaskOptions(retries, retry_delay, timeout, queue, max_output_size)

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
