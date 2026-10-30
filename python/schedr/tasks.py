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
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import TYPE_CHECKING, Any, NoReturn, Protocol, Self, overload

from schedr import _build
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
    assignment_problem,
    describe,
    encode,
    is_assignable,
    type_name,
)
from schedr._serialization.annotations import label, type_hints
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
        return Output(self._task, (*self._path, name), self._on)

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
            return self._fn(*args, **kwargs)
        task_id = builder.qualified(self._task_id)
        try:
            bound = self._signature.bind(*args, **kwargs)
        except TypeError as e:
            raise TypeError(f"task `{task_id}`: {e}") from None
        return self._record(builder, dict(bound.arguments), None)  # type: ignore[return-value]

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
            builder.add_task(
                task_id,
                PythonKind(arguments, branch=self._branch),
                output_type,
                self._options,
                spec,
                outcome_types,
                mapped,
            )
        except BaseException:
            del builder.none_checks[checks:]
            raise
        builder.functions[task_id] = self._fn
        for up, on in dict.fromkeys(upstream):
            builder.add_edge(up, task_id, EdgeKind.DATA, on)
        # A reference, which type checkers read as the value (see above).
        return Output[R](task_id)

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
        if isinstance(converted, TemplateValue) and not is_assignable(ScalarType("str"), expected):
            raise TypeError(f"{where}: expects `{type_name(expected)}`, but a t-string gives `str`")
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
        if isinstance(converted, LiteralValue):
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
            if isinstance(reference, DagCtxRef) or reference.task not in builder.mapped:
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

    def __init__(self, task_id: str | None, options: _build.TaskOptions) -> None:
        """Remembers the task's own ID and options.

        Args:
            task_id: The task's ID; the function's name when ``None``.
            options: The task's own options.
        """
        self._task_id = task_id
        self._options = options

    @overload
    def __call__[**Q](self, fn: Callable[Q, None], /) -> TaskFunction[Q, Done]: ...
    @overload
    def __call__[**Q, S](self, fn: Callable[Q, S], /) -> TaskFunction[Q, S]: ...
    def __call__[**Q, S](self, fn: Callable[Q, S], /) -> TaskFunction[Q, S] | TaskFunction[Q, Done]:
        """Makes ``fn`` a task (a function returning ``None`` gives ``Done`` when called).

        Args:
            fn: The function.

        Returns:
            The task function.
        """
        name: str = getattr(fn, "__name__", "")
        return TaskFunction(fn, self._task_id or name, self._options)


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
) -> TaskDecorator: ...


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
) -> TaskFunction[P, R] | TaskFunction[P, Done] | TaskDecorator:
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

    Returns:
        The task function, or a decorator making one.

    Raises:
        TypeError: If the function can't be a task.
    """
    options = _build.TaskOptions(
        retries, retry_delay, timeout, queue, max_output_size, preload, fresh_process, run_if
    )

    decorator = TaskDecorator(task_id, options)
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


def branch[**P](fn: Callable[P, list[Task] | Task], /) -> TaskFunction[P, Chosen]:
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
    function = TaskFunction(fn, name, _build.TaskOptions(), branch=True)
    # Its call in a DAG body is what it chose, ordered with `>>` (type checkers' view).
    return typing.cast("TaskFunction[P, Chosen]", function)
