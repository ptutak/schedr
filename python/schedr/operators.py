"""Operators: the task kinds other than Python functions.

``CommandOperator`` runs an inline command; ``EmptyOperator`` does nothing and is completed
by the master. Operators can't be subclassed: reuse is by composition, e.g. a function that
creates tasks (see PROJECT.md, Native API: no custom operators).
"""

import re
from collections.abc import Mapping, Sequence
from datetime import timedelta
from string.templatelib import Template as TString
from typing import Any, Self

from schedr import _build
from schedr._model import (
    ArgvCommand,
    CommandKind,
    EdgeKind,
    EmptyKind,
    ScalarType,
    ShellCommand,
    Template,
)
from schedr._templates import is_tstring, to_template
from schedr.outcomes import DefaultOutcome, Done, Outcome, RunIf, for_command
from schedr.tasks import Output
from schedr.types import ProcessResult

_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class Operator:
    """The base of the operator classes; not for subclassing outside schedr."""

    _task_id: str

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Refuses subclasses outside schedr.

        Raises:
            TypeError: For a subclass defined outside the ``schedr`` package.
        """
        if not cls.__module__.startswith("schedr."):
            raise TypeError(
                f"custom operators are not supported (`{cls.__name__}`): create tasks in a "
                "function instead, or share a @task function"
            )
        super().__init_subclass__(**kwargs)

    @property
    def task_id(self) -> str:
        """The task's ID."""
        return self._task_id

    @property
    def _schedr_task_id(self) -> str:
        return self._task_id

    def _on(self, members: tuple[Outcome, ...]) -> Output[Any]:
        """The task's output narrowed to ``members`` (see ``Output.on``)."""
        return Output[Any](self._task_id).on(*members)

    def _outcome(self) -> object:
        """The task's outcome as data (see ``Output.outcome``)."""
        return Output[Any](self._task_id).outcome

    def __rshift__(self, other: object) -> object:
        """``self >> other``: ``other`` runs after this task.

        Returns:
            ``other``, so dependencies can be chained.
        """
        _build.order(self, other, ">>")
        return other

    def __rrshift__(self, other: object) -> Self:
        """``[a, b] >> self``: this task runs after the tasks in the list.

        Returns:
            This task.
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

    def __rlshift__(self, other: object) -> Self:
        """``[a, b] << self``: the tasks in the list run after this task.

        Returns:
            This task.
        """
        _build.order(self, other, "<<")
        return self

    def __repr__(self) -> str:
        """A readable form, e.g. ``CommandOperator('notify')``."""
        return f"{type(self).__name__}({self._task_id!r})"


class CommandOperator[E: Outcome = DefaultOutcome](Operator):
    """Runs an inline command.

    A string or a t-string is run by a shell; in a t-string every interpolated value is
    shell-quoted unless its format spec is ``:raw`` (``t"./run.sh {flags:raw}"``). A list is a
    program and its arguments, run without a shell (each element one argument, so nothing needs
    quoting). List elements and ``env`` values may be strings, t-strings or references, filled
    in at run time. The output is a ``ProcessResult``; ``.stdout`` and ``.stderr`` reference its
    parts.

    Args:
        task_id: The task's ID.
        command: A shell string or t-string, or a list of arguments.
        env: Extra environment variables for the process.
        retries: How many times a failed attempt is retried.
        retry_delay: The delay before each retry.
        timeout: The attempt's timeout.
        queue: The queue the task goes to.
        max_output_size: The output size limit in bytes (per stream).
        outcomes: An ``Outcome`` enum: its members' values are the exit codes meaning them
            (each a success with that outcome, any other code a failure). Default: ``SUCCESS``
            on 0. Type checkers then read the operator as ``CommandOperator[Check]``: ``.on``
            takes that enum's members and ``.outcome`` is one.
        run_if: When it runs, from its upstream edges (default ``RunIf.NONE_FAILED``).

    Raises:
        RuntimeError: Outside ``with Dag(...)``.
        TypeError: For a command or environment value of the wrong type.
        ValueError: For an empty argument list or an invalid environment variable name.
    """

    def __init__(
        self,
        task_id: str,
        command: str | TString | Sequence[object],
        *,
        env: Mapping[str, object] | None = None,
        retries: int | None = None,
        retry_delay: timedelta | None = None,
        timeout: timedelta | None = None,
        queue: str | None = None,
        max_output_size: int | None = None,
        outcomes: type[E] | None = None,
        run_if: RunIf | None = None,
    ) -> None:
        """Records the command task in the DAG being defined."""
        builder = _build.require_active("CommandOperator")
        task_id = builder.qualified(task_id)
        self._task_id = task_id
        upstream: list[_build.Upstream] = []
        where = f"task `{task_id}`"
        line: ShellCommand | ArgvCommand
        if isinstance(command, str):
            line = ShellCommand(Template([command]))
        elif is_tstring(command):
            line = ShellCommand(to_template(command, where, shell=True))  # type: ignore[arg-type]
            for ref in line.template.references():
                _build.note_upstream(ref, upstream)
        elif isinstance(command, Sequence):
            line = ArgvCommand(
                [
                    _build.to_template(arg, f"{where}, command[{i}]", upstream)
                    for i, arg in enumerate(command)
                ]
            )
        else:
            raise TypeError(f"{where}: command must be a string, a t-string or a list")
        variables: dict[str, Template] = {}
        for name, value in (env or {}).items():
            if not isinstance(name, str) or not _ENV_NAME.fullmatch(name):
                raise ValueError(f"{where}: `{name}` is not a valid environment variable name")
            variables[name] = _build.to_template(value, f"{where}, env[{name!r}]", upstream)
        for template in [*line.templates(), *variables.values()]:
            _build.check_template_references(builder, template, where)
        options = _build.TaskOptions(
            retries, retry_delay, timeout, queue, max_output_size, run_if=run_if
        )
        spec = for_command(outcomes, where)
        builder.add_task(
            task_id, CommandKind(line, variables), ScalarType("process_result"), options, spec
        )
        for up, on in dict.fromkeys(upstream):
            builder.add_edge(up, task_id, EdgeKind.DATA, on)

    @property
    def output(self) -> ProcessResult:
        """The command's output: a reference, read by type checkers as its value."""
        return Output[ProcessResult](self._task_id)  # type: ignore[return-value]

    def on(self, *members: E) -> ProcessResult:
        """The output, depended on only for these outcomes (see ``Output.on``).

        Args:
            *members: Members of its ``outcomes=`` enum (``DefaultOutcome.SUCCESS`` without).

        Returns:
            The narrowed reference, e.g. ``probe.on(Check.CHANGED) >> notify``.
        """
        return self._on(members)  # type: ignore[return-value]

    @property
    def outcome(self) -> E:
        """The command's outcome as data, for a task taking its ``Outcome`` enum."""
        return self._outcome()  # type: ignore[return-value]

    @property
    def stdout(self) -> str:
        """The command's standard output: a reference, read by type checkers as its value."""
        return Output[str](self._task_id, ("stdout",))  # type: ignore[return-value]

    @property
    def stderr(self) -> str:
        """The command's standard error: a reference, read by type checkers as its value."""
        return Output[str](self._task_id, ("stderr",))  # type: ignore[return-value]


class EmptyOperator(Operator):
    """A task that does nothing; the master completes it without a worker.

    Args:
        task_id: The task's ID.
        run_if: When it completes, from its upstream edges (default ``RunIf.NONE_FAILED``);
            a join of branches that must all succeed takes ``RunIf.ALL_SUCCEEDED``.

    Raises:
        RuntimeError: Outside ``with Dag(...)``.
    """

    def __init__(self, task_id: str, *, run_if: RunIf | None = None) -> None:
        """Records the empty task in the DAG being defined."""
        builder = _build.require_active("EmptyOperator")
        task_id = builder.qualified(task_id)
        self._task_id = task_id
        builder.add_task(
            task_id, EmptyKind(), ScalarType("none"), _build.TaskOptions(run_if=run_if)
        )

    @property
    def output(self) -> Done:
        """Its output (``None``): a reference, to order with ``>>``."""
        return Output[None](self._task_id)

    def on(self, *members: DefaultOutcome) -> Done:
        """Depends on it only for these outcomes (it has only ``SUCCESS``; see ``Output.on``).

        Args:
            *members: ``DefaultOutcome.SUCCESS``.

        Returns:
            The narrowed reference, to order with ``>>``.
        """
        return self._on(members)

    @property
    def outcome(self) -> DefaultOutcome:
        """Its outcome as data (always ``SUCCESS``), for a task taking ``DefaultOutcome``."""
        return self._outcome()  # type: ignore[return-value]
