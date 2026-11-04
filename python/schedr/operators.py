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

from schedr import _build, _secrets
from schedr._model import (
    ArgvCommand,
    CommandKind,
    EdgeKind,
    EmptyKind,
    ScalarType,
    SecretRef,
    ShellCommand,
    Template,
)
from schedr._secrets import SecretsArg
from schedr._templates import is_tstring, shell_text
from schedr.connections import Connection, ConnectionsArg, declared_connections
from schedr.outcomes import DefaultOutcome, Done, Outcome, RunIf, for_command
from schedr.tasks import Output
from schedr.types import ProcessResult

_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class Operator:
    """The base of the operator classes; not for subclassing outside schedr."""

    _task_id: str
    _dag: _build.DagBuilder

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
    def _schedr_dag_id(self) -> str | None:
        """The DAG the task belongs to, checked when a branch returns it (B126)."""
        return self._dag.dag_id if self._dag is not None else None

    @property
    def _schedr_task_id(self) -> str:
        _build.check_same_dag(self._dag, self._task_id)
        return self._task_id

    def _on(self, members: tuple[Outcome, ...]) -> Output[Any]:
        """The task's output narrowed to ``members`` (see ``Output.on``)."""
        return Output[Any](self._task_id, dag=self._dag).on(*members)

    def _outcome(self) -> object:
        """The task's outcome as data (see ``Output.outcome``)."""
        return Output[Any](self._task_id, dag=self._dag).outcome

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


def _connection_env(
    given: Mapping[str, Connection[object] | str],
    env: Mapping[str, Template],
    where: str,
) -> dict[str, str]:
    """``connections={"DB": warehouse}``: each prefix to its connection's name, checked.

    Args:
        given: The prefixes and their connections (or names).
        env: The task's ``env=``, whose names a connection's variables mustn't take.
        where: The task, for messages.

    Returns:
        The connection's name by prefix.

    Raises:
        TypeError: For a prefix that isn't a string, or a value that isn't a connection.
        ValueError: For a prefix that can't start a variable's name, or one taken by ``env=``.
    """
    names: dict[str, str] = {}
    for prefix, connection in given.items():
        if not isinstance(prefix, str):
            raise TypeError(f"{where}: connections=: a prefix is a string, not {prefix!r}")
        if not _ENV_NAME.fullmatch(prefix):
            raise ValueError(
                f"{where}: connections=: `{prefix}`: a prefix is letters, digits and `_`, not "
                "starting with a digit"
            )
        if prefix.upper().startswith("SCHEDR"):
            raise ValueError(
                f"{where}: connections=: `{prefix}`: prefixes starting with SCHEDR are schedr's own"
            )
        taken = sorted(name for name in env if name.startswith(f"{prefix}_"))
        if taken:
            raise ValueError(
                f"{where}: `{taken[0]}` in env= starts with the prefix `{prefix}` of a "
                "connection's variables: rename one"
            )
        # One given: one name back (`None` only when nothing is).
        (names[prefix],) = declared_connections([connection], where) or ("",)
    return names


def _command_line(
    command: object, name: str, where: str, upstream: list[_build.Upstream]
) -> ShellCommand | ArgvCommand:
    """A command line as written: a shell string, or a list of arguments, without secrets.

    Args:
        command: The value given.
        name: The argument it was given in, for messages.
        where: The task, for messages.
        upstream: Takes the outputs it references.

    Returns:
        The command line.

    Raises:
        TypeError: For a value of the wrong type, or a secret in it.
    """
    line: ShellCommand | ArgvCommand
    if isinstance(command, str):
        line = ShellCommand(Template([command]))
    elif is_tstring(command):
        line = ShellCommand(Template([shell_text(command, where)]))  # type: ignore[arg-type]
    elif isinstance(command, Sequence):
        line = ArgvCommand(
            [
                _build.to_template(arg, f"{where}, {name}[{i}]", upstream)
                for i, arg in enumerate(command)
            ]
        )
    else:
        raise TypeError(f"{where}: {name} must be a string, a t-string or a list")
    for template in line.templates():
        for reference in template.references():
            if isinstance(reference, SecretRef):
                raise TypeError(
                    f"{where}: `{reference.label()}` in the {name} line (anyone on the "
                    "machine could read it in the process list): pass it in `env=`, or "
                    "declare it in `secrets=` and read it with `schedr task-ctx secret get`"
                )
    return line


def _cleanup_wanted(
    cleanup: object,
    handles: Sequence[str] | None,
    where: str,
    upstream: list[_build.Upstream],
) -> _build.CleanupWanted | None:
    """A command task's cleanup, from ``cleanup=`` and ``cleanup_handles=``."""
    if cleanup is None:
        if handles is not None:
            raise TypeError(f"{where}: `cleanup_handles` without `cleanup=`")
        return None
    if isinstance(handles, str):
        raise TypeError(f"{where}: cleanup_handles must be a list of type names")
    names = None if handles is None else tuple(handles)
    for name in names or ():
        if not isinstance(name, str) or not name:
            raise TypeError(f"{where}: cleanup_handles: {name!r} isn't a type name")
    return _build.CleanupWanted(_command_line(cleanup, "cleanup", where, upstream), names)


class CommandOperator[E: Outcome = DefaultOutcome](Operator):
    """Runs an inline command.

    A string is run by a shell, and takes no placeholders (a t-string with one is refused):
    the shell would read a value as code. A list is a program and its arguments, run without a
    shell (each element one argument, so nothing needs quoting). List elements and ``env``
    values may be strings, t-strings or references, filled in at run time; a shell command
    reads values from ``env`` (``command='./notify.sh "$URL"', env={"URL": out.report_url}``).
    The output is a ``ProcessResult``; ``.stdout`` and ``.stderr`` reference its parts.

    Args:
        task_id: The task's ID.
        command: A shell string, or a list of arguments.
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
        max_active: At most this many running at once, across all runs (``None``: no limit).
        max_active_per_run: Accepted for symmetry with ``@task``; an operator isn't mapped,
            so it limits nothing (a ``DagHint`` says so).
        secrets: The secrets the command reads on demand, with ``schedr task-ctx secret get``
            (never in its environment): ``secrets['path']['key']``, ``'path#key'``,
            ``'path'`` (every key) or ``SecretPrefix(...)``. A secret it gets in ``env`` needs no
            declaring; one in the command line is refused (the process list shows it).
        connections: The connections it reads with ``schedr task-ctx connection NAME``,
            each the ``@connection`` function or its name; or a dict of prefix to connection
            (``{"DB": warehouse}``), each given as variables (``DB_URL``, ...) and the files
            its tools read, written for the attempt and removed after it.
        cleanup: A command run after each attempt, whatever its end, like ``finally``: the
            same rules as ``command``, the same environment. It reads what the attempt
            registered (``schedr task-ctx cleanup add``) with ``schedr task-ctx cleanup list``.
        cleanup_timeout: How long one try of the cleanup may run (default 5 minutes).
        cleanup_retries: How many times a failed cleanup is retried (default 3).
        cleanup_retry_delay: The delay before each cleanup retry (default 30 seconds).
        cleanup_handles: The handle types the attempt may register; default: any.

    Raises:
        RuntimeError: Outside ``with Dag(...)``.
        TypeError: For a command or environment value of the wrong type.
        ValueError: For an empty argument list or an invalid environment variable name.
    """

    def __init__(  # noqa: PLR0913 (the task's options, by keyword)
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
        max_active: int | None = None,
        max_active_per_run: int | None = None,
        secrets: SecretsArg = None,
        connections: ConnectionsArg | Mapping[str, Connection[object] | str] = None,
        cleanup: str | TString | Sequence[object] | None = None,
        cleanup_timeout: timedelta | None = None,
        cleanup_retries: int | None = None,
        cleanup_retry_delay: timedelta | None = None,
        cleanup_handles: Sequence[str] | None = None,
    ) -> None:
        """Records the command task in the DAG being defined."""
        builder = _build.require_active("CommandOperator")
        task_id = builder.qualified(task_id)
        self._task_id = task_id
        self._dag = builder
        upstream: list[_build.Upstream] = []
        where = f"task `{task_id}`"
        line = _command_line(command, "command", where, upstream)
        wanted = _cleanup_wanted(cleanup, cleanup_handles, where, upstream)
        variables: dict[str, Template] = {}
        for name, value in (env or {}).items():
            if not isinstance(name, str) or not _ENV_NAME.fullmatch(name):
                raise ValueError(f"{where}: `{name}` is not a valid environment variable name")
            variables[name] = _build.to_template(value, f"{where}, env[{name!r}]", upstream)
        cleanup_templates = wanted.line.templates() if wanted and wanted.line else []
        for template in [*line.templates(), *variables.values(), *cleanup_templates]:
            _build.check_template_references(builder, template, where)
        connection_env: dict[str, str] = {}
        if isinstance(connections, Mapping):
            connection_env = _connection_env(connections, variables, where)
            connections = list(dict.fromkeys(connection_env.values()))
        options = _build.TaskOptions(
            retries,
            retry_delay,
            timeout,
            queue,
            max_output_size,
            run_if=run_if,
            max_active=max_active,
            max_active_per_run=max_active_per_run,
            secrets=_secrets.declared(secrets, where),
            connections=declared_connections(connections, where),
            cleanup_timeout=cleanup_timeout,
            cleanup_retries=cleanup_retries,
            cleanup_retry_delay=cleanup_retry_delay,
        )
        _build.hint_unmapped_per_run(options, task_id, stacklevel=2)
        spec = for_command(outcomes, where)
        builder.add_task(
            task_id,
            CommandKind(line, variables, connection_env),
            ScalarType("process_result"),
            options,
            spec,
            cleanup=wanted,
        )
        for up, on in dict.fromkeys(upstream):
            builder.add_edge(up, task_id, EdgeKind.DATA, on)

    @property
    def output(self) -> ProcessResult:
        """The command's output: a reference, read by type checkers as its value."""
        return Output[ProcessResult](self._task_id, dag=self._dag)  # type: ignore[return-value]

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
        return Output[str](self._task_id, ("stdout",), dag=self._dag)  # type: ignore[return-value]

    @property
    def stderr(self) -> str:
        """The command's standard error: a reference, read by type checkers as its value."""
        return Output[str](self._task_id, ("stderr",), dag=self._dag)  # type: ignore[return-value]


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
        self._dag = builder
        builder.add_task(
            task_id, EmptyKind(), ScalarType("none"), _build.TaskOptions(run_if=run_if)
        )

    @property
    def output(self) -> Done:
        """Its output (``None``): a reference, to order with ``>>``."""
        return Output[None](self._task_id, dag=self._dag)

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
