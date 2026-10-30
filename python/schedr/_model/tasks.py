"""Tasks in DAG JSON (``schedr-model``, ``task.rs``): kind, what they run, resolved settings."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from schedr._model.ids import validate_task_id
from schedr._model.types import TypeDesc
from schedr._model.values import Reference, Template, Value

DEFAULT_QUEUE = "default"
"""The queue a task goes to when it names none."""

DEFAULT_MAX_OUTPUT_SIZE = 1024 * 1024
"""The default output size limit in bytes (1 MiB)."""

DEFAULT_RETRY_DELAY_MS = 5 * 60 * 1000
"""The default delay between attempts in milliseconds (5 minutes)."""


DEFAULT_OUTCOMES: tuple[tuple[str, int | None], ...] = (("SUCCESS", 0),)
"""A task's outcomes when it declares none: ``SUCCESS``, exit code 0 for a command."""


@dataclass(frozen=True, slots=True)
class TaskSettings:
    """A task's settings, resolved from its own arguments and the defaults that apply.

    Attributes:
        retries: How many times a failed attempt is retried.
        retry_delay_ms: The delay before each retry.
        timeout_ms: The attempt's timeout; ``None`` for none.
        queue: The queue the task goes to.
        max_output_size: The output size limit in bytes.
        preload: A Python task forks from the preloaded fork server (``False``: only from the
            bare one).
        fresh_process: A Python task is always a new process, never forked.
        run_if: Its trigger rule, ``none_failed`` or ``all_succeeded``.
    """

    retries: int = 0
    retry_delay_ms: int = DEFAULT_RETRY_DELAY_MS
    timeout_ms: int | None = None
    queue: str = DEFAULT_QUEUE
    max_output_size: int = DEFAULT_MAX_OUTPUT_SIZE
    preload: bool = True
    fresh_process: bool = False
    run_if: str = "none_failed"

    def __post_init__(self) -> None:
        """Checks that the numbers are in range.

        Raises:
            ValueError: For a negative count, delay or size, a non-positive timeout, or an
                empty queue name.
        """
        if self.retries < 0 or self.retry_delay_ms < 0 or self.max_output_size < 0:
            raise ValueError("retries, retry_delay and max_output_size must not be negative")
        if self.timeout_ms is not None and self.timeout_ms <= 0:
            raise ValueError(f"timeout must be positive, got {self.timeout_ms} ms")
        if not self.queue:
            raise ValueError("queue must not be empty")
        if self.run_if not in {"none_failed", "all_succeeded"}:
            raise ValueError(f"run_if must be none_failed or all_succeeded, got {self.run_if!r}")

    def to_json(self) -> dict[str, Any]:
        """The settings as DAG JSON.

        Returns:
            Every setting, ``timeout_ms`` as ``null`` when unset; ``run_if`` only when not
            the default.
        """
        result: dict[str, Any] = {
            "retries": self.retries,
            "retry_delay_ms": self.retry_delay_ms,
            "timeout_ms": self.timeout_ms,
            "queue": self.queue,
            "max_output_size": self.max_output_size,
            "preload": self.preload,
            "fresh_process": self.fresh_process,
        }
        if self.run_if != "none_failed":
            result["run_if"] = self.run_if
        return result


@dataclass(frozen=True, slots=True)
class ShellCommand:
    """A string run by a shell; interpolated values are shell-quoted unless ``raw``.

    Attributes:
        template: The command.
    """

    template: Template

    def templates(self) -> list[Template]:
        """The command line's templates.

        Returns:
            A one-element list.
        """
        return [self.template]

    def to_json(self) -> dict[str, Any]:
        """The command line as DAG JSON.

        Returns:
            ``{"shell": [...]}``.
        """
        return {"shell": self.template.to_json()}


@dataclass(frozen=True, slots=True)
class ArgvCommand:
    """A program and its arguments, run without a shell.

    Attributes:
        args: One template per argument; the first is the program.
    """

    args: Sequence[Template]

    def __post_init__(self) -> None:
        """Checks that there is a program.

        Raises:
            ValueError: If ``args`` is empty.
        """
        if not self.args:
            raise ValueError("an argument-list command needs at least the program")

    def templates(self) -> list[Template]:
        """The command line's templates.

        Returns:
            The arguments, in order.
        """
        return list(self.args)

    def to_json(self) -> dict[str, Any]:
        """The command line as DAG JSON.

        Returns:
            ``{"argv": [[...], ...]}``.
        """
        return {"argv": [a.to_json() for a in self.args]}


type CommandLine = ShellCommand | ArgvCommand
"""Any command line."""


@dataclass(frozen=True, slots=True)
class PythonKind:
    """A ``@task`` function, found by task ID when the runner imports the DAG file.

    Attributes:
        arguments: The arguments by parameter name.
        branch: A ``@branch`` task: its output is the downstream task IDs it chose.
    """

    arguments: Mapping[str, Value] = field(default_factory=dict)
    branch: bool = False

    def references(self) -> list[Reference]:
        """The arguments' references.

        Returns:
            Every reference, by parameter name order (as stored).
        """
        return [r for name in sorted(self.arguments) for r in self.arguments[name].references()]

    def to_json(self) -> dict[str, Any]:
        """The kind's fields as DAG JSON.

        Returns:
            ``{"kind": "python", "arguments": {...}}`` with names sorted, and ``"branch": true``
            for a ``@branch`` task.
        """
        arguments = {name: self.arguments[name].to_json() for name in sorted(self.arguments)}
        result: dict[str, Any] = {"kind": "python", "arguments": arguments}
        if self.branch:
            result["branch"] = True
        return result


@dataclass(frozen=True, slots=True)
class CommandKind:
    """An inline command, run by the agent.

    Attributes:
        command: The command line.
        env: Extra environment variables for the process.
    """

    command: CommandLine
    env: Mapping[str, Template] = field(default_factory=dict)

    def references(self) -> list[Reference]:
        """The command line's and the environment's references.

        Returns:
            Command line references first, then environment ones by variable name.
        """
        templates = [*self.command.templates(), *(self.env[k] for k in sorted(self.env))]
        return [r for t in templates for r in t.references()]

    def to_json(self) -> dict[str, Any]:
        """The kind's fields as DAG JSON.

        Returns:
            ``{"kind": "command", "command": ..., "env": {...}}`` (``env`` only if not empty).
        """
        result: dict[str, Any] = {"kind": "command", "command": self.command.to_json()}
        if self.env:
            result["env"] = {key: self.env[key].to_json() for key in sorted(self.env)}
        return result


@dataclass(frozen=True, slots=True)
class EmptyKind:
    """An ``EmptyOperator``: completed by the master, no process."""

    def references(self) -> list[Reference]:
        """An empty task has no references.

        Returns:
            An empty list.
        """
        return []

    def to_json(self) -> dict[str, Any]:
        """The kind as DAG JSON.

        Returns:
            ``{"kind": "empty"}``.
        """
        return {"kind": "empty"}


type TaskKind = PythonKind | CommandKind | EmptyKind
"""Any task kind."""


@dataclass(frozen=True, slots=True)
class ExpandProduct:
    """``.expand(name=values, ...)``: one instance per element of their cartesian product.

    Attributes:
        over: Each mapped argument's list (a literal, or a reference to a list), in the order
            written: the first varies slowest (decision 79).
    """

    over: Mapping[str, Value]

    def references(self) -> list[Reference]:
        """The references in what it maps over.

        Returns:
            They, in argument order.
        """
        return [r for value in self.over.values() for r in value.references()]

    def to_json(self) -> dict[str, Any]:
        """As DAG JSON.

        Returns:
            ``{"product": [{"name": name, "value": value}, ...]}``, in the order written.
        """
        return {
            "product": [
                {"name": name, "value": value.to_json()} for name, value in self.over.items()
            ]
        }


@dataclass(frozen=True, slots=True)
class ExpandKwargs:
    """``.expand_kwargs(values)``: one instance per dict of the list, its arguments.

    Attributes:
        over: The list of dicts (a literal, or a reference to one).
    """

    over: Value

    def references(self) -> list[Reference]:
        """The references in what it maps over.

        Returns:
            They, in order.
        """
        return self.over.references()

    def to_json(self) -> dict[str, Any]:
        """As DAG JSON.

        Returns:
            ``{"kwargs": value}``.
        """
        return {"kwargs": self.over.to_json()}


type Expand = ExpandProduct | ExpandKwargs
"""What a mapped task's instances vary over (decision 69)."""


@dataclass(frozen=True, slots=True)
class Task:
    """A task of a DAG.

    Attributes:
        id: The task's ID, unique within its DAG.
        kind: Its kind and what it runs.
        output_type: The type of its output.
        settings: Its resolved settings.
        outcomes: Its outcomes, each a name and, for a command, the exit code meaning it.
        group: The group it belongs to (the innermost), if any.
        expand: What its instances vary over, for a mapped task.
    """

    id: str
    kind: TaskKind
    output_type: TypeDesc
    settings: TaskSettings = field(default_factory=TaskSettings)
    outcomes: tuple[tuple[str, int | None], ...] = DEFAULT_OUTCOMES
    group: str | None = None
    expand: Expand | None = None

    def __post_init__(self) -> None:
        """Validates the task ID.

        Raises:
            IdError: If it is invalid.
        """
        validate_task_id(self.id)

    def references(self) -> list[Reference]:
        """Every reference the task uses.

        Returns:
            The kind's references, then those of what it is mapped over.
        """
        mapped = self.expand.references() if self.expand is not None else []
        return [*self.kind.references(), *mapped]

    def to_json(self) -> dict[str, Any]:
        """The task as DAG JSON (the kind's fields are flattened into it).

        Returns:
            ``{"id": ..., "kind": ..., ..., "settings": ..., "output_type": ...}``, with
            ``"outcomes": [...]`` when they aren't just ``SUCCESS`` on exit 0 and ``"group"``
            when it is in one.
        """
        result: dict[str, Any] = {
            "id": self.id,
            **self.kind.to_json(),
            "settings": self.settings.to_json(),
            "output_type": self.output_type.to_json(),
        }
        if self.outcomes != DEFAULT_OUTCOMES:
            result["outcomes"] = [
                {"name": name} if code is None else {"name": name, "exit_code": code}
                for name, code in self.outcomes
            ]
        if self.group is not None:
            result["group"] = self.group
        if self.expand is not None:
            result["expand"] = self.expand.to_json()
        return result
