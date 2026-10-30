"""Testing DAGs: task functions with a run context, and whole runs inspected task by task.

Outside ``with Dag(...)`` a ``@task`` function is the plain function: calling it runs it.
``task_context(...)`` gives ``dag_ctx`` values to such calls, with no processes.
``run_dag(...)`` runs ``schedr dags test`` (one real run, every task its own process) and
returns its result, every task inspectable on its own (see PROJECT.md, Testing DAGs).
Run parameters, secrets, handles, ``step=`` and ``replace=`` come with milestone M4.
"""

import contextlib
import json
import os
import shutil
import subprocess
import sys
import sysconfig
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from schedr._model import CommandKind, PythonKind, registry
from schedr._serialization import decode, decode_any
from schedr._serialization.annotations import type_hints
from schedr.context import RunValues, TaskValues, running
from schedr.runner.parse import extend_sys_path, import_dag_entries
from schedr.types import Json, ProcessResult

RUN_KINDS = frozenset({"scheduled", "manual", "backfill", "chained", "event"})
"""The run kinds ``task_context(kind=...)`` accepts."""

BINARY_ENV = "SCHEDR_BIN"
"""The environment variable naming the ``schedr`` binary ``run_dag`` starts."""


class RunDagError(Exception):
    """A run that couldn't be made or read: no binary, or ``schedr dags test`` failed early."""


def _milestone_m4(what: str, feature: str) -> NotImplementedError:
    return NotImplementedError(f"`{what}` comes with {feature} (milestone M4)")


def _aware(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime, got {type(value).__name__}")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must have a time zone (e.g. tzinfo=UTC), got {value}")
    return value


@contextlib.contextmanager
def task_context(
    *,
    planned_for: datetime | None = None,
    since: datetime | None = None,
    dag_id: str = "test",
    run_id: str | None = None,
    kind: str = "manual",
    params: object = None,
    secrets: Mapping[str, str] | None = None,
    task_id: str = "test",
    try_number: int = 1,
    map_index: int | None = None,
) -> Iterator[RunValues]:
    """Gives ``dag_ctx`` values inside the block, as while the runner calls a task.

    For unit tests of task functions: ``with task_context(planned_for=...): extract()``.

    Args:
        planned_for: ``dag_ctx.run.planned_for`` (aware); now (UTC) by default.
        since: ``dag_ctx.run.since`` (aware, not after ``planned_for``); ``planned_for`` by
            default (an empty window).
        dag_id: ``dag_ctx.dag_id``.
        run_id: ``dag_ctx.run.id``; a new UUIDv7 by default.
        kind: ``dag_ctx.run.kind``: ``manual`` by default.
        params: Run parameters (milestone M4).
        secrets: Declared secrets (milestone M4).
        task_id: ``task_ctx.task_id``.
        try_number: ``task_ctx.try_number``.
        map_index: ``task_ctx.map_index`` (``None``: an unmapped task).

    Yields:
        The values ``dag_ctx`` gives.

    Raises:
        TypeError: For a time that isn't a ``datetime``.
        ValueError: For a naive time, ``since`` after ``planned_for``, or an unknown kind.
        NotImplementedError: For ``params`` or ``secrets``.
    """
    if params is not None:
        raise _milestone_m4("task_context(params=...)", "run parameters")
    if secrets is not None:
        raise _milestone_m4("task_context(secrets=...)", "secrets")
    if kind not in RUN_KINDS:
        raise ValueError(f"unknown run kind `{kind}` (known: {', '.join(sorted(RUN_KINDS))})")
    planned = _aware(planned_for, "planned_for") if planned_for is not None else _now()
    start = _aware(since, "since") if since is not None else planned
    if start > planned:
        raise ValueError(f"since ({start}) must not be after planned_for ({planned})")
    values = RunValues(dag_id, run_id or str(uuid.uuid7()), kind, planned, start)
    with running(values, TaskValues(task_id, try_number, map_index)):
        yield values


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class Attempt:
    """One attempt of a task in a ``run_dag`` result.

    Attributes:
        attempt_id: Its ID.
        try_number: Its try number.
        status: How it ended (``success``, ``failed``, ``timed_out``, ...).
        exit_code: The process's exit code (none for tasks without a process).
        error_kind: The failure kind schedr recognised (e.g. ``input_mismatch``), if any.
        error: The error summary of a failure (``exception``, ``message``, ``traceback``).
        started_at: When the worker started it.
        ended_at: When it ended.
        duration: How long it ran, measured by the worker (``None`` while it runs, or for an
            attempt the master ended); not ``ended_at - started_at``, which come from the
            master's wall clock and can step.
    """

    attempt_id: str
    try_number: int
    status: str | None
    exit_code: int | None
    error_kind: str | None
    error: Mapping[str, str] | None
    started_at: datetime | None
    ended_at: datetime | None
    duration: timedelta | None

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> Attempt:
        """Reads an attempt from ``schedr dags test --json``.

        Args:
            data: The attempt's JSON.

        Returns:
            The attempt.
        """
        return cls(
            attempt_id=data["attempt_id"],
            try_number=data["try_number"],
            status=data.get("status"),
            exit_code=data.get("exit_code"),
            error_kind=data.get("error_kind"),
            error=data.get("error_summary"),
            started_at=_time(data.get("started_at")),
            ended_at=_time(data.get("ended_at")),
            duration=None
            if data.get("duration_ms") is None
            else timedelta(milliseconds=data["duration_ms"]),
        )


def _time(text: str | None) -> datetime | None:
    return None if text is None else datetime.fromisoformat(text)


@dataclass(frozen=True, slots=True)
class TaskResult:
    """One task in a ``run_dag`` result.

    Attributes:
        task_id: The task's ID.
        state: Where it ended (``success``, ``failed``, ``upstream_failed``, ...).
        outcome: Its outcome on success (``SUCCESS``).
        try_number: Its last try number (0 if it never ran).
        output_json: Its output as stored (tagged JSON), or ``None``; a mapped task's is the
            list of its instances' outputs (``None`` for one that didn't succeed).
        attempts: Its attempts, in order (none for a mapped task as a whole: its instances
            have them).
        log: The log of all its attempts.
        map_index: Which instance of a mapped task it is, from 0; ``None`` for an unmapped
            task and a mapped one as a whole.
        instances: A mapped task's instances, in order (``result.tasks["fetch"][1]`` is one);
            ``None`` for an unmapped task.
    """

    task_id: str
    state: str
    outcome: str | None
    try_number: int
    output_json: Json
    attempts: tuple[Attempt, ...]
    log: str
    _decode: Callable[[str, Json], object] = field(repr=False, compare=False)
    map_index: int | None = None
    instances: tuple[TaskResult, ...] | None = None

    def __getitem__(self, index: int) -> TaskResult:
        """One instance of a mapped task (decision 85).

        Args:
            index: Its ``map_index``.

        Returns:
            Its result.

        Raises:
            TypeError: For a task that isn't mapped.
            IndexError: For an instance it doesn't have.
        """
        if self.instances is None:
            raise TypeError(f"task `{self.task_id}` isn't mapped: it has no instances")
        return self.instances[index]

    @property
    def output(self) -> object:
        """The output decoded by the task's return type, as a downstream task would get it.

        A command's output is a ``ProcessResult``; an empty task's is ``None``.

        Raises:
            RunDagError: If the DAG file can't be imported to find the type.
        """
        if self.output_json is None:
            return None
        if self.instances is not None and isinstance(self.output_json, list):
            # The list of the instances' outputs, each decoded by the task's return type.
            items: list[Json] = self.output_json
            return [None if item is None else self._decode(self.task_id, item) for item in items]
        return self._decode(self.task_id, self.output_json)


@dataclass(frozen=True, slots=True)
class RunResult:
    """The result of ``run_dag``: the run and each of its tasks.

    Attributes:
        run_id: The run's ID.
        dag_id: The DAG.
        kind: The run's kind (``manual``).
        state: How the run ended: ``success``, ``failed`` or ``cancelled``.
        planned_for: The moment it was for.
        since: The start of its window, ``(since, planned_for]``.
        tasks: Its tasks by ID.
    """

    run_id: str
    dag_id: str
    kind: str
    state: str
    planned_for: datetime
    since: datetime
    tasks: Mapping[str, TaskResult]


class _OutputTypes:
    """Finds each task's output type in the DAG file, importing it at most once."""

    def __init__(self, dag_id: str, root_dir: Path, location: Mapping[str, str] | None) -> None:
        self._dag_id = dag_id
        self._root_dir = root_dir
        self._location = location
        self._entry: registry.DefinedDag | None = None

    def decode(self, task_id: str, data: Json) -> object:
        if self._location is None:
            return decode_any(data)
        entry = self._load(self._location)
        kind = next((t.kind for t in entry.model.tasks if t.id == task_id), None)
        if isinstance(kind, PythonKind):
            hints = type_hints(entry.functions[task_id], f"task `{task_id}`")
            return decode(data, hints.get("return", Any))
        if isinstance(kind, CommandKind):
            return decode(data, ProcessResult)
        return decode_any(data)

    def _load(self, location: Mapping[str, str]) -> registry.DefinedDag:
        if self._entry is not None:
            return self._entry
        path = location["path"]
        file = self._root_dir / path
        entry = registry.defined_in(file, self._dag_id)
        if entry is None:
            extend_sys_path([str(self._root_dir)])
            try:
                import_dag_entries(location["root"], self._root_dir, path)
            except Exception as e:
                raise RunDagError(
                    f"can't import {file} to decode the outputs of `{self._dag_id}`: "
                    f"{type(e).__name__}: {e}; use `output_json` instead"
                ) from e
            entry = registry.defined_in(file, self._dag_id)
        if entry is None:
            raise RunDagError(f"{file} doesn't define DAG `{self._dag_id}` (any more?)")
        self._entry = entry
        return entry


def schedr_binary() -> str:
    """The ``schedr`` binary ``run_dag`` starts.

    In order: ``$SCHEDR_BIN``; the ``schedr`` installed with this Python's ``schedr``
    package (its scripts directory, so the binary matches the package); ``schedr`` on
    ``PATH``.

    Returns:
        Its path.

    Raises:
        RunDagError: If none is found.
    """
    override = os.environ.get(BINARY_ENV)
    if override:
        return override
    name = "schedr.exe" if sys.platform == "win32" else "schedr"
    installed = Path(sysconfig.get_path("scripts")) / name
    if installed.is_file():
        return str(installed)
    found = shutil.which("schedr")
    if found is not None:
        return found
    raise RunDagError(
        "the `schedr` binary wasn't found: install schedr into this environment "
        f"(`pip install schedr`), or set {BINARY_ENV} to its path"
    )


def run_dag(
    path: str | os.PathLike[str],
    dag_id: str,
    *,
    for_: datetime | None = None,
    params: object = None,
    secrets: Mapping[str, str] | None = None,
    step: bool = False,
    replace: Mapping[str, object] | None = None,
) -> RunResult:
    """Runs one real run of a DAG with ``schedr dags test`` and returns its result.

    Every task runs as its own process, as in production. The DAG's Python tasks run with
    this Python (``[interpreters] python`` is set to it unless the environment sets it).

    Args:
        path: A DAG file, or a directory of DAG files (a DAG root).
        dag_id: The DAG to run.
        for_: The run's ``planned_for`` (aware); now by default.
        params: Run parameters (milestone M4).
        secrets: Secret values (milestone M4).
        step: Yield each task as it ends (milestone M4).
        replace: Results replacing tasks (milestone M4).

    Returns:
        The run, whether it succeeded or not.

    Raises:
        RunDagError: If the binary isn't found, or the run couldn't be made (e.g. no such
            DAG, or a file that doesn't import).
        TypeError: If ``for_`` isn't a ``datetime``.
        ValueError: If ``for_`` is naive.
        NotImplementedError: For ``params``, ``secrets``, ``step`` or ``replace``.
    """
    for given, what, feature in (
        (params is not None, "params", "run parameters"),
        (secrets is not None, "secrets", "secrets"),
        (step, "step", "step-by-step runs"),
        (replace is not None, "replace", "replaced task results"),
    ):
        if given:
            raise _milestone_m4(f"run_dag({what}=...)", feature)
    root = Path(path).resolve()
    command = [schedr_binary(), "dags", "test", dag_id, "--path", str(root), "--json"]
    if for_ is not None:
        command += ["--for", _aware(for_, "for_").isoformat()]
    env = dict(os.environ)
    env.setdefault("SCHEDR_CONFIG__INTERPRETERS__PYTHON", sys.executable)
    done = subprocess.run(command, capture_output=True, text=True, env=env, check=False)
    try:
        report = json.loads(done.stdout)
    except json.JSONDecodeError:
        report = None
    if not isinstance(report, dict) or "run" not in report:
        raise RunDagError(
            f"`schedr dags test {dag_id}` failed (exit code {done.returncode}): "
            f"{done.stderr.strip() or done.stdout.strip()}"
        )
    return _result(report, root if root.is_dir() else root.parent)


def _result(report: Mapping[str, Any], root_dir: Path) -> RunResult:
    run = report["run"]
    types = _OutputTypes(run["dag_id"], root_dir, report.get("location"))

    def result(task: Mapping[str, Any], instances: tuple[TaskResult, ...] | None) -> TaskResult:
        index = task.get("map_index", -1)
        return TaskResult(
            task_id=task["task_id"],
            state=task["state"],
            outcome=task.get("outcome"),
            try_number=task["try_number"],
            output_json=task.get("output"),
            attempts=tuple(Attempt.from_json(a) for a in task["attempts"]),
            log=task["log"],
            _decode=types.decode,
            map_index=index if index >= 0 else None,
            instances=instances,
        )

    # A mapped task's instances follow its own row (decision 85).
    of: dict[str, list[TaskResult]] = {}
    for task in report["tasks"]:
        if task.get("map_index", -1) >= 0:
            of.setdefault(task["task_id"], []).append(result(task, None))
    tasks = {
        task["task_id"]: result(
            task,
            tuple(of.get(task["task_id"], ())) if task.get("instances") is not None else None,
        )
        for task in report["tasks"]
        if task.get("map_index", -1) < 0
    }
    return RunResult(
        run_id=run["run_id"],
        dag_id=run["dag_id"],
        kind=run["kind"],
        state=run["state"],
        planned_for=datetime.fromisoformat(run["planned_for"]),
        since=datetime.fromisoformat(run["since"]),
        tasks=tasks,
    )
