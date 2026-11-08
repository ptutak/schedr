"""Testing DAGs: task functions with a run context, and whole runs inspected task by task.

Outside ``with Dag(...)`` a ``@task`` function is the plain function: calling it runs it.
``task_context(...)`` gives ``dag_ctx`` values to such calls, with no processes.
``run_dag(...)`` runs ``schedr dags test`` (one real run, every task its own process) and
returns its result, every task inspectable on its own (see PROJECT.md, Testing DAGs).
Both take run parameters (``params=``) and secret values (``secrets=``); handles, ``step=``
and ``replace=`` come with milestone M4.
"""

import asyncio
import contextlib
import dataclasses
import inspect
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
from urllib.parse import parse_qsl

from schedr import run_of
from schedr._model import (
    CommandKind,
    PythonKind,
    SecretRef,
    SensorKind,
    WaitForEventKind,
    WaitForKind,
    registry,
)
from schedr._model.values import covers
from schedr._serialization import decode, decode_any
from schedr._serialization.annotations import type_hints
from schedr._serialization.codec import encode, encode_any, json_form
from schedr._serialization.models import is_model
from schedr.connections import ConnectionNotFoundError
from schedr.context import (
    ApiCaller,
    ConnectionEnter,
    ConnectionOpener,
    RunValues,
    SecretError,
    SecretReader,
    TaskValues,
    declared_secrets,
    running,
)
from schedr.event_triggers import Event, EventTrigger, TriggerFunction, event_for_run
from schedr.outcomes import FAILED_NAME, TIMED_OUT_NAME, Failed, TimedOut
from schedr.runner.parse import extend_sys_path, import_dag_entries
from schedr.runner.task import member_named
from schedr.runner.waiting import Batch
from schedr.runner.waiting import run_trigger as run_trigger_function
from schedr.sensors import output_hint
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


@dataclass(frozen=True, slots=True)
class TestRunValues(RunValues):
    """The values of a ``task_context``.

    Attributes:
        handles: The handles the block registered (``cleanup_handle``), in order.
        posted_events: The events the block posted (``schedr.api``), in order, as the API
            gives them (JSON).
    """

    __test__ = False  # not a test class, for pytest

    handles: list[object] = field(default_factory=list)
    posted_events: list[dict[str, object]] = field(default_factory=list)


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
    connections: Mapping[str, object] | None = None,
    task_id: str = "test",
    try_number: int = 1,
    map_index: int | None = None,
    event: Event[Any] | None = None,
    first_started: datetime | None = None,
) -> Iterator[TestRunValues]:
    """Gives ``dag_ctx`` values inside the block, as while the runner calls a task.

    For unit tests of task functions: ``with task_context(planned_for=...): extract()``.

    Args:
        planned_for: ``dag_ctx.run.planned_for`` (aware); now (UTC) by default.
        since: ``dag_ctx.run.since`` (aware, not after ``planned_for``); ``planned_for`` by
            default (an empty window).
        dag_id: ``dag_ctx.dag_id``.
        run_id: ``dag_ctx.run.id``; a new UUIDv7 by default.
        kind: ``dag_ctx.run.kind``: ``manual`` by default.
        params: ``dag_ctx.params``: an instance of the DAG's params class (a dataclass or a
            Pydantic model); ``dag_ctx.params(Cls)`` then takes that class.
        secrets: Secret values for ``task_ctx.secret``, by ``"path#key"`` (any storage);
            a whole secret (``"path"``) is its keys'. A ``@task`` function called in the
            block reads only what its ``secrets=`` declares, as on a worker (a plain or
            ``async`` function's reads aren't checked).
        connections: Test doubles for connections, by name: ``with mongodb_default() as db``
            in the block gives ``db = connections["mongodb_default"]`` (the connection's
            function isn't called); a connection without one raises ``ConnectionNotFoundError``.
        task_id: ``task_ctx.task_id``.
        try_number: ``task_ctx.try_number``.
        map_index: ``task_ctx.map_index`` (``None``: an unmapped task).
        event: ``dag_ctx.event``: the event the run was started by.
        first_started: When the step's first try started, for a wait step tried again
            (``delay`` counts from it); ``None``: this is its first try.

    Yields:
        The values ``dag_ctx`` gives, and ``.handles``: what ``cleanup_handle`` registered.

    Raises:
        TypeError: For a time that isn't a ``datetime``.
        ValueError: For a naive time, ``since`` after ``planned_for``, or an unknown kind.
    """
    reader = _local_secrets(secrets) if secrets is not None else None
    if kind not in RUN_KINDS:
        raise ValueError(f"unknown run kind `{kind}` (known: {', '.join(sorted(RUN_KINDS))})")
    planned = _aware(planned_for, "planned_for") if planned_for is not None else _now()
    start = _aware(since, "since") if since is not None else planned
    if start > planned:
        raise ValueError(f"since ({start}) must not be after planned_for ({planned})")
    stored, params_class = _given_params(params)
    values = TestRunValues(
        dag_id,
        run_id or str(uuid.uuid7()),
        kind,
        planned,
        start,
        stored,
        params_class,
        event=None
        if event is None
        else {
            "key": event.key,
            "data": encode_any(event.data),
            "at": None if event.at is None else event.at.isoformat(),
        },
    )
    opener = _doubles(connections) if connections is not None else None

    def add_handle(handle: object, _handle_type: str, _value: object) -> None:
        values.handles.append(handle)

    api = _fake_api(values, f"task:{dag_id}/{values.run_id}/{task_id}")
    own = TaskValues(task_id, try_number, map_index, reader, opener, add_handle, api, first_started)
    with running(values, own):
        yield values


def _fake_api(values: TestRunValues, caller: str) -> ApiCaller:
    """``schedr.api`` in a ``task_context``.

    Events are posted to ``values.posted_events`` and listed from them; another route is
    refused, as on a master.
    """
    keys: dict[str, dict[str, object]] = {}

    def call(method: str, path: str, body: object, key: str | None) -> tuple[int, object]:
        bare, _, query = path.partition("?")
        if (method, bare) == ("GET", "/api/v1/events"):
            asked = dict(parse_qsl(query))
            name, limit = asked.get("name"), int(asked.get("limit", "50"))
            listed = [e for e in reversed(values.posted_events) if name in {None, e["name"]}]
            return 200, {"events": listed[:limit]}
        if (method, bare) != ("POST", "/api/v1/events"):
            return 403, {"error": f"`{method} {bare}` isn't open to tasks"}
        if not isinstance(body, dict) or not body.get("name") or not body.get("key"):
            return 400, {"error": "the event: a name and a key"}
        if key is not None and key in keys:
            first = keys[key]
            # As the API: the same key with another event is refused.
            same = (first["name"], first["key"], first["data"]) == (
                body["name"],
                body["key"],
                body.get("data"),
            ) and (body.get("at") is None or body.get("at") == first["at"])
            if not same:
                return 422, {"error": "this `Idempotency-Key` was sent before with another event"}
            return 200, first
        now = _now().isoformat()
        event: dict[str, object] = {
            "id": str(uuid.uuid7()),
            "name": body["name"],
            "key": body["key"],
            "data": body.get("data"),
            "at": body.get("at") or now,
            "posted_at": now,
            "posted_by": caller,
        }
        values.posted_events.append(event)
        if key is not None:
            keys[key] = event
        return 201, event

    return call


def _doubles(connections: Mapping[str, object]) -> ConnectionOpener:
    """Opens connections as ``task_context(connections=...)``'s doubles."""
    given = dict(connections)

    def open_double(name: str, enter: ConnectionEnter) -> contextlib.AbstractContextManager[object]:
        del enter  # a double stands in for the function
        if name not in given:
            raise ConnectionNotFoundError(
                f"connection `{name}`: no double for it in task_context(connections=...)"
            )
        return contextlib.nullcontext(given[name])

    return open_double


def _local_secrets(secrets: Mapping[str, str]) -> SecretReader:
    """Reads secrets from ``task_context(secrets=...)``'s values."""
    values: dict[tuple[str, str], str] = {}
    for name, value in secrets.items():
        path, sep, key = name.partition("#")
        if not sep or not key or not isinstance(value, str):
            raise ValueError(f'secrets= takes `"path#key": "value"` strings, got {name!r}')
        values[path, key] = value

    def read(ref: SecretRef) -> str | dict[str, str]:
        declared = declared_secrets()
        if declared is not None and not any(covers(d, ref) for d in declared):
            raise SecretError(
                "secret_denied", f"`{ref.label()}` isn't declared in the task's `secrets=`"
            )
        if ref.key is not None:
            if (ref.path, ref.key) in values:
                return values[ref.path, ref.key]
        else:
            whole = {key: value for (path, key), value in values.items() if path == ref.path}
            if whole:
                return whole
        raise SecretError("secret_not_found", f"`{ref.label()}`: no such secret in secrets=")

    return read


def _given_params(params: object) -> tuple[dict[str, Any], type | None]:
    """A params instance as a run stores it, with its class."""
    if params is None:
        return {}, None
    cls = type(params)
    if not (dataclasses.is_dataclass(cls) or is_model(cls)):
        raise TypeError(
            f"params= takes an instance of the DAG's params class (a dataclass or a Pydantic "
            f"model), got {cls.__name__}"
        )
    stored = encode(params, cls, "params")
    return (stored["$dict"] if list(stored) == ["$dict"] else stored), cls


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class Cleanup:
    """The cleanup of an attempt in a ``run_dag`` result: what ran on its handles.

    Attributes:
        result: How it ended (``ok``, ``failed``, ``worker_lost``, ``not_needed``); ``None``
            until it has.
        tries: The tries it made.
        duration: How long its tries ran, measured by the worker.
        error_kind: The failure kind of a failed cleanup.
        error: The error summary of a failed cleanup.
        moves: How many times it moved to another worker.
        handles: The handles the attempt registered, in order, as stored:
            ``{"handle_type": ..., "value": ...}`` (masked).
    """

    result: str | None
    tries: int | None
    duration: timedelta | None
    error_kind: str | None
    error: Mapping[str, str] | None
    moves: int
    handles: tuple[Json, ...]

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> Cleanup:
        """Reads an attempt's cleanup from ``schedr dags test --json``.

        Args:
            data: The cleanup's JSON.

        Returns:
            The cleanup.
        """
        return cls(
            result=data.get("result"),
            tries=data.get("tries"),
            duration=None
            if data.get("duration_ms") is None
            else timedelta(milliseconds=data["duration_ms"]),
            error_kind=data.get("error_kind"),
            error=data.get("error_summary"),
            moves=data.get("moves", 0),
            handles=tuple(data.get("handles", ())),
        )


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
        cleanup: What ran on its handles (``cleanup_handle``), or ``None`` if it registered
            none and its task has no ``cleanup=``.
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
    cleanup: Cleanup | None = None

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
            cleanup=None if data.get("cleanup") is None else Cleanup.from_json(data["cleanup"]),
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
        cleanup: The task's own ``cleanup=``: how it ended over all its attempts (``ok``,
            ``failed``, ...); ``None`` for a task without one, or until it has ended.
    """

    task_id: str
    state: str
    outcome: str | None
    try_number: int
    output_json: Json
    attempts: tuple[Attempt, ...]
    log: str
    _decode: Callable[[str, Json, str | None], object] = field(repr=False, compare=False)
    map_index: int | None = None
    instances: tuple[TaskResult, ...] | None = None
    cleanup: str | None = None

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
            # The list of the instances' outputs, each decoded by the task's return type and
            # its own outcome (B117).
            items: list[Json] = self.output_json
            outcomes = [instance.outcome for instance in self.instances]
            if len(outcomes) != len(items):
                outcomes = [None] * len(items)
            return [
                None if item is None else self._decode(self.task_id, item, outcome)
                for item, outcome in zip(items, outcomes, strict=True)
            ]
        return self._decode(self.task_id, self.output_json, self.outcome)


@dataclass(frozen=True, slots=True)
class ResourceResult:
    """A resource of a run in a ``run_dag`` result.

    Attributes:
        setup: Its setup's task ID (the resource's name).
        cleanup: Its cleanup's task ID.
        users: The tasks that use it.
        blocking: Whether what follows the resource's group waits for its cleanup (a failed
            cleanup fails the run either way).
        state: Where it is: ``setting_up``, ``waiting_for_users``, ``cleaning_up``,
            ``cleaned``, ``cleanup_failed``, ``cleanup_cancelled`` (cancelled by hand: what
            the setup made may be left) or ``not_needed``.
    """

    setup: str
    cleanup: str
    users: tuple[str, ...]
    blocking: bool
    state: str


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
        resources: Its resources by name (the setup's task ID).
    """

    run_id: str
    dag_id: str
    kind: str
    state: str
    planned_for: datetime
    since: datetime
    tasks: Mapping[str, TaskResult]
    resources: Mapping[str, ResourceResult] = field(default_factory=dict)


class _OutputTypes:
    """Finds each task's output type in the DAG file, importing it at most once."""

    def __init__(self, dag_id: str, root_dir: Path, location: Mapping[str, str] | None) -> None:
        self._dag_id = dag_id
        self._root_dir = root_dir
        self._location = location
        self._entry: registry.DefinedDag | None = None

    def decode(self, task_id: str, data: Json, outcome: str | None) -> object:
        # The recorded outcome picks the member of a union, as for a downstream task (B117).
        if self._location is None:
            return decode_any(data)
        entry = self._load(self._location)
        kind = next((t.kind for t in entry.model.tasks if t.id == task_id), None)
        annotation = self._annotation(entry, task_id, kind, outcome)
        # A `CommandResource`'s setup and a branch: their value as JSON reads them.
        return decode_any(data) if annotation is None else decode(data, annotation)

    @staticmethod
    def _annotation(
        entry: registry.DefinedDag, task_id: str, kind: object, outcome: str | None
    ) -> object:
        if isinstance(kind, WaitForKind | WaitForEventKind):
            value = str if isinstance(kind, WaitForKind) else Event[Json]
            named = {TIMED_OUT_NAME: TimedOut, FAILED_NAME: Failed}
            return named.get(outcome or "", value)
        if isinstance(kind, SensorKind):
            if outcome == TIMED_OUT_NAME:
                return TimedOut
            hints = type_hints(entry.functions[task_id], f"task `{task_id}`")
            return member_named(output_hint(hints.get("return", Any)), outcome)
        # A branch's output is the IDs it chose: its annotation is the handles it returned.
        if isinstance(kind, PythonKind) and not kind.branch:
            hints = type_hints(entry.functions[task_id], f"task `{task_id}`")
            return member_named(hints.get("return", Any), outcome)
        if isinstance(kind, CommandKind) and (
            kind.resource is None or kind.resource.cleanup_of is not None
        ):
            return ProcessResult
        return None

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


def run_trigger(
    trigger: TriggerFunction[..., Any] | EventTrigger,
    *,
    after: str | None = None,
    take: int = 1,
    timeout: timedelta = timedelta(seconds=30),
    run: run_of.RunResult[Any] | Event[Any] | None = None,
) -> list[Event[Any]]:
    """Runs a trigger in this process until it has found ``take`` events, and gives them.

    For unit tests of a trigger's function: ``run_trigger(new_files(prefix="in/"), take=3)``. Run
    it inside ``task_context`` to give it secrets and connections. An
    ``@trigger(wait_for=...)`` is called once, for ``run``: ``run_trigger(on_orders(least=5),
    run=schedr.RunResult(...))`` gives the event it returned, as the master gets it, or none.
    An ``@trigger.poke(batch=...)`` gives one event per poke that found items, its data their
    list: the window isn't waited for.

    Args:
        trigger: The trigger with its arguments, or its function when it takes none.
        after: What it starts from: the last key acted on.
        take: How many events to wait for.
        timeout: How long to wait for them.
        run: For an ``@trigger(wait_for=...)``, the run it is called for; for one waiting for an
            ``EventNamed``, the ``Event``.

    Returns:
        The events it found, in order, fewer than ``take`` only when a generator returned.

    Raises:
        TimeoutError: If they don't come in time.
        ValueError: For a ``take`` below 1, or ``run`` given to a trigger without ``wait_for``
            or missing for one with it.
        RuntimeError: If a poke's call failed.
    """
    if take < 1:
        raise ValueError(f"take must be at least 1, got {take}")
    called = trigger if isinstance(trigger, EventTrigger) else trigger()
    wait_for = called.function.wait_for
    if wait_for is not None:
        if isinstance(wait_for, run_of.RunOf):
            if not isinstance(run, run_of.RunResult):
                raise ValueError(
                    f"@trigger `{called.function.task_id}` is called for a run of "
                    f"`{wait_for.dag}`: give it as run=schedr.RunResult(...)"
                )
            return _called_for_run(called, run, wait_for.lag, timeout)
        if not isinstance(run, Event):
            raise ValueError(
                f"@trigger `{called.function.task_id}` is called for an event "
                f"`{wait_for.name}`: give it as run=schedr.Event(...)"
            )
        return _called_for_run(called, run, None, timeout)
    if run is not None:
        raise ValueError("run= is for an @trigger(wait_for=...)")
    found: list[Event[Any]] = []

    class _EnoughError(Exception):
        pass

    async def tell(event: Event[Any]) -> None:
        found.append(event)
        if len(found) >= take:
            raise _EnoughError

    def poked(error: str | None) -> None:
        if error is not None:
            raise RuntimeError(f"a call of the poke failed: {error}")

    task = {} if called.function.every is None else {"every_ms": called.function.every_ms}
    # With `batch=`, each poke's items are one event: the window isn't waited for here.
    batch = None if called.function.batch is None else Batch(0.0, lambda _item: 0, sys.maxsize)

    async def go() -> None:
        async with asyncio.timeout(timeout.total_seconds()):
            await run_trigger_function(
                called.function.fn, called.arguments, task, after, tell, poked, batch
            )

    with contextlib.suppress(_EnoughError):
        asyncio.run(go())
    return found


def _called_for_run(
    called: EventTrigger,
    run: run_of.RunResult[Any] | Event[Any],
    lag: timedelta | None,
    timeout: timedelta,
) -> list[Event[Any]]:
    """An ``@trigger(wait_for=...)``'s one call, for ``run``: the event it returned, filled in.

    Raises:
        TypeError: If it returned something else than an ``Event`` or ``None``.
    """

    async def call() -> object:
        async with asyncio.timeout(timeout.total_seconds()):
            # By name, as the runner gives it: the run's parameter may be keyword-only.
            arguments = {**called.arguments, called.function.run_param: run}
            called_now = called.function.fn(**arguments)
            if not inspect.isawaitable(called_now):  # checked when defined: `async def`
                raise TypeError(f"@trigger `{called.function.task_id}` must be `async def`")
            return await called_now

    found = asyncio.run(call())
    if found is None:
        return []
    if not isinstance(found, Event):
        raise TypeError(
            f"@trigger `{called.function.task_id}` returns an `Event` or `None`, got "
            f"{type(found).__name__}"
        )
    event: Event[Any] = found
    return [event_for_run(event, run, lag)]


def run_dag(
    path: str | os.PathLike[str],
    dag_id: str,
    *,
    for_: datetime | None = None,
    params: object = None,
    event: Event[Any] | None = None,
    secrets: Mapping[str, str] | None = None,
    connections: Mapping[str, Mapping[str, object]] | None = None,
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
        params: Values for the run's parameters: an instance of the DAG's params class, or a
            mapping of field names to JSON values (``{"region": "us"}``), over its defaults.
        event: For a DAG a trigger starts, the event the run brings; its data encoded as the
            trigger's would be.
        secrets: Secret values, by ``"path#key"``, for every storage (``--secret``); masked
            in the logs and outputs as usual.
        connections: Values for connections, by name, each its parameters' values
            (``--connection``): the real ``@connection`` function runs with them instead of
            its own, which are never rendered. A task still opens only what it declares.
        step: Yield each task as it ends (milestone M4).
        replace: Results replacing tasks (milestone M4).

    Returns:
        The run, whether it succeeded or not.

    Raises:
        RunDagError: If the binary isn't found, or the run couldn't be made (e.g. no such
            DAG, or a file that doesn't import).
        TypeError: If ``for_`` isn't a ``datetime``.
        ValueError: If ``for_`` is naive.
        NotImplementedError: For ``step`` or ``replace``.
    """
    for given, what, feature in (
        (step, "step", "step-by-step runs"),
        (replace is not None, "replace", "replaced task results"),
    ):
        if given:
            raise _milestone_m4(f"run_dag({what}=...)", feature)
    root = Path(path).resolve()
    command = [schedr_binary(), "dags", "test", dag_id, "--path", str(root), "--json"]
    if for_ is not None:
        command += ["--for", _aware(for_, "for_").isoformat()]
    if event is not None:
        command += ["--event-key", event.key, "--event-data", json.dumps(encode_any(event.data))]
        if event.at is not None:
            command += ["--event-at", event.at.isoformat()]
    if params is not None:
        command += ["--params", json.dumps(_params_json(params))]
    for name, value in (secrets or {}).items():
        command += ["--secret", f"{name}={value}"]
    for name, values in (connections or {}).items():
        stored = {
            param: encode_any(value, f"connections[{name!r}][{param!r}]")
            for param, value in values.items()
        }
        command += ["--connection", f"{name}={json.dumps(stored)}"]
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


def _params_json(params: object) -> object:
    """``run_dag``'s ``params=`` as the JSON a trigger gives."""
    if isinstance(params, Mapping):
        return dict(params)
    stored, _ = _given_params(params)
    return json_form(stored)


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
            cleanup=task.get("cleanup"),
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
        resources={
            resource["setup"]: ResourceResult(
                setup=resource["setup"],
                cleanup=resource["cleanup"],
                users=tuple(resource["users"]),
                blocking=resource["blocking"],
                state=resource["state"],
            )
            for resource in report.get("resources", ())
        },
    )
