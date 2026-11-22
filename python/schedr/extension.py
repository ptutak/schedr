"""Writing an extension's plugin in Python (decision 510).

A plugin is a program a worker runs for each task of its extension, as its own process in a task
slot. It speaks the runner protocol over the socket the worker hands it (``SCHEDR_AGENT_FD``):
a ``hello`` naming the protocol version, then one ``extension_task`` request, answered with
``task_succeeded`` or ``task_failed``. This module is that loop; a plugin gives it a coroutine::

    from schedr.extension import ExtensionChannel, ExtensionRequest, main

    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        token = await channel.secret("spark/api", "token")
        job = submit(request.payload, token)
        await channel.add_handle("job", {"id": job.id})
        return {"rows": await job.wait()}

    if __name__ == "__main__":
        raise SystemExit(main(run, name="spark", versions=["1.0"]))

What ``run`` prints goes to the attempt's log; what it returns is the task's output (JSON); an
exception fails the attempt, retried as the task's ``retries`` say. A cancel or a timeout ends
the process (``SIGTERM``, then ``SIGKILL`` after the grace). With ``cleanup=`` given, the same
program is run again after each attempt it started, whatever its end, and ``cleanup`` is
called with the handles the attempt registered (maybe none) (``ExtensionTask(..., cleanup=True)``).

``main`` also answers ``--describe``, which a worker with ``[extensions] discover = true`` asks of
each ``schedr-ext-*`` on its ``PATH``.
"""

import asyncio
import contextlib
import json
import os
import signal
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Literal, NotRequired, TypedDict, cast

from schedr._serialization import CodecError, check_size
from schedr.log import install as install_structured_logging
from schedr.runner.main import FD_ENV
from schedr.runner.parse import import_error
from schedr.runner.protocol import Channel, ProtocolError, from_fd
from schedr.types import Json

__all__ = [
    "PROTOCOL",
    "ExtensionChannel",
    "ExtensionError",
    "ExtensionJson",
    "ExtensionRequest",
    "Handle",
    "LabelDeclaration",
    "MetricDeclaration",
    "PanelDeclaration",
    "main",
]

PROTOCOL = 1
"""The runner protocol version this module speaks with the worker."""

type ExtensionJson = (
    bool | int | float | str | list[ExtensionJson] | dict[str, ExtensionJson] | None
)
"""Decoded JSON, as the worker sends it: what a plugin is given (its payload, the run, its
handles, a secret, a connection's values, an API answer).

Precise where :data:`schedr.types.Json` (what a task may return) is lenient: a ``dict``'s values
narrow to ``ExtensionJson``, not ``object``, so a plugin reads its payload with ``isinstance``
checks alone.
"""


class LabelDeclaration(TypedDict):
    """A metric's label: its name, and perhaps the values it may take (others are ``other``)."""

    name: str
    values: NotRequired[list[str]]


class MetricDeclaration(TypedDict):
    """A metric a plugin reports (decision 513), for ``main(metrics=...)``.

    A list of these, annotated (``METRICS: list[MetricDeclaration] = [...]``), is checked by a
    type checker key by key; the worker checks it again at its hello.
    """

    name: str
    kind: Literal["counter", "gauge", "histogram"]
    help: str
    unit: NotRequired[Literal["count", "seconds", "bytes", "ratio"]]
    labels: NotRequired[list[LabelDeclaration]]
    buckets: NotRequired[list[float]]


type Color = Literal[
    "blue",
    "orange",
    "green",
    "amber",
    "pink",
    "forest",
    "violet",
    "red",
    "teal",
    "cyan",
    "indigo",
    "magenta",
    "brown",
    "olive",
    "slate",
    "gold",
    "coral",
    "sky",
    "plum",
    "mint",
]


class PanelDeclaration(TypedDict):
    """A panel of the extension's page (decisions 513, 516), for ``main(panels=...)``."""

    title: str
    metric: NotRequired[str]
    chart: Literal[
        "line", "rate", "stacked", "histogram", "quantiles", "number", "table", "bars", "events"
    ]
    row: int
    cell: Literal["full", "left", "center", "right"]
    quantiles: NotRequired[list[float]]
    color: NotRequired[Color]
    colors: NotRequired[dict[str, Color]]
    point: NotRequired[
        Literal["increase", "rate", "last", "mean", "max", "min", "count", "quantiles"]
    ]
    combine: NotRequired[Literal["sum", "max", "mean", "min"]]
    group_by: NotRequired[list[str]]
    limit: NotRequired[int]


# The worker's default `max_output_size`.
_DEFAULT_MAX_OUTPUT = 1024 * 1024
_EXIT_OK = 0
_EXIT_FAILED = 1
_EXIT_USAGE = 2


class ExtensionError(Exception):
    """The worker refused what the plugin asked: a secret, a connection, a handle."""

    def __init__(self, kind: str, message: str) -> None:
        """An error the worker gave.

        Args:
            kind: Its kind, e.g. ``secret_not_found``.
            message: What went wrong.
        """
        super().__init__(f"{kind}: {message}" if kind else message)
        self.kind = kind


@dataclass(frozen=True, slots=True)
class Handle:
    """A cleanup handle an attempt registered: its type's name and its value."""

    type: str
    value: ExtensionJson


@dataclass(frozen=True, slots=True)
class ExtensionRequest:
    """What the worker asks the plugin to do.

    Attributes:
        name: The extension's name.
        version: The version the task is written for.
        payload: What the task's ``payload=`` says.
        task_id: The task's ID.
        dag_id: Its DAG's ID.
        run: The run's information, as the worker sends it (``run_id``, ``try_number``, ...).
        handles: For a cleanup, the handles the attempt registered, in order; empty otherwise.
        cleanup: Whether this is a cleanup, not the task.
        max_output_size: The most bytes its output may take, as JSON.
        raw: The whole request, as sent.
    """

    name: str
    version: str
    payload: ExtensionJson
    task_id: str
    dag_id: str
    run: Mapping[str, ExtensionJson]
    handles: Sequence[Handle] = ()
    cleanup: bool = False
    max_output_size: int = _DEFAULT_MAX_OUTPUT
    raw: Mapping[str, ExtensionJson] = field(default_factory=dict, repr=False)

    @classmethod
    def from_message(cls, message: Mapping[str, Any]) -> ExtensionRequest:
        """The request in an ``extension_task`` message.

        Args:
            message: The message.

        Returns:
            Its request.

        Raises:
            ProtocolError: For a task that isn't an extension's.
        """
        task = message["task"]
        if task.get("kind") != "extension":
            raise ProtocolError(f"expected an extension task, got `{task.get('kind')}`")
        given = message.get("cleanup")
        return cls(
            name=task["name"],
            version=task["version"],
            payload=task.get("payload"),
            task_id=task["id"],
            dag_id=message["dag_id"],
            run=message["run"],
            handles=tuple(Handle(h["handle_type"], h["value"]) for h in given or ()),
            cleanup=given is not None,
            # The worker always sends it; a request built by hand (a plugin's test) may not.
            max_output_size=int(
                (task.get("settings") or {}).get("max_output_size") or _DEFAULT_MAX_OUTPUT
            ),
            # The worker's message, decoded from JSON.
            raw=cast("Mapping[str, ExtensionJson]", message),
        )


class ExtensionChannel:
    """What a plugin may ask the worker while it runs: what a Python task may.

    Each call waits for the worker's answer (in a thread: the event loop stays free).
    """

    def __init__(self, channel: Channel, request: Mapping[str, Any]) -> None:
        """Wraps the worker's channel.

        Args:
            channel: The channel.
            request: The request, for the secrets it already brought.
        """
        self._channel = channel
        self._given = {
            (s["name"].get("storage"), s["name"]["path"], s["name"].get("key")): s["value"]
            for s in request.get("secrets", ())
        }

    async def _ask(self, message: dict[str, Any]) -> dict[str, Any]:
        return await asyncio.to_thread(self._channel.ask, message)

    async def secret(
        self, path: str, key: str | None = None, *, storage: str | None = None
    ) -> ExtensionJson:
        """A secret the task declared (``secrets=``): a key's value, or the whole secret.

        Args:
            path: The secret's path.
            key: The key; ``None`` for the whole secret, an object of key to value.
            storage: The storage; ``None`` for the default.

        Returns:
            The value.

        Raises:
            ExtensionError: When the worker can't give it (not declared, not found, denied).
        """
        given = self._given.get((storage, path, key))
        if given is not None:
            # The worker's JSON: a key's value or a whole secret's object.
            return cast("ExtensionJson", given)
        name: dict[str, str] = {"path": path}
        if key is not None:
            name["key"] = key
        if storage is not None:
            name["storage"] = storage
        answer = await self._ask({"type": "secret_get", "secret": name})
        if answer["type"] != "secret":
            raise ExtensionError(answer.get("kind", ""), answer.get("message", str(answer)))
        # The worker's `secret` answer: its value is JSON.
        return cast("ExtensionJson", answer["value"])

    async def connection(self, name: str) -> dict[str, ExtensionJson]:
        """A connection the task declared (``connections=``): its values, rendered.

        Args:
            name: The connection's name.

        Returns:
            Its values by parameter.

        Raises:
            ExtensionError: When the worker can't give it.
        """
        answer = await self._ask({"type": "connection_get", "name": name})
        if answer["type"] != "connection":
            raise ExtensionError(answer.get("kind", ""), answer.get("message", str(answer)))
        # The worker's `connection` answer: its values by parameter, JSON.
        return cast("dict[str, ExtensionJson]", answer["values"])

    async def _tell(self, message: dict[str, Any]) -> None:
        await asyncio.to_thread(self._channel.send, message)

    async def status(
        self, phase: str, message: str | None = None, *, progress: float | None = None
    ) -> None:
        """Says where the task is now: kept with the attempt with its time, shown on the task.

        No answer: the worker keeps it, masked; a progress outside 0 to 1 is clamped.

        Args:
            phase: A short name for the phase, e.g. ``"pulling image"``.
            message: More about it.
            progress: How far along, 0 to 1, when known.
        """
        report: dict[str, Any] = {"type": "status", "phase": phase}
        if message is not None:
            report["message"] = message
        if progress is not None:
            report["progress"] = progress
        await self._tell(report)

    async def link(self, label: str, url: str) -> None:
        """Links to what the task started elsewhere (a pod, a job's page), shown on the task.

        A second link with the same label replaces it; only ``http`` and ``https`` are kept.

        Args:
            label: What it links to.
            url: Its URL.
        """
        await self._tell({"type": "link", "label": label, "url": url})

    async def metric(self, name: str, value: float, **labels: str) -> None:
        """Reports one point of a declared metric (``main(..., metrics=...)``).

        A counter adds ``value``, a gauge is set to it, a histogram observes it. A point that
        matches no declaration is dropped by the worker with a warning.

        Args:
            name: The metric.
            value: The value.
            **labels: Its labels' values.
        """
        point: dict[str, Any] = {"name": name, "value": value}
        if labels:
            point["labels"] = labels
        await self._tell({"type": "metrics", "points": [point]})

    async def stop_reason(self) -> tuple[str | None, float]:
        """Why the task is being stopped, asked after SIGTERM: the reason and the seconds left.

        The reason is ``"cancel"``, ``"timeout"``, ``"drain"`` or ``"master_lost"``; ``None``
        while the task isn't being stopped. SIGKILL comes once the seconds are up.

        Returns:
            The reason, and the grace left in seconds.
        """
        answer = await self._ask({"type": "stop_reason_get"})
        if answer["type"] != "stop_reason":
            raise ExtensionError("unexpected", f"expected `stop_reason`, got `{answer['type']}`")
        return answer.get("reason"), int(answer.get("grace_ms", 0)) / 1000

    async def add_handle(self, handle_type: str, value: Json) -> None:
        """Registers what the cleanup needs, made by this attempt (a job's ID, a pod's name).

        Kept by the worker as it is registered: a worker lost after it still has it, and its
        cleanup runs with it (``ExtensionTask(..., cleanup=True)``).

        Args:
            handle_type: Its type's name (one of the task's ``cleanup_handles``, if it names any).
            value: Its value, JSON.

        Raises:
            ExtensionError: When the worker refuses it (no cleanup, a type not allowed).
        """
        answer = await self._ask(
            {"type": "handle_add", "handle": {"handle_type": handle_type, "value": value}}
        )
        if answer["type"] != "handle_added":
            raise ExtensionError("handle_refused", answer.get("message", str(answer)))

    async def api(
        self, method: str, path: str, body: Json = None, *, idempotency_key: str | None = None
    ) -> tuple[int, ExtensionJson]:
        """A call to schedr's REST API, made by the master as the task (``schedr.api``'s).

        Args:
            method: ``GET``, ``POST``, ...
            path: The path and query, e.g. ``/api/v1/events?name=paid``.
            body: The JSON body, if any.
            idempotency_key: Its ``Idempotency-Key``, if any.

        Returns:
            The HTTP status and the JSON body.
        """
        message: dict[str, Any] = {"type": "api_call", "method": method, "path": path}
        if body is not None:
            message["body"] = body
        if idempotency_key is not None:
            message["idempotency_key"] = idempotency_key
        answer = await self._ask(message)
        return answer["status"], answer["body"]


type Run = Callable[[ExtensionRequest, ExtensionChannel], Awaitable[object]]
"""A plugin's task or cleanup: given the request and the channel, returns the output."""


def _plugin_version() -> str:
    try:
        return version("schedr")
    except PackageNotFoundError:
        return "unknown"


def main(
    run: Run,
    *,
    name: str,
    versions: Sequence[str],
    cleanup: Run | None = None,
    # Typed, or plain mappings as `Json` takes them (agy's review of M5.2i7: a list of dict
    # literals infers `dict[str, object]`); the worker checks them at the hello either way.
    metrics: Sequence[MetricDeclaration] | Sequence[Mapping[str, object]] = (),
    panels: Sequence[PanelDeclaration] | Sequence[Mapping[str, object]] = (),
    argv: Sequence[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> int:
    """Runs a plugin: answers ``--describe``, or runs the one request the worker sends.

    Args:
        run: The task: a coroutine function given the request and the channel; it returns the
            output (JSON), or raises to fail the attempt.
        name: The extension it runs.
        versions: The versions it serves.
        cleanup: The cleanup, called with the handles the attempt registered (``None``: the
            extension has none; a cleanup request then fails).
        metrics: Its metrics' declarations (:class:`MetricDeclaration`: name, kind, help,
            unit, labels, buckets), sent in its hello; a worker's ``[extensions.<name>]
            metrics`` wins over them.
        panels: Its page's panels (:class:`PanelDeclaration`: title, metric, chart, row from 1
            at the top, cell: ``full``,
            ``left``, ``center`` or ``right``; how it is drawn: ``point``, ``combine``,
            ``group_by``, ``limit``), sent in its hello; a metric none shows gets a ``full``
            row of its own below them.
        argv: The arguments (``sys.argv[1:]`` by default).
        environ: The environment (``os.environ`` by default).

    Returns:
        The exit code: 0 when the task succeeded, 1 when it failed or the worker's channel did,
        2 on a wrong start.
    """
    argv = sys.argv[1:] if argv is None else argv
    environ = os.environ if environ is None else environ
    if list(argv) == ["--describe"]:
        description = {"name": name, "versions": list(versions), "protocol": PROTOCOL}
        print(json.dumps(description), flush=True)
        return _EXIT_OK
    value = environ.get(FD_ENV, "")
    if not value.isdigit():
        print(f"{FD_ENV} is not set: a plugin is started by a schedr worker", file=sys.stderr)
        return _EXIT_USAGE
    channel = from_fd(int(value))
    try:
        declared: dict[str, Json] = {}
        if metrics:
            declared["metrics"] = [dict(m) for m in metrics]
        if panels:
            declared["panels"] = [dict(p) for p in panels]
        # Its `logging` as structured lines (decision 514), unless it configures its own.
        install_structured_logging()
        return _serve(channel, run, cleanup, declared)
    except ProtocolError as e:
        print(f"worker channel: {e}", file=sys.stderr)
        return _EXIT_FAILED
    finally:
        channel.close()


def _serve(channel: Channel, run: Run, cleanup: Run | None, declared: Mapping[str, Json]) -> int:
    hello: dict[str, Json] = {
        "type": "hello",
        "schedr_version": _plugin_version(),
        "mode": "extension",
        "protocol": PROTOCOL,
    }
    # Its declarations in its hello (decision 513), as NETCONF's hello announces its models.
    hello.update(declared)
    channel.send(hello)
    reply = channel.receive()
    if reply["type"] == "refused":
        print(f"the worker refused the plugin: {reply.get('reason', '')}", file=sys.stderr)
        return _EXIT_USAGE
    if reply["type"] != "welcome":
        raise ProtocolError(f"expected `welcome`, got `{reply['type']}`")
    message = channel.receive()
    if message["type"] != "extension_task":
        raise ProtocolError(f"expected `extension_task`, got `{message['type']}`")
    request = ExtensionRequest.from_message(message)
    result = _result(request, ExtensionChannel(channel, message), run, cleanup)
    channel.send(result)
    return _EXIT_OK if result["type"] == "task_succeeded" else _EXIT_FAILED


def _result(
    request: ExtensionRequest, channel: ExtensionChannel, run: Run, cleanup: Run | None
) -> dict[str, Any]:
    call = run
    if request.cleanup:
        if cleanup is None:
            error = ProtocolError(f"extension `{request.name}` has no cleanup")
            return {"type": "task_failed", "error": import_error(error), "kind": None}
        call = cleanup
    try:
        output = asyncio.run(_called(call, request, channel))
    except asyncio.CancelledError:
        # SIGTERM (decision 513): the worker ends the attempt with its own reason.
        stopped = RuntimeError("stopped by the worker (SIGTERM)")
        return {"type": "task_failed", "error": import_error(stopped), "kind": None}
    except Exception as e:  # the plugin's own failure
        return {"type": "task_failed", "error": import_error(e), "kind": None}
    try:
        json.dumps(output, allow_nan=False)
        check_size(output, request.max_output_size)
    except CodecError as e:
        return {"type": "task_failed", "error": import_error(e), "kind": "output_too_large"}
    except (TypeError, ValueError) as e:
        return {"type": "task_failed", "error": import_error(e), "kind": "output_not_serializable"}
    return {"type": "task_succeeded", "output": output, "outcome": "SUCCESS"}


async def _called(call: Run, request: ExtensionRequest, channel: ExtensionChannel) -> object:
    # SIGTERM cancels the plugin's coroutine (decision 513): `asyncio.CancelledError` at the
    # `await` it waits on, where it may ask `stop_reason()` and undo what it started. A plugin
    # installing its own handler replaces this one.
    task = asyncio.current_task()
    if task is not None:
        # Not on the main thread (a test running `main` in a thread): no handler.
        with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
            asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    return await call(request, channel)
