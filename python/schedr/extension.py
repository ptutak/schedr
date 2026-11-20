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
import json
import os
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from importlib.metadata import PackageNotFoundError, version
from typing import Any, cast

from schedr._serialization import CodecError, check_size
from schedr.runner.main import FD_ENV
from schedr.runner.parse import import_error
from schedr.runner.protocol import Channel, ProtocolError, from_fd
from schedr.types import Json

__all__ = [
    "PROTOCOL",
    "ExtensionChannel",
    "ExtensionError",
    "ExtensionRequest",
    "Handle",
    "main",
]

PROTOCOL = 1
"""The runner protocol version this module speaks with the worker."""

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
    value: Json


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
        raw: The whole request, as sent.
    """

    name: str
    version: str
    payload: Json
    task_id: str
    dag_id: str
    run: Mapping[str, Json]
    handles: Sequence[Handle] = ()
    cleanup: bool = False
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

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
            raw=message,
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
    ) -> Json:
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
            return cast("Json", given)
        name: dict[str, str] = {"path": path}
        if key is not None:
            name["key"] = key
        if storage is not None:
            name["storage"] = storage
        answer = await self._ask({"type": "secret_get", "secret": name})
        if answer["type"] != "secret":
            raise ExtensionError(answer.get("kind", ""), answer.get("message", str(answer)))
        # The worker's `secret` answer: its value is JSON.
        return cast("Json", answer["value"])

    async def connection(self, name: str) -> dict[str, Json]:
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
        return cast("dict[str, Json]", answer["values"])

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
    ) -> tuple[int, Json]:
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
        return _serve(channel, run, cleanup)
    except ProtocolError as e:
        print(f"worker channel: {e}", file=sys.stderr)
        return _EXIT_FAILED
    finally:
        channel.close()


def _serve(channel: Channel, run: Run, cleanup: Run | None) -> int:
    channel.send(
        {
            "type": "hello",
            "schedr_version": _plugin_version(),
            "mode": "extension",
            "protocol": PROTOCOL,
        }
    )
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
    except Exception as e:  # the plugin's own failure
        return {"type": "task_failed", "error": import_error(e), "kind": None}
    try:
        json.dumps(output, allow_nan=False)
        check_size(output, int(request.raw["task"]["settings"]["max_output_size"]))
    except CodecError as e:
        return {"type": "task_failed", "error": import_error(e), "kind": "output_too_large"}
    except (TypeError, ValueError) as e:
        return {"type": "task_failed", "error": import_error(e), "kind": "output_not_serializable"}
    return {"type": "task_succeeded", "output": output, "outcome": "SUCCESS"}


async def _called(call: Run, request: ExtensionRequest, channel: ExtensionChannel) -> object:
    return await call(request, channel)
