"""An extension's service in Python (decision 518), and a plugin's calls to it.

A service is one long-running process per worker for what its extension's attempts share (a
client, a watch, a warm session). The worker starts it with its control socket
(``SCHEDR_SERVICE_FD``); it says ``hello``, gets ``welcome`` with its declared secrets, says
``ready``; then each attempt of the extension gets a private connection, its descriptor passed
with ``attempt_connected`` and answered ``attempt_accepted`` (DN32). On that connection the
attempt's plugin calls the service: ``call {id, method, args}``, answered ``answer {id,
result}`` or ``call_failed {id, message}``, several in flight (DN29).

The control socket is served from a thread of its own with the blocking channel; the attempts'
connections and every handler run on one asyncio loop.
"""

import asyncio
import contextlib
import json
import os
import signal
import socket
import sys
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from schedr_extension._protocol import (
    MAX_MESSAGE_SIZE,
    Channel,
    ChannelClosedError,
    Message,
    ProtocolError,
    Wire,
    given_secrets,
    obj,
    text,
    whole,
)

if TYPE_CHECKING:
    from schedr_extension import ExtensionJson

SERVICE_FD_ENV = "SCHEDR_SERVICE_FD"
"""The environment variable naming a service's control socket, and a plugin's end of its
attempt's connection to the service."""

SERVICE_PROTOCOL = 1
"""The protocol a service speaks with its worker."""

_HEADER = 4
_EXIT_OK = 0
_EXIT_FAILED = 1
_EXIT_USAGE = 2


class ServiceError(Exception):
    """A call to the service failed: its handler raised, or the connection is gone."""


@dataclass(frozen=True)
class ServiceAttempt:
    """An attempt connected to the service."""

    attempt_id: str
    """The attempt."""
    extension: str
    """Its extension."""
    version: str
    """The version its task is written for."""


@dataclass
class ServiceContext:
    """What a service is given at its start, and may tell the worker."""

    secrets: Mapping[tuple[str | None, str, str | None], object]
    """Its declared secrets (``service_secrets``), by ``(storage, path, key)``."""
    _control: Channel
    _asking: asyncio.Lock = field(default_factory=asyncio.Lock)
    _answers: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)
    _gone: asyncio.Event = field(default_factory=asyncio.Event)
    # Answers owed to `secret_get`s sent: one a call cancelled never read is thrown away before
    # the next call's (Grok's review: the next call returned it, another secret's value).
    _owed: int = 0

    async def _next_answer(self) -> Message:
        """The next answer to a `secret_get`, counted off what is owed; the worker gone fails it.

        Raises:
            ServiceError: When the worker closed the control socket.
        """
        if self._gone.is_set() and self._answers.empty():
            raise ServiceError("the worker closed the control socket")
        answer_task = asyncio.ensure_future(self._answers.get())
        gone_task = asyncio.ensure_future(self._gone.wait())
        try:
            await asyncio.wait({answer_task, gone_task}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            gone_task.cancel()
            if not answer_task.done():
                answer_task.cancel()
            elif not answer_task.cancelled():
                # Taken off the queue, even if this call is being cancelled: no longer owed.
                self._owed -= 1
        if not answer_task.done() or answer_task.cancelled():
            raise ServiceError("the worker closed the control socket")
        return answer_task.result()

    def secret_value(
        self, path: str, key: str | None = None, *, storage: str | None = None
    ) -> object:
        """One of its secrets, as given at this start.

        Args:
            path: Its path.
            key: Its key; ``None`` for the whole secret.
            storage: Its storage; ``None`` for the default.

        Returns:
            Its value.

        Raises:
            KeyError: For a secret it wasn't given.
        """
        return self.secrets[(storage, path, key)]

    async def secret(
        self, path: str, key: str | None = None, *, storage: str | None = None
    ) -> object:
        """One of its declared secrets, read again by the worker now.

        Args:
            path: Its path.
            key: Its key; ``None`` for the whole secret.
            storage: Its storage; ``None`` for the default.

        Returns:
            Its value.

        Raises:
            ServiceError: When the worker can't give it (not declared, missing, unavailable).
        """
        name: dict[str, Wire] = {"path": path}
        if key is not None:
            name["key"] = key
        if storage is not None:
            name["storage"] = storage
        async with self._asking:
            if self._gone.is_set():
                raise ServiceError("the worker closed the control socket")
            while self._owed > 0:
                await self._next_answer()
            self._owed += 1
            try:
                await asyncio.to_thread(self._control.send, {"type": "secret_get", "secret": name})
            except Exception:
                # Not sent: no answer comes. A cancel leaves it owed (its thread sends it).
                self._owed -= 1
                raise
            answer = await self._next_answer()
        if answer["type"] == "secret":
            return answer["value"]
        raise ServiceError(f"{answer.get('kind', 'secret_failed')}: {answer.get('message', '')}")

    async def status(self, phase: str, message: str | None = None) -> None:
        """Tells the worker what it is doing (an event on its extension's page when it changes).

        Args:
            phase: A short name, e.g. ``watching``.
            message: More about it.
        """
        told: Message = {"type": "status", "phase": phase}
        if message is not None:
            told["message"] = message
        await asyncio.to_thread(self._control.send, told)

    async def metric(self, name: str, value: float, **labels: str) -> None:
        """A point of one of its extension's declared metrics, as a plugin's.

        Args:
            name: The metric.
            value: A counter's increase, a gauge's value, a histogram's observation.
            **labels: Its labels' values.
        """
        told_labels: dict[str, Wire] = dict(labels)
        point: Wire = {"name": name, "labels": told_labels, "value": value}
        await asyncio.to_thread(self._control.send, {"type": "metrics", "points": [point]})


# Its calls' times (M5.2i12's `service_call_seconds`): sent at most once a second, at most this
# many held between (past them, the oldest go) and this many in one message (a control line's
# 16 KiB).
_CALL_TIMES_HELD = 1000
_CALL_TIMES_PER_MESSAGE = 100


class _CallTimes:
    """Each call's time, by method, until sent as `service_call_seconds` points."""

    def __init__(self) -> None:
        self.held: deque[tuple[str, float]] = deque(maxlen=_CALL_TIMES_HELD)

    def add(self, method: str, seconds: float) -> None:
        self.held.append((method, seconds))

    def messages(self) -> list[Message]:
        """What was held, as `metrics` messages; none held after."""
        points: list[Wire] = [
            {"name": "service_call_seconds", "labels": {"method": method}, "value": seconds}
            for method, seconds in self.held
        ]
        self.held.clear()
        return [
            {"type": "metrics", "points": points[i : i + _CALL_TIMES_PER_MESSAGE]}
            for i in range(0, len(points), _CALL_TIMES_PER_MESSAGE)
        ]


async def _send_call_times(control: Channel, times: _CallTimes) -> None:
    """Sends the calls' times once a second, until cancelled with the service.

    It stops early when its control socket closes: the service ends then, its reader seeing it.
    """
    while True:
        await asyncio.sleep(1)
        try:
            for message in times.messages():
                await asyncio.to_thread(control.send, message)
        except OSError, ProtocolError:
            return


type OnCall = Callable[[ServiceAttempt, str, object], Awaitable[object]]
"""A service's handler: given the attempt, the method and its arguments; returns the result
(JSON), or raises to fail the call."""


async def _read_frame(reader: asyncio.StreamReader) -> Message | None:
    try:
        header = await reader.readexactly(_HEADER)
    except asyncio.IncompleteReadError as e:
        if e.partial:
            raise ProtocolError("the connection closed within a message") from e
        return None
    size = int.from_bytes(header, "big")
    if size > MAX_MESSAGE_SIZE:
        raise ProtocolError(f"a message of {size} bytes is over the limit")
    try:
        body = await reader.readexactly(size)
    except asyncio.IncompleteReadError as e:
        raise ProtocolError("the connection closed within a message") from e
    message: Wire = json.loads(body)
    if not isinstance(message, dict) or not isinstance(message.get("type"), str):
        raise ProtocolError("a message must be a JSON object with a string `type`")
    return message


def _frame(message: Mapping[str, object]) -> bytes:
    data = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8", "replace")
    if len(data) > MAX_MESSAGE_SIZE:
        raise ProtocolError(f"a message of {len(data)} bytes is over the limit")
    return len(data).to_bytes(_HEADER, "big") + data


@dataclass(frozen=True)
class _Hooks:
    """What a service is told, besides its calls."""

    start: Callable[[ServiceContext], Awaitable[None]] | None
    on_closed: Callable[[ServiceAttempt], Awaitable[None]] | None
    stop: Callable[[], Awaitable[None]] | None
    on_drained: Callable[[], Awaitable[None]] | None
    on_resumed: Callable[[], Awaitable[None]] | None


def serve(
    on_call: OnCall,
    *,
    name: str,
    versions: Sequence[str],
    start: Callable[[ServiceContext], Awaitable[None]] | None = None,
    on_closed: Callable[[ServiceAttempt], Awaitable[None]] | None = None,
    stop: Callable[[], Awaitable[None]] | None = None,
    on_drained: Callable[[], Awaitable[None]] | None = None,
    on_resumed: Callable[[], Awaitable[None]] | None = None,
    environ: Mapping[str, str] | None = None,
) -> int:
    """Runs a service until the worker stops it (``SIGTERM``) or goes away.

    Args:
        on_call: The handler of each call an attempt's plugin makes.
        name: The extension.
        versions: The versions it serves; the worker offers those of its configured ones.
        start: Called once, before it says ``ready``, with its context (its secrets).
        on_closed: Called when an attempt's connection closes (its attempt ended).
        stop: Called on ``SIGTERM``, before it exits.
        on_drained: Called when the worker is drained for maintenance (DN33): no attempts come
            meanwhile, and a service acting on its own may pause; it keeps running.
        on_resumed: Called when the worker is resumed after that.
        environ: The environment (``os.environ`` by default).

    Returns:
        The exit code: 0 when stopped, 1 when the worker's socket failed, 2 on a wrong start.
    """
    environ = os.environ if environ is None else environ
    value = environ.get(SERVICE_FD_ENV, "")
    if not value.isdigit():
        print(
            f"{SERVICE_FD_ENV} is not set: a service is started by a schedr worker", file=sys.stderr
        )
        return _EXIT_USAGE
    sock = socket.socket(fileno=int(value))
    sock.set_inheritable(False)
    sock.setblocking(True)
    control = Channel(sock)
    try:
        hooks = _Hooks(start, on_closed, stop, on_drained, on_resumed)
        return asyncio.run(_run(control, on_call, (name, versions), hooks))
    except ProtocolError as e:
        print(f"worker control socket: {e}", file=sys.stderr)
        return _EXIT_FAILED
    finally:
        control.close()


async def _run(
    control: Channel,
    on_call: OnCall,
    named: tuple[str, Sequence[str]],
    hooks: _Hooks,
) -> int:
    name, versions = named
    start, on_closed, stop = hooks.start, hooks.on_closed, hooks.stop
    from schedr_extension import _plugin_version  # noqa: PLC0415 (a cycle otherwise)

    hello: Message = {
        "type": "hello",
        "schedr_version": _plugin_version(),
        "mode": "service",
        "protocol": SERVICE_PROTOCOL,
        "name": name,
        "versions": [*versions],
    }
    await asyncio.to_thread(control.send, hello)
    reply = await asyncio.to_thread(control.receive)
    if reply["type"] == "refused":
        print(f"the worker refused the service: {reply.get('reason', '')}", file=sys.stderr)
        return _EXIT_USAGE
    if reply["type"] != "welcome":
        raise ProtocolError(f"expected `welcome`, got `{reply['type']}`")
    secrets = given_secrets(reply, "`welcome`")
    context = ServiceContext(secrets=secrets, _control=control)
    loop = asyncio.get_running_loop()
    stopping = asyncio.Event()
    with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
        loop.add_signal_handler(signal.SIGTERM, stopping.set)
    attempts: set[asyncio.Task[None]] = set()
    gone = context._gone
    times = _CallTimes()

    def connected(message: Message, fd: int) -> None:
        task = obj(message.get("task") or {}, "`attempt`: `task`")
        name, version = task.get("name"), task.get("version")
        attempt = ServiceAttempt(
            text(message, "attempt_id"),
            name if isinstance(name, str) else "",
            version if isinstance(version, str) else "",
        )
        handled = loop.create_task(_attempt(control, attempt, fd, (on_call, times), on_closed))
        attempts.add(handled)
        handled.add_done_callback(attempts.discard)

    told_tasks: set[asyncio.Future[None]] = set()
    # One hook at a time, in the order told (agy's review: a quick resume ran before the drain's
    # hook had ended, and left it paused).
    in_order = asyncio.Lock()

    async def in_turn(hook: Callable[[], Awaitable[None]]) -> None:
        async with in_order:
            await hook()

    def told(kind: str) -> None:
        hook = hooks.on_drained if kind == "drained" else hooks.on_resumed
        if hook is not None:
            running: asyncio.Future[None] = asyncio.ensure_future(in_turn(hook))
            told_tasks.add(running)
            running.add_done_callback(told_tasks.discard)

    # Read from before its start (agy's review: `context.secret` in `start` waited for an
    # answer no one read).
    reading = threading.Thread(
        target=_read_control,
        args=(control, loop, (connected, context._answers, gone, told)),
        name="schedr-service-control",
        daemon=True,
    )
    reading.start()
    if start is not None:
        await start(context)
    await asyncio.to_thread(control.send, {"type": "ready"})
    sending = loop.create_task(_send_call_times(control, times))
    waiting = [loop.create_task(stopping.wait()), loop.create_task(gone.wait())]
    await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
    for waited in [*waiting, sending]:
        waited.cancel()
    if stop is not None and stopping.is_set():
        await stop()
    for attempt in list(attempts):
        attempt.cancel()
    return _EXIT_OK if stopping.is_set() else _EXIT_FAILED


def _read_control(
    control: Channel,
    loop: asyncio.AbstractEventLoop,
    to: tuple[
        Callable[[Message, int], None],
        asyncio.Queue[Message],
        asyncio.Event,
        Callable[[str], None],
    ],
) -> None:
    connected, answers, gone, told = to
    try:
        while True:
            # Close-on-exec as they arrive (Grok's review: an attempt's end waiting in the queue
            # was inherited by what the service started meanwhile).
            message, fds = control.receive_with_fds(1, socket.MSG_CMSG_CLOEXEC)
            kind = message["type"]
            if kind == "attempt_connected" and fds:
                loop.call_soon_threadsafe(connected, message, fds[0])
            elif kind in ("secret", "secret_failed"):
                loop.call_soon_threadsafe(answers.put_nowait, message)
            elif kind in ("drained", "resumed"):
                loop.call_soon_threadsafe(told, kind)
            else:
                for fd in fds:
                    os.close(fd)
    except ChannelClosedError, ProtocolError, OSError:
        loop.call_soon_threadsafe(gone.set)


async def _attempt(
    control: Channel,
    attempt: ServiceAttempt,
    fd: int,
    calls_to: tuple[OnCall, _CallTimes],
    on_closed: Callable[[ServiceAttempt], Awaitable[None]] | None,
) -> None:
    on_call, times = calls_to
    sock = socket.socket(fileno=fd)
    sock.set_inheritable(False)
    sock.setblocking(False)
    reader, writer = await asyncio.open_unix_connection(sock=sock)
    # Taken: its plugin may start (DN32).
    accepted = {"type": "attempt_accepted", "attempt_id": attempt.attempt_id}
    await asyncio.to_thread(control.send, accepted)
    writing = asyncio.Lock()
    calls: set[asyncio.Task[None]] = set()

    async def answer(message: Mapping[str, object]) -> None:
        async with writing:
            writer.write(_frame(message))
            await writer.drain()

    async def handle(call_id: int, method: str, args: object) -> None:
        began = time.monotonic()
        try:
            try:
                result = await on_call(attempt, method, args)
            finally:
                # Its handler's time, once, whatever its end (agy's review: an answer that
                # couldn't be sent timed it twice).
                times.add(method, time.monotonic() - began)
            json.dumps(result, allow_nan=False)
            await answer({"type": "answer", "id": call_id, "result": result})
        except ConnectionError, asyncio.CancelledError:
            raise
        except Exception as e:  # the handler's own failure: the call's
            await answer(
                {"type": "call_failed", "id": call_id, "message": f"{type(e).__name__}: {e}"}
            )

    try:
        while (message := await _read_frame(reader)) is not None:
            if message["type"] != "call":
                continue
            task = asyncio.create_task(
                handle(whole(message, "id"), text(message, "method"), message.get("args"))
            )
            calls.add(task)
            task.add_done_callback(calls.discard)
    except ConnectionError, ProtocolError:
        pass
    finally:
        for task in list(calls):
            task.cancel()
        writer.close()
        if on_closed is not None:
            with contextlib.suppress(Exception):
                await on_closed(attempt)


class ServiceCalls:
    """A plugin's connection to its extension's service: calls by ID, several at once."""

    def __init__(self, fd: int) -> None:
        """Takes the plugin's end of its attempt's connection.

        Args:
            fd: The descriptor (``SCHEDR_SERVICE_FD``).
        """
        # Not passed on to what the plugin starts, from the first moment (agy's review).
        os.set_inheritable(fd, False)
        self._fd = fd
        self._next = 1
        self._waiting: dict[int, asyncio.Future[ExtensionJson]] = {}
        self._writer: asyncio.StreamWriter | None = None
        self._writing: asyncio.Lock | None = None
        self._reading: asyncio.Task[None] | None = None
        self._opening: asyncio.Future[asyncio.StreamWriter] | None = None
        self._closed: str | None = None

    async def _open(self) -> asyncio.StreamWriter:
        # Opened once, by whichever call comes first; the others wait for it (two calls started
        # together would open the descriptor twice, and close it twice).
        if self._opening is None:
            self._opening = asyncio.ensure_future(self._really_open())
        return await self._opening

    async def _really_open(self) -> asyncio.StreamWriter:
        sock = socket.socket(fileno=self._fd)
        sock.set_inheritable(False)
        sock.setblocking(False)
        reader, writer = await asyncio.open_unix_connection(sock=sock)
        self._writer = writer
        self._writing = asyncio.Lock()
        self._reading = asyncio.create_task(self._read(reader))
        return writer

    async def _read(self, reader: asyncio.StreamReader) -> None:
        why = "the service closed the connection"
        try:
            while (message := await _read_frame(reader)) is not None:
                call_id = message.get("id")
                if not isinstance(call_id, int) or isinstance(call_id, bool):
                    # Grok's review: a bad id ended the reader, its calls waiting for ever.
                    raise ProtocolError(f"an answer's id isn't a number: {call_id!r}")
                waiting = self._waiting.pop(call_id, None)
                if waiting is None or waiting.done():
                    continue
                if message["type"] == "answer":
                    waiting.set_result(message.get("result"))
                else:
                    waiting.set_exception(ServiceError(str(message.get("message", ""))))
        except (ConnectionError, ProtocolError) as e:
            why = f"the service's connection broke: {e}"
        self._closed = why
        for waiting in self._waiting.values():
            if not waiting.done():
                waiting.set_exception(ServiceError(why))
        self._waiting.clear()

    async def call(self, method: str, args: object = None) -> ExtensionJson:
        """Calls the service and waits for its answer.

        Args:
            method: What is asked for.
            args: Its arguments (JSON).

        Returns:
            The service's result.

        Raises:
            ServiceError: When the service's handler failed, or the connection is gone (the
                service crashed or was stopped).
        """
        if self._closed is not None:
            raise ServiceError(self._closed)
        writer = await self._open()
        # Closed while it opened (agy's review: the call then waited for an answer for ever).
        if self._closed is not None:
            raise ServiceError(self._closed)
        call_id = self._next
        self._next += 1
        frame = _frame({"type": "call", "id": call_id, "method": method, "args": args})
        answered: asyncio.Future[ExtensionJson] = asyncio.get_running_loop().create_future()
        self._waiting[call_id] = answered
        assert self._writing is not None
        try:
            async with self._writing:
                writer.write(frame)
                await writer.drain()
        except ConnectionError as e:
            self._waiting.pop(call_id, None)
            raise ServiceError(f"the service's connection broke: {e}") from e
        return await answered
