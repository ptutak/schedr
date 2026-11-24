"""The agent <-> runner channel: length-prefixed JSON over a Unix socket.

Each message is a 4-byte big-endian length followed by that many bytes of UTF-8 JSON, an
object tagged by ``type`` (the shapes are ``schema/runner.schema.json``, generated from
``schedr-model``, ``runner.rs``).
"""

import array
import json
import os
import socket
import threading
from collections.abc import Mapping
from typing import Protocol

FD_ENV = "SCHEDR_AGENT_FD"
"""The variable naming the socket the agent hands a runner or a plugin."""

MAX_MESSAGE_SIZE = 64 * 1024 * 1024
"""The largest message either side accepts, in bytes (as in ``schedr-model``)."""

_HEADER = 4

type Wire = bool | int | float | str | list[Wire] | dict[str, Wire] | None
"""A JSON value as the channel carries it: what ``json`` reads and writes, nothing else."""

type Message = dict[str, Wire]
"""A message as received: a JSON object."""


class Asking(Protocol):
    """What a task needs of the channel: a task's whole channel, or a watcher's part of a host's."""

    def ask(self, message: Mapping[str, Wire]) -> Message:
        """Sends a request and receives its answer."""
        ...

    def send(self, message: Mapping[str, Wire]) -> None:
        """Sends one message."""
        ...


class Telling(Asking, Protocol):
    """A watcher's channel, which also tells what a trigger found (M5.2h2)."""

    async def tell_event(self, message: Mapping[str, Wire], in_flight: int) -> None:
        """Sends an event, then waits while ``in_flight`` of its events aren't stored yet."""
        ...

    async def all_stored(self) -> None:
        """Waits until every event it sent is stored: before telling how its trigger ended."""
        ...


class ProtocolError(Exception):
    """The other side closed the connection or sent something that isn't a valid message."""


class ChannelClosedError(ProtocolError):
    """The other side closed the connection between messages (the fork server's way to stop)."""


def _where(message: Mapping[str, Wire]) -> str:
    """A field's message, as its errors name it: its type, if it has one (not a nested object)."""
    kind = message.get("type")
    return f"`{kind}`: " if isinstance(kind, str) else ""


def text(message: Mapping[str, Wire], key: str) -> str:
    """A message's string field.

    Raises:
        ProtocolError: If it is missing or not a string.
    """
    value = message.get(key)
    if not isinstance(value, str):
        raise ProtocolError(f"{_where(message)}`{key}` must be a string")
    return value


def whole(message: Mapping[str, Wire], key: str) -> int:
    """A message's integer field (not a boolean).

    Raises:
        ProtocolError: If it is missing or not an integer.
    """
    value = message.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ProtocolError(f"{_where(message)}`{key}` must be an integer")
    return value


def optional_whole(message: Mapping[str, Wire], key: str) -> int | None:
    """A message's integer field that may be missing or null.

    Raises:
        ProtocolError: If it is there and not an integer.
    """
    return None if message.get(key) is None else whole(message, key)


def flag(message: Mapping[str, Wire], key: str) -> bool:
    """A message's boolean field; missing or null is false.

    Raises:
        ProtocolError: If it is there and not a boolean.
    """
    value = message.get(key)
    if value is None:
        return False
    if not isinstance(value, bool):
        raise ProtocolError(f"{_where(message)}`{key}` must be a boolean")
    return value


def obj(value: Wire, what: str) -> dict[str, Wire]:
    """A value that must be a JSON object.

    Raises:
        ProtocolError: If it isn't.
    """
    if not isinstance(value, dict):
        raise ProtocolError(f"{what} must be an object")
    return value


def listed(value: Wire, what: str) -> list[Wire]:
    """A value that must be a JSON array; null is an empty one.

    Raises:
        ProtocolError: If it isn't.
    """
    if value is None:
        return []
    if not isinstance(value, list):
        raise ProtocolError(f"{what} must be an array")
    return value


def texts(message: Mapping[str, Wire], key: str) -> list[str]:
    """A message's field that must be an array of strings; missing or null is empty.

    Raises:
        ProtocolError: If it isn't.
    """
    items = listed(message.get(key), f"{_where(message)}`{key}`")
    if not all(isinstance(item, str) for item in items):
        raise ProtocolError(f"{_where(message)}`{key}` must hold strings")
    return [item for item in items if isinstance(item, str)]


type SecretName = tuple[str | None, str, str | None]
"""A secret by ``(storage, path, key)``."""


def given_secrets(message: Mapping[str, Wire], what: str) -> dict[SecretName, Wire]:
    """The secrets a message brings (a ``welcome``, an ``extension_task``), by name.

    Raises:
        ProtocolError: If one isn't ``{"name": {"path", "storage"?, "key"?}, "value"}``.
    """
    secrets: dict[SecretName, Wire] = {}
    for given in listed(message.get("secrets"), f"{what}: `secrets`"):
        secret = obj(given, f"{what}: a secret")
        name = obj(secret.get("name"), f"{what}: a secret's `name`")
        path, storage, key = name.get("path"), name.get("storage"), name.get("key")
        if (
            not isinstance(path, str)
            or not (storage is None or isinstance(storage, str))
            or not (key is None or isinstance(key, str))
        ):
            raise ProtocolError(f"{what}: a secret's `path`, `storage` and `key` must be strings")
        secrets[storage, path, key] = secret.get("value")
    return secrets


def as_wire(value: object, what: str) -> Wire:
    """A value built by the caller (a declaration, a result), checked to be plain JSON.

    Mappings and lists or tuples pass, copied as ``dict`` and ``list``; a mapping's number,
    boolean or null key becomes a string, as ``json.dumps`` writes it.

    Raises:
        TypeError: If it holds anything else (as ``json.dumps`` would).
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [as_wire(item, what) for item in value]
    if isinstance(value, Mapping):
        wired: dict[str, Wire] = {}
        for key, item in value.items():
            if isinstance(key, str):
                wired[key] = as_wire(item, what)
            elif key is None or isinstance(key, (bool, int, float)):
                wired[json.dumps(key)] = as_wire(item, what)  # as `json.dumps` writes it
            else:
                raise TypeError(f"{what}: a key {key!r} isn't a string")
        return wired
    raise TypeError(f"{what}: {type(value).__name__} isn't JSON")


class Channel:
    """One end of the channel, over a connected socket."""

    def __init__(self, sock: socket.socket) -> None:
        """Wraps a connected socket.

        Args:
            sock: The socket.
        """
        self._sock = sock
        self._asking = threading.Lock()
        # One message's bytes at a time: a report sent beside a request can't split it.
        self._sending = threading.Lock()

    def ask(self, message: Mapping[str, Wire]) -> Message:
        """Sends a request and receives its answer, one request at a time on this channel.

        A task's threads may read secrets and open connections at once: each answer goes to
        the thread that asked.

        Args:
            message: The request.

        Returns:
            The answer.
        """
        with self._asking:
            self.send(message)
            return self.receive()

    def send(self, message: Mapping[str, Wire]) -> None:
        """Sends one message.

        Args:
            message: A JSON object with a ``type``.

        Raises:
            ProtocolError: If the encoded message is over :data:`MAX_MESSAGE_SIZE`.
        """
        # A lone surrogate (a file name that isn't UTF-8, in an error) can't be sent as itself.
        data = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8", "replace"
        )
        if len(data) > MAX_MESSAGE_SIZE:
            raise ProtocolError(
                f"message of {len(data)} bytes is over the limit of {MAX_MESSAGE_SIZE} bytes"
            )
        with self._sending:
            self._sock.sendall(len(data).to_bytes(_HEADER, "big") + data)

    def receive(self) -> Message:
        """Receives one message.

        Returns:
            The message, a JSON object with a string ``type``.

        Raises:
            ChannelClosedError: If the connection closed before the message.
            ProtocolError: If the connection closed within it, the size is over the limit, or
                the body isn't a JSON object with a ``type``.
        """
        return self.receive_with_fds(0)[0]

    def receive_with_fds(self, max_fds: int, flags: int = 0) -> tuple[Message, list[int]]:
        """Receives one message and the descriptors sent with it (``SCM_RIGHTS``).

        Args:
            max_fds: The most descriptors accepted.
            flags: ``recvmsg`` flags (``socket.MSG_CMSG_CLOEXEC``: the descriptors close-on-exec
                as they arrive).

        Returns:
            The message and the received descriptors, which the caller owns.

        Raises:
            ChannelClosedError: If the connection closed before the message.
            ProtocolError: As :meth:`receive`, or if more descriptors came than accepted (the
                received ones are closed).
        """
        try:
            header, fds, got = _recv_fds(self._sock, max_fds, flags)
        except ConnectionResetError as e:  # closed with our messages unread
            raise ChannelClosedError("the other side closed the connection") from e
        if not header:
            raise ChannelClosedError("the other side closed the connection")
        try:
            if got & socket.MSG_CTRUNC:
                raise ProtocolError(f"more than {max_fds} descriptors came with a message")
            return self._receive_body(header + self._read(_HEADER - len(header))), fds
        except BaseException:
            for fd in fds:
                os.close(fd)
            raise

    def _receive_body(self, header: bytes) -> Message:
        size = int.from_bytes(header, "big")
        if size > MAX_MESSAGE_SIZE:
            raise ProtocolError(
                f"incoming message of {size} bytes is over the limit of {MAX_MESSAGE_SIZE} bytes"
            )
        try:
            message = json.loads(self._read(size))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ProtocolError(f"a message isn't valid JSON: {e}") from e
        if not isinstance(message, dict) or not isinstance(message.get("type"), str):
            raise ProtocolError("a message must be a JSON object with a string `type`")
        return message

    def settimeout(self, seconds: float | None) -> None:
        """Sets how long a send or a receive may wait.

        Args:
            seconds: The time; ``None``: no limit.
        """
        self._sock.settimeout(seconds)

    def close(self) -> None:
        """Closes the socket."""
        self._sock.close()

    def _read(self, size: int) -> bytes:
        chunks: list[bytes] = []
        remaining = size
        while remaining:
            chunk = self._sock.recv(min(remaining, 1 << 20))
            if not chunk:
                raise ProtocolError("the other side closed the connection")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)


def _recv_fds(sock: socket.socket, max_fds: int, flags: int) -> tuple[bytes, list[int], int]:
    """A message's first bytes and the descriptors that came with them.

    ``recvmsg`` itself: ``socket.recv_fds`` doesn't pass ``MSG_CMSG_CLOEXEC`` on (CPython 3.14,
    seen in Grok's review of M5.2i9).
    """
    item = array.array("i").itemsize
    data, ancillary, got, _ = sock.recvmsg(_HEADER, socket.CMSG_SPACE(max_fds * item), flags)
    fds = array.array("i")
    for level, kind, payload in ancillary:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            fds.frombytes(payload[: len(payload) - (len(payload) % item)])
    return data, list(fds), got


def from_fd(fd: int) -> Channel:
    """The channel on the socket the agent handed over (``SCHEDR_AGENT_FD``).

    The descriptor is made blocking (Python's socket would think it is, and a non-blocking
    one would fail its reads), and non-inheritable, so processes the task starts don't get it.

    Args:
        fd: The inherited descriptor of the runner's end of the agent's socket pair.

    Returns:
        The channel.

    Raises:
        OSError: If the descriptor isn't an open socket.
    """
    os.set_blocking(fd, True)  # the flag comes with the descriptor; the channel blocks
    sock = socket.socket(fileno=fd)
    sock.set_inheritable(False)
    return Channel(sock)
