"""The agent <-> runner channel: length-prefixed JSON over a Unix socket.

Each message is a 4-byte big-endian length followed by that many bytes of UTF-8 JSON, an
object tagged by ``type`` (the shapes are ``schema/runner.schema.json``, generated from
``schedr-model``, ``runner.rs``).
"""

import json
import os
import socket
from typing import Any

MAX_MESSAGE_SIZE = 64 * 1024 * 1024
"""The largest message either side accepts, in bytes (as in ``schedr-model``)."""

_HEADER = 4


class ProtocolError(Exception):
    """The other side closed the connection or sent something that isn't a valid message."""


class ChannelClosedError(ProtocolError):
    """The other side closed the connection between messages (the fork server's way to stop)."""


class Channel:
    """One end of the channel, over a connected socket."""

    def __init__(self, sock: socket.socket) -> None:
        """Wraps a connected socket.

        Args:
            sock: The socket.
        """
        self._sock = sock

    def send(self, message: dict[str, Any]) -> None:
        """Sends one message.

        Args:
            message: A JSON object with a ``type``.

        Raises:
            ProtocolError: If the encoded message is over :data:`MAX_MESSAGE_SIZE`.
        """
        data = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode()
        if len(data) > MAX_MESSAGE_SIZE:
            raise ProtocolError(
                f"message of {len(data)} bytes is over the limit of {MAX_MESSAGE_SIZE} bytes"
            )
        self._sock.sendall(len(data).to_bytes(_HEADER, "big") + data)

    def receive(self) -> dict[str, Any]:
        """Receives one message.

        Returns:
            The message, a JSON object with a string ``type``.

        Raises:
            ChannelClosedError: If the connection closed before the message.
            ProtocolError: If the connection closed within it, the size is over the limit, or
                the body isn't a JSON object with a ``type``.
        """
        return self.receive_with_fds(0)[0]

    def receive_with_fds(self, max_fds: int) -> tuple[dict[str, Any], list[int]]:
        """Receives one message and the descriptors sent with it (``SCM_RIGHTS``).

        Args:
            max_fds: The most descriptors accepted.

        Returns:
            The message and the received descriptors, which the caller owns.

        Raises:
            ChannelClosedError: If the connection closed before the message.
            ProtocolError: As :meth:`receive`, or if more descriptors came than accepted (the
                received ones are closed).
        """
        try:
            header, fds, flags, _ = socket.recv_fds(self._sock, _HEADER, max_fds)
        except ConnectionResetError as e:  # closed with our messages unread
            raise ChannelClosedError("the other side closed the connection") from e
        if not header:
            raise ChannelClosedError("the other side closed the connection")
        try:
            if flags & socket.MSG_CTRUNC:
                raise ProtocolError(f"more than {max_fds} descriptors came with a message")
            return self._receive_body(header + self._read(_HEADER - len(header))), fds
        except BaseException:
            for fd in fds:
                os.close(fd)
            raise

    def _receive_body(self, header: bytes) -> dict[str, Any]:
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
