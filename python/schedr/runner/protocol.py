"""The agent <-> runner channel: length-prefixed JSON over a Unix socket.

Each message is a 4-byte big-endian length followed by that many bytes of UTF-8 JSON, an
object tagged by ``type`` (the shapes are ``schema/runner.schema.json``, generated from
``schedr-model``, ``runner.rs``).
"""

import json
import socket
from typing import Any

MAX_MESSAGE_SIZE = 64 * 1024 * 1024
"""The largest message either side accepts, in bytes (as in ``schedr-model``)."""

_HEADER = 4


class ProtocolError(Exception):
    """The other side closed the connection or sent something that isn't a valid message."""


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
            ProtocolError: If the connection closed, the size is over the limit, or the body
                isn't a JSON object with a ``type``.
        """
        size = int.from_bytes(self._read(_HEADER), "big")
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


def connect(path: str) -> Channel:
    """Connects to the agent's socket.

    Args:
        path: The socket's path (``SCHEDR_AGENT_SOCKET``).

    Returns:
        The channel.

    Raises:
        OSError: If the socket can't be reached.
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(path)
    except OSError:
        sock.close()
        raise
    return Channel(sock)
