import json
import socket
from collections.abc import Iterator
from pathlib import Path

import pytest

from schedr.runner.protocol import MAX_MESSAGE_SIZE, Channel, ProtocolError, connect


@pytest.fixture
def pair() -> Iterator[tuple[Channel, socket.socket]]:
    ours, theirs = socket.socketpair()
    channel = Channel(ours)
    yield channel, theirs
    channel.close()
    theirs.close()


def frame(body: bytes) -> bytes:
    return len(body).to_bytes(4, "big") + body


def test_send_writes_a_length_prefixed_compact_json_object(
    pair: tuple[Channel, socket.socket],
) -> None:
    channel, theirs = pair
    channel.send({"type": "hello", "text": "zażółć"})
    data = theirs.recv(1000)
    body = json.dumps(
        {"type": "hello", "text": "zażółć"}, ensure_ascii=False, separators=(",", ":")
    ).encode()
    assert data == frame(body)


def test_receive_reads_one_message_at_a_time(pair: tuple[Channel, socket.socket]) -> None:
    channel, theirs = pair
    theirs.sendall(frame(b'{"type":"welcome"}') + frame(b'{"type":"parse","files":[]}'))
    assert channel.receive() == {"type": "welcome"}
    assert channel.receive() == {"type": "parse", "files": []}


def test_receive_handles_messages_split_across_reads(pair: tuple[Channel, socket.socket]) -> None:
    channel, theirs = pair
    data = frame(b'{"type":"welcome"}')
    theirs.sendall(data[:2])
    theirs.sendall(data[2:7])
    theirs.sendall(data[7:])
    assert channel.receive() == {"type": "welcome"}


def test_round_trip_between_two_channels() -> None:
    a, b = socket.socketpair()
    left, right = Channel(a), Channel(b)
    try:
        left.send({"type": "x", "n": [1, 2]})
        assert right.receive() == {"type": "x", "n": [1, 2]}
    finally:
        left.close()
        right.close()


@pytest.mark.parametrize(
    ("data", "message"),
    [
        (b"", "the other side closed the connection"),
        (b"\x00\x00", "the other side closed the connection"),
        ((20).to_bytes(4, "big") + b'{"type":"w', "the other side closed the connection"),
        (frame(b"not json"), "isn't valid JSON"),
        (frame(b"\xff\xfe"), "isn't valid JSON"),
        (frame(b"[1]"), "must be a JSON object with a string `type`"),
        (frame(b'{"type": 5}'), "must be a JSON object with a string `type`"),
        ((MAX_MESSAGE_SIZE + 1).to_bytes(4, "big"), "is over the limit"),
    ],
)
def test_receive_rejects_broken_input(
    pair: tuple[Channel, socket.socket], data: bytes, message: str
) -> None:
    channel, theirs = pair
    theirs.sendall(data)
    theirs.shutdown(socket.SHUT_WR)
    with pytest.raises(ProtocolError, match=message):
        channel.receive()


def test_send_rejects_messages_over_the_limit(
    pair: tuple[Channel, socket.socket], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("schedr.runner.protocol.MAX_MESSAGE_SIZE", 10)
    channel, _ = pair
    with pytest.raises(ProtocolError, match="is over the limit of 10 bytes"):
        channel.send({"type": "too long for ten"})


def test_connect_reaches_a_listening_socket(tmp_path: Path) -> None:
    path = str(tmp_path / "agent.sock")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(path)
    server.listen(1)
    try:
        channel = connect(path)
        peer, _ = server.accept()
        channel.send({"type": "hello"})
        assert Channel(peer).receive() == {"type": "hello"}
        channel.close()
        peer.close()
    finally:
        server.close()


def test_connect_fails_without_a_listener(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        connect(str(tmp_path / "missing.sock"))
