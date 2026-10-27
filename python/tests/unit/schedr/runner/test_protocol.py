import json
import os
import socket
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

from schedr.runner.protocol import (
    MAX_MESSAGE_SIZE,
    Channel,
    ChannelClosedError,
    ProtocolError,
    from_fd,
)


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


def test_from_fd_uses_the_handed_over_socket_and_keeps_it_from_children() -> None:
    ours, theirs = socket.socketpair()
    fd = os.dup(theirs.fileno())
    os.set_inheritable(fd, True)
    theirs.close()
    channel = from_fd(fd)
    assert not os.get_inheritable(fd)
    channel.send({"type": "hello"})
    assert Channel(ours).receive() == {"type": "hello"}
    channel.close()
    ours.close()


def test_from_fd_fails_for_a_descriptor_that_isnt_a_socket(tmp_path: Path) -> None:
    path = tmp_path / "plain.txt"
    path.write_text("x")
    with path.open() as file, pytest.raises(OSError, match="non-socket"):
        from_fd(os.dup(file.fileno()))


def test_descriptors_sent_with_a_message_are_received(pair: tuple[Channel, socket.socket]) -> None:
    channel, theirs = pair
    read, write = os.pipe()
    body = b'{"type":"fork"}'
    socket.send_fds(theirs, [frame(body)[:2]], [write])
    theirs.sendall(frame(body)[2:])
    os.close(write)
    message, fds = channel.receive_with_fds(4)
    assert message == {"type": "fork"}
    (received,) = fds
    os.write(received, b"x")
    os.close(received)
    assert os.read(read, 1) == b"x"
    os.close(read)


def test_more_descriptors_than_accepted_are_closed_and_refused(
    pair: tuple[Channel, socket.socket],
) -> None:
    channel, theirs = pair
    before = len(os.listdir("/proc/self/fd"))
    fds = [os.open(os.devnull, os.O_RDONLY) for _ in range(3)]
    socket.send_fds(theirs, [frame(b'{"type":"fork"}')], fds)
    for fd in fds:
        os.close(fd)
    with pytest.raises(ProtocolError, match="more than 1 descriptors"):
        channel.receive_with_fds(1)
    assert len(os.listdir("/proc/self/fd")) == before


def test_a_close_between_messages_is_channel_closed(pair: tuple[Channel, socket.socket]) -> None:
    channel, theirs = pair
    theirs.shutdown(socket.SHUT_WR)
    with pytest.raises(ChannelClosedError):
        channel.receive()


def test_a_close_with_our_messages_unread_is_channel_closed(
    pair: tuple[Channel, socket.socket],
) -> None:
    channel, theirs = pair
    channel.send({"type": "hello"})
    theirs.close()
    with pytest.raises(ChannelClosedError):
        channel.receive()


def test_a_close_within_a_message_isnt_channel_closed(pair: tuple[Channel, socket.socket]) -> None:
    channel, theirs = pair
    theirs.sendall(frame(b'{"type":"x"}')[:2])
    theirs.shutdown(socket.SHUT_WR)
    with pytest.raises(ProtocolError, match="closed") as raised:
        channel.receive()
    assert not isinstance(raised.value, ChannelClosedError)


def test_from_fd_makes_a_non_blocking_descriptor_blocking() -> None:
    ours, theirs = socket.socketpair()
    os.set_blocking(ours.fileno(), False)
    channel = from_fd(ours.detach())
    theirs.settimeout(5)
    try:
        threading.Timer(0.2, lambda: theirs.sendall(b'\x00\x00\x00\x0f{"type":"late"}')).start()
        assert channel.receive() == {"type": "late"}  # waited instead of failing at once
    finally:
        channel.close()
        theirs.close()
