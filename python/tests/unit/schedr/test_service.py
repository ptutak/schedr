"""An extension's service in Python (decision 518): its handshake with the worker, attempts'
connections handed over and accepted (DN32), calls by ID several at once (DN29), and a
plugin's side of them. The test plays the worker over real socket pairs."""

import asyncio
import json
import os
import socket
import threading
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from schedr.runner.protocol import Channel
from schedr_extension import (
    ExtensionChannel,
    ExtensionError,
    ServiceAttempt,
    ServiceCalls,
    ServiceContext,
    ServiceError,
    serve,
)


@contextmanager
def running_service(
    calls: list[tuple[str, str, object]], closed: list[str]
) -> Iterator[tuple[Channel, threading.Thread, list[int]]]:
    """A service run in a thread on a control socket the test holds as the worker."""
    worker, service = socket.socketpair()
    exit_code: list[int] = []
    started: list[object] = []

    async def on_call(attempt: ServiceAttempt, method: str, args: object) -> object:
        calls.append((attempt.attempt_id, method, args))
        if method == "fail":
            raise ValueError("as asked")
        if method == "slow":
            await asyncio.sleep(0.2)
        return {"method": method, "args": args, "version": attempt.version}

    async def start(context: ServiceContext) -> None:
        started.append(context.secret_value("svc/api", "token"))

    async def on_closed(attempt: ServiceAttempt) -> None:
        closed.append(attempt.attempt_id)

    async def on_drained() -> None:
        closed.append("drained")

    async def on_resumed() -> None:
        closed.append("resumed")

    environ = {"SCHEDR_SERVICE_FD": str(service.detach())}

    def run() -> None:
        exit_code.append(
            serve(
                on_call,
                name="dummy",
                versions=["1.0", "2.0"],
                start=start,
                on_closed=on_closed,
                on_drained=on_drained,
                on_resumed=on_resumed,
                environ=environ,
            )
        )

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    control = Channel(worker)
    hello = control.receive()
    assert hello["type"] == "hello"
    assert (hello["mode"], hello["protocol"], hello["name"]) == ("service", 1, "dummy")
    assert hello["versions"] == ["1.0", "2.0"]
    secret = {"name": {"path": "svc/api", "key": "token"}, "value": "s3cret-token"}
    control.send({"type": "welcome", "secrets": [secret]})
    assert control.receive() == {"type": "ready"}
    assert started == ["s3cret-token"]
    try:
        yield control, thread, exit_code
    finally:
        control.close()
        thread.join(timeout=5)


def connect(control: Channel, attempt_id: str) -> socket.socket:
    """Hands the service an attempt's connection, as the worker does: the plugin's end back."""
    plugin, theirs = socket.socketpair()
    message = {
        "type": "attempt_connected",
        "attempt_id": attempt_id,
        "task": {"name": "dummy", "version": "2.0"},
    }
    body = json.dumps(message).encode()
    socket.send_fds(control._sock, [len(body).to_bytes(4, "big") + body], [theirs.fileno()])
    theirs.close()
    assert control.receive() == {"type": "attempt_accepted", "attempt_id": attempt_id}
    return plugin


def test_a_service_answers_its_attempts_calls_several_at_once() -> None:
    calls: list[tuple[str, str, object]] = []
    closed: list[str] = []
    with running_service(calls, closed) as (control, _thread, _code):
        plugin = connect(control, "a-1")
        service = ServiceCalls(plugin.detach())

        async def calling() -> list[object]:
            slow = asyncio.create_task(service.call("slow", {"n": 1}))
            fast = await service.call("fast", [2])
            with pytest.raises(ServiceError, match="ValueError: as asked"):
                await service.call("fail")
            return [fast, await slow]

        fast, slow = asyncio.run(calling())
        # The fast call's answer came before the slow one's, each by its ID.
        assert fast == {"method": "fast", "args": [2], "version": "2.0"}
        assert slow == {"method": "slow", "args": {"n": 1}, "version": "2.0"}
        assert {c[1] for c in calls} == {"slow", "fast", "fail"}
        assert all(c[0] == "a-1" for c in calls)


def test_an_attempts_connection_closing_is_told_and_the_worker_leaving_ends_it() -> None:
    calls: list[tuple[str, str, object]] = []
    closed: list[str] = []
    with running_service(calls, closed) as (control, thread, exit_code):
        plugin = connect(control, "a-2")
        plugin.close()
        for _ in range(100):
            if closed:
                break
            threading.Event().wait(0.05)
        assert closed == ["a-2"]
        control.close()
        thread.join(timeout=5)
        # The worker gone: the service ends, not as stopped.
        assert exit_code == [1]


def test_a_call_to_a_service_gone_fails_and_a_plugin_without_one_is_told() -> None:
    plugin, service_end = socket.socketpair()
    service_end.close()
    calls = ServiceCalls(plugin.detach())

    async def calling() -> None:
        with pytest.raises(ServiceError, match=r"closed|broke"):
            await calls.call("anything")

    asyncio.run(calling())
    worker, _ = socket.socketpair()
    channel = ExtensionChannel(Channel(worker), {}, None)
    with pytest.raises(ExtensionError, match="no service"):
        _ = channel.service


def test_a_service_started_without_its_socket_says_so() -> None:
    async def on_call(attempt: ServiceAttempt, method: str, args: object) -> object:
        return None

    assert serve(on_call, name="dummy", versions=["1.0"], environ={}) == 2


def test_a_maintenance_drain_and_resume_are_told_to_the_service() -> None:
    calls: list[tuple[str, str, object]] = []
    told: list[str] = []
    with running_service(calls, told) as (control, _thread, _code):
        control.send({"type": "drained"})
        control.send({"type": "resumed"})
        for _ in range(100):
            if len(told) == 2:
                break
            threading.Event().wait(0.05)
        assert told == ["drained", "resumed"]


def test_a_service_reads_a_secret_again_in_its_start() -> None:
    """agy's review: `context.secret` in `start` waited for an answer no one read."""
    worker, service = socket.socketpair()
    environ = {"SCHEDR_SERVICE_FD": str(service.detach())}
    read: list[object] = []

    async def start(context: ServiceContext) -> None:
        read.append(await context.secret("svc/api", "token"))

    async def on_call(attempt: ServiceAttempt, method: str, args: object) -> object:
        return None

    thread = threading.Thread(
        target=lambda: serve(on_call, name="dummy", versions=["1.0"], start=start, environ=environ),
        daemon=True,
    )
    thread.start()
    control = Channel(worker)
    control.settimeout(5)
    assert control.receive()["type"] == "hello"
    control.send({"type": "welcome", "secrets": []})
    asked = control.receive()
    assert asked["type"] == "secret_get"
    control.send({"type": "secret", "value": "fresh"})
    assert control.receive() == {"type": "ready"}
    assert read == ["fresh"]
    control.close()
    thread.join(timeout=5)


def test_drain_hooks_run_in_order_one_at_a_time() -> None:
    """agy's review: a quick resume ran before the drain's hook ended."""
    worker, service = socket.socketpair()
    environ = {"SCHEDR_SERVICE_FD": str(service.detach())}
    order: list[str] = []

    async def on_drained() -> None:
        order.append("drained_start")
        await asyncio.sleep(0.2)
        order.append("drained_end")

    async def on_resumed() -> None:
        order.append("resumed")

    async def on_call(attempt: ServiceAttempt, method: str, args: object) -> object:
        return None

    def run() -> None:
        serve(
            on_call,
            name="dummy",
            versions=["1.0"],
            on_drained=on_drained,
            on_resumed=on_resumed,
            environ=environ,
        )

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    control = Channel(worker)
    control.settimeout(5)
    assert control.receive()["type"] == "hello"
    control.send({"type": "welcome", "secrets": []})
    assert control.receive() == {"type": "ready"}
    control.send({"type": "drained"})
    control.send({"type": "resumed"})
    for _ in range(100):
        if len(order) == 3:
            break
        threading.Event().wait(0.05)
    assert order == ["drained_start", "drained_end", "resumed"]
    control.close()
    thread.join(timeout=5)


def test_a_plugins_service_end_is_never_inherited_and_a_close_while_opening_fails_calls() -> None:
    """agy's review: the end stayed inheritable until the first call; a call racing the close
    waited for ever."""
    plugin, service_end = socket.socketpair()
    plugin.set_inheritable(True)
    fd = plugin.detach()
    calls = ServiceCalls(fd)
    assert not os.get_inheritable(fd)
    service_end.shutdown(socket.SHUT_WR)

    async def calling() -> None:
        first = asyncio.create_task(calls.call("a"))
        second = asyncio.create_task(calls.call("b"))
        for call in (first, second):
            with pytest.raises(ServiceError):
                await asyncio.wait_for(call, timeout=5)

    asyncio.run(calling())
    service_end.close()


def test_a_cancelled_secret_read_never_answers_the_next() -> None:
    """Grok's review: the next `secret()` returned the cancelled one's value."""
    worker, service = socket.socketpair()
    control = Channel(service)
    context = ServiceContext(secrets={}, _control=control)
    worker_end = Channel(worker)
    worker_end.settimeout(5)

    async def run() -> object:
        first = asyncio.create_task(context.secret("path-a", "k"))
        asked = await asyncio.to_thread(worker_end.receive)
        assert asked["secret"]["path"] == "path-a"
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(context.secret("path-b", "k"))
        # The first's answer comes late: the second throws it away, then asks.
        await asyncio.to_thread(worker_end.send, {"type": "secret", "value": "VALUE-A"})
        asked = await asyncio.to_thread(worker_end.receive)
        assert asked["secret"]["path"] == "path-b"
        await asyncio.to_thread(worker_end.send, {"type": "secret", "value": "VALUE-B"})
        return await second

    # The answers reach the context's queue through its reader, played here.
    async def with_reader() -> object:
        loop = asyncio.get_running_loop()

        def read() -> None:
            while True:
                try:
                    message = control.receive()
                except Exception:
                    return
                loop.call_soon_threadsafe(context._answers.put_nowait, message)

        threading.Thread(target=read, daemon=True).start()
        return await run()

    assert asyncio.run(with_reader()) == "VALUE-B"


def test_an_answer_with_a_bad_id_fails_the_calls_waiting() -> None:
    """Grok's review: a bad id ended the reader and left its calls waiting for ever."""
    plugin, service_end = socket.socketpair()
    calls = ServiceCalls(plugin.detach())

    async def calling() -> None:
        call = asyncio.create_task(calls.call("m"))
        await asyncio.sleep(0.1)
        body = json.dumps({"type": "answer", "id": "nope", "result": 1}).encode()
        service_end.sendall(len(body).to_bytes(4, "big") + body)
        with pytest.raises(ServiceError, match="isn't a number"):
            await asyncio.wait_for(call, timeout=5)

    asyncio.run(calling())
    service_end.close()


def test_descriptors_received_with_cloexec_are_never_inherited() -> None:
    """Grok's review: an attempt's end waiting in the service's queue was inheritable."""
    a, b = socket.socketpair()
    passed, _kept = socket.socketpair()
    body = json.dumps({"type": "attempt_connected"}).encode()
    socket.send_fds(a, [len(body).to_bytes(4, "big") + body], [passed.fileno()])
    _, fds = Channel(b).receive_with_fds(1, socket.MSG_CMSG_CLOEXEC)
    assert len(fds) == 1
    assert not os.get_inheritable(fds[0])
    os.close(fds[0])


def test_each_calls_time_reaches_the_worker_as_a_builtin_metric() -> None:
    """M5.2i12: `serve` times each call by method, the failed ones too, and sends the times as
    `service_call_seconds` points at most once a second."""
    calls: list[tuple[str, str, object]] = []
    closed: list[str] = []
    with running_service(calls, closed) as (control, _thread, _code):
        plugin = connect(control, "a-1")
        service = ServiceCalls(plugin.detach())

        async def calling() -> None:
            await service.call("slow")
            with pytest.raises(ServiceError):
                await service.call("fail")

        asyncio.run(calling())
        # Sent once a second: the two calls' times may come in one message or two (agy's
        # review: a single read was flaky across a second's boundary).
        points: dict[str, dict[str, object]] = {}
        while set(points) != {"slow", "fail"}:
            told = control.receive()
            assert told["type"] == "metrics"
            for point in told["points"]:
                assert point["labels"]["method"] not in points, "timed once"
                points[point["labels"]["method"]] = point
        assert all(p["name"] == "service_call_seconds" for p in points.values())
        slow = points["slow"]["value"]
        assert isinstance(slow, float)
        assert slow >= 0.2
