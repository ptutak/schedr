"""What the watcher host does by itself; the rest with the agent in ``tests/watchers.rs``."""

import asyncio
import concurrent.futures
import contextlib
import ctypes.util
import io
import os
import platform
import socket
import subprocess
import threading
import time
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

import pytest

from schedr.runner import watchers
from schedr.runner.protocol import Channel
from schedr.runner.waiting import (
    ProcessInWaitStepError,
    StepEndedError,
    WatcherMovedError,
    waited,
)
from schedr.sensors import NOT_YET


def _host() -> tuple[watchers._Host, Channel]:
    ours, theirs = socket.socketpair()
    return watchers._Host(Channel(ours)), Channel(theirs)


def test_text_goes_to_the_watcher_it_is_printed_for_and_else_to_the_host() -> None:
    host, agent = _host()
    own = io.StringIO()
    routed = watchers._Routed("stdout", own)
    routed.write("the host's\n")
    token = watchers._WATCHER.set(watchers.WatcherChannel(host, 7))
    try:
        routed.write("a watcher's\n")
    finally:
        watchers._WATCHER.reset(token)
    assert own.getvalue() == "the host's\n"
    assert agent.receive() == {
        "type": "watcher_message",
        "watcher": 7,
        "message": {"type": "watcher_log", "stream": "stdout", "text": "a watcher's\n"},
    }


def test_text_that_isnt_utf_8_and_huge_text_get_through() -> None:
    host, agent = _host()
    routed = watchers._Routed("stderr", io.StringIO())
    token = watchers._WATCHER.set(watchers.WatcherChannel(host, 1))
    size = watchers._LOG_CHUNK + 5
    received: list[str] = []

    def read() -> None:
        for _ in range(3):
            received.append(agent.receive()["message"]["text"])

    reading = threading.Thread(target=read)
    reading.start()
    try:
        routed.write("\udc80")
        routed.write("x" * size)
    finally:
        watchers._WATCHER.reset(token)
    reading.join()
    assert received[0] == "\\udc80"
    assert [len(text) for text in received[1:]] == [watchers._LOG_CHUNK, 5]


def test_an_answer_goes_to_who_asked_and_one_for_a_watcher_gone_is_dropped() -> None:
    host, agent = _host()
    own = watchers.WatcherChannel(host, 3)
    host._watchers[3] = (own, None)
    answers: list[dict[str, str]] = []
    asking = threading.Thread(target=lambda: answers.append(own.ask({"type": "secret_get"})))
    asking.start()
    assert agent.receive()["message"] == {"type": "secret_get"}
    host.answer(9, {"type": "secret", "value": "not for it"})
    host.answer(3, {"type": "secret", "value": "its own"})
    asking.join()
    assert answers == [{"type": "secret", "value": "its own"}]


def test_a_dag_file_is_imported_once_but_again_after_failing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    host, _ = _host()
    calls: list[str] = []

    def importing(root: str, root_dir: object, path: str) -> list[object]:
        calls.append(path)
        if path == "broken.py":
            raise ImportError("no")
        return []

    monkeypatch.setattr(watchers, "import_dag_entries", importing)
    host.imported("etl", Path("/x"), "a.py")
    host.imported("etl", Path("/x"), "a.py")
    for _ in range(2):
        with pytest.raises(ImportError):
            host.imported("etl", Path("/x"), "broken.py")
    assert calls == ["a.py", "broken.py", "broken.py"]


def test_a_cancelled_watcher_says_so_and_cancelling_one_ended_is_harmless(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    host, agent = _host()

    async def waits(*_args: object) -> dict[str, str]:
        await asyncio.sleep(600)
        return {}

    monkeypatch.setattr(watchers, "execute_watcher", waits)

    async def main() -> None:
        host.start(5, {})
        await asyncio.sleep(0)
        host.cancel(5)
        await asyncio.sleep(0.01)
        host.cancel(5)

    asyncio.run(main())
    assert agent.receive()["message"] == {"type": "watcher_cancelled"}


def test_a_watcher_cancelled_before_its_first_turn_says_so(monkeypatch: pytest.MonkeyPatch) -> None:
    host, agent = _host()
    ran: list[bool] = []

    async def waits(*_args: object) -> dict[str, str]:
        ran.append(True)
        return {}

    monkeypatch.setattr(watchers, "execute_watcher", waits)

    async def main() -> None:
        host.start(5, {})
        host.cancel(5)
        await asyncio.wait(host._ending)

    asyncio.run(main())
    assert ran == []
    assert host._watchers == {}
    assert agent.receive()["message"] == {"type": "watcher_cancelled"}


@pytest.mark.parametrize("cancelled", [False, True])
def test_what_a_watcher_left_running_ends_before_its_end_is_told(
    monkeypatch: pytest.MonkeyPatch, cancelled: bool
) -> None:
    host, agent = _host()
    events: list[str] = []

    async def child() -> None:
        try:
            await asyncio.sleep(600)
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            events.append("child ended")
            raise

    async def starts_a_child(*_args: object) -> dict[str, str]:
        task = asyncio.create_task(child())
        await asyncio.sleep(0)
        events.append(str(task.done()))
        if cancelled:
            await asyncio.sleep(600)
        return {"type": "task_succeeded"}

    monkeypatch.setattr(watchers, "execute_watcher", starts_a_child)

    async def main() -> None:
        host.start(5, {})
        await asyncio.sleep(0.01)
        if cancelled:
            host.cancel(5)
        await asyncio.wait(host._ending)
        events.append("told")

    asyncio.run(main())
    assert events == ["False", "child ended", "told"]
    assert agent.receive()["message"]["type"] == (
        "watcher_cancelled" if cancelled else "task_succeeded"
    )


def test_an_error_naming_a_file_that_isnt_utf_8_still_gets_through() -> None:
    host, agent = _host()
    own = watchers.WatcherChannel(host, 5)
    own.send({"type": "task_failed", "error": {"message": "missing \udc80"}})
    assert agent.receive()["message"]["error"]["message"] == "missing ?"


def test_a_result_too_large_to_send_is_a_failure_not_silence() -> None:
    host, agent = _host()
    own = watchers.WatcherChannel(host, 5)
    watchers._tell(own, {"type": "task_failed", "x": "y" * (watchers.MAX_MESSAGE_SIZE + 1)})
    message = agent.receive()["message"]
    assert message["type"] == "task_failed"
    assert message["kind"] == "output_too_large"


def test_a_poke_stops_between_calls_when_it_is_to_move() -> None:
    calls: list[int] = []
    moving = asyncio.Event()

    async def poke() -> object:
        calls.append(1)
        moving.set()
        return NOT_YET

    async def main() -> None:
        with pytest.raises(WatcherMovedError):
            await waited(poke, {}, {"every_ms": 600_000, "max_wait_ms": 600_000}, moving)

    asyncio.run(main())
    assert calls == [1]


def test_a_value_coming_as_it_is_to_move_is_its_value() -> None:
    moving = asyncio.Event()

    async def poke() -> object:
        moving.set()
        return 7

    assert asyncio.run(waited(poke, {}, {"every_ms": 600_000}, moving)) == 7


def test_a_poke_moving_in_waits_for_go_and_gets_what_is_left(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    host, _ = _host()
    seen: list[object] = []

    async def runs(
        _request: object, _own: object, _entries: object, _moving: object, go: Any
    ) -> dict[str, str]:
        seen.append(await go())
        return {"type": "task_succeeded"}

    monkeypatch.setattr(watchers, "execute_watcher", runs)

    async def main() -> None:
        host.start(5, {}, moved_in=True)
        await asyncio.sleep(0.01)
        assert seen == []
        host.go(5, 1234)
        await asyncio.wait(host._ending)

    asyncio.run(main())
    assert seen == [1234]


def test_losing_the_agent_kills_only_a_group_the_host_leads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    killed: list[int] = []
    ended: list[int] = []
    monkeypatch.setattr(watchers.os, "getpid", lambda: 40)
    monkeypatch.setattr(watchers.os, "getpgrp", lambda: 41)
    monkeypatch.setattr(watchers.os, "killpg", lambda group, _signal: killed.append(group))
    monkeypatch.setattr(watchers.os, "_exit", ended.append)
    watchers._end_the_host()
    assert (killed, ended) == ([], [0])
    monkeypatch.setattr(watchers.os, "getpgrp", lambda: 40)
    watchers._end_the_host()
    assert killed == [40]


def _in_a_wait_step(call: Callable[[], object]) -> object:
    async def function() -> object:
        return call()

    return asyncio.run(waited(function, {}, {}))


@pytest.mark.parametrize(
    "starting",
    [
        lambda: subprocess.run(["true"], check=True),
        lambda: os.system("true"),
        lambda: os.posix_spawn("/bin/true", ["true"], {}),
        lambda: os.spawnv(os.P_WAIT, "/bin/true", ["true"]),
    ],
)
def test_a_wait_step_cant_start_a_process(starting: Callable[[], object]) -> None:
    with pytest.raises(ProcessInWaitStepError, match="can't start a process"):
        _in_a_wait_step(starting)


@pytest.mark.parametrize("factory", [None, watchers.guarded_loop_factory()])
def test_a_wait_step_cant_start_a_process_through_its_loop(
    factory: Callable[[], asyncio.AbstractEventLoop] | None,
) -> None:
    async def function() -> object:
        return await asyncio.create_subprocess_exec("true")

    with pytest.raises(ProcessInWaitStepError):
        asyncio.run(waited(function, {}, {}), loop_factory=factory)


def test_what_a_wait_step_runs_in_a_thread_is_refused_too() -> None:
    async def function() -> object:
        return await asyncio.to_thread(subprocess.run, ["true"], check=True)

    with pytest.raises(ProcessInWaitStepError):
        asyncio.run(waited(function, {}, {}))


def test_helpers_that_wait_for_their_own_command_are_left_alone() -> None:
    assert isinstance(_in_a_wait_step(lambda: platform.platform(aliased=True, terse=True)), str)
    _in_a_wait_step(lambda: ctypes.util.find_library("no_such_library"))


def test_outside_a_wait_step_processes_start_as_usual() -> None:
    _in_a_wait_step(lambda: None)
    assert subprocess.run(["true"], check=True).returncode == 0
    factory = watchers.guarded_loop_factory()

    async def main() -> int:
        process = await asyncio.create_subprocess_exec("true")
        return await process.wait()

    assert asyncio.run(main(), loop_factory=factory) == 0


def test_a_thread_outliving_its_step_prints_to_the_hosts_log() -> None:
    host, _agent = _host()
    own = io.StringIO()
    routed = watchers._Routed("stdout", own)
    watcher = watchers.WatcherChannel(host, 7)
    watcher.ended = True
    token = watchers._WATCHER.set(watcher)
    try:
        routed.write("too late\n")
    finally:
        watchers._WATCHER.reset(token)
    assert own.getvalue() == "too late\n"


async def _with_watcher_threads[T](main: Callable[[], Coroutine[Any, Any, T]]) -> T:
    asyncio.get_running_loop().set_default_executor(watchers._WatcherThreads())
    return await main()


def _watching(host: watchers._Host, watcher: int) -> watchers.WatcherChannel:
    own = watchers.WatcherChannel(host, watcher)
    watchers._WATCHER.set(own)
    return own


def _spin(ended: list[str]) -> None:
    try:
        while True:
            time.sleep(0.005)
    except StepEndedError:
        ended.append("stopped")
        raise


def test_a_timed_out_to_thread_call_stops_and_its_thread_serves_the_next() -> None:
    ended: list[str] = []

    async def main() -> str | None:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.to_thread(_spin, ended), 0.05)
        while not ended:
            await asyncio.sleep(0.005)
        return await asyncio.to_thread(lambda: threading.current_thread().name)

    assert asyncio.run(_with_watcher_threads(main)) is not None
    assert ended == ["stopped"]


def test_except_exception_doesnt_keep_a_stopped_call_going() -> None:
    ended: list[str] = []

    def keeps_trying() -> None:
        try:
            while True:
                with contextlib.suppress(Exception):  # what authors write
                    time.sleep(0.005)
        finally:
            ended.append("stopped")

    async def main() -> None:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.to_thread(keeps_trying), 0.05)
        while not ended:
            await asyncio.sleep(0.005)

    asyncio.run(_with_watcher_threads(main))


def test_a_call_still_queued_when_cancelled_never_runs() -> None:
    ran: list[str] = []
    release = threading.Event()

    async def main() -> None:
        asyncio.get_running_loop().set_default_executor(watchers._WatcherThreads(max_workers=1))
        busy = asyncio.to_thread(release.wait)
        running = asyncio.ensure_future(busy)
        await asyncio.sleep(0.01)
        queued = asyncio.ensure_future(asyncio.to_thread(ran.append, "ran"))
        await asyncio.sleep(0.01)
        queued.cancel()
        await asyncio.wait({queued})
        release.set()
        await running
        await asyncio.to_thread(time.sleep, 0)

    asyncio.run(main())
    assert ran == []


def test_a_call_runs_for_the_watcher_that_made_it_not_for_the_one_a_thread_started_for() -> None:
    host, _agent = _host()

    async def main() -> list[int | None]:
        def who() -> int | None:
            own = watchers._WATCHER.get()
            return None if own is None else own._watcher

        async def from_watcher(watcher: int) -> int | None:
            _watching(host, watcher)
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, who)

        asyncio.get_running_loop().set_default_executor(watchers._WatcherThreads(max_workers=1))
        return [
            await asyncio.create_task(from_watcher(1)),
            await asyncio.create_task(from_watcher(2)),
        ]

    assert asyncio.run(main()) == [1, 2]


def test_the_end_of_a_step_waits_for_its_thread_and_says_when_one_wont_stop() -> None:
    host, _agent = _host()
    ended: list[str] = []
    release = threading.Event()

    async def main() -> tuple[bool, bool]:
        asyncio.get_running_loop().set_default_executor(watchers._WatcherThreads())
        own = _watching(host, 1)
        loop = asyncio.get_running_loop()
        stops = loop.run_in_executor(None, _spin, ended)
        await asyncio.sleep(0.01)
        stopped = await watchers._stopped_threads(own, 5)
        await asyncio.wait({stops})
        assert isinstance(stops.exception(), StepEndedError)
        blocked = loop.run_in_executor(None, release.wait)
        await asyncio.sleep(0.01)
        stuck = await watchers._stopped_threads(own, 0.05)
        release.set()
        await asyncio.wait({blocked})
        return stopped, stuck

    assert asyncio.run(main()) == (True, False)
    assert ended == ["stopped"]


def test_a_call_stopped_while_asking_ends_and_the_next_ask_doesnt_get_its_answer() -> None:
    host, agent = _host()
    stopped: list[str] = []

    def asks(own: watchers.WatcherChannel) -> None:
        try:
            own.ask({"type": "secret_get", "secret": "first"})
        except StepEndedError:
            stopped.append("stopped")
            raise

    async def main() -> dict[str, Any]:
        asyncio.get_running_loop().set_default_executor(watchers._WatcherThreads())
        own = _watching(host, 1)
        loop = asyncio.get_running_loop()
        first = loop.run_in_executor(None, asks, own)
        await asyncio.to_thread(agent.receive)
        first.cancel()
        while not stopped:
            await asyncio.sleep(0.005)
        second = loop.run_in_executor(None, own.ask, {"type": "secret_get", "secret": "second"})
        await asyncio.sleep(0.02)
        own.answers.put({"value": "the first's"})
        await asyncio.to_thread(agent.receive)
        own.answers.put({"value": "the second's"})
        return await second

    assert asyncio.run(main()) == {"value": "the second's"}


def test_a_call_asked_to_stop_before_it_got_to_run_doesnt_run() -> None:
    call: watchers._ThreadCall[None] = watchers._ThreadCall()
    running: concurrent.futures.Future[None] = concurrent.futures.Future()
    running.set_running_or_notify_cancel()
    call.queued(running)
    assert not call.cancel()
    ran: list[int] = []
    with pytest.raises(StepEndedError):
        call.run(lambda: ran.append(1))
    assert ran == []


def test_nothing_is_raised_in_a_thread_after_its_call_returned() -> None:
    call: watchers._ThreadCall[int] = watchers._ThreadCall()
    running: concurrent.futures.Future[int] = concurrent.futures.Future()
    running.set_running_or_notify_cancel()
    call.queued(running)
    assert call.run(lambda: 1) == 1
    call.cancel()
    for _ in range(1000):
        time.sleep(0)


def test_a_call_is_raised_in_once_however_often_it_is_cancelled() -> None:
    raised: list[str] = []
    started = threading.Event()
    goes_on = threading.Event()

    def goes_on_after_it() -> None:
        started.set()
        try:
            while True:
                time.sleep(0.001)
        except StepEndedError:
            raised.append("raised")
        goes_on.wait(5)
        for _ in range(100):
            time.sleep(0.001)

    async def main() -> None:
        asyncio.get_running_loop().set_default_executor(watchers._WatcherThreads())
        own = _watching(*_host_only())
        call = asyncio.get_running_loop().run_in_executor(None, goes_on_after_it)
        await asyncio.to_thread(started.wait)
        for held in tuple(own.calls):
            held.cancel()
        while not raised:
            await asyncio.sleep(0.001)
        for held in tuple(own.calls):
            held.cancel()
        goes_on.set()
        await call

    asyncio.run(main())
    assert raised == ["raised"]


def _host_only() -> tuple[watchers._Host, int]:
    return _host()[0], 1


def test_a_call_stopped_waiting_for_its_turn_to_ask_never_asks() -> None:
    host, agent = _host()

    async def main() -> None:
        asyncio.get_running_loop().set_default_executor(watchers._WatcherThreads())
        own = _watching(host, 1)
        loop = asyncio.get_running_loop()
        first = loop.run_in_executor(None, own.ask, {"type": "secret_get", "secret": "first"})
        assert (await asyncio.to_thread(agent.receive))["message"]["secret"] == "first"
        second = loop.run_in_executor(None, own.ask, {"type": "secret_get", "secret": "second"})
        await asyncio.sleep(0.02)
        second.cancel()
        await asyncio.sleep(0.02)
        own.answers.put({"value": "the first's"})
        assert await first == {"value": "the first's"}
        while own.calls:
            await asyncio.sleep(0.005)
        agent._sock.setblocking(False)
        with pytest.raises(BlockingIOError):
            agent._sock.recv(1)

    asyncio.run(main())
