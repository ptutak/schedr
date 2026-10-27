import atexit
import ctypes
import json
import locale
import logging
import logging.handlers
import os
import select
import signal
import socket
import sys
import tempfile
import threading
import time
import weakref
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from schedr.runner import fork_server, usage
from schedr.runner.fork_server import FDS, run_fork_server, thread_names
from schedr.runner.protocol import Channel, ChannelClosedError, ProtocolError

PROBE = """\
import os, random, sys
from schedr import Dag
print("out", os.getcwd(), os.environ.get("PROBE"), sorted(os.environ), random.random())
print("err", os.getpgid(0) == os.getpid(), os.isatty(0), file=sys.stderr)
with Dag("probe"):
    pass
"""

ENVIRONMENT_PROBE = """\
import locale, tempfile, time
from schedr import Dag
print(time.tzname[0], locale.setlocale(locale.LC_CTYPE), tempfile.gettempdir(), flush=True)
with Dag("probe"):
    pass
"""

EXIT_PROBE = """\
import atexit, logging, threading, time, weakref
from schedr import Dag


class Held:
    pass


held = Held()
weakref.finalize(held, print, "task-finalizer", flush=True)
atexit.register(print, "task-atexit", flush=True)
logging.getLogger().warning("task-log")


def late():
    time.sleep(0.3)
    print("thread-done", flush=True)


threading.Thread(target=late).start()
with Dag("probe"):
    pass
"""

KEPT: list[object] = []
"""Objects the forked test server keeps alive."""

type Validate = Callable[[Any], None]


@pytest.fixture(autouse=True)
def sigchld() -> Iterator[None]:
    """An in-process fork server installs its SIGCHLD reaper here: put back the old one."""
    before = signal.getsignal(signal.SIGCHLD)
    yield
    signal.signal(signal.SIGCHLD, before)


def send(sock: socket.socket, message: dict[str, Any], fds: list[int]) -> None:
    """Sends a message with descriptors on its first bytes, as the agent does."""
    body = json.dumps(message).encode()
    socket.send_fds(sock, [len(body).to_bytes(4, "big")], fds)
    sock.sendall(body)


class Attempt:
    """The agent's ends for one forked process: its channel, output pipes and status socket."""

    def __init__(self) -> None:
        channel, channel_end = socket.socketpair()
        status, status_end = socket.socketpair()
        for sock in (channel, status):
            sock.settimeout(20)
        self.runner, self.status = Channel(channel), Channel(status)
        self.status_socket = status
        self.stdout, stdout_end = os.pipe()
        self.stderr, stderr_end = os.pipe()
        self.ends = [channel_end.detach(), stdout_end, stderr_end, status_end.detach()]

    def close_ends(self) -> None:
        for fd in self.ends:
            os.close(fd)

    def output(self) -> tuple[str, str]:
        def read(fd: int) -> str:
            chunks = []
            while chunk := os.read(fd, 4096):
                chunks.append(chunk)
            os.close(fd)
            return b"".join(chunks).decode()

        return read(self.stdout), read(self.stderr)

    def started(self, validate: Validate) -> int:
        message = self.status.receive()
        validate(message)
        assert message["type"] == "process_started"
        pid: int = message["pid"]
        return pid

    def exited(self, validate: Validate) -> dict[str, Any]:
        message = self.status.receive()
        validate(message)
        return message

    def parse(self, root: Path) -> None:
        hello = self.runner.receive()
        assert (hello["type"], hello["mode"]) == ("hello", "parse")
        self.runner.send({"type": "welcome"})
        self.runner.send(
            {
                "type": "parse",
                "root": "r",
                "root_dir": str(root),
                "files": ["probe.py"],
                "sys_path": [],
            }
        )
        while self.runner.receive()["type"] != "parse_finished":
            pass

    def release(self) -> None:
        """Closes the agent's end of the status socket: the shim reaps the process."""
        self.status.close()


class Server:
    """A fork server in its own process, started before any attempt's sockets exist, as the
    agent's is: a separate process never holding the agent's ends of other attempts."""

    def __init__(self, preload: list[str], setup: Callable[[], object] = lambda: None) -> None:
        server_end, agent_end = socket.socketpair()
        self.output, output_end = os.pipe()  # the server's stdout and stderr
        self.pid = os.fork()
        if self.pid == 0:
            agent_end.close()
            os.close(self.output)
            os.dup2(output_end, 1)
            os.dup2(output_end, 2)
            os.close(output_end)
            code = 1
            try:
                setup()
                code = run_fork_server(Channel(server_end), {"preload": preload}, threads=list)
            finally:
                os._exit(code)
        server_end.close()
        os.close(output_end)
        agent_end.settimeout(20)
        self.sock, self.agent = agent_end, Channel(agent_end)
        self.requests = 0
        self.code: int | None = None

    def fork(
        self,
        mode: str = "parse",
        cwd: str = "/",
        env: dict[str, str] | None = None,
        *,
        non_blocking: bool = False,
    ) -> Attempt:
        attempt = Attempt()
        for fd in attempt.ends if non_blocking else []:
            os.set_blocking(fd, False)  # as tokio makes its sockets and pipes
        self.requests += 1
        message = {"type": "fork", "id": self.requests, "mode": mode, "env": env or {}, "cwd": cwd}
        send(self.sock, message, attempt.ends)
        attempt.close_ends()
        return attempt

    def stop(self) -> int:
        if self.code is None:
            self.sock.close()
            _, status = os.waitpid(self.pid, 0)
            self.code = os.waitstatus_to_exitcode(status)
        return self.code


@pytest.fixture
def server() -> Iterator[Server]:
    started = Server(["json"])
    yield started
    started.stop()
    os.close(started.output)


def gone(pid: int, timeout: float = 10) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.01)
    return False


def test_a_forked_process_runs_the_runner_set_up_as_a_spawned_one(
    server: Server, tmp_path: Path, validate_runner_message: Validate
) -> None:
    (tmp_path / "probe.py").write_text(PROBE)
    ready = server.agent.receive()
    validate_runner_message(ready)
    assert ready == {"type": "fork_server_ready"}
    numbers = []
    for n in (1, 2):
        attempt = server.fork(env={"PROBE": "yes"}, cwd=str(tmp_path))
        forked = server.agent.receive()
        validate_runner_message(forked)
        assert forked == {"type": "forked", "id": n}
        pid = attempt.started(validate_runner_message)
        assert os.getpgid(pid) == pid  # its own group before the agent learns the PID
        attempt.parse(tmp_path)
        exited = attempt.exited(validate_runner_message)
        used = exited.pop("usage")
        assert exited == {
            "type": "process_exited",
            "code": 0,
            "signal": None,
            "core_dumped": False,
        }
        # The parent's view: CPU spent importing `schedr` and the DAG, the fork server's
        # pages included in the peak RSS.
        assert used["user_cpu_micros"] + used["system_cpu_micros"] > 0
        assert used["peak_rss_bytes"] > 1024 * 1024
        os.kill(pid, 0)  # a zombie, so the PID isn't reused, until the agent closes
        out, err = attempt.output()
        words = out.split()
        assert words[:4] == ["out", str(tmp_path), "yes", "['PROBE']"]
        numbers.append(words[4])
        assert err.split() == ["err", "True", "False"]
        attempt.release()
        assert gone(pid)
    assert numbers[0] != numbers[1]  # `random` reseeded in each process
    assert server.stop() == 0


def zombie(pid: int) -> bool:
    with open(f"/proc/{pid}/stat") as stat:
        return stat.read().rsplit(")", 1)[1].split()[0] == "Z"


def test_ends_the_agent_made_non_blocking_work_and_keep_the_zombie(
    server: Server, tmp_path: Path, validate_runner_message: Validate
) -> None:
    (tmp_path / "probe.py").write_text(PROBE)
    attempt = server.fork(cwd=str(tmp_path), non_blocking=True)
    pid = attempt.started(validate_runner_message)
    attempt.parse(tmp_path)
    assert attempt.exited(validate_runner_message)["code"] == 0
    out, _ = attempt.output()
    assert out.startswith("out ")
    time.sleep(0.3)  # a shim whose wait returned at once would have reaped it by now
    assert zombie(pid)
    attempt.release()
    assert gone(pid)


def test_the_process_takes_the_time_zone_locale_and_tmpdir_from_its_environment(
    tmp_path: Path, validate_runner_message: Validate
) -> None:
    (tmp_path / "probe.py").write_text(ENVIRONMENT_PROBE)
    cached = tmp_path / "server-tmp"
    task_tmp = tmp_path / "task-tmp"
    cached.mkdir()
    task_tmp.mkdir()

    def server_state() -> None:
        os.environ.update({"TZ": "UTC", "TMPDIR": str(cached)})
        time.tzset()
        locale.setlocale(locale.LC_CTYPE, "C.UTF-8")
        tempfile.tempdir = None
        tempfile.gettempdir()  # cached in the server
        ctypes.CDLL(None).printf(b"from-a-preloaded-printf\n")  # left in C's buffer

    server = Server(["json"], setup=server_state)
    try:
        env = {"TZ": "Pacific/Kiritimati", "LC_ALL": "C", "TMPDIR": str(task_tmp)}
        attempt = server.fork(cwd=str(tmp_path), env=env)
        pid = attempt.started(validate_runner_message)
        attempt.parse(tmp_path)
        assert attempt.exited(validate_runner_message)["code"] == 0
        out, _ = attempt.output()
        assert out.split() == ["+14", "C", str(task_tmp)]
        attempt.release()
        assert gone(pid)
        assert server.stop() == 0
        assert os.read(server.output, 4096) == b"from-a-preloaded-printf\n"  # the server's
    finally:
        server.stop()
        os.close(server.output)


def test_the_process_shuts_down_as_an_interpreter_without_the_servers_exit_handlers(
    tmp_path: Path, validate_runner_message: Validate
) -> None:
    (tmp_path / "probe.py").write_text(EXIT_PROBE)

    class Held:
        pass

    def server_state() -> None:
        atexit.register(print, "server-atexit", flush=True)
        held = Held()
        KEPT.append(held)
        weakref.finalize(held, print, "server-finalizer", flush=True)
        buffered = logging.handlers.MemoryHandler(100, target=logging.StreamHandler(sys.__stdout__))
        logging.getLogger().addHandler(buffered)  # flushed only by logging's shutdown

    server = Server(["json"], setup=server_state)
    try:
        attempt = server.fork(cwd=str(tmp_path))
        pid = attempt.started(validate_runner_message)
        attempt.parse(tmp_path)
        assert attempt.exited(validate_runner_message)["code"] == 0
        out, _ = attempt.output()
        # As a spawned interpreter ends (the same order, checked): the thread finished first;
        # then the exit handlers, last registered first: the task's handler, its finalizer,
        # logging's shutdown flushing the buffered record. None of the server's.
        assert out.split("\n") == [
            "thread-done",
            "task-atexit",
            "task-finalizer",
            "task-log",
            "",
        ]
        attempt.release()
        assert gone(pid)
    finally:
        server.stop()
        os.close(server.output)


def test_a_server_without_logging_leaves_it_to_the_task_to_register_its_shutdown(
    tmp_path: Path, validate_runner_message: Validate
) -> None:
    (tmp_path / "probe.py").write_text(EXIT_PROBE)

    def without_logging() -> None:
        for name in [n for n in sys.modules if n == "logging" or n.startswith("logging.")]:
            del sys.modules[name]

    server = Server(["json"], setup=without_logging)
    try:
        attempt = server.fork(cwd=str(tmp_path))
        pid = attempt.started(validate_runner_message)
        attempt.parse(tmp_path)
        assert attempt.exited(validate_runner_message)["code"] == 0
        out, err = attempt.output()
        assert out.split() == ["thread-done", "task-atexit", "task-finalizer"]
        assert err == "task-log\n"  # logging imported by the task: its last-resort handler
        attempt.release()
        assert gone(pid)
    finally:
        server.stop()
        os.close(server.output)


def test_a_fork_server_started_without_stdin_still_gives_the_process_its_channel(
    tmp_path: Path, validate_runner_message: Validate
) -> None:
    (tmp_path / "probe.py").write_text(PROBE)
    server = Server(["json"], setup=lambda: os.close(0))
    try:
        attempt = server.fork(cwd=str(tmp_path))
        pid = attempt.started(validate_runner_message)
        attempt.parse(tmp_path)  # descriptor 0 was free: the channel would have taken it
        assert attempt.exited(validate_runner_message)["code"] == 0
        attempt.release()
        assert gone(pid)
    finally:
        server.stop()
        os.close(server.output)


def test_a_killed_group_is_reported_with_its_signal_by_the_shim_outside_it(
    server: Server, validate_runner_message: Validate
) -> None:
    waiting, killed = server.fork(mode="task"), server.fork(mode="task")
    pids = [attempt.started(validate_runner_message) for attempt in (waiting, killed)]
    for attempt in (waiting, killed):
        assert attempt.runner.receive()["mode"] == "task"
    os.killpg(pids[1], signal.SIGKILL)
    exited = killed.exited(validate_runner_message)
    assert (exited["code"], exited["signal"], exited["core_dumped"]) == (
        None,
        signal.SIGKILL,
        False,
    )
    assert exited["usage"] is not None
    killed.status_socket.sendall(b"ignored")  # anything the agent sends before closing
    killed.release()
    assert gone(pids[1])
    os.kill(pids[0], 0)  # the other attempt's shim and process carry on
    os.killpg(pids[0], signal.SIGKILL)
    assert waiting.exited(validate_runner_message)["signal"] == signal.SIGKILL
    waiting.release()
    assert gone(pids[0])


def test_the_fork_server_output_closes_when_it_ends_while_its_processes_run(
    server: Server, validate_runner_message: Validate
) -> None:
    attempt = server.fork(mode="task")
    pid = attempt.started(validate_runner_message)
    assert attempt.runner.receive()["mode"] == "task"  # running, waiting for the agent
    assert server.stop() == 0
    ready, _, _ = select.select([server.output], [], [], 10)
    assert ready
    assert os.read(server.output, 4096) == b""  # no shim holds the server's pipe
    os.kill(pid, 0)
    os.killpg(pid, signal.SIGKILL)
    assert attempt.exited(validate_runner_message)["signal"] == signal.SIGKILL
    attempt.release()
    assert gone(pid)


def test_a_shim_that_fails_closes_the_attempts_sockets_without_a_report(server: Server) -> None:
    attempt = Attempt()
    read, write = os.pipe()  # a pipe, not a socket, as the status socket
    os.close(attempt.ends[3])
    attempt.ends[3] = write
    server.requests += 1
    send(
        server.sock, {"type": "fork", "id": 1, "mode": "task", "env": {}, "cwd": "/"}, attempt.ends
    )
    attempt.close_ends()
    with pytest.raises(ChannelClosedError):
        attempt.runner.receive()
    assert os.read(read, 1) == b""
    os.close(read)


def test_a_usage_that_cant_be_read_still_reports_the_exit(
    monkeypatch: pytest.MonkeyPatch, validate_runner_message: Validate
) -> None:
    def broken(*_: object) -> int:
        raise OSError("no syscall")

    monkeypatch.setattr(usage, "prepare", lambda: usage.Reader(broken, 0))
    server = Server(["json"])  # forked after the patch: its shims get the broken reader
    try:
        attempt = server.fork(mode="task")
        pid = attempt.started(validate_runner_message)
        os.killpg(pid, signal.SIGKILL)
        exited = attempt.exited(validate_runner_message)
        assert (exited["signal"], exited["usage"]) == (signal.SIGKILL, None)
        attempt.release()
        assert gone(pid)
    finally:
        server.stop()
        os.close(server.output)


def test_the_shim_reports_after_the_fork_server_died(
    server: Server, tmp_path: Path, validate_runner_message: Validate
) -> None:
    (tmp_path / "probe.py").write_text(PROBE)
    attempt = server.fork(cwd=str(tmp_path))
    pid = attempt.started(validate_runner_message)
    os.kill(server.pid, signal.SIGKILL)
    assert server.stop() == -signal.SIGKILL
    attempt.parse(tmp_path)
    assert attempt.exited(validate_runner_message)["code"] == 0
    attempt.release()
    assert gone(pid)


def test_a_process_that_cant_be_set_up_exits_with_its_traceback_on_stderr(
    server: Server, tmp_path: Path, validate_runner_message: Validate
) -> None:
    attempt = server.fork(cwd=str(tmp_path / "missing"))
    pid = attempt.started(validate_runner_message)
    assert attempt.exited(validate_runner_message)["code"] == 1
    _, err = attempt.output()
    assert "FileNotFoundError" in err
    attempt.release()
    assert gone(pid)


class InProcess:
    """A fork server run in the test's process, for the paths that don't fork: the requests
    are queued, then the channel half-closed, so it returns once it has handled them."""

    def __init__(self) -> None:
        self.server_end, self.agent_end = socket.socketpair()
        self.agent_end.settimeout(10)

    def serve(
        self, preload: list[str] | None = None, threads: Callable[[], list[str]] = list
    ) -> int:
        self.agent_end.shutdown(socket.SHUT_WR)
        channel = Channel(self.server_end)
        try:
            return run_fork_server(channel, {"preload": preload or []}, threads=threads)
        finally:
            channel.close()

    def replies(self) -> list[dict[str, Any]]:
        agent, found = Channel(self.agent_end), []
        while True:
            try:
                found.append(agent.receive())
            except ChannelClosedError:
                return found


def request(n: int, mode: str = "task") -> dict[str, Any]:
    return {"type": "fork", "id": n, "mode": mode, "env": {}, "cwd": "/"}


def open_fds(count: int) -> list[int]:
    return [os.open(os.devnull, os.O_RDONLY) for _ in range(count)]


def fd_count() -> int:
    return len(os.listdir("/proc/self/fd"))


def test_requests_that_cant_be_carried_out_fail_and_their_descriptors_are_closed(
    validate_runner_message: Validate,
) -> None:
    server = InProcess()
    for message, count in ((request(1), FDS - 1), (request(2, mode="fork_server"), FDS)):
        fds = open_fds(count)
        send(server.agent_end, message, fds)
        for fd in fds:
            os.close(fd)
    before = fd_count()
    assert server.serve() == 0
    assert fd_count() == before - 1  # the server's end closed; every received one too
    replies = server.replies()
    for reply in replies:
        validate_runner_message(reply)
    assert replies == [
        {"type": "fork_server_ready"},
        {"type": "fork_failed", "id": 1, "reason": f"expected {FDS} descriptors, got {FDS - 1}"},
        {"type": "fork_failed", "id": 2, "reason": "can't fork a process in mode `fork_server`"},
    ]


def test_a_preload_calling_sys_exit_disables_the_fork_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "schedr_exiting_probe.py").write_text("import sys\nsys.exit('no')\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    server = InProcess()
    assert server.serve(["schedr_exiting_probe"]) == 0
    (disabled,) = server.replies()
    assert disabled["reason"] == "preloading `schedr_exiting_probe` failed: SystemExit: no"


def test_without_libc_the_c_buffers_arent_flushed_and_forks_go_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(*_: object, **__: object) -> None:
        raise OSError("no libc")

    def fork() -> int:
        raise BlockingIOError(11, "Resource temporarily unavailable")

    monkeypatch.setattr(fork_server.ctypes, "CDLL", missing)
    monkeypatch.setattr(fork_server.os, "fork", fork)
    server = InProcess()
    fds = open_fds(FDS)
    send(server.agent_end, request(1), fds)
    for fd in fds:
        os.close(fd)
    assert server.serve() == 0
    assert server.replies()[1]["reason"].startswith("fork: ")  # it got as far as forking


def test_a_failing_fork_fails_the_request(monkeypatch: pytest.MonkeyPatch) -> None:
    def fork() -> int:
        raise BlockingIOError(11, "Resource temporarily unavailable")

    monkeypatch.setattr(fork_server.os, "fork", fork)
    server = InProcess()
    fds = open_fds(FDS)
    send(server.agent_end, request(1), fds)
    for fd in fds:
        os.close(fd)
    before = fd_count()
    assert server.serve() == 0
    assert fd_count() == before - 1
    assert server.replies()[1] == {
        "type": "fork_failed",
        "id": 1,
        "reason": "fork: [Errno 11] Resource temporarily unavailable",
    }


def test_a_thread_started_after_the_check_disables_the_fork_server(
    validate_runner_message: Validate,
) -> None:
    server = InProcess()
    for n in (1, 2):
        fds = open_fds(FDS)
        send(server.agent_end, request(n), fds)
        for fd in fds:
            os.close(fd)
    checks = iter([[], ["late-pool"]])
    assert server.serve(threads=lambda: next(checks)) == 0
    replies = server.replies()
    for reply in replies:
        validate_runner_message(reply)
    # The answer to the fork that found the thread is `fork_server_disabled` alone (one
    # message per request, so the agent pairs them).
    assert [r["type"] for r in replies] == [
        "fork_server_ready",
        "fork_server_disabled",
        "fork_failed",
    ]
    assert "late-pool" in replies[1]["reason"]
    assert replies[1]["threads"] == ["late-pool"]
    # Still answering until the agent closes: leaving a request unread would reset the
    # connection, and the agent could lose the `fork_server_disabled`.
    assert replies[2]["id"] == 2
    assert replies[2]["reason"].startswith("the fork server is disabled: ")


@pytest.mark.parametrize(
    ("preload", "threads", "reason"),
    [
        (
            ["schedr_no_such_module"],
            [],
            "preloading `schedr_no_such_module` failed: ModuleNotFoundError",
        ),
        (["json"], ["jemalloc_bg_thd", "polars-ooc-clea"], "has 3 threads after preloading"),
    ],
)
def test_a_failing_preload_or_threads_disable_the_fork_server(
    preload: list[str], threads: list[str], reason: str, validate_runner_message: Validate
) -> None:
    server = InProcess()
    fds = open_fds(FDS)
    send(server.agent_end, request(1), fds)
    for fd in fds:
        os.close(fd)
    before = fd_count()
    assert server.serve(preload, threads=lambda: threads) == 0
    assert fd_count() == before - 1
    disabled, failed = server.replies()
    validate_runner_message(disabled)
    assert disabled["type"] == "fork_server_disabled"
    assert reason in disabled["reason"]
    assert disabled["threads"] == threads
    assert failed == {
        "type": "fork_failed",
        "id": 1,
        "reason": f"the fork server is disabled: {disabled['reason']}",
    }


def test_outside_linux_the_fork_server_is_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "darwin")
    server = InProcess()
    assert server.serve() == 0
    assert server.replies() == [
        {
            "type": "fork_server_disabled",
            "reason": "the fork server runs on Linux only",
            "threads": [],
        }
    ]


@pytest.mark.parametrize(
    ("message", "error"),
    [
        ({"type": "task"}, "expected `fork`, got `task`"),
        ({"type": "fork", "id": 1}, "a malformed `fork` request"),
        ({"type": "fork", "id": "x", "mode": "task", "env": {}, "cwd": "/"}, "a malformed `fork`"),
    ],
)
def test_a_message_that_isnt_a_fork_request_is_a_protocol_error(
    message: dict[str, Any], error: str
) -> None:
    server = InProcess()
    send(server.agent_end, message, [])
    with pytest.raises(ProtocolError, match=error):
        server.serve()


def test_thread_names_skips_a_thread_that_ended_meanwhile(monkeypatch: pytest.MonkeyPatch) -> None:
    def ended(path: str) -> Any:
        raise FileNotFoundError(path)

    monkeypatch.setattr(fork_server, "open", ended, raising=False)
    stop = threading.Event()
    thread = threading.Thread(target=stop.wait)
    thread.start()
    try:
        assert thread_names() == []
    finally:
        stop.set()
        thread.join()


def test_thread_names_lists_the_threads_besides_the_main_one() -> None:
    stop = threading.Event()
    thread = threading.Thread(target=stop.wait, name="probe-thread")
    thread.start()
    try:
        assert "probe-thread" in thread_names()
    finally:
        stop.set()
        thread.join()
    assert "probe-thread" not in thread_names()
