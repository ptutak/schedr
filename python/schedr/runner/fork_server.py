"""Fork server mode: preload libraries once, then fork a runner process per request.

The agent starts the fork server when the worker starts and sends it the preload list. Per
attempt it sends ``fork`` with four descriptors: the process's channel, its stdout and stderr,
and a status socket. The fork server forks a **shim**, and the shim forks the **process**:
the process runs the normal runner code (``parse`` / ``task``) on its own channel, as a
spawned runner does; the shim reports its PID once its process group exists, then its exit
status, and reaps it when the agent closes the status socket, so the PID isn't reused while
the agent may still signal the group (see PROJECT.md, How Python tasks are started: the fork
server). Linux only.
"""

import atexit
import contextlib
import ctypes
import gc
import importlib
import locale
import os
import signal
import socket
import sys
import tempfile
import threading
import time
import traceback
import weakref
from collections.abc import Callable, Mapping
from typing import Any, NoReturn

from schedr.runner import usage
from schedr.runner.protocol import Channel, ChannelClosedError, ProtocolError

FDS = 4
"""The descriptors a ``fork`` request carries: channel, stdout, stderr, status socket."""

FORKED_MODES = ("parse", "task")
"""The modes a forked process can run."""


def thread_names() -> list[str]:
    """The names of the process's threads besides the main one.

    Returns:
        Each other thread's name (``/proc/self/task/<tid>/comm``), by thread ID.
    """
    names = []
    for tid in sorted(os.listdir("/proc/self/task"), key=int):
        if int(tid) == os.getpid():
            continue
        try:
            with open(f"/proc/self/task/{tid}/comm") as comm:
                names.append(comm.read().strip())
        except FileNotFoundError:  # the thread ended meanwhile
            continue
    return names


def run_fork_server(
    channel: Channel,
    request: Mapping[str, Any],
    *,
    threads: Callable[[], list[str]] = thread_names,
) -> int:
    """Preloads the libraries, then forks a process per request until the agent closes.

    A disabled fork server stays until the agent closes the channel too, answering every
    request with ``fork_failed``: exiting with a request unread would reset the connection,
    and the agent could lose the ``fork_server_disabled`` before it.

    Args:
        channel: The channel to the agent.
        request: The ``fork_server`` request (the preload list).
        threads: Lists the threads besides the main one (replaced in tests).

    Returns:
        0, once the agent closed the channel.

    Raises:
        ProtocolError: For a message that isn't a valid ``fork`` request.
    """
    _open_standard_descriptors()
    disabled = _prepare(request["preload"], threads)
    reader = usage.prepare()  # loaded here: a shim loads nothing
    flush_c_stdio = _c_stdio_flush()
    if disabled:
        channel.send(_disabled(*disabled))
    else:
        signal.signal(signal.SIGCHLD, _reap_shims)
        channel.send({"type": "fork_server_ready"})
    while True:
        try:
            message, fds = channel.receive_with_fds(FDS)
        except ChannelClosedError:
            return 0
        try:
            disabled = _fork(channel, message, fds, threads, disabled, reader, flush_c_stdio)
        finally:
            for fd in fds:
                os.close(fd)


type Disabled = tuple[str, list[str]] | None
"""Why the fork server is disabled and the threads besides the main one; ``None`` if it isn't."""


def _prepare(preload: list[str], threads: Callable[[], list[str]]) -> Disabled:
    """Preloads the modules; why the fork server can't be used, if it can't."""
    if not sys.platform.startswith("linux"):
        return "the fork server runs on Linux only", []
    for module in preload:
        try:
            importlib.import_module(module)
        except (Exception, SystemExit) as e:  # a preload calling sys.exit() disables too
            return f"preloading `{module}` failed: {type(e).__name__}: {e}", []
    gc.freeze()  # the collector won't touch, so copy, the preloaded objects in every child
    if names := threads():
        return _threads_reason(names), names
    return None


def _fork(
    channel: Channel,
    message: Mapping[str, Any],
    fds: list[int],
    threads: Callable[[], list[str]],
    disabled: Disabled,
    reader: usage.Reader | None,
    flush_c_stdio: Callable[[], object],
) -> Disabled:
    """Carries out one request; returns whether the fork server is disabled now."""
    if message["type"] != "fork":
        raise ProtocolError(f"expected `fork`, got `{message['type']}`")
    try:
        request_id = int(message["id"])
        mode, env, cwd = message["mode"], dict(message["env"]), str(message["cwd"])
    except (KeyError, TypeError, ValueError) as e:
        raise ProtocolError(f"a malformed `fork` request: {e!r}") from e
    if disabled:
        channel.send(_failed(request_id, f"the fork server is disabled: {disabled[0]}"))
        return disabled
    if len(fds) != FDS:
        channel.send(_failed(request_id, f"expected {FDS} descriptors, got {len(fds)}"))
        return None
    if mode not in FORKED_MODES:
        channel.send(_failed(request_id, f"can't fork a process in mode `{mode}`"))
        return None
    if names := threads():
        reason = _threads_reason(names)
        channel.send(_failed(request_id, reason))
        channel.send(_disabled(reason, names))
        return reason, names
    # Or the processes would write the buffered text again: Python's buffers, and C's (a
    # preloaded extension's printf).
    for stream in (sys.stdout, sys.stderr):
        stream.flush()
    flush_c_stdio()
    try:
        pid = os.fork()
    except OSError as e:
        channel.send(_failed(request_id, f"fork: {e}"))
        return None
    if pid == 0:  # pragma: no cover - the child's frame (tests: test_fork_server)
        _shim(channel, {"mode": mode, "env": env, "cwd": cwd}, fds, reader)
    channel.send({"type": "forked", "id": request_id})
    return None


def _shim(
    control: Channel, request: Mapping[str, Any], fds: list[int], reader: usage.Reader | None
) -> NoReturn:
    """The shim: forks the process, reports it, and reaps it after the agent closes."""
    code = 1
    try:
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)  # or the fork server's reaper takes it
        # Off the fork server's stdout and stderr: a shim lives as long as its process, and
        # holding the server's pipes would keep them open after the server ends (and could
        # block it on a full pipe no one reads). It prints nothing; the agent sees a failure
        # as the status socket closing without a report.
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, 1)
        os.dup2(devnull, 2)
        os.close(devnull)
        control.close()
        channel_fd, stdout_fd, stderr_fd, status_fd = fds
        # Blocking, whatever the agent made its end (tokio's are non-blocking, and the flag
        # comes with the descriptor): a non-blocking recv would end the wait for the agent at
        # once, and the process would be reaped while the agent may still signal its group.
        os.set_blocking(status_fd, True)
        status = socket.socket(fileno=status_fd)
        pid = os.fork()
        if pid == 0:  # pragma: no cover - the child's frame (tests: test_fork_server)
            status.close()
            _process(request, channel_fd, stdout_fd, stderr_fd)
        for fd in (channel_fd, stdout_fd, stderr_fd):
            os.close(fd)
        # The process sets its group too; whichever call comes first, the group exists before
        # the agent learns the PID. ESRCH: it has ended already (a zombie stays our child).
        with contextlib.suppress(ProcessLookupError):
            os.setpgid(pid, pid)
        _report(status, {"type": "process_started", "pid": pid})
        ended = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOWAIT)
        assert ended is not None  # None only with WNOHANG
        _report(status, _exited(ended, reader.read(pid) if reader else None))
        _wait_closed(status)
        os.waitpid(pid, 0)
        code = 0
    except BaseException:
        pass
    finally:
        os._exit(code)


def _process(
    request: Mapping[str, Any], channel_fd: int, stdout_fd: int, stderr_fd: int
) -> NoReturn:
    """The forked process: set up as a spawned runner would be, then run the runner."""
    from schedr.runner.main import EXIT_ERROR, FD_ENV, main  # noqa: PLC0415 - main imports us

    code = EXIT_ERROR
    own_exit_handlers = False
    try:
        os.setpgid(0, 0)  # not setsid: a task in an orphaned session discards SIGTSTP
        devnull = os.open(os.devnull, os.O_RDONLY)
        for fd, target in ((devnull, 0), (stdout_fd, 1), (stderr_fd, 2)):
            os.dup2(fd, target)
            os.close(fd)
            os.set_blocking(target, True)  # as a spawned process's pipes are
        # The interpreter's own streams, on descriptors 1 and 2, even if a preloaded library
        # replaced them (as a spawned process has them).
        sys.stdin, sys.stdout, sys.stderr = sys.__stdin__, sys.__stdout__, sys.__stderr__
        os.environ.clear()
        os.environ.update(request["env"])
        # What an interpreter takes from its environment at start, taken again from the
        # task's: the time zone, the character type locale (Python sets LC_CTYPE from the
        # environment, the other categories stay "C") and the temporary directory.
        time.tzset()
        locale.setlocale(locale.LC_ALL, "C")
        with contextlib.suppress(locale.Error):  # unknown: "C", as a spawned process falls back
            locale.setlocale(locale.LC_CTYPE, "")
        tempfile.tempdir = None
        os.chdir(request["cwd"])
        sys.argv[1:] = [request["mode"]]
        _forget_server_exit_handlers()
        own_exit_handlers = True
        code = main([request["mode"]], {FD_ENV: str(channel_fd)})
    except BaseException:
        traceback.print_exc()
    finally:
        if own_exit_handlers:
            _shut_down()
        for stream in (sys.stdout, sys.stderr):
            with contextlib.suppress(Exception):
                stream.flush()
        os._exit(code)


def _forget_server_exit_handlers() -> None:
    """Drops the exit handlers the fork server registered (decision 15A).

    They belong to the server: a preloaded library's handler may act on the server's state
    (remove a directory it made at import). Two kept per process, as each interpreter has
    them: `logging`'s shutdown (flushes and closes the handlers) and the `weakref.finalize`
    hook, for the process's own finalizers only.
    """
    atexit._clear()
    if logging := sys.modules.get("logging"):
        atexit.register(logging.shutdown)
    for info in weakref.finalize._registry.values():  # type: ignore[attr-defined]
        info.atexit = False  # the server's objects
    weakref.finalize._registered_with_atexit = False  # type: ignore[attr-defined]


def _shut_down() -> None:
    """What an interpreter does on its way out, done before ``os._exit``.

    It waits for the threads that aren't daemons, then runs the exit handlers (the process's
    own), in CPython's order.
    """
    with contextlib.suppress(Exception):
        threading._shutdown()  # type: ignore[attr-defined]
    with contextlib.suppress(Exception):
        atexit._run_exitfuncs()


def _open_standard_descriptors() -> None:
    """Opens /dev/null on any of descriptors 0-2 that is closed.

    So no received descriptor takes one of their numbers (the process puts its own there).
    """
    for fd in (0, 1, 2):
        try:
            os.fstat(fd)
        except OSError:
            opened = os.open(os.devnull, os.O_RDWR)  # the lowest free number: this one
            if opened != fd:  # pragma: no cover - only if a lower one closed meanwhile
                os.close(opened)


def _c_stdio_flush() -> Callable[[], object]:
    """Libc's ``fflush(NULL)``, loaded before any fork; a no-op if libc can't be loaded."""
    try:
        fflush = ctypes.CDLL(None).fflush
    except OSError, AttributeError:
        return lambda: None
    fflush.argtypes = (ctypes.c_void_p,)
    return lambda: fflush(None)


def _report(status: socket.socket, message: dict[str, Any]) -> None:
    """Sends a message on the status socket; an agent that is gone isn't an error."""
    with contextlib.suppress(OSError):
        Channel(status).send(message)


def _wait_closed(status: socket.socket) -> None:
    """Waits until the agent closes its end of the status socket (or is gone)."""
    with contextlib.suppress(OSError):
        while status.recv(4096):
            pass


def _exited(ended: os.waitid_result, used: dict[str, int] | None) -> dict[str, Any]:
    exited = ended.si_code == os.CLD_EXITED
    return {
        "type": "process_exited",
        "code": ended.si_status if exited else None,
        "signal": None if exited else ended.si_status,
        "core_dumped": ended.si_code == os.CLD_DUMPED,
        "usage": used,
    }


def _reap_shims(_signum: int, _frame: object) -> None:
    """Reaps the shims that ended (the fork server has no other children)."""
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return


def _disabled(reason: str, names: list[str]) -> dict[str, Any]:
    return {"type": "fork_server_disabled", "reason": reason, "threads": names}


def _failed(request_id: int, reason: str) -> dict[str, Any]:
    return {"type": "fork_failed", "id": request_id, "reason": reason}


def _threads_reason(names: list[str]) -> str:
    return (
        f"the fork server has {len(names) + 1} threads after preloading "
        f"(besides the main one: {', '.join(names)}); forking with threads can deadlock"
    )
