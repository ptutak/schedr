"""The watcher host: every wait step of one code set, as coroutines in one process.

A long-lived runner (decision 320), multiplexed by watcher ID over the agent's socket. A wait
step spends its life waiting, so one process and one slot-free coroutine each is cheap where a
process per step isn't. Its DAG file is imported once per host, on the loop: imports behave as
in a task, and its top level isn't run again for each attempt.

The agent's socket is read by a thread of its own, so a watcher waiting for an answer (a secret,
a connection) can be answered while the loop is held; the answers go to who asked. Watchers share
the process: its working directory and environment are the host's, never a run's.

What a watcher runs with ``asyncio.to_thread`` belongs to it: its log and secrets, and its time
(decision 337). A call cancelled, or still running when its watcher ends, has
``StepEndedError`` raised in its thread, the one way Python has to reach one; a thread that doesn't
end within the cancel grace (blocked in C, or ignoring it) can only end with its process.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import contextvars
import ctypes
import functools
import io
import itertools
import os
import queue
import signal
import sys
import threading
import time
import traceback
from typing import TYPE_CHECKING, Any, TextIO

from schedr.runner.coroutines import loop_factory
from schedr.runner.parse import extend_sys_path, import_dag_entries
from schedr.runner.protocol import MAX_MESSAGE_SIZE, Channel, ProtocolError
from schedr.runner.task import execute_trigger, execute_watcher, failure
from schedr.runner.waiting import StepEndedError, refuse_a_process, set_poke_overruns

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from schedr._model import registry

_CHECK_EVERY = 1.0
"""How often, in seconds, the watchdog looks at the loop."""

_POOL_THREADS = "schedr-wait-step"
"""How the loop's own threads are named."""

_LOG_CHUNK = MAX_MESSAGE_SIZE // 16
"""The most text of one log message: a huge ``print`` goes in several."""


_WAKE: dict[str, Any] = {}
"""Put among a watcher's answers to wake a call that was stopped while asking."""

_running = threading.local()
"""``call``: the :class:`_ThreadCall` this thread is running, if any."""

_thread_stops = itertools.count(1)
"""Each next one is how many ``to_thread`` calls were stopped so far, this one among them."""
_thread_stops_so_far = 0


def _thread_stopped() -> None:
    """Counts a ``to_thread`` call stopped: it may have left shared state half updated."""
    global _thread_stops_so_far  # noqa: PLW0603  # the process's own count, for its watchdog
    _thread_stops_so_far = next(_thread_stops)


def _raise_in(thread: int, error: type[BaseException] | None) -> None:
    """Raises in a running thread at its next Python line; ``None`` takes back one not raised."""
    ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_ulong(thread), None if error is None else ctypes.py_object(error)
    )


class _ThreadCall[T](concurrent.futures.Future[T]):
    """A call in the host's threads: cancelling one running raises in its thread, once.

    Only inside :meth:`run`, never in the pool's own code around it, which would lose the thread
    and leave the call unfinished for good.
    """

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._thread: int | None = None
        self._stop_asked = False
        self._raised = False
        self._holding = False
        self._answers: queue.SimpleQueue[dict[str, Any]] | None = None
        self._queued: concurrent.futures.Future[T] | None = None

    def running_in(self) -> int | None:
        """The thread running it now, if one is."""
        return None if self.done() else self._thread

    def queued(self, future: concurrent.futures.Future[T]) -> None:
        self._queued = future
        future.add_done_callback(self._ended)

    def run(self, call: Callable[[], T]) -> T:
        with self._lock:
            self._thread = threading.get_ident()
        _running.call = self
        try:
            try:
                self.stop_held_back()  # Asked before it got here.
                return call()
            finally:
                self._left()
        except StepEndedError:
            self._left()  # Raised once: what's left now can't be hit.
            raise

    def _left(self) -> None:
        """Nothing is raised in this thread after this, and nothing raised is still on its way."""
        while True:
            try:
                _running.call = None
                with self._lock:
                    thread, self._thread = self._thread, None
                if thread is not None:
                    _raise_in(thread, None)
            except StepEndedError:
                continue
            return

    def cancel(self) -> bool:
        if self._queued is not None and self._queued.cancel():
            return True
        with self._lock:
            self._stop_asked = True
            if self._holding:
                if self._answers is not None:
                    self._answers.put(_WAKE)
            elif self._thread is not None and not self._raised:
                self._raised = True
                _thread_stopped()
                _raise_in(self._thread, StepEndedError)
        return False

    def hold(self, answers: queue.SimpleQueue[dict[str, Any]]) -> None:
        """Raises nothing until :meth:`release`, waking ``answers`` instead: asking is counted."""
        with self._lock:
            self._holding = True
            self._answers = answers

    def stop_held_back(self) -> None:
        """Ends the call here, if it was asked to while nothing could be raised in it.

        Raises:
            StepEndedError: If so.
        """
        with self._lock:
            stop = self._stop_asked and not self._raised
            self._raised = self._raised or stop
        if stop:
            _thread_stopped()
            raise StepEndedError

    def release(self) -> None:
        """Ends what was held off, if anything was."""
        with self._lock:
            self._holding = False
        self.stop_held_back()

    def _ended(self, future: concurrent.futures.Future[T]) -> None:
        if future.cancelled():
            concurrent.futures.Future.cancel(self)
            self.set_running_or_notify_cancel()
            return
        self.set_running_or_notify_cancel()
        error = future.exception()
        if error is not None:
            self.set_exception(error)
        else:
            self.set_result(future.result())


class _WatcherThreads(concurrent.futures.ThreadPoolExecutor):
    """The loop's default threads: each call belongs to the watcher that made it.

    A pool's thread would otherwise keep the context of whoever made it start, and print to that
    watcher's log.
    """

    def submit[**P, T](
        self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs
    ) -> concurrent.futures.Future[T]:
        call: _ThreadCall[T] = _ThreadCall()
        own = _WATCHER.get()
        if own is not None:
            own.calls.add(call)
            call.add_done_callback(own.calls.discard)
        running = functools.partial(contextvars.copy_context().run, fn, *args, **kwargs)
        call.queued(super().submit(call.run, running))
        return call


class WatcherChannel:
    """What one watcher sees of the channel: its own messages, its own answers."""

    def __init__(self, host: _Host, watcher: int) -> None:
        """Binds it to a watcher.

        Args:
            host: The host.
            watcher: The watcher's ID.
        """
        self._host = host
        self.watcher = watcher
        self._asking = threading.Lock()
        self.answers: queue.SimpleQueue[dict[str, Any]] = queue.SimpleQueue()
        self._owed = 0
        self.ended = False
        self.calls: set[_ThreadCall[Any]] = set()
        self.threads: list[threading.Thread] = []
        self.moving = asyncio.Event()
        self.go: asyncio.Future[int | None] | None = None

    def send(self, message: dict[str, Any]) -> None:
        """Sends one message of this watcher.

        Args:
            message: A JSON object with a ``type``.
        """
        self._host.send({"type": "watcher_message", "watcher": self.watcher, "message": message})

    def ask(self, message: dict[str, Any]) -> dict[str, Any]:
        """Sends a request and waits for its answer, holding what called it.

        That is the loop for a call from a coroutine, as ``task_ctx.secret`` is: other
        watchers wait for as long as the agent takes to answer.

        Args:
            message: The request.

        Returns:
            The answer.
        """
        call: _ThreadCall[Any] | None = getattr(_running, "call", None)
        if call is not None:
            call.hold(self.answers)
        try:
            with self._asking:
                if call is not None:
                    call.stop_held_back()  # Asked while waiting for its turn.
                # Answers come in any order: one given up on is waited for before asking again.
                while self._owed:
                    self._next_answer(call)
                self.send(message)
                self._owed += 1
                return self._next_answer(call)
        finally:
            if call is not None:
                call.release()

    def _next_answer(self, call: _ThreadCall[Any] | None) -> dict[str, Any]:
        while True:
            answer = self.answers.get()
            if answer is not _WAKE:
                self._owed -= 1
                return answer
            if call is not None:
                call.stop_held_back()


_WATCHER: contextvars.ContextVar[WatcherChannel | None] = contextvars.ContextVar(
    "schedr_watcher", default=None
)
"""The watcher this code runs for; a thread it started runs for it too (the host's interpreter
option ``thread_inherit_context``)."""


class _Routed(io.TextIOBase):
    """``sys.stdout`` or ``sys.stderr``: a watcher's text to its log, the rest to the host's."""

    def __init__(self, stream: str, own: TextIO) -> None:
        self._stream = stream
        self._own = own

    def writable(self) -> bool:
        return True

    def write(self, text: str) -> int:
        watcher = _WATCHER.get()
        # A thread can outlive the step that started it: no attempt reads its log any more.
        if watcher is None or watcher.ended:
            return self._own.write(text)
        clean = text.encode("utf-8", "backslashreplace").decode()
        for start in range(0, len(clean), _LOG_CHUNK):
            watcher.send(
                {
                    "type": "watcher_log",
                    "stream": self._stream,
                    "text": clean[start : start + _LOG_CHUNK],
                }
            )
        return len(text)

    def flush(self) -> None:
        self._own.flush()

    def fileno(self) -> int:
        return self._own.fileno()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._own, name)


class _Host:
    """The running host: its channel, its watchers, and the DAG files it imported."""

    def __init__(self, channel: Channel) -> None:
        self._channel = channel
        self._sending = threading.Lock()
        self._watchers: dict[int, tuple[WatcherChannel, asyncio.Task[Any]]] = {}
        self._ending: set[asyncio.Task[None]] = set()
        self._entries: dict[tuple[str, str, str], list[registry.DefinedDag]] = {}
        self.importing = 0
        self.loop_lag = 0.0
        """The longest its event loop took to get to a call, since it last said how it is."""

    def send(self, message: dict[str, Any]) -> None:
        with self._sending:
            self._channel.send(message)

    def say_how_it_is(self) -> None:
        """Says it is there, and how it is doing."""
        threads, tied = _threads_of(self)
        lag, self.loop_lag = self.loop_lag, 0.0
        self.send(
            {
                "type": "host_alive",
                "threads": threads,
                "tied_threads": tied,
                "thread_stops": _thread_stops_so_far,
                "loop_lag_ms": round(lag * 1000),
            }
        )

    def imported(self, root: str, root_dir: Path, path: str) -> list[registry.DefinedDag]:
        """A DAG file's DAGs, imported once: on failure, again by the next attempt."""
        key = (root, str(root_dir), path)
        if key not in self._entries:
            self.importing += 1
            try:
                self._entries[key] = import_dag_entries(root, root_dir, path)
            finally:
                self.importing -= 1
        return self._entries[key]

    def start(
        self,
        watcher: int,
        request: dict[str, Any],
        *,
        moved_in: bool = False,
        grace: float = 30.0,
    ) -> None:
        own = WatcherChannel(self, watcher)
        context = contextvars.copy_context()
        context.run(_WATCHER.set, own)
        loop = asyncio.get_running_loop()
        going = None
        if moved_in:
            own.go = loop.create_future()
            going = lambda: own.go  # noqa: E731
        running = (
            execute_trigger(request, own, self.imported)
            if request.get("trigger") is not None
            else execute_watcher(request, own, self.imported, own.moving, going)
        )
        step = loop.create_task(running, context=context)
        self._watchers[watcher] = (own, step)
        # Not what is cancelled: a step cancelled before its first turn never runs a line of its
        # own, and its end has to be told all the same.
        self._ending.add(loop.create_task(self._end(watcher, own, step, grace)))
        for ending in tuple(self._ending):
            if ending.done():
                self._ending.discard(ending)

    async def _end(
        self, watcher: int, own: WatcherChannel, step: asyncio.Task[Any], grace: float
    ) -> None:
        """Tells a step's end, once what it started has ended too."""
        await asyncio.wait({step})
        if step.cancelled():
            result: dict[str, Any] = {"type": "watcher_cancelled"}
        elif (error := step.exception()) is not None:
            result = failure(error, None)
        else:
            result = step.result()
        await _stopped_children(own)
        stopped = await _stopped_threads(own, grace)
        _warn_of_threads(own)
        if not stopped:
            own.send({"type": "watcher_stuck"})
            own.send(
                {
                    "type": "watcher_log",
                    "stream": "stderr",
                    "text": "a thread of this wait step didn't stop in time: Python can't end one "
                    "blocked in C code, so its host ends with it, and the wait steps on it\n",
                }
            )
        del self._watchers[watcher]
        # Before its result: a host left with no wait steps is stopped, and what its threads'
        # ends were wouldn't be heard otherwise.
        self.say_how_it_is()
        _tell(own, result)
        own.ended = True
        if not stopped:
            _end_the_host()

    def cancel(self, watcher: int) -> None:
        found = self._watchers.get(watcher)
        if found is not None:
            found[1].cancel()

    def move(self, watcher: int) -> None:
        """Has a poke stop between its calls; one ended already has said how."""
        found = self._watchers.get(watcher)
        if found is not None:
            found[0].moving.set()

    def go(self, watcher: int, max_wait_ms: int | None) -> None:
        """Lets a poke that moved here make its first call."""
        found = self._watchers.get(watcher)
        if found is not None and found[0].go is not None and not found[0].go.done():
            found[0].go.set_result(max_wait_ms)

    def answer(self, watcher: int, message: dict[str, Any]) -> None:
        """An answer for a watcher, on the reader's thread; one already ended is ignored."""
        found = self._watchers.get(watcher)
        if found is not None:
            found[0].answers.put(message)


async def _stopped_children(own: WatcherChannel) -> None:
    """Cancels what a step left running and waits for it, as ``asyncio.run`` does for a task's.

    One refusing to end keeps its step from ending: its time runs out and its host is killed.
    """
    current = asyncio.current_task()
    while True:
        children = {
            task
            for task in asyncio.all_tasks()
            if task is not current and task.get_context().get(_WATCHER) is own
        }
        if not children:
            return
        for child in children:
            child.cancel()
        await asyncio.wait(children)
        for child in children:
            if not child.cancelled() and (error := child.exception()) is not None:
                print(f"warning: a task it left running failed: {error!r}", file=sys.stderr)


async def _stopped_threads(own: WatcherChannel, grace: float) -> bool:
    """Stops what a step still runs in the host's threads.

    Returns:
        Whether they all ended within ``grace`` seconds.
    """
    calls = tuple(own.calls)
    if not calls:
        return True
    ended = [asyncio.wrap_future(call) for call in calls]
    for call in calls:
        call.cancel()
    _, late = await asyncio.wait(ended, timeout=grace)
    for waited in ended:
        if waited.done() and not waited.cancelled():
            waited.exception()  # Ended as asked: nobody waits for it now.
    return not late


def _warn_of_threads(own: WatcherChannel) -> None:
    """Says what a step's library left running: not refused, but nothing can stop it (339)."""
    alive = sorted({thread.name for thread in own.threads if thread.is_alive()})
    if alive:
        own.send(
            {
                "type": "watcher_log",
                "stream": "stderr",
                "text": f"warning: threads started by this wait step are still running: {alive}\n",
            }
        )


def _tell(own: WatcherChannel, result: dict[str, Any]) -> None:
    """Sends a step's end: a result that can't be sent is its failure, never its silence.

    Without a channel the host has no use, and its agent's reader ends it.
    """
    try:
        own.send(result)
    except ProtocolError as e:
        own.send(failure(e, "output_too_large"))


def _end_the_host() -> None:
    """Ends the host and what its watchers started: no one else could stop them, or find them."""
    with contextlib.suppress(OSError):
        if os.getpgrp() == os.getpid():
            os.killpg(os.getpid(), signal.SIGKILL)
    os._exit(0)


def _read(channel: Channel, host: _Host, loop: asyncio.AbstractEventLoop) -> None:
    """Reads the agent's messages for as long as it is there: without it the host has no use."""
    while True:
        try:
            message = channel.receive()
        except ProtocolError:
            _end_the_host()
        kind = message["type"]
        if kind == "start_watcher":
            starting = functools.partial(
                host.start,
                message["watcher"],
                message["task"],
                moved_in=message.get("moved_in", False),
                grace=message["cancel_grace_ms"] / 1000,
            )
            loop.call_soon_threadsafe(starting)
        elif kind == "move_watcher":
            loop.call_soon_threadsafe(host.move, message["watcher"])
        elif kind == "go_watcher":
            loop.call_soon_threadsafe(host.go, message["watcher"], message.get("max_wait_ms"))
        elif kind == "cancel_watcher":
            loop.call_soon_threadsafe(host.cancel, message["watcher"])
        elif kind == "to_watcher":
            host.answer(message["watcher"], message["message"])


def _warn(_loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
    """A background task's error, in the log of the watcher it belongs to, if any."""
    error = context.get("exception")
    detail = f": {error!r}" if error is not None else ""
    print(f"warning: {context.get('message', 'asyncio')}{detail}", file=sys.stderr)


def _watch_threads() -> None:
    """Has each Python thread remembered by the watcher that started it, for its end."""
    start = threading.Thread.start

    def started(thread: threading.Thread) -> None:
        own = _WATCHER.get()
        if own is not None and not thread.name.startswith(_POOL_THREADS):
            # A step starting threads for days keeps only those alive.
            # One still being started isn't alive yet.
            own.threads = [t for t in own.threads if t.ident is None or t.is_alive()]
            own.threads.append(thread)
        start(thread)

    threading.Thread.start = started  # type: ignore[method-assign, assignment]  # the one way to see them all


def _threads_of(host: _Host) -> tuple[int, int]:
    """How many threads the process has, and how many of them wait steps are tied to.

    Tied: those running a step's ``to_thread`` calls, and Python threads a step started. All
    the rest (a library's pools, the host's own) can't be told apart, so they aren't.

    Args:
        host: The host.

    Returns:
        All of them, and the tied ones.
    """
    try:
        threads = len(os.listdir("/proc/self/task"))
    except OSError:  # Not Linux.
        threads = threading.active_count()
    tied: set[int] = set()
    for own, _step in tuple(host._watchers.values()):
        tied.update(
            running for call in tuple(own.calls) if (running := call.running_in()) is not None
        )
        tied.update(
            thread.ident
            for thread in tuple(own.threads)
            if thread.ident is not None and thread.is_alive()
        )
    return threads, len(tied)


def _watch_the_loop(
    host: _Host,
    loop: asyncio.AbstractEventLoop,
    limits: tuple[float, float],
) -> None:
    """The host's watchdog: says it is there, and who holds the loop past its limit.

    The agent kills the host for that: Python can't cancel what doesn't yield.

    Args:
        host: The host.
        loop: Its loop, on this process's main thread.
        limits: How long the loop may be held, in seconds, and while a DAG file is imported.
    """
    block_limit, import_limit = limits
    loop_thread = threading.main_thread().ident
    turned = time.monotonic()
    holding: object = None
    holding_since = turned

    def turn(asked: float) -> None:
        nonlocal turned
        turned = time.monotonic()
        host.loop_lag = max(host.loop_lag, turned - asked)

    while True:
        time.sleep(min(_CHECK_EVERY, block_limit / 4))
        try:
            loop.call_soon_threadsafe(turn, time.monotonic())
        except RuntimeError:  # Its loop has closed: there is nothing left to watch.
            return
        host.say_how_it_is()
        now = time.monotonic()
        running = asyncio.current_task(loop)
        # Many taking their turns can hold up a turn of the loop: only one on its own is to blame.
        if running is not holding:
            holding, holding_since = running, now
        limit = import_limit if host.importing else block_limit
        if min(now - turned, now - holding_since) > limit:
            own = None if running is None else running.get_context().get(_WATCHER)
            frame = sys._current_frames().get(loop_thread or 0)  # the one way to see it
            host.send(
                {
                    "type": "loop_blocked",
                    "watcher": None if own is None else own.watcher,
                    "stack": "" if frame is None else "".join(traceback.format_stack(frame)),
                }
            )
            return


def guarded_loop_factory() -> Callable[[], asyncio.AbstractEventLoop] | None:
    """The host's event loop: ``uvloop`` starts processes where the audit hook can't see them.

    Returns:
        Its factory; ``None`` for ``asyncio``'s own, which the hook sees.
    """
    if loop_factory() is None:
        return None
    import uvloop  # noqa: PLC0415  # as ``loop_factory``, optional

    class Guarded(uvloop.Loop):
        async def subprocess_exec(self, *args: Any, **kwargs: Any) -> Any:
            refuse_a_process()
            return await super().subprocess_exec(*args, **kwargs)

        async def subprocess_shell(self, *args: Any, **kwargs: Any) -> Any:
            refuse_a_process()
            return await super().subprocess_shell(*args, **kwargs)

    return Guarded


def run_watchers(channel: Channel, request: dict[str, Any]) -> int:
    """Handles a ``watchers`` request: serves watchers until the agent stops the host.

    Args:
        channel: The channel to the agent.
        request: The ``watchers`` message (the ``sys.path`` entries of its code set).

    Returns:
        The exit code; it ends when the agent closes the channel, or is killed.
    """
    extend_sys_path(request["sys_path"])
    sys.stdout = _Routed("stdout", sys.stdout)
    sys.stderr = _Routed("stderr", sys.stderr)
    host = _Host(channel)

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.set_default_executor(_WatcherThreads(thread_name_prefix=_POOL_THREADS))
        _watch_threads()
        loop.set_exception_handler(_warn)
        threading.Thread(
            target=_read, args=(channel, host, loop), name="schedr-agent", daemon=True
        ).start()
        limits = (request["block_limit_ms"] / 1000, request["import_limit_ms"] / 1000)
        set_poke_overruns(request["poke_overruns"])
        threading.Thread(
            target=_watch_the_loop, args=(host, loop, limits), name="schedr-watchdog", daemon=True
        ).start()
        await asyncio.Event().wait()

    asyncio.run(main(), loop_factory=guarded_loop_factory())
    return 0
