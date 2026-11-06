"""How a wait step waits: until its value or its deadline (see ``schedr.sensors``)."""

from __future__ import annotations

import asyncio
import contextlib
import sys
from contextvars import ContextVar
from typing import TYPE_CHECKING

from schedr.outcomes import TimedOut
from schedr.sensors import NOT_YET

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping
    from types import FrameType


class ProcessInWaitStepError(RuntimeError):
    """A wait step tried to start a process (decision 336)."""


class StepEndedError(BaseException):
    """Raised in a wait step's ``asyncio.to_thread`` call when its time is up (decision 337).

    As ``asyncio.CancelledError``, not an ``Exception``: ``except Exception`` doesn't keep it going.
    """


_REFUSAL = (
    "a wait step can't start a process: it shares one with other wait steps, so nothing could "
    "stop what it left running. Use an async client, or run the command in a task after it"
)

_STARTS_A_PROCESS = frozenset(
    {
        "subprocess.Popen",
        "os.system",
        "os.fork",
        "os.forkpty",
        "os.posix_spawn",
        "os.spawn",
        "os.exec",
    }
)
"""The audit events that start a process, however it is asked for."""

_WAITS_FOR_ITS_OWN = frozenset({"platform", "ctypes.util", "uuid"})
"""Standard library modules whose helpers run a command and wait for it: client libraries call
them to describe the machine, and they can leave nothing running."""

_IN_WAIT_STEP: ContextVar[bool] = ContextVar("schedr_in_wait_step", default=False)
"""Whether this code runs for a wait step's function."""

_guarded = False


def refuse_a_process() -> None:
    """Refuses to start a process, in a wait step's function.

    Raises:
        ProcessInWaitStepError: When this code runs for one.
    """
    if _IN_WAIT_STEP.get():
        raise ProcessInWaitStepError(_REFUSAL)


def _on_audit(event: str, _args: object) -> None:
    if event not in _STARTS_A_PROCESS or not _IN_WAIT_STEP.get():
        return
    frame: FrameType | None = sys._getframe(1)  # who asked: no other way to know
    while frame is not None:
        if frame.f_globals.get("__name__") in _WAITS_FOR_ITS_OWN:
            return
        frame = frame.f_back
    raise ProcessInWaitStepError(_REFUSAL)


def _guard() -> None:
    """Installs the audit hook, once: it can't be removed, and does nothing outside wait steps."""
    global _guarded  # noqa: PLW0603  # once per process
    if not _guarded:
        sys.addaudithook(_on_audit)
        _guarded = True


class WatcherMovedError(Exception):
    """A poke stopped between calls: its next one is made on new code (decision 335)."""


class PokeTooSlowError(Exception):
    """A ``@sensor.poke`` call ran out its ``every`` (decision 322)."""


async def _within(
    call: Coroutine[object, object, object], limit: float | None
) -> tuple[bool, object]:
    """Runs ``call`` for at most ``limit`` seconds.

    Args:
        call: The coroutine.
        limit: Seconds; ``None``: no limit.

    Returns:
        ``(True, value)``, or ``(False, None)`` when it was cancelled at the limit.
    """
    loop = asyncio.get_running_loop()
    took: list[float] = []

    async def timed() -> object:
        started = loop.time()
        try:
            return await call
        finally:
            took.append(loop.time() - started)

    running = asyncio.ensure_future(timed())
    late = False
    try:
        await asyncio.wait({running}, timeout=limit)
        # Its own time: a call blocking the loop ends done, its limit never having fired; and a
        # stall after it ended, by something else on the loop, isn't its doing.
        late = bool(took) and limit is not None and took[0] >= limit
        if not running.done():
            late = True
            running.cancel()
            await asyncio.wait({running})
    except asyncio.CancelledError:
        # Stopped from outside: it ends first, so its `finally` runs; one already cancelled
        # at the limit is left to finish that `finally`.
        if not late:
            running.cancel()
        await asyncio.wait({running})
        raise
    if not late:
        # Its own `CancelledError`, not at the limit, is no timeout: it goes up.
        return True, running.result()
    # The limit decides, however it took the cancel: a value it then returned came too late.
    if not running.cancelled() and (error := running.exception()) is not None:
        print(f"warning: while being cancelled at its limit: {error!r}", file=sys.stderr)
    return False, None


async def _until(moving: asyncio.Event | None, delay: float) -> None:
    """Sleeps, unless the poke is to move: then it ends here, between its calls."""
    if moving is None:
        await asyncio.sleep(delay)
        return
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(delay):
            await moving.wait()
    if moving.is_set():
        raise WatcherMovedError


async def _poked(
    call: Callable[[], Coroutine[object, object, object]],
    every: float,
    deadline: float | None,
    moving: asyncio.Event | None,
) -> object:
    loop = asyncio.get_running_loop()
    while True:
        await _until(moving, 0.0)
        started = loop.time()
        left = None if deadline is None else deadline - started
        if left is not None and left <= 0:
            return TimedOut()
        ended, value = await _within(call(), every if left is None else min(every, left))
        if not ended:
            if left is not None and left <= every:
                return TimedOut()
            raise PokeTooSlowError(
                f"a call took longer than `every` ({every:g} s): each call is limited to it, as "
                "calls never overlap. Make it faster, or poke less often"
            )
        if value is not NOT_YET:
            return value
        following = started + every
        if deadline is not None and following >= deadline:
            await _until(moving, max(0.0, deadline - loop.time()))
            return TimedOut()
        await _until(moving, max(0.0, following - loop.time()))


async def waited(
    function: Callable[..., Coroutine[object, object, object]],
    arguments: Mapping[str, object],
    task: Mapping[str, object],
    moving: asyncio.Event | None = None,
) -> object:
    """Runs a wait step to its end.

    Args:
        function: The wait step's function.
        arguments: Its arguments.
        task: The task as DAG JSON: its ``every_ms`` and ``max_wait_ms``.
        moving: Set when a poke is to stop between its calls; ``None``: it never is.

    Returns:
        Its value, or ``TimedOut()`` if its time ran out.

    Raises:
        PokeTooSlowError: If a ``@sensor.poke`` call ran out its ``every``.
        WatcherMovedError: If ``moving`` was set before a value came.
    """
    _guard()
    token = _IN_WAIT_STEP.set(True)
    try:
        return await _waited(function, arguments, task, moving)
    finally:
        _IN_WAIT_STEP.reset(token)


async def _waited(
    function: Callable[..., Coroutine[object, object, object]],
    arguments: Mapping[str, object],
    task: Mapping[str, object],
    moving: asyncio.Event | None,
) -> object:
    max_wait = task.get("max_wait_ms")
    every = task.get("every_ms")
    seconds = None if not isinstance(max_wait, int) else max_wait / 1000
    if isinstance(every, int):
        loop = asyncio.get_running_loop()
        deadline = None if seconds is None else loop.time() + seconds
        return await _poked(lambda: function(**arguments), every / 1000, deadline, moving)
    ended, value = await _within(function(**arguments), seconds)
    return value if ended else TimedOut()
