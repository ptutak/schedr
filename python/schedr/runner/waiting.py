"""How a wait step waits: until its value or its deadline (see ``schedr.sensors``)."""

from __future__ import annotations

import asyncio
import contextlib
import sys
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from schedr.outcomes import TimedOut
from schedr.sensors import NOT_YET

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Mapping, Sequence
    from types import FrameType

    from schedr.event_triggers import Event


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


class CallTimedOutError(Exception):
    """An ``@sensor(wait_for=...)``'s call ran out its ``timeout`` (decision 353)."""


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
        CallTimedOutError: If an ``@sensor(wait_for=...)`` call ran out its ``timeout``.
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
    wait_for = task.get("wait_for")
    call_ms = (
        cast("dict[str, object]", wait_for).get("timeout_ms")
        if isinstance(wait_for, dict)
        else None
    )
    if isinstance(call_ms, int) and (seconds is None or call_ms / 1000 < seconds):
        # Its own limit comes first: a call over it hung, a failure its retries cover (the
        # same run), not the end of the wait.
        ended, value = await _within(function(**arguments), call_ms / 1000)
        if not ended:
            raise CallTimedOutError(
                f"a call took longer than its `timeout` ({call_ms / 1000:g} s), and was cancelled"
            )
        return value
    ended, value = await _within(function(**arguments), seconds)
    return value if ended else TimedOut()


async def _poked_trigger(
    function: Callable[..., object],
    arguments: Mapping[str, object],
    every: float,
    after: str | None,
    tell: Callable[[Event[Any]], Awaitable[None]],
    poked: Callable[[str | None], None],
    batch: Batch | None = None,
) -> None:
    """Calls a poke every ``every`` seconds, for as long as it is left running.

    With ``batch``, what it returns is gathered and told as one event per window (decision 352):
    ``after`` moves on with the items gathered, so the next poke goes on from them, while the
    master's bookmark moves only with a window sent.
    """
    loop = asyncio.get_running_loop()
    window = _Window(batch, tell)
    while True:
        called = loop.time()
        # What telling raises (the window sent meanwhile) isn't the poke's: it goes up.
        found, error = await window.meanwhile(_poke_once(function, after, arguments, every))
        for event in cast("Sequence[Event[Any]]", found):
            await window.add(event)
            after = event.key
        poked(error)
        await window.meanwhile(asyncio.sleep(max(0.0, called + every - loop.time())))


async def _poke_once(
    function: Callable[..., object],
    after: str | None,
    arguments: Mapping[str, object],
    every: float,
) -> tuple[object, str | None]:
    """One call of a trigger's poke: what it found, or its error as shown on its status."""
    try:
        call = cast("Coroutine[object, object, object]", function(after, **arguments))
        ended, found = await _within(call, every)
        if not ended:
            raise PokeTooSlowError(  # as what it raises is, below
                f"a call took longer than `every` ({every:g} s): each call is limited to it, "
                "as calls never overlap. Make it faster, or poke less often"
            )
    except Exception as e:  # the author's: shown on its status, and the next call comes
        return (), f"{type(e).__name__}: {e}"
    return found, None


class _Window:
    """A batched poke's open window: its items, its size, and when it ends (decision 352).

    Without a batch, each item is told at once.
    """

    def __init__(self, batch: Batch | None, tell: Callable[[Event[Any]], Awaitable[None]]) -> None:
        self._batch = batch
        self._tell = tell
        self._items: list[Event[Any]] = []
        self._size = 2  # `[]`: a list is stored as compact JSON
        self._ends = 0.0

    async def add(self, event: Event[Any]) -> None:
        """Gathers an item: one over the window's size goes to the next.

        Raises:
            TypeError: For an item without a key, as one told alone would (Grok's review: the
                window it ended was lost with it).
        """
        batch = self._batch
        if batch is None:
            await self._tell(event)
            return
        if not event.key:
            raise TypeError("an event's key must be a non-empty string, got ''")
        loop = asyncio.get_running_loop()
        # Each item sized once: the list's size is theirs, with its brackets and commas.
        size = batch.size(event)
        if self._items and self._size + 1 + size > batch.limit:
            # Near its `max_output_size`: this window goes now.
            await self.send()
        if not self._items:
            self._ends = loop.time() + batch.seconds
        self._size += size + (1 if self._items else 0)
        self._items.append(event)

    async def send(self) -> None:
        """Tells the window's items as one event, if it has any."""
        if self._items:
            items, self._items, self._size = self._items, [], 2
            await self._tell(batched(items))

    async def meanwhile[T](self, work: Coroutine[object, object, T]) -> T:
        """Awaits ``work``, sending the window when it ends: a poke or a sleep doesn't hold it.

        Returns:
            What ``work`` gives.
        """
        running = asyncio.ensure_future(work)
        try:
            while self._items:
                left = max(0.0, self._ends - asyncio.get_running_loop().time())
                done, _ = await asyncio.wait({running}, timeout=left)
                if not done or left == 0.0:
                    # Ended meanwhile, or as it did: what it gives goes to the next window.
                    await self.send()
                if done:
                    break
            return await running
        finally:
            # Stopped meanwhile (the trigger cancelled): what it waited for stops too.
            running.cancel()


@dataclass(frozen=True, slots=True)
class Batch:
    """An ``@trigger.poke(batch=...)``'s window (decision 352).

    Attributes:
        seconds: How long it gathers from the first item.
        size: An item's size as stored, in bytes; one that can't be stored is given as over
            ``limit``, so it goes alone and fails when it is told, as it would unbatched.
        limit: The most one event's data may take: its ``max_output_size``.
    """

    seconds: float
    size: Callable[[Event[Any]], int]
    limit: int


def batched(items: list[Event[Any]]) -> Event[Any]:
    """One event for a window's items: the last one's key, the items as its data (decision 352).

    Returns:
        The event to tell.
    """
    from schedr.event_triggers import Event  # noqa: PLC0415 (slow to import here)

    return Event(items[-1].key, list(items))


async def run_trigger(
    function: Callable[..., object],
    arguments: Mapping[str, object],
    task: Mapping[str, object],
    after: str | None,
    tell: Callable[[Event[Any]], Awaitable[None]],
    poked: Callable[[str | None], None],
    batch: Batch | None = None,
) -> None:
    """Calls a trigger's function, telling each event found; returns when a generator does.

    Args:
        function: The function.
        arguments: Its arguments, without ``after``.
        task: Its task: called every ``every_ms``, if it has that.
        after: The last key acted on.
        tell: Tells an event and waits until it is safe: the next one's order is kept.
        poked: Told how each call of a poke ended: why it failed, if it did.
        batch: A poke's ``batch=`` window, if it has one.
    """
    every = task.get("every_ms")
    if isinstance(every, int):
        await _poked_trigger(function, arguments, every / 1000, after, tell, poked, batch)
        return
    generator = cast("AsyncIterator[Event[Any]]", function(after, **arguments))
    async for event in generator:
        await tell(event)
