"""How a wait step waits: until its value or its deadline (see ``schedr.sensors``)."""

from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

from schedr.outcomes import TimedOut
from schedr.sensors import NOT_YET

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping


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


async def _poked(
    call: Callable[[], Coroutine[object, object, object]],
    every: float,
    deadline: float | None,
) -> object:
    loop = asyncio.get_running_loop()
    while True:
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
            await asyncio.sleep(max(0.0, deadline - loop.time()))
            return TimedOut()
        await asyncio.sleep(max(0.0, following - loop.time()))


async def waited(
    function: Callable[..., Coroutine[object, object, object]],
    arguments: Mapping[str, object],
    task: Mapping[str, object],
) -> object:
    """Runs a wait step to its end.

    Args:
        function: The wait step's function.
        arguments: Its arguments.
        task: The task as DAG JSON: its ``every_ms`` and ``max_wait_ms``.

    Returns:
        Its value, or ``TimedOut()`` if its time ran out.

    Raises:
        PokeTooSlowError: If a ``@sensor.poke`` call ran out its ``every``.
    """
    max_wait = task.get("max_wait_ms")
    every = task.get("every_ms")
    seconds = None if not isinstance(max_wait, int) else max_wait / 1000
    if isinstance(every, int):
        loop = asyncio.get_running_loop()
        deadline = None if seconds is None else loop.time() + seconds
        return await _poked(lambda: function(**arguments), every / 1000, deadline)
    ended, value = await _within(function(**arguments), seconds)
    return value if ended else TimedOut()
