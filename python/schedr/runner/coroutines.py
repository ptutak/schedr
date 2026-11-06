"""Runs an ``async def`` task's coroutine in the attempt's own event loop.

Imported only for such a task: ``asyncio`` is slow to import, and most tasks don't need it.
The loop is ``uvloop``'s whenever this interpreter can import it, however it was installed
(decision 316; ``schedr[uvloop]`` is one way); otherwise ``asyncio``'s own.
"""

import asyncio
import os
import signal
import sys
from collections.abc import Callable, Coroutine, Mapping


def loop_factory() -> Callable[[], asyncio.AbstractEventLoop] | None:
    """What makes the event loop: ``uvloop``'s if it is installed.

    Returns:
        Its factory; ``None`` for ``asyncio``'s own.
    """
    try:
        import uvloop  # noqa: PLC0415  # optional; most tasks don't start a loop at all
    except ImportError:
        return None
    return uvloop.new_event_loop


def _warn(_loop: asyncio.AbstractEventLoop, context: Mapping[str, object]) -> None:
    """A background task's error, in the task's log: it doesn't change the result (decision 49)."""
    error = context.get("exception")
    detail = f": {error!r}" if error is not None else ""
    print(f"warning: {context.get('message', 'asyncio')}{detail}", file=sys.stderr)


def run_coroutine[T](coroutine: Coroutine[object, object, T]) -> T:
    """Runs a task's coroutine until it ends, in a new event loop.

    SIGTERM cancels the coroutine, so its ``finally`` and ``async with`` blocks run within the
    grace time; the process then ends by that signal, as a sync task's does at once, whatever
    the coroutine did with the cancellation. A loop blocked by sync code can't run them, and
    is killed when the grace time ends. Background tasks still running at the end are
    cancelled, as ``asyncio.run`` does. The task's log says which loop it ran on.

    Args:
        coroutine: The coroutine.

    Returns:
        What it returned.

    Raises:
        RuntimeError: When the coroutine was cancelled by something other than the stop signal.
    """
    stopped = False
    factory = loop_factory()
    # Installing `uvloop` is the only switch, so a difference between machines shows here.
    print(f"event loop: {'asyncio' if factory is None else 'uvloop'}", file=sys.stderr, flush=True)

    async def main() -> T:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(_warn)
        running = asyncio.current_task()

        def stop() -> None:
            nonlocal stopped
            stopped = True
            if running is not None:
                running.cancel()

        loop.add_signal_handler(signal.SIGTERM, stop)
        return await coroutine

    try:
        return asyncio.run(main(), loop_factory=factory)
    except asyncio.CancelledError as e:
        if stopped:
            raise
        raise RuntimeError("the task's coroutine was cancelled, not by schedr") from e
    finally:
        if stopped:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            os.kill(os.getpid(), signal.SIGTERM)
