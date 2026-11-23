"""An exception as sent to the agent, its traceback from the user's code on.

Shared by a task's runner and an extension's plugin.
"""

import traceback

TRACEBACK_LINES = 20
"""How many traceback lines an error keeps."""


def error_summary(error: BaseException, internal: tuple[str, ...]) -> dict[str, str]:
    """An exception as sent to the agent.

    The traceback starts at the user's code: the frames before it in files starting with one of
    `internal` are left out (all of them are kept when every frame is internal).

    Args:
        error: The exception.
        internal: The path prefixes of the frames that aren't the user's.

    Returns:
        Its class name, message and the last :data:`TRACEBACK_LINES` lines of the traceback.
    """
    tb = error.__traceback__
    while tb is not None and tb.tb_frame.f_code.co_filename.startswith(internal):
        tb = tb.tb_next
    shown = error.__traceback__ if tb is None else tb
    lines = "".join(traceback.format_exception(type(error), error, shown)).splitlines()
    return {
        "exception": type(error).__name__,
        "message": str(error),
        "traceback": "\n".join(lines[-TRACEBACK_LINES:]),
    }
