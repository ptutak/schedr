"""The runner's entry point: the handshake with the agent, then the requested mode."""

import sys
from collections.abc import Callable, Mapping
from importlib.metadata import version
from typing import TextIO

from schedr.runner.parse import run_parse
from schedr.runner.protocol import Channel, ProtocolError, from_fd
from schedr.runner.task import run_task

FD_ENV = "SCHEDR_AGENT_FD"
"""The environment variable with the descriptor of the socket the agent handed over."""

MODES = ("parse", "task")
"""The modes the runner can be started in."""

EXIT_ERROR = 1
"""Exit code for a failure talking to the agent."""

EXIT_USAGE = 2
"""Exit code for a wrong start: unknown mode, no agent socket, refused handshake."""


def main(
    argv: list[str],
    environ: Mapping[str, str],
    *,
    open_channel: Callable[[int], Channel] = from_fd,
    stderr: TextIO | None = None,
) -> int:
    """Runs the runner: ``parse`` or ``task``.

    Args:
        argv: The arguments after ``-m schedr.runner``.
        environ: The environment (for ``SCHEDR_AGENT_FD``).
        open_channel: Opens the channel on the inherited descriptor (replaced in tests).
        stderr: Where messages go; ``sys.stderr`` by default.

    Returns:
        The exit code: 0 on success, 1 on a failed task or a failure talking to the agent, 2
        on a wrong start.
    """
    err = stderr or sys.stderr
    if len(argv) != 1 or argv[0] not in MODES:
        print(f"usage: python -m schedr.runner {{{','.join(MODES)}}}", file=err)
        return EXIT_USAGE
    mode = argv[0]
    value = environ.get(FD_ENV)
    if not value:
        print(f"{FD_ENV} is not set: the runner is started by the schedr agent", file=err)
        return EXIT_USAGE
    if not value.isdigit():
        print(f"{FD_ENV}={value} isn't a file descriptor", file=err)
        return EXIT_USAGE
    try:
        channel = open_channel(int(value))
    except OSError as e:
        print(f"can't use the agent's socket (descriptor {value}): {e}", file=err)
        return EXIT_ERROR
    try:
        return _serve(channel, mode, err)
    except ProtocolError as e:
        print(f"agent channel: {e}", file=err)
        return EXIT_ERROR
    finally:
        channel.close()


def _serve(channel: Channel, mode: str, err: TextIO) -> int:
    channel.send({"type": "hello", "schedr_version": version("schedr"), "mode": mode})
    reply = channel.receive()
    if reply["type"] == "refused":
        print(f"the agent refused the runner: {reply.get('reason', '')}", file=err)
        return EXIT_USAGE
    if reply["type"] != "welcome":
        raise ProtocolError(f"expected `welcome`, got `{reply['type']}`")
    request = channel.receive()
    if request["type"] != mode:
        raise ProtocolError(f"expected `{mode}`, got `{request['type']}`")
    if mode == "task":
        return run_task(channel, request)
    run_parse(channel, request)
    return 0
