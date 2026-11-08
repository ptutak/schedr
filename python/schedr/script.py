"""Plain Python scripts in a command task: the same ``dag_ctx`` as a Python task.

``from schedr.script import dag_ctx`` gives a script run by a command task (``python
report.py``, or any process the task starts) the run's context as values:
``dag_ctx.params.region``, ``dag_ctx.params(ReportParams)``, ``dag_ctx.run.planned_for``. It
is loaded from the agent over the task's context channel (``SCHEDR_CTX_FD``) the first time
it is read, with the same requests as ``schedr dag-ctx`` (decisions 143-146).

The values are decoded as in a task: a date is a ``date``, a duration a ``timedelta``, a tuple
a tuple. A script doesn't know the class the DAG declares, so ``dag_ctx.params.x`` is a field
decoded by its stored form (a dataclass or model field holding an object is a ``dict``), and
``dag_ctx.params(ReportParams)`` decodes strictly into the class passed.

Inside a Python task, the task's own values are used; anywhere else (a Python task's
subprocess included: it has no channel) reading ``dag_ctx`` raises ``RuntimeError``.
"""

import array
import contextlib
import os
import select
import socket
import sys
import threading
import time
from collections.abc import Mapping
from datetime import datetime
from importlib.metadata import version
from typing import Any

from schedr.context import RunValues, dag_ctx, use_script_values
from schedr.runner.protocol import Channel, ProtocolError

__all__ = ["dag_ctx"]

CTX_FD_ENV = "SCHEDR_CTX_FD"
"""The environment variable with a command task's descriptor of its context channel."""

CTX_PROTOCOL = 1
"""The context channel's protocol (decision 148): accepted by any agent speaking it."""

ANSWER_TIMEOUT = 30.0
"""How long the agent has to answer, in seconds."""

_OUTSIDE = (
    "`schedr.script` must be run inside a schedr task: a command task's processes can read "
    f"its context ({CTX_FD_ENV} isn't set here); a Python task reads `dag_ctx` itself"
)

_STDERR = 2
"""The highest standard descriptor (stdin, stdout, stderr): never the channel."""

_MAX_FD = 2**31 - 1
"""The highest descriptor number (a C ``int``); larger text isn't one."""

_lock = threading.Lock()
_loaded: RunValues | None = None


def _closed(fd: int) -> str:
    return (
        f"a parent process closed it (Python's `subprocess` closes inherited descriptors "
        f"unless given `pass_fds=[{fd}]`)"
    )


def _channel(environ: Mapping[str, str]) -> socket.socket:
    """A copy of the task's context channel (the inherited descriptor stays as it is)."""
    text = environ.get(CTX_FD_ENV)
    if text is None:
        raise RuntimeError(_OUTSIDE)
    if not text.isdigit() or not _STDERR < int(text) <= _MAX_FD:
        raise RuntimeError(f"{CTX_FD_ENV}=`{text}` isn't a descriptor number")
    fd = int(text)
    try:
        copy = os.dup(fd)  # not inheritable, as Python makes every descriptor
    except OSError:
        raise RuntimeError(
            f"the task's context channel ({CTX_FD_ENV}={fd}) isn't open here: {_closed(fd)}"
        ) from None
    try:
        sock = socket.socket(fileno=copy)
    except OSError:
        os.close(copy)
        sock = None
    if sock is None or sock.family != socket.AF_UNIX or sock.type != socket.SOCK_DGRAM:
        if sock is not None:
            sock.close()
        raise RuntimeError(
            f"descriptor {fd} ({CTX_FD_ENV}) isn't the task's context channel any more: "
            f"{_closed(fd)}, and the number was reused"
        )
    return sock


def _send_end(channel: socket.socket, end: socket.socket, wait: float) -> None:
    """Sends ``end`` over the channel, waiting for room in it until ``wait`` seconds.

    The send itself never waits (``MSG_DONTWAIT``): the inherited socket is shared with every
    process of the task, and may block.

    Raises:
        TimeoutError: If the channel had no room in time.
        OSError: If the send failed (``ConnectionRefusedError``: the task ended).
    """
    deadline = time.monotonic() + wait
    while True:
        try:
            # Not `socket.send_fds`: it drops its `flags` (CPython 3.14), so it would block.
            rights = array.array("i", [end.fileno()])
            channel.sendmsg(
                [b"c"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)], socket.MSG_DONTWAIT
            )
        except BlockingIOError:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError from None
            select.select([], [channel], [], left)
        else:
            return


def _connect(environ: Mapping[str, str]) -> Channel:
    """A connection of this script's own: a fresh pair, one end sent to the agent."""
    channel = _channel(environ)
    mine, theirs = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        _send_end(channel, theirs, ANSWER_TIMEOUT)
    except ConnectionRefusedError:
        mine.close()
        raise RuntimeError(
            "the task's agent no longer serves its context: the task has ended (a process it "
            "left running can't read it)"
        ) from None
    except TimeoutError:
        mine.close()
        raise RuntimeError(
            f"the task's agent didn't take the call within {ANSWER_TIMEOUT:g}s"
        ) from None
    finally:
        theirs.close()
        channel.close()
    mine.settimeout(ANSWER_TIMEOUT)
    return Channel(mine)


def _ask(channel: Channel, request: str, answer: str) -> dict[str, Any]:
    channel.send({"type": request})
    reply = channel.receive()
    if reply["type"] == "error":
        raise RuntimeError(f"the task's agent: {reply.get('reason', '')}")
    if reply["type"] != answer:
        raise ProtocolError(f"expected `{answer}`, got `{reply['type']}`")
    return reply


def load(environ: Mapping[str, str] | None = None) -> RunValues:
    """The run's context, from the task's agent.

    Args:
        environ: The environment (for ``SCHEDR_CTX_FD``); ``os.environ`` by default.

    Returns:
        The values, with ``from_script`` set.

    Raises:
        RuntimeError: Outside a command task, when its channel is closed or the task ended, or
            when the agent refuses (another protocol).
    """
    channel = _connect(os.environ if environ is None else environ)
    try:
        # An agent refusing the connection (too many open) says why before closing it, maybe
        # before the hello arrives: its answer is read even when the hello can't be sent.
        with contextlib.suppress(OSError):
            channel.send(
                {"type": "hello", "protocol": CTX_PROTOCOL, "schedr_version": version("schedr")}
            )
        reply = channel.receive()
        if reply["type"] == "refused":
            raise RuntimeError(f"the task's agent refused: {reply.get('reason', '')}")
        if reply["type"] == "error":
            raise RuntimeError(str(reply.get("reason", "")))
        if reply["type"] != "welcome":
            raise ProtocolError(f"expected `welcome`, got `{reply['type']}`")
        params = _ask(channel, "params_get", "params")["params"]
        run = _ask(channel, "run_get", "run")
    except (ProtocolError, OSError) as e:
        raise RuntimeError(f"talking to the task's agent: {e} (the task may have ended)") from None
    finally:
        channel.close()
    return RunValues(
        dag_id=run["dag_id"],
        run_id=run["id"],
        kind=run["kind"],
        planned_for=datetime.fromisoformat(run["planned_for"]),
        since=datetime.fromisoformat(run["since"]),
        params=params,
        params_class=None,
        from_script=True,
    )


API_ANSWER_TIMEOUT = 100.0
"""How long an API call's answer may take, in seconds: the agent waits for its master up to a
minute, over reconnects."""


def call_api(
    method: str,
    path: str,
    body: object,
    key: str | None,
    environ: Mapping[str, str] | None = None,
) -> tuple[int, object]:
    """A request to schedr's REST API, made as the command task through its agent.

    Args:
        method: ``GET``, ``POST``, ...
        path: The path and its query.
        body: The JSON body; ``None`` for none.
        key: Its ``Idempotency-Key``.
        environ: The environment (for ``SCHEDR_CTX_FD``); ``os.environ`` by default.

    Returns:
        The status and the JSON answer.

    Raises:
        RuntimeError: Outside a command task, when its channel is closed or the task ended, or
            when the agent refuses.
    """
    channel = _connect(os.environ if environ is None else environ)
    try:
        with contextlib.suppress(OSError):
            channel.send(
                {"type": "hello", "protocol": CTX_PROTOCOL, "schedr_version": version("schedr")}
            )
        reply = channel.receive()
        if reply["type"] in {"refused", "error"}:
            raise RuntimeError(f"the task's agent refused: {reply.get('reason', '')}")
        if reply["type"] != "welcome":
            raise ProtocolError(f"expected `welcome`, got `{reply['type']}`")
        request: dict[str, Any] = {"type": "api_call", "method": method, "path": path}
        if body is not None:
            request["body"] = body
        if key is not None:
            request["idempotency_key"] = key
        channel.settimeout(API_ANSWER_TIMEOUT)
        channel.send(request)
        reply = channel.receive()
        if reply["type"] == "error":
            raise RuntimeError(f"the task's agent: {reply.get('reason', '')}")
        if reply["type"] != "api_answer":
            raise ProtocolError(f"expected `api_answer`, got `{reply['type']}`")
        if isinstance(note := reply.get("note"), str):
            print(note, file=sys.stderr)
    except (ProtocolError, OSError) as e:
        raise RuntimeError(f"talking to the task's agent: {e} (the task may have ended)") from None
    finally:
        channel.close()
    return int(reply["status"]), reply.get("body")


def _values() -> RunValues:
    """The values, loaded once (a failure is raised again on the next read)."""
    global _loaded  # noqa: PLW0603 - the script's one cache
    with _lock:
        if _loaded is None:
            _loaded = load()
        return _loaded


use_script_values(_values)
