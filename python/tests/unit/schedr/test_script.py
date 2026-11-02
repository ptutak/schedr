"""``schedr.script``: a plain script reading the run's context over the task's channel."""

import contextlib
import os
import resource
import socket
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from importlib.metadata import version
from typing import Any

import pytest

from schedr import script
from schedr.context import dag_ctx, use_script_values
from schedr.runner.protocol import Channel
from schedr.testing import task_context

# Importing `schedr.script` installs its hook; other test modules expect `dag_ctx` without it.
use_script_values(None)

PARAMS = {
    "name": "q3",
    "day": {"$date": "2026-10-01"},
    "window": {"$timedelta": 5_400_000_000},
    "pair": {"$tuple": [1, "a"]},
    "limits": {"low": 1},
}
RUN = {
    "type": "run",
    "dag_id": "report",
    "id": "r1",
    "kind": "manual",
    "planned_for": "2026-11-01T08:00:00Z",
    "since": "2026-10-31T08:00:00Z",
}


@dataclass
class Limits:
    low: int


@dataclass
class ReportParams:
    name: str
    day: date
    window: timedelta
    pair: tuple[int, str]
    limits: Limits


class FakeAgent:
    """Serves the agent's end of a context channel in a thread, as the agent does."""

    def __init__(self, protocol: int = 1) -> None:
        self.agent, self.task = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.task.set_inheritable(True)
        self.protocol = protocol
        self.requests: list[str] = []
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    @property
    def env(self) -> dict[str, str]:
        return {script.CTX_FD_ENV: str(self.task.fileno())}

    def _serve(self) -> None:
        while True:
            try:
                _, fds, _, _ = socket.recv_fds(self.agent, 16, 4)
            except OSError:
                return
            if len(fds) != 1:
                for fd in fds:
                    os.close(fd)
                continue
            channel = Channel(socket.socket(fileno=fds[0]))
            try:
                self._converse(channel)
            finally:
                channel.close()

    def _converse(self, channel: Channel) -> None:
        hello = channel.receive()
        assert hello["schedr_version"] == version("schedr")
        if hello.get("protocol") != self.protocol:
            channel.send({"type": "refused", "reason": f"the agent speaks {self.protocol}"})
            return
        channel.send({"type": "welcome"})
        while True:
            try:
                request = channel.receive()
            except Exception:  # the caller closed it
                return
            self.requests.append(request["type"])
            if request["type"] == "params_get":
                channel.send({"type": "params", "params": PARAMS})
            elif request["type"] == "run_get":
                channel.send(RUN)

    def close(self) -> None:
        self.agent.close()
        self.task.close()


@pytest.fixture
def agent() -> Iterator[FakeAgent]:
    fake = FakeAgent()
    yield fake
    fake.close()


@pytest.fixture(autouse=True)
def script_hook(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Each test loads afresh; other test modules get ``dag_ctx`` without the hook."""
    monkeypatch.setattr(script, "_loaded", None)
    use_script_values(script._values)
    yield
    use_script_values(None)


def test_load_decodes_params_and_run_as_in_a_task(agent: FakeAgent) -> None:
    values = script.load(agent.env)
    assert values.from_script
    assert values.params == PARAMS
    assert values.run_id == "r1"
    assert values.dag_id == "report"
    assert values.kind == "manual"
    assert values.planned_for == datetime(2026, 11, 1, 8, tzinfo=UTC)
    assert values.since == datetime(2026, 10, 31, 8, tzinfo=UTC)
    assert agent.requests == ["params_get", "run_get"]


def test_dag_ctx_reads_the_loaded_values_once(
    agent: FakeAgent, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(os, "environ", agent.env)
    assert dag_ctx.params.name == "q3"
    assert dag_ctx.params.day == date(2026, 10, 1)
    assert dag_ctx.params["window"] == timedelta(hours=1, minutes=30)
    assert dag_ctx.params.pair == (1, "a")
    assert dag_ctx.params.limits == {"low": 1}
    assert dag_ctx.run.id == "r1"
    assert dag_ctx.run.planned_for == datetime(2026, 11, 1, 8, tzinfo=UTC)
    assert dag_ctx.dag_id == "report"
    # One connection, two requests: loaded on first use only.
    assert agent.requests == ["params_get", "run_get"]
    with pytest.raises(AttributeError, match="no field `nope`"):
        _ = dag_ctx.params.nope


def test_params_cls_decodes_strictly_into_the_class(
    agent: FakeAgent, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(os, "environ", agent.env)
    p = dag_ctx.params(ReportParams)
    assert p == ReportParams("q3", date(2026, 10, 1), timedelta(minutes=90), (1, "a"), Limits(1))

    @dataclass
    class Other:
        name: int

    with pytest.raises(TypeError, match="don't fit `Other`"):
        dag_ctx.params(Other)


def test_inside_a_python_task_its_own_values_win(agent: FakeAgent) -> None:
    with task_context(dag_id="mine"):
        assert dag_ctx.dag_id == "mine"
    assert agent.requests == []


def test_outside_a_task_it_says_where_it_works(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(os, "environ", {})
    with pytest.raises(RuntimeError, match=r"`schedr.script` must be run inside a schedr task"):
        _ = dag_ctx.run.id


def test_without_the_hook_the_task_message_stays() -> None:
    use_script_values(None)
    with pytest.raises(RuntimeError, match=r"`dag_ctx` must be run inside a schedr task"):
        _ = dag_ctx.run.id


def test_a_closed_channel_names_pass_fds() -> None:
    # The highest number allowed: closed, and nothing opened meanwhile takes it (descriptors
    # are given lowest first).
    fd = resource.getrlimit(resource.RLIMIT_NOFILE)[0] - 1
    with pytest.raises(RuntimeError, match=rf"isn't open here: .*pass_fds=\[{fd}\]"):
        script.load({script.CTX_FD_ENV: str(fd)})


def test_a_reused_number_is_not_taken_for_the_channel() -> None:
    with (
        open(os.devnull) as f,
        pytest.raises(RuntimeError, match="isn't the task's context channel any more"),
    ):
        script.load({script.CTX_FD_ENV: str(f.fileno())})
    stream, peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    with pytest.raises(RuntimeError, match="isn't the task's context channel any more"):
        script.load({script.CTX_FD_ENV: str(stream.fileno())})
    # The inherited descriptor is left open: only the copy was closed.
    stream.send(b"x")
    assert peer.recv(1) == b"x"
    stream.close()
    peer.close()


@pytest.mark.parametrize("text", ["x", "-1", "2", "", "99999999999999999999"])
def test_a_bad_number_is_refused(text: str) -> None:
    with pytest.raises(RuntimeError, match="isn't a descriptor number"):
        script.load({script.CTX_FD_ENV: text})


def test_a_task_that_ended_is_said_so() -> None:
    agent_end, task_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
    agent_end.close()
    try:
        with pytest.raises(RuntimeError, match="the task has ended"):
            script.load({script.CTX_FD_ENV: str(task_end.fileno())})
    finally:
        task_end.close()


def test_another_protocol_is_refused() -> None:
    fake = FakeAgent(protocol=2)
    try:
        with pytest.raises(RuntimeError, match="refused: the agent speaks 2"):
            script.load(fake.env)
    finally:
        fake.close()


def test_the_messages_fit_the_schema(validate_ctx_message: Callable[[Any], None]) -> None:
    for message in [
        {"type": "hello", "protocol": 1, "schedr_version": "1"},
        {"type": "params_get"},
        {"type": "run_get"},
        {"type": "params", "params": PARAMS},
        RUN,
        {"type": "welcome"},
    ]:
        validate_ctx_message(message)


def test_a_connection_over_the_limit_reads_the_agents_reason() -> None:
    agent_end, task_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)

    def refuse() -> None:
        # As the agent at its limit: the reason on the fresh connection, then closed unread.
        _, fds, _, _ = socket.recv_fds(agent_end, 16, 1)
        end = socket.socket(fileno=fds[0])
        Channel(end).send({"type": "error", "reason": "the task has 64 context connections open"})
        end.close()

    thread = threading.Thread(target=refuse)
    thread.start()
    try:
        with pytest.raises(RuntimeError, match="64 context connections open"):
            script.load({script.CTX_FD_ENV: str(task_end.fileno())})
    finally:
        thread.join()
        agent_end.close()
        task_end.close()


def test_a_full_channel_is_waited_for_until_the_deadline_not_forever() -> None:
    agent_end, task_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
    task_end.setblocking(False)
    with contextlib.suppress(BlockingIOError):  # an agent not reading: the queue fills
        while True:
            task_end.send(b"x")
    task_end.setblocking(True)  # the inherited socket may be blocking: that must not matter
    mine, theirs = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            script._send_end(task_end, theirs, 0.2)
        assert time.monotonic() - started < 5
    finally:
        for sock in (agent_end, task_end, mine, theirs):
            sock.close()
