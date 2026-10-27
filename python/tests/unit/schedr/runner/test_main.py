import io
import socket
from collections.abc import Callable
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pytest

from schedr.runner.main import EXIT_ERROR, EXIT_USAGE, FD_ENV, main
from schedr.runner.parse import import_dag_file
from schedr.runner.protocol import Channel

ENV = {FD_ENV: "7"}


class FakeAgent:
    """The agent's end of a socket pair, with its replies queued before the runner starts."""

    def __init__(self, *replies: dict[str, Any]) -> None:
        self.runner_end, agent_end = socket.socketpair()
        self.runner_end.settimeout(10)  # a runner waiting for a message fails instead of hanging
        self.agent = Channel(agent_end)
        for reply in replies:
            self.agent.send(reply)
        self.opened: list[int] = []

    def open(self, fd: int) -> Channel:
        self.opened.append(fd)
        return Channel(self.runner_end)

    def messages(self) -> list[dict[str, Any]]:
        self.agent._sock.settimeout(5)
        found: list[dict[str, Any]] = []
        while True:
            try:
                found.append(self.agent.receive())
            except Exception:
                return found


def run(argv: list[str], agent: FakeAgent, env: dict[str, str] | None = None) -> tuple[int, str]:
    err = io.StringIO()
    code = main(argv, ENV if env is None else env, open_channel=agent.open, stderr=err)
    return code, err.getvalue()


def test_parse_mode_handshakes_then_parses(
    tmp_path: Path, validate_runner_message: Callable[[Any], None]
) -> None:
    (tmp_path / "a.py").write_text("from schedr import Dag\nwith Dag('a'):\n    pass\n")
    request = {
        "type": "parse",
        "root": "etl",
        "root_dir": str(tmp_path),
        "files": ["a.py"],
        "sys_path": [],
    }
    agent = FakeAgent({"type": "welcome"}, request)
    code, err = run(["parse"], agent)
    assert (code, err) == (0, "")
    assert agent.opened == [7]
    messages = agent.messages()
    assert messages[0] == {"type": "hello", "schedr_version": version("schedr"), "mode": "parse"}
    assert [m["type"] for m in messages[1:]] == ["file_started", "file_parsed", "parse_finished"]
    for message in messages:
        validate_runner_message(message)


@pytest.mark.parametrize("argv", [[], ["parse", "extra"], ["serve"]])
def test_wrong_arguments_print_usage(argv: list[str]) -> None:
    code, err = run(argv, FakeAgent())
    assert code == EXIT_USAGE
    assert err == "usage: python -m schedr.runner {parse,task}\n"


def test_without_the_socket_variable_it_explains_who_starts_it() -> None:
    code, err = run(["parse"], FakeAgent(), env={})
    assert code == EXIT_USAGE
    assert "SCHEDR_AGENT_FD is not set" in err


def test_a_value_that_isnt_a_descriptor_is_a_wrong_start() -> None:
    code, err = run(["parse"], FakeAgent(), env={FD_ENV: "/run/agent.sock"})
    assert code == EXIT_USAGE
    assert err == "SCHEDR_AGENT_FD=/run/agent.sock isn't a file descriptor\n"


def test_an_unusable_descriptor_is_an_error() -> None:
    def refuse(fd: int) -> Channel:
        raise OSError(9, "Bad file descriptor")

    err = io.StringIO()
    code = main(["parse"], ENV, open_channel=refuse, stderr=err)
    assert code == EXIT_ERROR
    assert "can't use the agent's socket (descriptor 7): " in err.getvalue()


def test_a_refused_handshake_stops_the_runner() -> None:
    code, err = run(["parse"], FakeAgent({"type": "refused", "reason": "version mismatch"}))
    assert code == EXIT_USAGE
    assert err == "the agent refused the runner: version mismatch\n"


@pytest.mark.parametrize(
    ("replies", "message"),
    [
        ([{"type": "parse"}], "expected `welcome`, got `parse`"),
        ([{"type": "welcome"}, {"type": "task"}], "expected `parse`, got `task`"),
        ([], "the other side closed the connection"),
    ],
)
def test_unexpected_messages_are_channel_errors(
    replies: list[dict[str, Any]], message: str
) -> None:
    agent = FakeAgent(*replies)
    agent.agent._sock.shutdown(socket.SHUT_WR)
    code, err = run(["parse"], agent)
    assert code == EXIT_ERROR
    assert err == f"agent channel: {message}\n"


def test_task_mode_runs_the_task_and_exits_with_its_code(
    tmp_path: Path, validate_runner_message: Callable[[Any], None]
) -> None:
    (tmp_path / "a.py").write_text(
        "from schedr import Dag, task\n"
        "@task\n"
        "def fails() -> None:\n"
        "    raise ValueError('no rows')\n"
        "with Dag('a'):\n"
        "    fails()\n"
    )
    (dag,) = import_dag_file("etl", tmp_path, "a.py")
    request = {
        "type": "task",
        "dag_file": {"root": "etl", "root_dir": str(tmp_path), "path": "a.py"},
        "dag_id": "a",
        "task": dag.to_json()["tasks"][0],
        "inputs": {},
        "run": {
            "dag_id": "a",
            "run_id": "0190a3e5-0000-7000-8000-000000000000",
            "kind": "manual",
            "planned_for": "2026-03-04T05:06:07Z",
            "since": "2026-03-04T05:06:07Z",
            "try_number": 1,
            "map_index": None,
        },
        "sys_path": [],
    }
    validate_runner_message(request)
    agent = FakeAgent({"type": "welcome"}, request)
    code, err = run(["task"], agent)
    assert (code, err) == (1, "")
    hello, imported, result = agent.messages()
    assert hello["mode"] == "task"
    validate_runner_message(imported)
    assert imported == {"type": "task_imported"}
    assert (result["type"], result["kind"]) == ("task_failed", None)
