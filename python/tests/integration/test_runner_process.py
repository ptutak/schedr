"""The runner as a real process: ``python -m schedr.runner`` with an inherited socket.

The agent's side is played here: a socket pair, one end passed to the child as
``SCHEDR_AGENT_FD``, the protocol spoken on the other (see PROJECT.md, Rust/Python
integration).
"""

import os
import socket
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pytest

from schedr.runner.protocol import Channel

DAG_FILE = """
from schedr import Dag, task

@task
def extract() -> int:
    return 3

with Dag("sync"):
    extract()
"""


def start(mode: str, fd: int | None, cwd: Path) -> subprocess.Popen[bytes]:
    env = {"PATH": os.environ.get("PATH", "")}
    if fd is not None:
        env["SCHEDR_AGENT_FD"] = str(fd)
    return subprocess.Popen(
        [sys.executable, "-m", "schedr.runner", mode],
        env=env,
        cwd=cwd,
        pass_fds=() if fd is None else (fd,),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def runner(mode: str, cwd: Path) -> tuple[subprocess.Popen[bytes], Channel]:
    ours, theirs = socket.socketpair()
    process = start(mode, theirs.fileno(), cwd)
    theirs.close()
    ours.settimeout(30)
    return process, Channel(ours)


def finish(process: subprocess.Popen[bytes]) -> tuple[int, str]:
    _, stderr = process.communicate(timeout=30)
    return process.returncode, stderr.decode()


def test_parse_mode_over_an_inherited_socket(tmp_path: Path) -> None:
    (tmp_path / "sync.py").write_text(DAG_FILE)
    (tmp_path / "broken.py").write_text("raise RuntimeError('boom')\n")
    process, agent = runner("parse", tmp_path)
    hello = agent.receive()
    assert hello == {"type": "hello", "schedr_version": version("schedr"), "mode": "parse"}
    agent.send({"type": "welcome"})
    agent.send(
        {
            "type": "parse",
            "root": "etl",
            "root_dir": str(tmp_path),
            "files": ["broken.py", "sync.py"],
            "sys_path": [str(tmp_path)],
        }
    )
    messages: list[dict[str, Any]] = []
    while not messages or messages[-1]["type"] != "parse_finished":
        messages.append(agent.receive())
    parsed = {m["path"]: m for m in messages if m["type"] == "file_parsed"}
    assert parsed["broken.py"]["error"]["exception"] == "RuntimeError"
    assert [d["id"] for d in parsed["sync.py"]["dags"]] == ["sync"]
    assert finish(process) == (0, "")


def test_task_mode_runs_the_task_and_exits_with_its_code(tmp_path: Path) -> None:
    (tmp_path / "sync.py").write_text(DAG_FILE)
    process, agent = runner("parse", tmp_path)
    agent.receive()
    agent.send({"type": "welcome"})
    request = {"root": "etl", "root_dir": str(tmp_path), "files": ["sync.py"], "sys_path": []}
    agent.send({"type": "parse", **request})
    messages = [agent.receive(), agent.receive(), agent.receive()]
    finish(process)
    task = messages[1]["dags"][0]["tasks"][0]

    process, agent = runner("task", tmp_path)
    agent.receive()
    agent.send({"type": "welcome"})
    agent.send(
        {
            "type": "task",
            "dag_file": {"root": "etl", "root_dir": str(tmp_path), "path": "sync.py"},
            "dag_id": "sync",
            "task": task,
            "inputs": {},
            "run": {
                "dag_id": "sync",
                "run_id": "r1",
                "kind": "manual",
                "planned_for": "2026-10-21T08:00:00Z",
                "since": "2026-10-21T08:00:00Z",
                "try_number": 1,
                "map_index": None,
            },
            "sys_path": [str(tmp_path)],
        }
    )
    assert agent.receive() == {"type": "task_imported"}
    assert agent.receive() == {"type": "task_succeeded", "output": 3, "outcome": "SUCCESS"}
    assert finish(process) == (0, "")


def test_the_socket_isnt_passed_on_to_the_tasks_children(tmp_path: Path) -> None:
    """The runner makes its descriptor non-inheritable, so a task's subprocess can't use it."""
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import os, sys\n"
        "from schedr.runner.protocol import from_fd\n"
        "fd = int(os.environ['SCHEDR_AGENT_FD'])\n"
        "channel = from_fd(fd)\n"
        "sys.exit(0 if not os.get_inheritable(fd) else 1)\n"
    )
    ours, theirs = socket.socketpair()
    env = {"PATH": os.environ.get("PATH", ""), "SCHEDR_AGENT_FD": str(theirs.fileno())}
    result = subprocess.run(
        [sys.executable, str(probe)], env=env, pass_fds=(theirs.fileno(),), check=False
    )
    theirs.close()
    ours.close()
    assert result.returncode == 0


@pytest.mark.parametrize(
    ("fd_env", "message"),
    [
        (None, "SCHEDR_AGENT_FD is not set: the runner is started by the schedr agent"),
        ("not-a-number", "SCHEDR_AGENT_FD=not-a-number isn't a file descriptor"),
    ],
)
def test_a_wrong_start_exits_with_2(tmp_path: Path, fd_env: str | None, message: str) -> None:
    env = {"PATH": os.environ.get("PATH", "")}
    if fd_env is not None:
        env["SCHEDR_AGENT_FD"] = fd_env
    result = subprocess.run(
        [sys.executable, "-m", "schedr.runner", "parse"],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 2
    assert result.stderr.decode().strip() == message


def test_a_refused_handshake_exits_with_2(tmp_path: Path) -> None:
    process, agent = runner("parse", tmp_path)
    agent.receive()
    agent.send({"type": "refused", "reason": "versions differ"})
    code, stderr = finish(process)
    assert code == 2
    assert "versions differ" in stderr
