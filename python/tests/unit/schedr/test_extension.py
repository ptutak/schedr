"""The Python helper for extension plugins (decision 510): its handshake, request and answers."""

import json
import socket
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from schedr.extension import (
    PROTOCOL,
    ExtensionChannel,
    ExtensionError,
    ExtensionRequest,
    Run,
    main,
)
from schedr.runner.protocol import Channel

# The plugin protocol's schema (M5.2i4), generated from the Rust crate's types: what the helper
# sends and what this test sends as the worker are both checked against it.
_SCHEMA = json.loads((Path(__file__).parents[4] / "schema" / "extension.schema.json").read_text())


def _validator(side: str) -> Draft202012Validator:
    return Draft202012Validator({"$ref": f"#/$defs/{side}", "$defs": _SCHEMA["$defs"]})


_FROM_PLUGIN = _validator("PluginMessage")
_FROM_WORKER = _validator("WorkerMessage")


class CheckedChannel(Channel):
    """The worker's end, every message either way checked against the protocol's schema."""

    def send(self, message: dict[str, Any]) -> None:
        _FROM_WORKER.validate(message)
        super().send(message)

    def receive(self) -> dict[str, Any]:
        message = super().receive()
        _FROM_PLUGIN.validate(message)
        return message


def request(payload: object, cleanup: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    message: dict[str, Any] = {
        "type": "extension_task",
        "dag_file": {"root": "etl", "root_dir": "/dags/etl", "path": "x.py"},
        "dag_id": "extended",
        "task": {
            "id": "job",
            "kind": "extension",
            "name": "dummy",
            "version": "1.0",
            "payload": payload,
            "settings": {"max_output_size": 1024},
        },
        "inputs": {},
        "outcomes": {},
        "run": {"run_id": "r-1", "try_number": 1},
        "sys_path": [],
        "secrets": [{"name": {"path": "given", "key": "k"}, "value": "inline"}],
    }
    if cleanup is not None:
        message["cleanup"] = cleanup
    return message


@contextmanager
def plugin(
    run: Run, *, cleanup: Run | None = None, argv: list[str] | None = None
) -> Iterator[tuple[Channel, Callable[[], int]]]:
    """A plugin started on one end of a socket pair; the test is the worker on the other."""
    worker_end, plugin_end = socket.socketpair()
    worker = CheckedChannel(worker_end)
    exit_code: list[int] = []
    thread = threading.Thread(
        target=lambda: exit_code.append(
            main(
                run,
                name="dummy",
                versions=["1.0"],
                cleanup=cleanup,
                argv=argv or [],
                environ={"SCHEDR_AGENT_FD": str(plugin_end.detach())},
            )
        )
    )
    thread.start()

    def ended() -> int:
        thread.join(10)
        return exit_code[0]

    try:
        yield worker, ended
    finally:
        worker.close()
        thread.join(10)


def welcomed(worker: Channel, message: dict[str, Any]) -> None:
    hello = worker.receive()
    assert hello["type"] == "hello"
    assert hello["mode"] == "extension"
    assert hello["protocol"] == PROTOCOL
    worker.send({"type": "welcome"})
    worker.send(message)


def test_describe_prints_one_json_line(capsys: pytest.CaptureFixture[str]) -> None:
    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        raise AssertionError

    assert main(run, name="dummy", versions=["1.0", "1.1"], argv=["--describe"]) == 0
    line = capsys.readouterr().out
    assert json.loads(line) == {"name": "dummy", "versions": ["1.0", "1.1"], "protocol": PROTOCOL}


def test_without_the_workers_socket_it_refuses_to_start(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        raise AssertionError

    assert main(run, name="dummy", versions=["1.0"], argv=[], environ={}) == 2
    assert "started by a schedr worker" in capsys.readouterr().err


def test_a_task_asks_the_worker_and_returns_its_output() -> None:
    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        assert request.payload == {"go": True}
        assert (request.name, request.version, request.task_id) == ("dummy", "1.0", "job")
        assert not request.cleanup
        inline = await channel.secret("given", "k")
        asked = await channel.secret("etl/api", "token", storage="vault")
        values = await channel.connection("warehouse")
        await channel.add_handle("job", {"id": 7})
        status, body = await channel.api("GET", "/api/v1/dags", idempotency_key="k-1")
        return {"inline": inline, "asked": asked, "url": values["url"], "api": [status, body]}

    with plugin(run) as (worker, ended):
        welcomed(worker, request({"go": True}))
        assert worker.receive() == {
            "type": "secret_get",
            "secret": {"path": "etl/api", "key": "token", "storage": "vault"},
        }
        worker.send({"type": "secret", "value": "s3cret"})
        assert worker.receive() == {"type": "connection_get", "name": "warehouse"}
        worker.send({"type": "connection", "values": {"url": "pg://x"}})
        assert worker.receive() == {
            "type": "handle_add",
            "handle": {"handle_type": "job", "value": {"id": 7}},
        }
        worker.send({"type": "handle_added"})
        assert worker.receive() == {
            "type": "api_call",
            "method": "GET",
            "path": "/api/v1/dags",
            "idempotency_key": "k-1",
        }
        worker.send({"type": "api_answer", "status": 200, "body": []})
        result = worker.receive()
        assert ended() == 0
    assert result == {
        "type": "task_succeeded",
        "output": {"inline": "inline", "asked": "s3cret", "url": "pg://x", "api": [200, []]},
        "outcome": "SUCCESS",
    }


@pytest.mark.parametrize(
    ("answer", "kind"),
    [
        (
            {"type": "secret_failed", "kind": "secret_not_found", "message": "no"},
            "secret_not_found",
        ),
        (
            {"type": "connection_failed", "kind": "connection_denied", "message": "no"},
            "connection_denied",
        ),
        ({"type": "handle_refused", "message": "no cleanup"}, "handle_refused"),
    ],
)
def test_a_refusal_raises_the_workers_error(answer: dict[str, Any], kind: str) -> None:
    caught: list[ExtensionError] = []

    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        try:
            if kind.startswith("secret"):
                await channel.secret("etl/api")
            elif kind.startswith("connection"):
                await channel.connection("warehouse")
            else:
                await channel.add_handle("job", 1)
        except ExtensionError as e:
            caught.append(e)
        return None

    with plugin(run) as (worker, ended):
        welcomed(worker, request(None))
        worker.receive()
        worker.send(answer)
        assert worker.receive()["type"] == "task_succeeded"
        assert ended() == 0
    assert caught[0].kind == kind


@pytest.mark.parametrize(
    ("output", "kind"),
    [({"big": "x" * 4096}, "output_too_large"), ({"at": {1, 2}}, "output_not_serializable")],
)
def test_an_output_that_cant_be_stored_fails_the_attempt(output: object, kind: str) -> None:
    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        return output

    with plugin(run) as (worker, ended):
        welcomed(worker, request(None))
        result = worker.receive()
        assert ended() == 1
    assert result["type"] == "task_failed"
    assert result["kind"] == kind


def test_an_exception_fails_the_attempt_with_its_error() -> None:
    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        raise RuntimeError("the cluster said no")

    with plugin(run) as (worker, ended):
        welcomed(worker, request(None))
        result = worker.receive()
        assert ended() == 1
    assert result["type"] == "task_failed"
    assert result["kind"] is None
    assert "the cluster said no" in result["error"]["message"]


def test_a_cleanup_is_called_with_the_handles() -> None:
    seen: list[ExtensionRequest] = []

    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        raise AssertionError

    async def cleanup(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        seen.append(request)
        return None

    handles = [{"handle_type": "job", "value": {"id": 7}}]
    with plugin(run, cleanup=cleanup) as (worker, ended):
        welcomed(worker, request(None, cleanup=handles))
        assert worker.receive()["type"] == "task_succeeded"
        assert ended() == 0
    assert seen[0].cleanup
    assert [(h.type, h.value) for h in seen[0].handles] == [("job", {"id": 7})]


def test_a_cleanup_request_fails_without_a_cleanup() -> None:
    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        raise AssertionError

    with plugin(run) as (worker, ended):
        welcomed(worker, request(None, cleanup=[]))
        result = worker.receive()
        assert ended() == 1
    assert result["type"] == "task_failed"
    assert "has no cleanup" in result["error"]["message"]


def test_a_refused_plugin_exits_and_a_wrong_message_is_a_protocol_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        raise AssertionError

    with plugin(run) as (worker, ended):
        worker.receive()
        worker.send({"type": "refused", "reason": "protocol 2"})
        assert ended() == 2
    assert "refused the plugin: protocol 2" in capsys.readouterr().err
    with plugin(run) as (worker, ended):
        worker.receive()
        worker.send({"type": "welcome"})
        Channel.send(worker, {"type": "task"})  # off the protocol on purpose: unchecked
        assert ended() == 1
    assert "expected `extension_task`, got `task`" in capsys.readouterr().err
    with plugin(run) as (worker, ended):
        worker.receive()
        Channel.send(worker, {"type": "parse"})  # off the protocol on purpose: unchecked
        assert ended() == 1
    assert "expected `welcome`" in capsys.readouterr().err
    with plugin(run) as (worker, ended):
        message = request(None)
        message["task"]["kind"] = "python"
        welcomed(worker, message)
        assert ended() == 1
    assert "expected an extension task" in capsys.readouterr().err
