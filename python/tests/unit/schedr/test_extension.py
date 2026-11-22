"""The Python helper for extension plugins (decision 510): its handshake, request and answers."""

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import typing
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
    LabelDeclaration,
    MetricDeclaration,
    PanelDeclaration,
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


def test_a_plugin_reports_status_links_and_metrics_without_waiting() -> None:
    """Decision 513: reports have no answer; each is a message of the protocol's schema."""

    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        await channel.status("pulling image", "busybox", progress=0.25)
        await channel.status("running")
        await channel.link("Pod", "https://dash/pods/x")
        await channel.metric("queue_seconds", 4.5)
        await channel.metric("pods_started", 1, phase="Succeeded")
        return "done"

    with plugin(run) as (worker, ended):
        welcomed(worker, request(None))
        reports = [worker.receive() for _ in range(5)]
        result = worker.receive()
        assert ended() == 0
    assert reports == [
        {"type": "status", "phase": "pulling image", "message": "busybox", "progress": 0.25},
        {"type": "status", "phase": "running"},
        {"type": "link", "label": "Pod", "url": "https://dash/pods/x"},
        {"type": "metrics", "points": [{"name": "queue_seconds", "value": 4.5}]},
        {
            "type": "metrics",
            "points": [{"name": "pods_started", "value": 1, "labels": {"phase": "Succeeded"}}],
        },
    ]
    assert result["type"] == "task_succeeded"


def test_a_stopped_plugin_asks_why() -> None:
    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        return list(await channel.stop_reason())

    with plugin(run) as (worker, ended):
        welcomed(worker, request(None))
        assert worker.receive() == {"type": "stop_reason_get"}
        worker.send({"type": "stop_reason", "reason": "drain", "grace_ms": 30000})
        result = worker.receive()
        assert ended() == 0
    assert result["output"] == ["drain", 30.0]


def test_the_hello_carries_the_declarations_and_describe_doesnt(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Decision 513: declared in the hello, as NETCONF's hello announces its models."""

    async def run(request: ExtensionRequest, channel: ExtensionChannel) -> object:
        return None

    metrics = [{"name": "queue_seconds", "kind": "histogram", "help": "Queue", "buckets": [1, 5]}]
    panels = [
        {
            "title": "Queue",
            "metric": "queue_seconds",
            "chart": "histogram",
            "row": 1,
            "cell": "full",
        }
    ]
    assert (
        main(run, name="k", versions=["1"], metrics=metrics, panels=panels, argv=["--describe"])
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {
        "name": "k",
        "versions": ["1"],
        "protocol": PROTOCOL,
    }
    worker_end, plugin_end = socket.socketpair()
    worker = CheckedChannel(worker_end)
    env = {"SCHEDR_AGENT_FD": str(plugin_end.detach())}
    thread = threading.Thread(
        target=main,
        args=(run,),
        kwargs={
            "name": "k",
            "versions": ["1"],
            "metrics": metrics,
            "panels": panels,
            "argv": [],
            "environ": env,
        },
    )
    thread.start()
    try:
        hello = worker.receive()
        worker.send({"type": "refused", "reason": "enough"})
    finally:
        thread.join(10)
        worker.close()
    assert hello["metrics"] == metrics
    assert hello["panels"] == panels


_STOPPED_PLUGIN = """
import asyncio, sys
from schedr.extension import main

async def run(request, channel):
    print("waiting", flush=True)
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        reason, left = await channel.stop_reason()
        print(f"stopped: {reason} {left:.0f}", flush=True)
        raise

sys.exit(main(run, name="dummy", versions=["1.0"]))
"""


def test_sigterm_cancels_the_plugins_coroutine_which_may_ask_why() -> None:
    """Decision 513: SIGTERM is `CancelledError` at the plugin's `await`; it asks, then ends."""

    worker_end, plugin_end = socket.socketpair()
    worker = CheckedChannel(worker_end)
    process = subprocess.Popen(
        [sys.executable, "-c", _STOPPED_PLUGIN],
        env={**os.environ, "SCHEDR_AGENT_FD": str(plugin_end.fileno())},
        pass_fds=(plugin_end.fileno(),),
        stdout=subprocess.PIPE,
        text=True,
    )
    plugin_end.close()
    try:
        welcomed(worker, request(None))
        assert process.stdout is not None
        assert process.stdout.readline() == "waiting\n"
        process.send_signal(signal.SIGTERM)
        assert worker.receive() == {"type": "stop_reason_get"}
        worker.send({"type": "stop_reason", "reason": "cancel", "grace_ms": 29000})
        result = worker.receive()
        assert process.wait(10) == 1
        assert process.stdout.read() == "stopped: cancel 29\n"
    finally:
        process.kill()
        worker.close()
    assert result["type"] == "task_failed"
    assert "SIGTERM" in json.dumps(result["error"])


def test_the_typed_declarations_match_the_workers_schema() -> None:
    # The TypedDicts a plugin's declarations are checked by, key by key against the Rust
    # model's schema: a field or value added there and not here fails.
    schema = json.loads((Path(__file__).parents[4] / "schema" / "runner.schema.json").read_text())
    defs = schema["$defs"]

    def values(name: str) -> set[object]:
        return {v.get("const") or v["enum"][0] for v in defs[name]["oneOf"]}

    def literal(hint: object) -> set[object]:
        if typing.get_origin(hint) is typing.NotRequired:
            return literal(typing.get_args(hint)[0])
        if isinstance(hint, typing.TypeAliasType):
            return literal(hint.__value__)
        if typing.get_origin(hint) is dict:
            return literal(typing.get_args(hint)[1])
        return set(typing.get_args(hint))

    for typed, name in [
        (LabelDeclaration, "LabelDeclaration"),
        (MetricDeclaration, "MetricDeclaration"),
        (PanelDeclaration, "Panel"),
    ]:
        assert set(typed.__annotations__) == set(defs[name]["properties"]), name
        assert set(typed.__required_keys__) == set(defs[name].get("required", [])), name
    hints = typing.get_type_hints(PanelDeclaration, include_extras=True)
    assert literal(hints["chart"]) == values("ChartKind")
    assert literal(hints["cell"]) == values("Cell")
    assert literal(hints["point"]) == values("Point")
    assert literal(hints["combine"]) == values("Combine")
    assert literal(hints["color"]) == values("Color")
    assert literal(hints["colors"]) == values("Color")
    metric = typing.get_type_hints(MetricDeclaration, include_extras=True)
    assert literal(metric["kind"]) == values("MetricKind")
    assert literal(metric["unit"]) == values("MetricUnit")


def test_a_request_built_without_settings_takes_the_default_output_limit() -> None:
    # agy's review of M5.2i7: a plugin's test building a request by hand hit a KeyError.
    message: dict[str, Any] = {
        "dag_id": "d",
        "task": {"id": "t", "kind": "extension", "name": "x", "version": "1.0"},
        "run": {},
    }
    assert ExtensionRequest.from_message(message).max_output_size == 1024 * 1024
    message["task"]["settings"] = {"max_output_size": 64}
    assert ExtensionRequest.from_message(message).max_output_size == 64
