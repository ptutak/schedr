import contextlib
import select
import socket
import sys
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from schedr import SecretError, SecretPrefix, task_ctx
from schedr import task as task_decorator
from schedr._model import SecretRef
from schedr.runner.parse import import_dag_file
from schedr.runner.protocol import Channel
from schedr.runner.task import (
    TaskInputError,
    connection_opener,
    execute,
    resolve,
    secret_reader,
)
from schedr.testing import task_context

DAG = """
from schedr import Dag, SecretPrefix, secrets, task, task_ctx

@task
def connect(password: str, db: dict[str, str], label: str) -> str:
    return f"{password}|{db['user']}|{label}"

@task(secrets=[SecretPrefix("tenants/")])
def tenant(name: str) -> str:
    return task_ctx.secret_value(f"tenants/{name}#token")

with Dag("d"):
    connect(
        secrets["etl/db"]["password"],
        secrets["etl/db"],
        t"user {secrets.other['etl/db']['user']}",
    )
    tenant("a")
"""

RUN = {
    "dag_id": "d",
    "run_id": "0190a3e5-0000-7000-8000-000000000000",
    "kind": "manual",
    "planned_for": "2026-03-04T05:06:07Z",
    "since": "2026-03-03T05:06:07Z",
    "try_number": 1,
    "map_index": None,
}

SECRETS = [
    {"name": {"path": "etl/db", "key": "password"}, "value": "hunter22"},
    {"name": {"path": "etl/db"}, "value": {"user": "etl", "password": "hunter22"}},
    {"name": {"storage": "other", "path": "etl/db", "key": "user"}, "value": "etl-other"},
]


@pytest.fixture
def root(tmp_path: Path) -> Iterator[Path]:
    (tmp_path / "d.py").write_text(DAG)
    saved = list(sys.path)
    yield tmp_path
    sys.path[:] = saved


def request_for(root: Path, task_id: str, **changes: Any) -> dict[str, Any]:
    (dag,) = import_dag_file("etl", root, "d.py")
    (task,) = [t for t in dag.to_json()["tasks"] if t["id"] == task_id]
    request = {
        "type": "task",
        "dag_file": {"root": "etl", "root_dir": str(root), "path": "d.py"},
        "dag_id": "d",
        "task": task,
        "inputs": {},
        "run": RUN,
        "sys_path": [str(root)],
    }
    request.update(changes)
    return request


def test_secret_arguments_come_from_the_request(root: Path) -> None:
    result = execute(request_for(root, "connect", secrets=SECRETS))
    assert result["type"] == "task_succeeded", result
    assert result["output"] == "hunter22|etl|user etl-other"


def test_a_secret_not_fetched_is_an_input_error(root: Path) -> None:
    reference = {"kind": "secret", "path": "etl/db", "key": "port"}
    with pytest.raises(TaskInputError, match=r"secrets\['etl/db'\]\['port'\]` wasn't fetched"):
        resolve(reference, {}, RUN)
    result = execute(request_for(root, "connect"))
    assert result["kind"] == "input_mismatch"


def test_a_whole_secret_shaped_like_a_tag_reads_back_as_written() -> None:
    run = {**RUN, "$secrets": {(None, "x", None, None): {"$date": "not a date"}}}
    stored = resolve({"kind": "secret", "path": "x"}, {}, run)
    assert stored == {"$dict": {"$date": "not a date"}}


def test_task_ctx_secret_asks_the_reader(root: Path) -> None:
    asked: list[SecretRef] = []

    def reader(ref: SecretRef) -> str:
        asked.append(ref)
        return "token-of-a"

    result = execute(request_for(root, "tenant"), secrets=reader)
    assert result["output"] == "token-of-a"
    assert asked == [SecretRef("tenants/a", "token")]


@pytest.mark.parametrize("kind", ["secret_denied", "secret_not_found", "secret_unavailable"])
def test_a_failed_read_fails_the_attempt_with_its_kind(root: Path, kind: str) -> None:
    def reader(ref: SecretRef) -> str:
        raise SecretError(kind, f"`{ref.label()}`: no")

    result = execute(request_for(root, "tenant"), secrets=reader)
    assert result["type"] == "task_failed"
    assert result["kind"] == kind
    assert "secrets['tenants/a']['token']" in result["error"]["message"]


def test_without_a_reader_a_read_is_denied(root: Path) -> None:
    result = execute(request_for(root, "tenant"))
    assert result["kind"] == "secret_denied"


def test_the_reader_asks_the_agent_over_the_channel() -> None:
    ours, theirs = socket.socketpair()
    with ours, theirs:
        read = secret_reader(Channel(ours))
        agent = Channel(theirs)
        answers = [
            {"type": "secret", "value": "v-1"},
            {"type": "secret", "value": {"k": "v"}},
            {"type": "secret_failed", "kind": "secret_not_found", "message": "gone"},
        ]
        asked: list[dict[str, Any]] = []

        def serve() -> None:
            for answer in answers:
                asked.append(agent.receive())
                agent.send(answer)

        server = threading.Thread(target=serve)
        server.start()
        assert read(SecretRef("a", "k")) == "v-1"
        assert read(SecretRef("a", storage="vault")) == {"k": "v"}
        with pytest.raises(SecretError, match="gone") as failed:
            read(SecretRef("b", "k"))
        server.join()
    assert failed.value.kind == "secret_not_found"
    assert asked == [
        {"type": "secret_get", "secret": {"path": "a", "key": "k"}},
        {"type": "secret_get", "secret": {"path": "a", "storage": "vault"}},
        {"type": "secret_get", "secret": {"path": "b", "key": "k"}},
    ]


def test_task_context_gives_secret_values() -> None:
    with task_context(secrets={"etl/db#password": "hunter22", "etl/db#user": "etl"}):
        assert task_ctx.secret_value("etl/db#password") == "hunter22"
        assert task_ctx.secret_value("etl/db#user", storage="vault") == "etl"
        assert task_ctx.secret("etl/db") == {"password": "hunter22", "user": "etl"}
        assert task_ctx.secret("etl/db", "password") == "hunter22"
        assert task_ctx.secret("etl/db", "user", storage="vault") == "etl"
        with pytest.raises(ValueError, match=r"task_ctx.secret\('etl/db', 'user'\)"):
            task_ctx.secret("etl/db#user")
        with pytest.raises(ValueError, match=r"the whole secret is `task_ctx.secret\('etl/db'\)`"):
            task_ctx.secret_value("etl/db")
        with pytest.raises(SecretError) as missing:
            task_ctx.secret_value("etl/db#port")
    assert missing.value.kind == "secret_not_found"
    with task_context(), pytest.raises(SecretError) as denied:
        task_ctx.secret_value("etl/db#password")
    assert denied.value.kind == "secret_denied"
    with pytest.raises(ValueError, match="path#key"), task_context(secrets={"etl/db": "x"}):
        pass


def test_task_context_refuses_what_a_task_doesnt_declare() -> None:
    @task_decorator(secrets=["etl/db#user", SecretPrefix("tenants/")])
    def reads(name: str) -> str:
        return task_ctx.secret_value(name)

    def plain(name: str) -> str:
        return task_ctx.secret_value(name)

    given = {"etl/db#user": "etl", "etl/db#password": "pw", "tenants/a#t": "ta"}
    with task_context(secrets=given):
        assert reads("etl/db#user") == "etl"
        assert reads("tenants/a#t") == "ta"
        with pytest.raises(SecretError, match="isn't declared") as denied:
            reads("etl/db#password")
        # A plain function's reads aren't checked: it declares nothing.
        assert plain("etl/db#password") == "pw"
    assert denied.value.kind == "secret_denied"


def test_a_secret_and_a_connection_asked_at_once_each_get_their_own_answer() -> None:
    # Two threads of one task, one channel: the second request waits for the first's answer.
    ours, theirs = socket.socketpair()
    with ours, theirs:
        channel = Channel(ours)
        read, open_connection = secret_reader(channel), connection_opener(channel)
        agent = Channel(theirs)
        first_asked = threading.Event()
        answer_of = {
            "secret_get": {"type": "secret", "value": "s3cret"},
            "connection_get": {"type": "connection", "values": {"host": "db"}},
        }

        def serve() -> None:
            pending = [agent.receive()]
            first_asked.set()
            # Were they mixed, the second would come now, and be answered first.
            if select.select([theirs], [], [], 0.5)[0]:
                pending.insert(0, agent.receive())
            for _ in range(2):
                request = pending.pop(0) if pending else agent.receive()
                agent.send(answer_of[request["type"]])

        results: dict[str, object] = {}

        def secret() -> None:
            results["secret"] = read(SecretRef("a", "k"))

        def connection() -> None:
            with open_connection("db", lambda values: _held(dict(values))) as values:
                results["connection"] = values

        server = threading.Thread(target=serve)
        server.start()
        first = threading.Thread(target=secret)
        first.start()
        assert first_asked.wait(5)
        second = threading.Thread(target=connection)
        second.start()
        for thread in (first, second, server):
            thread.join(5)
    assert results == {"secret": "s3cret", "connection": {"host": "db"}}


def _held(values: dict[str, object]) -> contextlib.AbstractContextManager[object]:
    return contextlib.nullcontext(values)
