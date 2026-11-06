"""``async def`` tasks, their in-task calls and connections, without a runner (decision 49)."""

import asyncio
import functools
import inspect
import signal
import subprocess
import sys
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from schedr import (
    AsyncConnection,
    Dag,
    SecretError,
    acleanup_handle,
    branch,
    connection,
    connection_types,
    resource,
    task,
    task_ctx,
)
from schedr.tasks import Task
from schedr.testing import task_context


@dataclass
class Job:
    job_id: str


def test_a_plain_call_returns_the_coroutine_to_await() -> None:
    @task
    async def double(x: int) -> int:
        await asyncio.sleep(0)
        return x * 2

    called = double.fn(2)
    assert inspect.iscoroutine(called)
    assert asyncio.run(called) == 4
    # A plain call gives it as well (decision 49), though it reads as what the task returns.
    plain: object = double(2)
    assert inspect.iscoroutine(plain)
    assert asyncio.run(plain) == 4


def test_a_plain_call_with_bad_arguments_fails_at_the_call() -> None:
    @task
    async def double(x: int) -> int:
        return x * 2

    with pytest.raises(TypeError, match="x"):
        double()  # type: ignore[call-arg]


def test_an_async_task_is_recorded_like_a_sync_one() -> None:
    @task
    async def extract() -> int:
        return 1

    @task
    async def load(x: int) -> None:
        del x

    with Dag("d") as dag:
        load(extract())
    assert dag.model is not None
    assert {t.id for t in dag.model.tasks} == {"extract", "load"}


def test_async_branches_cleanups_and_resources_are_accepted() -> None:
    async def undo() -> None: ...

    @task(cleanup=undo)
    async def work() -> None: ...

    @branch
    async def choose() -> list[Any]:
        return []

    @resource
    async def area() -> str:
        return "area"

    assert work.task_id == "work"
    assert choose.task_id == "choose"
    assert area.task_id == "area"
    assert asyncio.run(area.fn()) == "area"


def test_asecret_reads_like_secret_and_refuses_what_isnt_declared() -> None:
    @task(secrets=["etl/api#token"])
    async def reads() -> str:
        return await task_ctx.asecret("etl/api", "token")

    @task(secrets=["etl/api#token"])
    async def reads_value() -> str:
        return await task_ctx.asecret_value("etl/api#token")

    @task(secrets=["etl/api#token"])
    async def reads_other() -> str:
        return await task_ctx.asecret("etl/other", "token")

    with task_context(secrets={"etl/api#token": "t", "etl/other#token": "o"}):
        assert asyncio.run(reads.fn()) == "t"
        assert asyncio.run(reads_value.fn()) == "t"
        # Declared while the body runs, not only while the call is made.
        with pytest.raises(SecretError):
            asyncio.run(reads_other.fn())


def test_asecret_reads_a_whole_secret() -> None:
    @task(secrets=["etl/api"])
    async def reads() -> dict[str, str]:
        return await task_ctx.asecret("etl/api")

    with task_context(secrets={"etl/api#token": "t"}):
        assert asyncio.run(reads.fn()) == {"token": "t"}


def test_acleanup_handle_records_like_cleanup_handle() -> None:
    async def undo(handles: list[Job]) -> None:
        del handles

    @task(cleanup=undo)
    async def work() -> None:
        await acleanup_handle(Job("j-1"))

    with task_context() as ctx:
        asyncio.run(work.fn())
    assert ctx.handles == [Job("j-1")]


def test_an_async_generator_connection_opens_with_async_with() -> None:
    entered: list[str] = []

    @connection(host="h")
    async def db(host: str) -> AsyncIterator[str]:
        entered.append(host)
        yield host

    assert isinstance(db, AsyncConnection)

    async def body() -> str:
        async with db() as client:
            return client

    with task_context(connections={"db": "double"}):
        assert asyncio.run(body()) == "double"
    assert entered == []

    async def own() -> str:
        async with db.fn(host="mine") as client:
            return client

    assert asyncio.run(own()) == "mine"
    assert entered == ["mine"]


def test_an_async_connection_is_refused_outside_a_task_and_when_undeclared() -> None:
    @connection(host="h")
    async def db(host: str) -> AsyncIterator[str]:
        yield host

    async def body() -> None:
        async with db():
            pass

    with pytest.raises(RuntimeError, match="inside a running task"):
        asyncio.run(body())

    @task(connections=[])
    async def undeclared() -> None:
        async with db():
            pass

    with (
        task_context(connections={"db": "double"}),
        pytest.raises(LookupError, match="isn't declared"),
    ):
        asyncio.run(undeclared.fn())


def test_a_connection_needs_a_generator_either_way() -> None:
    async def plain(host: str) -> str:
        return host

    with pytest.raises(TypeError, match="generator function"):
        connection(host="h")(plain)  # type: ignore[call-overload]


RUNS = """
import asyncio, os, signal, sys
if sys.argv[1] == "plain":
    sys.modules["uvloop"] = None  # as if the extra weren't installed
from schedr.runner.coroutines import run_coroutine

async def body():
    print(type(asyncio.get_running_loop()).__module__, flush=True)
    os.kill(os.getpid(), signal.SIGTERM)
    try:
        await asyncio.sleep(30)
    finally:
        await asyncio.sleep(0)
        print("finally ran", flush=True)
    print("never here", flush=True)

run_coroutine(body())
print("nor here", flush=True)
"""

ANSWERS = """
import asyncio, sys
if sys.argv[1] == "plain":
    sys.modules["uvloop"] = None
from schedr.runner.coroutines import run_coroutine

async def body():
    return await asyncio.to_thread(lambda: 7)

print(run_coroutine(body()))
"""


@pytest.mark.parametrize(("extra", "loop"), [("plain", "asyncio"), ("uvloop", "uvloop")])
def test_each_loop_runs_the_coroutine_and_a_stop_ends_it_by_the_signal(
    extra: str, loop: str
) -> None:
    # Decision 316: `uvloop` whenever it can be imported, else asyncio's; a stop the same on both.
    stopped = subprocess.run(
        [sys.executable, "-c", RUNS, extra], capture_output=True, text=True, check=False, timeout=30
    )
    lines = stopped.stdout.splitlines()
    assert lines[0].startswith(loop), stopped
    assert stopped.stderr.splitlines()[0] == f"event loop: {loop}", stopped
    assert lines[1:] == ["finally ran"], stopped
    assert stopped.returncode == -signal.SIGTERM, stopped
    answered = subprocess.run(
        [sys.executable, "-c", ANSWERS, extra],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert answered.stdout.strip() == "7", answered


def test_a_connections_test_lets_its_finally_run_when_stopped() -> None:
    # Codex's review: `connections test` didn't go through the task's stop handling.
    source = """
import asyncio, os, signal
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from schedr import connection
from schedr.context import RunValues, TaskValues, running

@connection(host="h")
async def db(host: str) -> AsyncIterator[str]:
    try:
        yield host
    finally:
        await asyncio.sleep(0)
        print("connection closed", flush=True)

@db.check
async def check(client: str) -> None:
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.sleep(30)

def opener(name, enter):
    return enter({"host": "h"})

now = datetime.now(UTC)
values = RunValues(dag_id="d", run_id="r", kind="manual", planned_for=now, since=now)
with running(values, TaskValues("t", 1, None, None, opener)):
    db.run_test()
print("not here", flush=True)
"""
    stopped = subprocess.run(
        [sys.executable, "-c", source], capture_output=True, text=True, check=False, timeout=30
    )
    assert stopped.stdout.splitlines() == ["connection closed"], stopped
    assert stopped.returncode == -signal.SIGTERM, stopped


class _FakeMongo:
    """A pymongo client that, like one resolving SRV late, reads its CA file after being made."""

    def __init__(self, uri: str, **options: object) -> None:
        del uri
        self.ca_file = options["tlsCAFile"]

    def __getitem__(self, name: str) -> Path:
        del name
        assert isinstance(self.ca_file, str)
        return Path(self.ca_file)

    def close(self) -> None:
        pass


class _FakeAsyncMongo(_FakeMongo):
    async def close(self) -> None:  # type: ignore[override]  # pyright: ignore[reportIncompatibleMethodOverride]
        await asyncio.sleep(0)


def test_a_mongo_ca_file_stays_while_the_connection_is_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Codex's review: mongodb+srv:// reads the CA file again at the first command, after SRV.
    monkeypatch.setattr("pymongo.AsyncMongoClient", _FakeAsyncMongo)
    monkeypatch.setattr("pymongo.MongoClient", _FakeMongo)

    async def read_async() -> str:
        opened = connection_types._amongo("mongodb+srv://h/", "d", ca_cert="PEM")
        ca_file = await anext(opened)
        assert isinstance(ca_file, Path)
        read = ca_file.read_text()
        with pytest.raises(StopAsyncIteration):
            await anext(opened)
        return read

    assert asyncio.run(read_async()) == "PEM"
    opened = connection_types._mongo("mongodb+srv://h/?connect=false", "d", ca_cert="PEM")
    ca_file = next(opened)
    assert isinstance(ca_file, Path)
    assert ca_file.read_text() == "PEM"
    opened.close()


def test_a_decorated_async_function_keeps_its_fn_and_a_void_one_gives_none() -> None:
    def logged[**P, R](
        fn: Callable[P, Coroutine[object, object, R]],
    ) -> Callable[P, Coroutine[object, object, R]]:
        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Coroutine[object, object, R]:
            return fn(*args, **kwargs)

        return wrapper

    async def add(n: int) -> int:
        await asyncio.sleep(0)
        return n + 1

    @task
    @logged
    async def wrapped(n: int) -> int:
        return await add(n)

    @task
    async def void() -> None:
        await asyncio.sleep(0)

    @branch
    async def choose() -> list[Task]:
        await asyncio.sleep(0)
        return []

    @resource
    @logged
    async def area() -> str:
        await asyncio.sleep(0)
        return "area"

    assert asyncio.run(wrapped.fn(1)) == 2
    assert asyncio.run(wrapped.override(retries=1).fn(2)) == 3
    assert asyncio.run(void.fn()) is None
    assert asyncio.run(choose.fn()) == []
    assert asyncio.run(area.fn()) == "area"
