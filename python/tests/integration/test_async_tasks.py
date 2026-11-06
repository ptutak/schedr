"""``async def`` tasks in real runs, each task a process of its own (M4.10)."""

from pathlib import Path

from schedr.testing import RunResult, run_dag


def run(tmp_path: Path, source: str) -> RunResult:
    path = tmp_path / "async_dag.py"
    path.write_text(source)
    return run_dag(path, "d")


CONCURRENT = """
import asyncio
import time

from schedr import Dag, task

with Dag("d"):

    @task
    async def fetch() -> float:
        started = time.monotonic()
        await asyncio.gather(*(asyncio.sleep(0.3) for _ in range(5)))
        return time.monotonic() - started

    @task
    async def show(took: float) -> None:
        print("took", round(took, 1))

    show(fetch())
"""


def test_an_async_task_waits_concurrently_and_its_output_flows_on(tmp_path: Path) -> None:
    result = run(tmp_path, CONCURRENT)
    assert result.state == "success", result
    # Five waits of 0.3 s: together, not one after another (1.5 s).
    assert result.tasks["fetch"].output < 1.2
    assert "took" in result.tasks["show"].log
    assert "event loop: uvloop" in result.tasks["show"].log


STOPPED = """
import asyncio
from datetime import timedelta
from pathlib import Path

from schedr import Dag, task

with Dag("d"):

    @task(timeout=timedelta(seconds=1))
    async def slow() -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            Path("{marker}").write_text("cancelled")
            raise
        finally:
            await asyncio.sleep(0.1)
            with Path("{marker}").open("a") as f:
                f.write(" finally")

    slow()
"""


def test_the_stop_signal_cancels_the_coroutine_and_its_finally_runs(tmp_path: Path) -> None:
    marker = tmp_path / "marker"
    result = run(tmp_path, STOPPED.format(marker=marker))
    assert result.state == "failed", result
    assert marker.read_text() == "cancelled finally"
    assert result.tasks["slow"].attempts[0].status == "timed_out"


SWALLOWED = """
import asyncio
from datetime import timedelta

from schedr import Dag, task

with Dag("d"):

    @task(timeout=timedelta(seconds=1))
    async def slow() -> int:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            pass
        return 1

    slow()
"""


def test_a_coroutine_swallowing_the_cancel_still_ends_stopped(tmp_path: Path) -> None:
    result = run(tmp_path, SWALLOWED)
    assert result.state == "failed", result
    assert result.tasks["slow"].attempts[0].status == "timed_out"


LEFTOVER = """
import asyncio

from schedr import Dag, task

async def background() -> None:
    try:
        await asyncio.sleep(60)
    finally:
        raise ValueError("background failed")

with Dag("d"):

    @task
    async def work() -> int:
        kept = asyncio.create_task(background())
        await asyncio.sleep(0.1)
        assert not kept.done()
        return 7

    work()
"""


def test_a_leftover_tasks_error_is_a_warning_and_the_result_stands(tmp_path: Path) -> None:
    result = run(tmp_path, LEFTOVER)
    assert result.state == "success", result
    assert result.tasks["work"].output == 7
    assert "warning:" in result.tasks["work"].log
    assert "background failed" in result.tasks["work"].log


FAILS = """
import asyncio

from schedr import Dag, task

with Dag("d"):

    @task
    async def self_cancelled() -> None:
        current = asyncio.current_task()
        assert current is not None
        current.cancel()
        await asyncio.sleep(1)

    @task
    async def raises() -> None:
        await asyncio.sleep(0)
        raise ValueError("async failure")

    self_cancelled()
    raises()
"""


def test_a_raising_or_self_cancelled_coroutine_fails_its_attempt(tmp_path: Path) -> None:
    result = run(tmp_path, FAILS)
    assert result.state == "failed", result
    assert "async failure" in str(result.tasks["raises"].attempts[0].error)
    assert "cancelled, not by schedr" in str(result.tasks["self_cancelled"].attempts[0].error)


EVERYTHING = """
import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from functools import wraps

from schedr import Dag, acleanup_handle, branch, connection, resource, task, task_ctx

@dataclass
class Job:
    job_id: str

async def undo(handles: list[Job]) -> None:
    await asyncio.sleep(0)
    print("undid", [h.job_id for h in handles])

@connection(host="h")
async def db(host: str) -> AsyncIterator[str]:
    await asyncio.sleep(0)
    yield host + "!"

@resource
async def area() -> str:
    await asyncio.sleep(0)
    return "area"

@area.cleanup
async def drop(value: str) -> None:
    await asyncio.sleep(0)
    print("dropped", value)

def logged(fn):
    @wraps(fn)
    def wrapper():
        return fn()
    return wrapper

with Dag("d"):

    @task(cleanup=undo, connections=[db])
    async def work(where: str) -> str:
        await acleanup_handle(Job("j-1"))
        async with db() as client:
            return where + client

    @branch
    async def choose() -> list:
        await asyncio.sleep(0)
        return [chosen]

    @task
    async def picked() -> None:
        print("picked")

    @task
    async def skipped() -> None:
        print("skipped")

    @task
    @logged
    async def wrapped() -> int:
        await asyncio.sleep(0)
        return 3

    a = area()
    work(a)
    chosen = picked()
    choose() >> [chosen, skipped()]
    wrapped()
"""


def test_cleanups_handles_connections_resources_and_branches(tmp_path: Path) -> None:
    result = run(tmp_path, EVERYTHING)
    assert result.state == "success", result
    assert result.tasks["work"].output == "areah!"
    assert "undid ['j-1']" in result.tasks["work"].log
    assert result.tasks["picked"].state == "success"
    assert result.tasks["skipped"].state == "skipped"
    assert result.tasks["wrapped"].output == 3
    assert "dropped area" in result.tasks["area.cleanup"].log
