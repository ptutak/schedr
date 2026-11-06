"""What a wait step's code can't call, found when its DAG is defined (decision 336)."""

import asyncio
import ctypes.util
import functools
import os
import platform
import subprocess
import threading
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Process
from subprocess import run as imported_run
from threading import Thread

import pytest

from schedr import Dag, sensor, task


def _refused(function: Callable[[], object]) -> str:
    with pytest.raises(TypeError) as refused, Dag("d"):
        sensor(function)()  # type: ignore[arg-type]  # each async, whatever it returns
    return str(refused.value)


async def uses_run() -> None:
    subprocess.run(["true"], check=True)


async def uses_popen() -> None:
    subprocess.Popen(["true"])


async def uses_imported_name() -> None:
    imported_run(["true"], check=True)


async def uses_system() -> None:
    os.system("true")


async def uses_fork() -> None:
    os.fork()


async def uses_asyncio() -> None:
    await asyncio.create_subprocess_exec("true")


async def uses_process() -> None:
    Process().start()


async def uses_pool() -> None:
    ProcessPoolExecutor()


async def uses_local_import() -> None:
    import subprocess as sp  # noqa: PLC0415  # what this reads

    sp.run(["true"], check=True)


async def uses_local_from_import() -> None:
    from subprocess import PIPE, run  # noqa: PLC0415  # what this reads

    run(["true"], check=True, stdout=PIPE)


async def uses_in_a_closure() -> None:
    def inner() -> None:
        subprocess.run(["true"], check=True)

    await asyncio.to_thread(inner)


def _helper() -> None:
    _deeper()


def _deeper() -> None:
    subprocess.run(["true"], check=True)


async def uses_helper() -> None:
    _helper()


class _Poller:
    def poll(self) -> None:
        subprocess.run(["true"], check=True)


async def uses_authors_class() -> None:
    _Poller().poll()


@pytest.mark.parametrize(
    ("function", "what"),
    [
        (uses_run, "subprocess.run"),
        (uses_popen, "subprocess.Popen"),
        (uses_imported_name, "subprocess.run"),
        (uses_system, "os.system"),
        (uses_fork, "os.fork"),
        (uses_asyncio, "asyncio.create_subprocess_exec"),
        (uses_process, "multiprocessing.Process"),
        (uses_pool, "concurrent.futures.ProcessPoolExecutor"),
        (uses_local_import, "subprocess.run"),
        (uses_local_from_import, "subprocess.run"),
        (uses_in_a_closure, "subprocess.run"),
        (uses_helper, "subprocess.run"),
        (uses_authors_class, "subprocess.run"),
    ],
)
def test_starting_a_process_is_refused_saying_what_and_where(
    function: Callable[[], object], what: str
) -> None:
    message = _refused(function)
    assert f"`{what}`" in message
    assert "test_wait_rules.py:" in message
    assert "starts a process" in message


async def uses_thread() -> None:
    threading.Thread(target=print).start()


async def uses_imported_thread() -> None:
    Thread(target=print).start()


async def uses_timer() -> None:
    threading.Timer(1, print).start()


async def uses_thread_pool() -> None:
    ThreadPoolExecutor()


class _Worker(threading.Thread):
    pass


async def uses_subclass() -> None:
    _Worker().start()


@pytest.mark.parametrize(
    ("function", "what"),
    [
        (uses_thread, "threading.Thread"),
        (uses_imported_thread, "threading.Thread"),
        (uses_timer, "threading.Timer"),
        (uses_thread_pool, "concurrent.futures.thread.ThreadPoolExecutor"),
        (uses_subclass, "threading.Thread"),
    ],
)
def test_starting_a_thread_is_refused_pointing_at_to_thread(
    function: Callable[[], object], what: str
) -> None:
    message = _refused(function)
    assert f"`{what}`" in message
    assert "asyncio.to_thread" in message


async def allowed_all_of_it() -> int:
    await asyncio.to_thread(print, "fine")
    lock = threading.Lock()
    with lock:
        await asyncio.sleep(0)
    _ = (os.environ.get("X"), os.getpid(), subprocess.PIPE, subprocess.CalledProcessError)
    _ = (platform.platform(), ctypes.util.find_library("c"), threading.current_thread())
    return 1


async def reached_dynamically() -> None:
    # Not readable: the refusal when it runs is what stops this one.
    getattr(subprocess, "run")(["true"], check=True)  # noqa: B009


@pytest.mark.parametrize("function", [allowed_all_of_it, reached_dynamically])
def test_what_is_allowed_or_cant_be_read_defines_as_usual(function: Callable[[], object]) -> None:
    with Dag("d"):
        sensor(function)()  # type: ignore[arg-type]  # each async


async def uses_it_recursively() -> None:
    await uses_it_recursively()


def test_a_function_calling_itself_is_read_once() -> None:
    with Dag("d"):
        sensor(uses_it_recursively)()


def _starts_around(function: Callable[[], object]) -> Callable[[], object]:
    @functools.wraps(function)
    async def wrapper() -> object:
        threading.Thread(target=print).start()
        return await function()  # type: ignore[misc]  # each async

    return wrapper


async def harmless() -> int:
    return 1


wrapped_starts = _starts_around(harmless)


def _closing_over_thread() -> Callable[[], object]:
    start = threading.Thread

    async def inner() -> None:
        start(target=print).start()

    return inner


class _WithClassMethod:
    @classmethod
    def poll(cls) -> None:
        threading.Thread(target=print).start()


class _Parent:
    def poll(self) -> None:
        threading.Thread(target=print).start()


class _Child(_Parent):
    pass


async def uses_class_method() -> None:
    _WithClassMethod.poll()


async def uses_inherited() -> None:
    _Child().poll()


@pytest.mark.parametrize(
    "function",
    [wrapped_starts, _closing_over_thread(), uses_class_method, uses_inherited],
)
def test_what_wrappers_closures_and_class_methods_do_is_read(
    function: Callable[[], object],
) -> None:
    assert "`threading.Thread`" in _refused(function)


async def checks_a_type() -> bool:
    return isinstance(threading.current_thread(), threading.Thread)


async def checks_a_subclass() -> bool:
    return issubclass(_Worker, threading.Thread)


async def annotates_a_nested_function() -> None:
    def inner(thread: threading.Thread) -> None:
        print(thread)

    print(inner)


class _Custom:
    pass


@functools.wraps(_Custom)
def _makes_custom() -> _Custom:
    return _Custom()


async def calls_a_wrapper_of_a_class() -> None:
    _makes_custom()


@pytest.mark.parametrize(
    "function",
    [checks_a_type, checks_a_subclass, annotates_a_nested_function, calls_a_wrapper_of_a_class],
)
def test_naming_a_class_isnt_starting_one(function: Callable[[], object]) -> None:
    with Dag("d"):
        sensor(function)()  # type: ignore[arg-type]  # each async


def test_a_task_isnt_held_to_it() -> None:
    @task
    def runs() -> None:
        subprocess.run(["true"], check=True)

    with Dag("d"):
        runs()
