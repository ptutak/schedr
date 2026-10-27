import ctypes
import os
import time

import pytest

from schedr.runner import usage
from schedr.runner.usage import Reader, prepare


def ended_child() -> int:
    """A child that used CPU and memory, ended, and is still a zombie."""
    pid = os.fork()
    if pid == 0:  # pragma: no cover - the child's frame
        block = bytearray(32 * 1024 * 1024)
        for i in range(0, len(block), 4096):
            block[i] = 1
        start = time.process_time()
        while time.process_time() - start < 0.05:
            pass
        os._exit(0)
    os.waitid(os.P_PID, pid, os.WEXITED | os.WNOWAIT)
    return pid


def test_the_usage_of_an_ended_child_is_read_and_it_stays_a_zombie() -> None:
    reader = prepare()
    assert reader is not None
    pid = ended_child()
    try:
        used = reader.read(pid)
        assert used is not None
        assert used["user_cpu_micros"] + used["system_cpu_micros"] >= 40_000
        assert used["peak_rss_bytes"] >= 32 * 1024 * 1024
        assert set(used) == {
            "user_cpu_micros",
            "system_cpu_micros",
            "peak_rss_bytes",
            "major_faults",
            "voluntary_context_switches",
            "involuntary_context_switches",
        }
        with open(f"/proc/{pid}/stat") as stat:
            assert stat.read().split()[2] == "Z"
    finally:
        _, status, reaped = os.wait4(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert used["peak_rss_bytes"] == reaped.ru_maxrss * 1024  # what reaping would have given


def test_a_process_that_isnt_an_ended_child_has_no_usage() -> None:
    reader = prepare()
    assert reader is not None
    assert reader.read(os.getpid()) is None


def test_a_failing_call_gives_no_usage_instead_of_an_error() -> None:
    def broken(*_: object) -> int:
        raise ctypes.ArgumentError("bad")

    assert Reader(broken, 247).read(1) is None


def test_an_unknown_architecture_has_no_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(usage.platform, "machine", lambda: "riscv64")
    assert prepare() is None


def test_a_libc_that_cant_be_loaded_gives_no_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(*_: object, **__: object) -> None:
        raise OSError("no libc")

    monkeypatch.setattr(usage.ctypes, "CDLL", missing)
    assert prepare() is None
