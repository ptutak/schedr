"""What an ended child used (``rusage``), read by its parent without reaping it.

The shim keeps its ended process a zombie until the agent closes the status socket (so the PID
isn't reused), which ``os.wait4`` can't do. The ``waitid`` system call can: with ``WNOWAIT``
it fills the ``rusage`` its fifth argument points to and leaves the zombie, but Python's
``os.waitid`` doesn't pass that argument, so the call goes through ``ctypes``.

**It must never break the shim:** :func:`prepare` loads everything in the fork server, before
any fork; :func:`read` is called only after the exit was seen (it returns at once), makes no
allocation that can fail halfway, and gives ``None`` for anything unexpected: an unknown CPU
architecture, a failing call, any exception.
"""

import ctypes
import os
import platform
from typing import Any

_SYS_WAITID = {"x86_64": 247, "aarch64": 95}
"""The ``waitid`` system call's number by CPU architecture (the images' two)."""

_P_PID = 1
_WEXITED = 4
_WNOWAIT = 0x01000000


class _Timeval(ctypes.Structure):
    _fields_ = (("sec", ctypes.c_long), ("usec", ctypes.c_long))


class _Rusage(ctypes.Structure):
    _fields_ = (
        ("utime", _Timeval),
        ("stime", _Timeval),
        *(
            (name, ctypes.c_long)
            for name in [
                "maxrss",
                "ixrss",
                "idrss",
                "isrss",
                "minflt",
                "majflt",
                "nswap",
                "inblock",
                "oublock",
                "msgsnd",
                "msgrcv",
                "nsignals",
                "nvcsw",
                "nivcsw",
            ]
        ),
    )


class Reader:
    """Reads an ended child's usage; set up by :func:`prepare` before forking."""

    def __init__(self, syscall: Any, number: int) -> None:
        """Keeps what the call needs, so nothing is loaded after a fork.

        Args:
            syscall: libc's ``syscall`` function.
            number: The ``waitid`` system call's number.
        """
        self._syscall, self._number = syscall, number
        self._info = ctypes.create_string_buffer(256)  # siginfo_t is 128 bytes
        self._usage = _Rusage()

    def read(self, pid: int) -> dict[str, int] | None:
        """The usage of an ended child that is still a zombie, which it stays.

        Args:
            pid: The child, already seen ended (``os.waitid`` with ``WNOWAIT``).

        Returns:
            The ``ProcessUsage`` fields, or ``None`` if they can't be read.
        """
        try:
            # Every integer as a C long: `syscall` is variadic, so a plain int would leave the
            # upper half of the register undefined.
            done = self._syscall(
                ctypes.c_long(self._number),
                ctypes.c_long(_P_PID),
                ctypes.c_long(pid),
                self._info,
                ctypes.c_long(_WEXITED | _WNOWAIT),
                ctypes.byref(self._usage),
            )
            if done != 0:
                return None
            usage = self._usage
            return {
                "user_cpu_micros": usage.utime.sec * 1_000_000 + usage.utime.usec,
                "system_cpu_micros": usage.stime.sec * 1_000_000 + usage.stime.usec,
                "peak_rss_bytes": usage.maxrss * 1024,  # Linux reports kilobytes
                "major_faults": usage.majflt,
                "voluntary_context_switches": usage.nvcsw,
                "involuntary_context_switches": usage.nivcsw,
            }
        except Exception:
            return None


def prepare() -> Reader | None:
    """Sets up the reader in the fork server, before any fork.

    Returns:
        The reader, or ``None`` on a CPU architecture whose ``waitid`` number isn't known, or
        if libc can't be loaded.
    """
    number = _SYS_WAITID.get(platform.machine())
    if number is None or not hasattr(os, "waitid"):
        return None
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        syscall = libc.syscall
    except OSError, AttributeError:
        return None
    syscall.restype = ctypes.c_long
    return Reader(syscall, number)
