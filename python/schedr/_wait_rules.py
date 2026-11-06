"""What a wait step's own code can't call, found when its DAG is defined (decision 336).

A wait step shares a process with others: nothing could stop a process or a thread it left
behind. This reads the compiled function and what it refers to, following the author's own
functions and classes, so ``schedr dags check`` says so before anything is deployed. What can't
be read this way (a library's insides, ``getattr``, a function passed in) is met when it runs
(``schedr.runner.waiting``).
"""

from __future__ import annotations

import builtins
import contextlib
import dis
import importlib
import inspect
import os
import sys
import types
from dataclasses import dataclass

_PROCESS = (
    "starts a process, and nothing could stop what it left running: "
    "use an async client, or run the command in a task after the wait step"
)
_THREAD = (
    "starts a thread its wait step knows nothing of, so its time can't stop it: "
    "use `asyncio.to_thread` or an async client"
)

_REFUSED: dict[str, tuple[tuple[str, ...], str]] = {
    "subprocess": (
        ("Popen", "run", "call", "check_call", "check_output", "getoutput", "getstatusoutput"),
        _PROCESS,
    ),
    "os": (
        (
            "system",
            "popen",
            "fork",
            "forkpty",
            "posix_spawn",
            "posix_spawnp",
            "spawnl",
            "spawnle",
            "spawnlp",
            "spawnlpe",
            "spawnv",
            "spawnve",
            "spawnvp",
            "spawnvpe",
            "execl",
            "execle",
            "execlp",
            "execlpe",
            "execv",
            "execve",
            "execvp",
            "execvpe",
        ),
        _PROCESS,
    ),
    "pty": (("fork", "spawn"), _PROCESS),
    "asyncio": (("create_subprocess_exec", "create_subprocess_shell"), _PROCESS),
    "multiprocessing": (("Process", "Pool", "get_context"), _PROCESS),
    "concurrent.futures": (("ProcessPoolExecutor",), _PROCESS),
    "concurrent.futures.thread": (("ThreadPoolExecutor",), _THREAD),
    "threading": (("Thread", "Timer"), _THREAD),
    "_thread": (("start_new_thread", "start_new"), _THREAD),
}


@dataclass(frozen=True)
class Refused:
    """A call a wait step's code can't make."""

    what: str
    """As it is written, ``subprocess.run``."""
    why: str
    """What is wrong with it, and what to do."""
    where: str
    """The file and line."""


def _known() -> dict[int, tuple[str, str]]:
    """The refused objects, by identity: of modules loaded, as no other can be in use."""
    known: dict[int, tuple[str, str]] = {}
    for module_name, (names, why) in _REFUSED.items():
        module = sys.modules.get(module_name)
        for name in names:
            found = getattr(module, name, None)
            if found is not None:
                known[id(found)] = (f"{module_name}.{name}", why)
    return known


def _is_authors(item: object) -> bool:
    """Whether its code is the author's: neither the standard library nor an installed package."""
    module = getattr(item, "__module__", None)
    if not isinstance(module, str):
        return False
    top = module.partition(".")[0]
    if top == "schedr" or top in sys.stdlib_module_names:
        return False
    file = getattr(sys.modules.get(module), "__file__", None)
    return not (isinstance(file, str) and "site-packages" in file.split(os.sep))


def _refused_base(item: type, known: dict[int, tuple[str, str]]) -> tuple[str, str] | None:
    for base in item.__mro__:
        if id(base) in known:
            return known[id(base)]
    return None


class _Reader:
    """Reads functions, each once, for what they refer to."""

    def __init__(self) -> None:
        self._known = _known()
        self._seen: set[int] = set()

    def start(self, function: object) -> Refused | None:
        if isinstance(function, types.FunctionType):
            return self.function(function, f"{function.__code__.co_filename}")
        return self._item(function, "the function given")

    def function(self, function: types.FunctionType, where: str) -> Refused | None:
        """Reads a function, and what it wraps: each is code that runs."""
        if id(function) in self._seen:
            return None
        self._seen.add(id(function))
        cells: dict[str, object] = {}
        for name, cell in zip(
            function.__code__.co_freevars, function.__closure__ or (), strict=True
        ):
            with contextlib.suppress(ValueError):  # Not set yet.
                cells[name] = cell.cell_contents
        refused = self._code(function.__code__, function.__globals__, cells)
        return refused or self._item(_wrapped(function), where)

    def _code(
        self, code: types.CodeType, names: dict[str, object], cells: dict[str, object]
    ) -> Refused | None:
        loads = _Loads(names, cells)
        checking = _TypeChecks()
        line = code.co_firstlineno
        for instruction in dis.get_instructions(code):
            if instruction.positions is not None and instruction.positions.lineno:
                line = instruction.positions.lineno
            operation, name = instruction.opname, str(instruction.argval)
            if operation == "IMPORT_NAME":
                loads.imported(self._imported(name), name)
                brought = True
            else:
                brought = loads.step(operation, name)
            refused = None
            if brought and not checking.only_checks(loads.current):
                refused = self._item(loads.current, f"{code.co_filename}:{line}")
            if refused is not None:
                return refused
            checking.after(instruction, brought, loads.current)
        for const in code.co_consts:
            # A nested function's annotations are never run.
            if isinstance(const, types.CodeType) and const.co_name != "__annotate__":
                refused = self._code(const, names, loads.seen_by_nested())
                if refused is not None:
                    return refused
        return None

    def _imported(self, name: str) -> object:
        """The module an ``import`` in a function names, if it is loaded or is a refused one."""
        if name in _REFUSED and name not in sys.modules:
            importlib.import_module(name)
            self._known = _known()
        return sys.modules.get(name)

    def _item(self, item: object, where: str) -> Refused | None:
        if id(item) in self._known:
            what, why = self._known[id(item)]
            return Refused(what, why, where)
        if isinstance(item, type):
            return self._class(item, where)
        if isinstance(item, types.FunctionType) and _is_authors(item):
            return self.function(item, where)
        wrapped = _wrapped(item)
        return None if wrapped is None else self._item(wrapped, where)

    def _class(self, item: type, where: str) -> Refused | None:
        base = _refused_base(item, self._known)
        if base is not None:
            return Refused(base[0], base[1], where)
        for one in item.__mro__:
            if not _is_authors(one) or id(one) in self._seen:
                continue
            self._seen.add(id(one))
            for member in vars(one).values():
                refused = self._item(_function_of(member), where)
                if refused is not None:
                    return refused
        return None


def _wrapped(item: object) -> object:
    """What ``functools.wraps`` says it wraps, read without running anything of the item's."""
    try:
        return inspect.getattr_static(item, "__wrapped__")
    except AttributeError:
        return None


def _function_of(member: object) -> object:
    """The function behind what a class holds."""
    if isinstance(member, classmethod | staticmethod):
        return member.__func__
    if isinstance(member, property):
        return member.fget
    return member if isinstance(member, types.FunctionType) else _wrapped(member)


class _TypeChecks:
    """Follows ``isinstance`` and ``issubclass`` calls: a class named in one isn't started.

    By the instructions' effect on the stack; a check's arguments rarely jump, and a reading gone
    wrong only ends its check early or late.
    """

    def __init__(self) -> None:
        self._depth = 0
        self._within: list[int] = []

    def only_checks(self, item: object) -> bool:
        return bool(self._within) and isinstance(item, type)

    def after(self, instruction: dis.Instruction, brought: bool, current: object) -> None:
        before = self._depth
        self._depth += dis.stack_effect(instruction.opcode, instruction.arg, jump=False)
        if brought and (current is isinstance or current is issubclass):
            self._within.append(before)
        while self._within and self._depth <= self._within[-1] + 1:
            self._within.pop()


class _Loads:
    """What the instructions so far have on hand, as far as names and modules can tell."""

    def __init__(self, names: dict[str, object], cells: dict[str, object]) -> None:
        self._names = names
        self._local = dict(cells)
        self._importing: object = None
        self.current: object = None

    def seen_by_nested(self) -> dict[str, object]:
        return self._local

    def imported(self, module: object, name: str) -> None:
        self._importing = module
        self.current = sys.modules.get(name.partition(".")[0])

    def step(self, operation: str, name: str) -> bool:
        """Follows one instruction.

        Returns:
            Whether it brought something new to look at.
        """
        if operation == "IMPORT_FROM":
            self.current = _member(self._importing, name)
        elif operation in {"LOAD_GLOBAL", "LOAD_NAME"}:
            self.current = self._names.get(name, vars(builtins).get(name))
        elif operation in {"LOAD_FAST", "LOAD_FAST_BORROW", "LOAD_DEREF"}:
            self.current = self._local.get(name)
        elif operation == "LOAD_ATTR":
            self.current = _member(self.current, name)
        elif operation in {"STORE_FAST", "STORE_DEREF"}:
            self._local[name] = self.current
            return False
        else:
            return False
        return True


def _member(item: object, name: str) -> object:
    """A module's member; anything else's could run code to be read, and isn't read."""
    return vars(item).get(name) if isinstance(item, types.ModuleType) else None


def refused_in(function: object) -> Refused | None:
    """What a wait step's function can't do, if it does.

    Args:
        function: The function given to ``@sensor``.

    Returns:
        The first thing found, or ``None``. Only what its code names: what it reaches in other
        ways is refused when it runs.
    """
    return _Reader().start(function)
