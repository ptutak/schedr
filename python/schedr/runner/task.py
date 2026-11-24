"""Task mode: run one Python task attempt.

The runner imports the DAG file (the agent's copy of the root's latest code) under the same
module name as parse mode, finds the task's function, builds its arguments from the stored
values, calls it with ``dag_ctx`` giving values, and sends back the encoded output or an error
summary. It doesn't enforce timeouts or watch memory: the agent does (see archive/PROJECT.md,
Rust/Python integration).
"""

import asyncio
import contextlib
import dataclasses
import functools
import inspect
import operator
import sys
import types
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union, cast, get_args, get_origin, is_typeddict
from urllib.parse import quote

from schedr._model import SecretRef, registry
from schedr._serialization import (
    CodecError,
    check_size,
    decode,
    decode_any,
    encode,
    encoded_size,
    models,
)
from schedr._serialization.annotations import (
    MAPPING_ORIGINS,
    SEQUENCE_ORIGINS,
    TypeModelError,
    is_union,
    label,
    shaped_members,
    tuple_items,
    type_hints,
    unwrap,
)
from schedr._serialization.codec import escaped
from schedr._templates import as_text
from schedr.connections import ConnectionBase, ConnectionDeniedError, ConnectionNotFoundError
from schedr.context import (
    HANDLES_PARAMETER,
    ApiCaller,
    ConnectionEnter,
    ConnectionOpener,
    HandleAdder,
    RunValues,
    SecretError,
    SecretReader,
    TaskValues,
    handle_classes,
    running,
)
from schedr.event_triggers import Event, event_data_hint, event_for_run
from schedr.log import install as install_structured_logging
from schedr.outcomes import (
    FIRST_OF_VALUE,
    TIMED_OUT_NAME,
    TimedOut,
    from_return_type,
    outcome_enum,
)
from schedr.runner.parse import extend_sys_path, import_dag_entries, import_error, module_name
from schedr.runner.protocol import (
    Asking,
    Channel,
    Message,
    ProtocolError,
    Telling,
    Wire,
    as_wire,
    listed,
    obj,
    optional_text,
    optional_whole,
    text,
    texts,
    whole,
)
from schedr.sensors import NOT_YET, NOT_YET_OUTCOME, output_hint

if TYPE_CHECKING:
    import asyncio

type EntriesOf = Callable[[str, Path, str], list[registry.DefinedDag]]
"""Gives a DAG file's DAGs: its root's name and folder, and its path in it."""

type Stored = Mapping[str, Wire]
"""A stored form as the agent sends it: a task, a value, a reference, the inputs, the run."""

SUCCESS = "SUCCESS"
"""The outcome of a task without outcomes of its own (``DefaultOutcome.SUCCESS``)."""


class TaskInputError(Exception):
    """A stored input or argument that can't be given to the task's function."""


class TaskSetupError(Exception):
    """The task can't be started: its DAG or function isn't in the DAG file."""


def resolve(
    reference: Stored,
    inputs: Stored,
    run: Stored,
    outcomes: Stored | None = None,
    *,
    secrets: Secrets | None = None,
) -> Wire:
    """The stored value a reference stands for.

    Args:
        reference: A reference as stored (``{"kind": "output", "task": ..., "path": [...],
            "on": [...]}``, ``{"kind": "outcome", "task": ...}`` or ``{"kind": "dag_ctx",
            "field": ...}``).
        inputs: The upstream outputs, by task ID, as stored JSON.
        run: The request's run information.
        outcomes: The upstream tasks' outcomes, by task ID: a name, or ``None`` for one that
            was skipped.
        secrets: The secrets the request brought, by :func:`secret_key`.

    Returns:
        The stored value (``planned_for`` / ``since`` tagged as ``$datetime``); ``None`` for
        a reference whose edge wasn't taken (its upstream skipped, or narrowed by ``.on()``
        to outcomes that didn't happen); the outcome's name for an ``outcome`` reference.

    Raises:
        TaskInputError: For a missing input or path, or a truncated stream.
        ProtocolError: For a reference or run information that isn't as the agent sends it.
    """
    outcomes = outcomes or {}
    kind = reference.get("kind")
    if kind == "secret":
        name = secret_key(reference)
        if secrets is None or name not in secrets:
            raise TaskInputError(f"the secret `{_label(reference)}` wasn't fetched")
        value = secrets[name]
        return escaped(value) if isinstance(value, dict) else value
    if kind == "param":
        return _follow("dag_ctx.params", run.get("params") or {}, _path(reference))
    if kind == "dag_ctx":
        return _run_field(text(reference, "field"), run)
    task = text(reference, "task")
    if kind == "outcome":
        if task not in outcomes:
            raise TaskInputError(f"the outcome of `{task}` isn't among the task's inputs")
        return outcomes[task]
    if task in outcomes:
        outcome = outcomes[task]
        on = listed(reference.get("on"), "a reference's `on`")
        # Skipped, or another outcome than the edge is taken on: the data rule's `None`.
        if outcome is None or (on and outcome not in on):
            return None
    if task not in inputs:
        raise TaskInputError(f"the output of `{task}` isn't among the task's inputs")
    return _follow(task, inputs[task], _path(reference))


def _path(reference: Stored) -> list[Wire]:
    return listed(reference.get("path"), "a reference's `path`")


def _run_field(field: str, run: Stored) -> Wire:
    """A ``dag_ctx`` field's stored value, from the run information."""
    planned_for = text(run, "planned_for")
    values: dict[str, Wire] = {
        "dag_id": text(run, "dag_id"),
        "run_id": text(run, "run_id"),
        "run_kind": text(run, "kind"),
        "planned_for": {"$datetime": planned_for},
        "since": {"$datetime": text(run, "since")},
    }
    event = run.get("event")
    if event is not None:
        told = obj(event, "the run's `event`")
        values["event_key"] = told.get("key")
        values["event_data"] = told.get("data")
        values["event_at"] = {"$datetime": told.get("at") or planned_for}
    if field not in values:
        raise TaskInputError("`dag_ctx.event`: this run wasn't started by an event")
    return values[field]


CONNECTION_NOT_FOUND = "connection_not_found"
"""The error kind of a connection the agent doesn't know (not in the DAG folder's registry)."""

CONNECTION_DENIED = "connection_denied"
"""The error kind of a connection the task doesn't declare in ``connections=``."""

type SecretKey = tuple[str | None, str, str | None, str | None]
"""A secret by ``(storage, path, key, default)``."""

type Secrets = Mapping[SecretKey, Wire]
"""The secrets a request brought, by their keys."""


def secret_key(name: Stored) -> SecretKey:
    """A secret's name as stored, as a key.

    ``{"storage": ..., "path": ..., "key": ..., "default": ...}``.

    Args:
        name: The stored name (or a secret reference).

    Returns:
        ``(storage, path, key, default)``.

    Raises:
        ProtocolError: If a part isn't a string.
    """
    return (
        optional_text(name, "storage"),
        text(name, "path"),
        optional_text(name, "key"),
        optional_text(name, "default"),
    )


def _label(name: Stored) -> str:
    storage, path, key, default = secret_key(name)
    return SecretRef(path, key, storage, default).label()


DEFAULT_IN_FLIGHT = 16
"""A trigger's events on their way to the master at once, unless it says (M5.2h2; the model's
``DEFAULT_IN_FLIGHT``)."""


def secret_reader(channel: Asking) -> SecretReader:
    """Reads a secret through the agent (``secret_get``), one request at a time.

    Args:
        channel: The channel to the agent.

    Returns:
        The reader ``task_ctx.secret`` calls.
    """

    def read(ref: SecretRef) -> str | dict[str, str]:
        name: dict[str, Wire] = {k: v for k, v in ref.to_json().items() if k != "kind"}
        answer = channel.ask({"type": "secret_get", "secret": name})
        if answer.get("type") == "secret":
            return _secret_value(answer.get("value"))
        if answer.get("type") == "secret_failed":
            raise SecretError(text(answer, "kind"), text(answer, "message"))
        raise SecretError("secret_unavailable", f"unexpected answer {answer.get('type')!r}")

    return read


def _secret_value(value: Wire) -> str | dict[str, str]:
    """A ``secret`` answer's value: a key's text, or a whole secret's keys and texts.

    Raises:
        ProtocolError: If it is anything else.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and all(isinstance(v, str) for v in value.values()):
        return {k: v for k, v in value.items() if isinstance(v, str)}
    raise ProtocolError("`secret`: `value` must be a string or an object of strings")


def connection_opener(channel: Asking) -> ConnectionOpener:
    """Opens connections, their values asked of the agent (``connection_get``).

    The agent renders them from what the parser recorded; one request at a time.

    Args:
        channel: The channel to the agent.

    Returns:
        The opener ``with name():`` calls.
    """

    def values_of(name: str) -> Mapping[str, object]:
        answer = channel.ask({"type": "connection_get", "name": name})
        if answer.get("type") == "connection":
            return obj(answer.get("values"), "`connection`: `values`")
        if answer.get("type") == "connection_failed":
            kind, message = text(answer, "kind"), text(answer, "message")
            if kind == CONNECTION_NOT_FOUND:
                raise ConnectionNotFoundError(message)
            if kind == CONNECTION_DENIED:
                raise ConnectionDeniedError(message)
            raise SecretError(kind, message)
        raise SecretError("secret_unavailable", f"unexpected answer {answer.get('type')!r}")

    def open_connection(
        name: str, enter: ConnectionEnter
    ) -> contextlib.AbstractContextManager[object]:
        return enter(values_of(name))

    return open_connection


PROCESS_RESULT = frozenset(
    {"stdout", "stderr", "exit_code", "stdout_truncated", "stderr_truncated"}
)
"""The fields of a stored ``ProcessResult``: only its streams are ever truncated (B75)."""


def _is_process_result(value: Stored) -> bool:
    return value.keys() == PROCESS_RESULT


def _follow(task: str, value: Wire, path: list[Wire]) -> Wire:
    """The part of ``task``'s stored output ``value`` at ``path`` (keys and indexes)."""
    for step in path:
        if isinstance(step, str):
            if isinstance(value, dict) and list(value) == ["$dict"]:
                value = value["$dict"]  # a dict shaped like a tag, stored escaped (B66)
            if not isinstance(value, dict) or step not in value:
                raise TaskInputError(f"`{task}`: `{step}` doesn't exist in the output")
            if _is_process_result(value) and value.get(f"{step}_truncated") is True:
                raise TaskInputError(
                    f"`{task}.{step}` was truncated (over the output size limit); "
                    "take the whole ProcessResult instead"
                )
            value = value[step]
        elif isinstance(step, int) and not isinstance(step, bool):
            items = value.get("$tuple") if isinstance(value, dict) else value
            if not isinstance(items, list) or step >= len(items):
                raise TaskInputError(f"`{task}`: index {step} doesn't exist in the output")
            value = items[step]
        else:
            raise ProtocolError(f"a reference's path holds {step!r}: a key or an index")
    return value


def render(
    parts: list[Wire],
    inputs: Stored,
    run: Stored,
    outcomes: Stored | None = None,
    *,
    secrets: Secrets | None = None,
) -> str:
    """Renders a stored template to text, the way the agent renders command lines.

    Args:
        parts: The template's parts (``{"text": ...}`` and ``{"ref": {...}}``).
        inputs: The upstream outputs.
        run: The run information.
        outcomes: The upstream tasks' outcomes (see :func:`resolve`).
        secrets: The secrets the request brought (see :func:`resolve`).

    Returns:
        The text.

    Raises:
        TaskInputError: For a reference that can't be resolved or written as text.
    """
    out: list[str] = []
    for given in parts:
        part = obj(given, "a template's part")
        if "text" in part:
            out.append(text(part, "text"))
            continue
        reference = obj(part.get("ref"), "a template's reference")
        value = decode_any(resolve(reference, inputs, run, outcomes, secrets=secrets))
        spec = optional_text(reference, "format")
        if value is None or isinstance(value, list | dict | tuple):
            raise TaskInputError(f"a {type(value).__name__} can't be rendered into text")
        written = value.strftime(spec) if spec and "%" in spec else as_text(value)
        # A typed connection's URL (decision 188): only the text, never the URL's structure.
        out.append(quote(written, safe="") if reference.get("encode") == "url" else written)
    return "".join(out)


def build(
    value: Stored,
    annotation: object,
    inputs: Stored,
    run: Stored,
    outcomes: Stored | None = None,
    *,
    secrets: Secrets | None = None,
) -> object:
    """An argument's Python value from its stored form, decoded against the parameter type.

    Args:
        value: The stored value (``literal``, ``ref``, ``template``, ``list`` or ``map``).
        annotation: The parameter's annotation.
        inputs: The upstream outputs.
        run: The run information.
        outcomes: The upstream tasks' outcomes (see :func:`resolve`).
        secrets: The secrets the request brought (see :func:`resolve`).

    Returns:
        The value to pass.

    Raises:
        TaskInputError: If a stored value doesn't fit the parameter.
    """
    given = _Given(inputs, run, outcomes, secrets)
    try:
        if "literal" in value:
            literal = value["literal"]
            return decode(literal, _run_result_narrowed(annotation, literal))
        if "ref" in value:
            reference = obj(value["ref"], "a stored reference")
            return decode(
                resolve(reference, inputs, run, outcomes, secrets=secrets),
                _narrowed(reference, annotation, outcomes),
            )
        if "template" in value:
            # Text, which must fit the place it fills (B124: a union member taking it).
            parts = listed(value["template"], "a stored template")
            return decode(render(parts, inputs, run, outcomes, secrets=secrets), annotation)
        return _built(value, unwrap(annotation), given)
    except CodecError as e:
        raise TaskInputError(f"a stored value doesn't fit the parameter's type, {e}") from e


@dataclasses.dataclass(frozen=True, slots=True)
class _Given:
    """What stored values are built from: the inputs, the run, the outcomes, the secrets."""

    inputs: Stored
    run: Stored
    outcomes: Stored | None
    secrets: Secrets | None

    def build(self, value: Wire, annotation: object) -> object:
        """A stored value inside a list or a map, built as :func:`build` builds one."""
        stored = obj(value, "a stored value")
        return build(stored, annotation, self.inputs, self.run, self.outcomes, secrets=self.secrets)


def _built(value: Stored, target: object, given: _Given) -> object:
    """A list or dict written with references inside, as the parameter's type.

    For a union, as the first member of that shape it builds as (B121: ``tuple[int, int] |
    None`` gets a tuple, not a list).
    """
    shape = list if "list" in value else dict
    members = shaped_members(target, shape) if is_union(target) else [target]
    error: Exception = TaskInputError(f"a {shape.__name__} fits none of `{label(target)}`")
    for member in members:
        try:
            if shape is list:
                return _built_list(listed(value.get("list"), "a stored list"), member, given)
            return _built_map(obj(value.get("map"), "a stored map"), member, given)
        except (TaskInputError, CodecError) as e:
            error = e
    raise error


def _built_list(items: list[Wire], target: object, given: _Given) -> object:
    """A list written with references inside, as the parameter's type.

    A tuple for a tuple parameter (B65), else a list.
    """
    origin, args = get_origin(target), get_args(target)
    if origin is tuple:
        types_ = tuple_items(args)
        if len(types_) != len(items):
            raise TaskInputError(f"expected a tuple of {len(types_)}, got {len(items)} values")
        return tuple(given.build(v, t) for v, t in zip(items, types_, strict=True))
    item = args[0] if origin in SEQUENCE_ORIGINS and args else Any
    return [given.build(v, item) for v in items]


def _built_map(fields: Stored, target: object, given: _Given) -> object:
    """A dict written with references inside, as the parameter's type.

    A dataclass or a Pydantic model is built from its fields (B65); a ``TypedDict`` or a
    ``dict`` is a dict, each value of its field's or item's type.
    """
    origin, args = get_origin(target), get_args(target)
    if origin in MAPPING_ORIGINS or target in MAPPING_ORIGINS or not isinstance(target, type):
        item = args[1] if origin in MAPPING_ORIGINS and args else Any
        return {k: given.build(v, item) for k, v in fields.items()}
    hints = type_hints(target, f"`{target.__name__}`")
    if is_typeddict(target):
        # Its keys, as a dataclass or a model checks its fields when built (B124).
        if unknown := sorted(set(fields) - set(hints)):
            raise TaskInputError(f"`{target.__name__}` has no field `{unknown[0]}`")
        if missing := sorted(set(getattr(target, "__required_keys__", ())) - set(fields)):
            raise TaskInputError(f"`{target.__name__}` needs `{missing[0]}`")
    built = {k: given.build(v, hints.get(k, Any)) for k, v in fields.items()}
    try:
        if dataclasses.is_dataclass(target):
            return target(**built)
        if models.is_model(target):
            return models.construct(target, built)
    except (TypeError, ValueError) as e:  # a field missing or refused
        raise TaskInputError(f"`{target.__name__}` can't be built: {e}") from e
    return built


def _narrowed(reference: Stored, annotation: object, outcomes: Stored | None) -> object:
    """The member of a union annotation the upstream's outcome names, else the annotation.

    A plain JSON object can fit several dataclasses of an outcome union: the stored outcome
    says which one it is (only a whole output: a path reaches into one member already; a
    ``first_of``'s value is its winner's whole output, with the winner's outcome).
    """
    if reference.get("kind") != "output":
        return annotation  # a secret's `path` is the secret's, not a path into an output
    path = _path(reference)
    if path and path != [FIRST_OF_VALUE]:
        return annotation
    task = reference.get("task")
    outcome = (outcomes or {}).get(task) if isinstance(task, str) else None
    return member_named(annotation, outcome)


def _run_result_narrowed(annotation: object, data: object) -> object:
    """``RunResult[A | B]`` narrowed to the class its task's outcome names (decision 353).

    ``RunResult[B]`` for a ``B``, as an input's union is narrowed by its upstream's outcome.
    """
    from schedr.run_of import RunResult  # noqa: PLC0415 (only an @sensor(wait_for=...) needs it)

    if get_origin(annotation) is not RunResult or not isinstance(data, Mapping):
        return annotation
    (output,) = get_args(annotation)
    return RunResult[member_named(output, data.get("outcome"))]  # type: ignore[misc]


def member_named(annotation: object, outcome: object) -> object:
    """The member of a union annotation named by an outcome.

    A member is named by its class, through ``Annotated`` and generic parameters (B123),
    as outcome detection names it.

    Args:
        annotation: The receiving type.
        outcome: The recorded outcome's name (anything else picks nothing).

    Returns:
        The member, else the annotation as it is.
    """
    if not isinstance(outcome, str) or get_origin(unwrap(annotation)) not in {
        Union,
        types.UnionType,
    }:
        return annotation
    if outcome == SUCCESS:
        # A wait step's value, in a parameter expecting `TimedOut` too (decision 324).
        kept = [m for m in get_args(unwrap(annotation)) if unwrap(m) is not TimedOut]
        if len(kept) < len(get_args(unwrap(annotation))):
            return functools.reduce(operator.or_, kept)
    for member in get_args(unwrap(annotation)):
        target = unwrap(member)
        if getattr(get_origin(target) or target, "__name__", None) == outcome:
            return member
    return annotation


def mapped_arguments(
    inputs: Stored,
    hints: Mapping[str, object],
    run: Stored | None = None,
    upstream_outcomes: Stored | None = None,
    *,
    secrets: Secrets | None = None,
) -> dict[str, object]:
    """A mapped task instance's own arguments: the elements the master resolved for it.

    They come with the inputs under ``$expand`` (decision 76); one taken element by element
    from a mapped upstream task has that instance's outcome under ``$expand_outcomes``, which
    picks the member of a union to decode it as. An element written in the file with a
    t-string or a ``dag_ctx`` value comes in its stored form under ``$expand_values``, built
    here as an argument is (decision 118).

    Args:
        inputs: The instance's inputs.
        hints: The task function's type hints.
        run: The run information, for ``$expand_values``.
        upstream_outcomes: The upstream tasks' outcomes, for ``$expand_values``.
        secrets: The secrets the request brought, for ``$expand_values``.

    Returns:
        The arguments by parameter name (none for a task that isn't mapped).

    Raises:
        TaskInputError: If an element doesn't fit its parameter.
    """
    outcomes = obj(inputs.get("$expand_outcomes") or {}, "`$expand_outcomes`")
    arguments: dict[str, object] = {}
    for name, value in obj(inputs.get("$expand") or {}, "`$expand`").items():
        annotation = member_named(hints.get(name, Any), outcomes.get(name))
        try:
            arguments[name] = decode(value, annotation)
        except CodecError as e:
            raise TaskInputError(
                f"mapped argument `{name}`: the element doesn't fit the parameter's type, {e}"
            ) from e
    given = _Given(inputs, run or {}, upstream_outcomes, secrets)
    for name, value in obj(inputs.get("$expand_values") or {}, "`$expand_values`").items():
        try:
            arguments[name] = given.build(value, hints.get(name, Any))
        except TaskInputError as e:
            raise TaskInputError(f"mapped argument `{name}`: {e}") from e
    return arguments


def run_values(run: Stored, params_class: type | None = None) -> RunValues:
    """The ``dag_ctx`` values for a request's run information.

    Args:
        run: The run information.
        params_class: The class the DAG declares its parameters with.

    Returns:
        The values.

    Raises:
        ProtocolError: For run information that isn't as the agent sends it.
    """
    event = run.get("event")
    return RunValues(
        dag_id=text(run, "dag_id"),
        run_id=text(run, "run_id"),
        kind=text(run, "kind"),
        planned_for=datetime.fromisoformat(text(run, "planned_for")),
        since=datetime.fromisoformat(text(run, "since")),
        params=obj(run.get("params") or {}, "the run's `params`"),
        params_class=params_class,
        event=None if event is None else obj(event, "the run's `event`"),
    )


def find_function(request: Stored) -> Callable[..., object]:
    """Imports the DAG file and finds the task's function.

    Args:
        request: The ``task`` message.

    Returns:
        The task's function.
    """
    return find_task(request)[0]


def find_task(request: Stored) -> tuple[Callable[..., object], frozenset[str]]:
    """Imports the DAG file and finds the task's function and its direct downstream tasks.

    Args:
        request: The ``task`` message.

    Returns:
        The function, and the IDs of the tasks right after it (a branch chooses among them).
    """
    function, downstream, _ = find_task_of_dag(request)
    return function, downstream


def find_task_of_dag(
    request: Stored,
) -> tuple[Callable[..., object], frozenset[str], type | None]:
    """As :func:`find_task`, with the class the DAG declares its parameters with.

    Args:
        request: The ``task`` message.

    Returns:
        The function, the IDs of the tasks right after it, and the params class.

    Raises:
        TaskSetupError: If the DAG or the task isn't in the file.
    """
    entry = find_entry(request)
    task_id = _task_id(request)
    downstream = frozenset(e.to for e in entry.model.edges if e.from_ == task_id)
    return entry.functions[task_id], downstream, entry.params_class


def _task(request: Stored) -> dict[str, Wire]:
    """A request's ``task``: the stored task."""
    return obj(request.get("task"), "the request's `task`")


def _task_id(request: Stored) -> str:
    return text(_task(request), "id")


def _dag_file(request: Stored) -> tuple[str, Path, str]:
    """A request's DAG file: its root's name, the root's folder, its path in the root."""
    dag_file = obj(request.get("dag_file"), "the request's `dag_file`")
    return text(dag_file, "root"), Path(text(dag_file, "root_dir")), text(dag_file, "path")


def find_entry(request: Stored, entries_of: EntriesOf = import_dag_entries) -> registry.DefinedDag:
    """The DAG of a task request, its file imported, with the task's function in it.

    Args:
        request: The ``task`` message.
        entries_of: Gives a DAG file's DAGs (imports it; a watcher host only once).

    Returns:
        The DAG.

    Raises:
        TaskSetupError: If the DAG or the task isn't in the file.
    """
    root, root_dir, path = _dag_file(request)
    entries = entries_of(root, root_dir, path)
    task_id, dag_id = _task_id(request), text(request, "dag_id")
    for entry in entries:
        if entry.model.id == dag_id:
            if task_id not in entry.functions:
                raise TaskSetupError(f"DAG `{dag_id}` has no Python task `{task_id}` (any more?)")
            return entry
    raise TaskSetupError(f"`{path}` doesn't define DAG `{dag_id}` (any more?)")


def chosen_ids(result: object, downstream: frozenset[str], dag_id: str) -> list[str]:
    """The task IDs a ``@branch`` function's result chose.

    Args:
        result: What it returned: a task handle, or a list of them.
        downstream: Its direct downstream tasks, the ones it may choose.
        dag_id: The DAG being run: a handle of another DAG's task is refused, though a task of
            this DAG has its ID (B126).

    Returns:
        The IDs, in the order returned, without repeats.

    Raises:
        CodecError: For ``None``, something that isn't a handle, or a task it can't choose.
    """
    if result is None:
        raise CodecError("", "a branch returns the chosen tasks, or [] for none (not None)")
    handles = result if isinstance(result, list | tuple) else [result]
    chosen: list[str] = []
    for handle in handles:
        task_id = getattr(handle, "_schedr_task_id", None)
        if not isinstance(task_id, str):
            raise CodecError("", f"a branch returns task handles, got {type(handle).__name__}")
        owner = getattr(handle, "_schedr_dag_id", None)
        if isinstance(owner, str) and owner != dag_id:
            raise CodecError(
                "",
                f"`{task_id}` is a task of DAG `{owner}`, not `{dag_id}`: return this DAG's task",
            )
        if task_id not in downstream:
            raise CodecError("", f"`{task_id}` isn't right after the branch, so it can't choose it")
        if task_id not in chosen:
            chosen.append(task_id)
    return chosen


def failure(error: BaseException, kind: str | None, message: str | None = None) -> Message:
    """A ``task_failed`` message.

    Args:
        error: The exception.
        kind: The failure kind schedr recognises, or ``None`` for the task's own exception.
        message: A message replacing the exception's, if given.

    Returns:
        The message.
    """
    summary: dict[str, Wire] = dict(import_error(error))
    if message is not None:
        summary["message"] = message
    return {"type": "task_failed", "error": summary, "kind": kind}


@dataclasses.dataclass
class _Begun:
    """A task ready for its function: everything before the call."""

    function: Callable[..., object]
    arguments: dict[str, object]
    hints: Mapping[str, object]
    values: RunValues
    own: TaskValues
    downstream: frozenset[str]


def _begin(
    request: Stored,
    imported: Callable[[], None],
    secrets: SecretReader | None,
    channel: Asking | None,
    entries: EntriesOf = import_dag_entries,
    *,
    binding: bool = False,
) -> _Begun | Message:
    """Imports the DAG file and builds the call; a ``task_failed`` message when that fails.

    With ``binding``, a call its function's signature doesn't take fails here, before anything
    is told it can start (a poke moving to new code, which then stays on its own).
    """
    task = _task(request)
    inputs = obj(request.get("inputs") or {}, "the request's `inputs`")
    run = obj(request.get("run"), "the request's `run`")
    fetched = _fetched(request)
    outcomes = obj(request.get("outcomes") or {}, "the request's `outcomes`")
    task_id = text(task, "id")
    try:
        entry = find_entry(request, entries)
        function, params_class = entry.functions[task_id], entry.params_class
        downstream = frozenset(e.to for e in entry.model.edges if e.from_ == task_id)
        hints = type_hints(function, f"task `{task_id}`")
        given = _Given(inputs, run, outcomes, fetched)
        stored = obj(task.get("arguments") or {}, "the task's `arguments`")
        arguments = {
            name: given.build(value, hints.get(name, Any)) for name, value in stored.items()
        }
        arguments.update(mapped_arguments(inputs, hints, run, outcomes, secrets=fetched))
        handles = request.get("cleanup")
        if handles is not None:
            registered = listed(handles, "the request's `cleanup`")
            function, arguments = cleanup_call(entry, function, task_id, arguments, registered)
        if binding:
            inspect.signature(function).bind(**arguments)
    except TaskSetupError as e:
        return failure(e, "task_not_found")
    except (TaskInputError, TypeModelError) as e:
        return failure(e, "input_mismatch")
    except (Exception, SystemExit) as e:
        return failure(e, "import_failed")
    imported()
    opener = connection_opener(channel) if channel is not None else None
    adder = handle_adder(channel) if channel is not None else None
    api = api_caller(channel) if channel is not None else None
    first = optional_text(run, "first_started")
    own = TaskValues(
        task_id,
        optional_whole(run, "try_number") or 1,
        optional_whole(run, "map_index"),
        secrets,
        opener,
        adder,
        api,
        None if first is None else datetime.fromisoformat(first),
    )
    return _Begun(function, arguments, hints, run_values(run, params_class), own, downstream)


def _fetched(request: Stored) -> dict[SecretKey, Wire]:
    """The secrets a request brought, by :func:`secret_key`."""
    fetched: dict[SecretKey, Wire] = {}
    for given in listed(request.get("secrets"), "the request's `secrets`"):
        secret = obj(given, "a secret")
        fetched[secret_key(obj(secret.get("name"), "a secret's `name`"))] = secret.get("value")
    return fetched


async def execute_trigger(request: Stored, channel: Telling, entries: EntriesOf) -> Message:
    """Runs a trigger in the watcher host's loop, until it is stopped or it fails.

    Args:
        request: The task, as for :func:`execute`, with what it starts from in ``trigger``.
        channel: The watcher's channel to the agent.
        entries: Gives a DAG file's DAGs.

    Returns:
        ``task_failed``: a trigger only ends with an error, or by being cancelled.
    """
    begun = _begin(
        request,
        lambda: channel.send({"type": "task_imported"}),
        secret_reader(channel),
        channel,
        entries,
    )
    if isinstance(begun, dict):
        return begun
    # Slow to import; most tasks aren't triggers.
    from schedr.runner.waiting import Batch, run_trigger  # noqa: PLC0415

    task = _task(request)
    limit = _max_output_size(task)
    in_flight = optional_whole(task, "in_flight") or DEFAULT_IN_FLIGHT
    if task.get("wait_for") is not None:
        return await _called_for_run(request, begun, channel, limit)
    hint = event_data_hint(begun.hints.get("return"), f"@trigger `{text(task, 'id')}`")
    batch_ms = task.get("batch_ms")
    batch = None
    if isinstance(batch_ms, int):
        # One event of the items gathered: its data the items themselves (decision 352).
        hint = list[Event[hint]]  # type: ignore[valid-type]
        item_hint = get_args(hint)[0]
        batch = Batch(batch_ms / 1000, lambda item: _size(item, item_hint, limit), limit)
    try:
        with running(begun.values, begun.own):
            await run_trigger(
                begun.function,
                begun.arguments,
                task,
                optional_text(obj(request.get("trigger"), "the request's `trigger`"), "after"),
                lambda event: _tell_event(channel, event, hint, limit, in_flight),
                lambda error: channel.send({"type": "trigger_poked", "error": error}),
                batch,
            )
    except CodecError as error:
        await channel.all_stored()
        return failure(error, "event_refused", f"an event's data: {error.reason}")
    except Exception as error:  # an author's error is the trigger's status
        await channel.all_stored()
        return _raised(error)
    await channel.all_stored()
    return failure(
        RuntimeError("its generator returned: a trigger must keep yielding"), "trigger_returned"
    )


def _max_output_size(task: Stored) -> int:
    """The most bytes a task's output may take, from its settings."""
    return whole(obj(task.get("settings"), "the task's `settings`"), "max_output_size")


async def _called_for_run(request: Stored, begun: _Begun, channel: Telling, limit: int) -> Message:
    """An ``@trigger(wait_for=...)``'s one call, for the run it was given (decision 353).

    The event it returns is told, its key that run's ID unless it gives one, planned for that
    run's moment plus ``lag``; then a clean end, which moves its trigger past the run.
    """
    task = _task(request)
    wait_for = obj(task.get("wait_for"), "the task's `wait_for`")
    run = begun.arguments[text(wait_for, "param")]
    where = f"@trigger `{text(task, 'id')}`"
    limit_ms = optional_whole(wait_for, "timeout_ms")
    deadline = asyncio.timeout(None if limit_ms is None else limit_ms / 1000)
    try:
        try:
            async with deadline:
                with running(begun.values, begun.own):
                    called = begun.function(**begun.arguments)
                    if not inspect.isawaitable(called):
                        raise TypeError(f"{where} must be an `async def`")
                    found = await called
        except TimeoutError as error:
            if not deadline.expired():
                raise
            took = timedelta(milliseconds=limit_ms or 0)
            return failure(
                error,
                "trigger_timed_out",
                f"{where}: its call for {_called_for(run)} took over its timeout, {took}",
            )
        if found is not None:
            if not isinstance(found, Event):
                raise TypeError(f"{where} returns an `Event` or `None`, got {type(found).__name__}")
            lag = optional_whole(obj(wait_for.get("run_of") or {}, "`run_of`"), "lag_ms")
            from schedr.run_of import RunResult  # noqa: PLC0415 (only a wait_for needs it)

            if not isinstance(run, RunResult | Event):
                raise TypeError(f"{where} was given {type(run).__name__}, not a run or an event")
            event = event_for_run(found, run, None if lag is None else timedelta(milliseconds=lag))
            returned = begun.hints.get("return")
            hint = next(
                (get_args(a)[0] for a in get_args(returned) if get_origin(a) is Event), None
            )
            await _tell_event(channel, event, hint, limit)
    except CodecError as error:
        return failure(error, "event_refused", f"an event's data: {error.reason}")
    except Exception as error:  # an author's error is the trigger's status
        return _raised(error)
    return {"type": "task_succeeded", "output": None, "outcome": SUCCESS}


def _called_for(run: object) -> str:
    """What an ``@trigger(wait_for=...)``'s call was for, in a message: a run or an event."""
    if isinstance(run, Event):
        return f"event `{run.key}`"
    return f"run {getattr(run, 'run_id', '?')}"


def _size[T](item: Event[T], hint: object, limit: int) -> int:
    """An item's size in a batch, as stored (decision 352), with room for the agent's masking.

    A secret masked grows only as a number (``12345`` becomes ``"****"``), by one byte at most;
    a string never grows (Grok's review: a window filled to the limit went over it, refused
    again on each restart). One that can't be stored at all is given as over ``limit``: it goes
    alone, and fails when it is told, as one item would.
    """
    try:
        data = encode(item, hint)
    except CodecError:
        return limit + 1
    return encoded_size(data) + _numbers(data)


def _numbers(data: object) -> int:
    """How many numbers encoded data holds (``True`` and ``False`` aren't)."""
    count = 0
    stack = [data]
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            stack.extend(cast("dict[str, object]", value).values())
        elif isinstance(value, list):
            stack.extend(cast("list[object]", value))
        elif isinstance(value, int | float) and not isinstance(value, bool):
            count += 1
    return count


async def _tell_event[T](
    channel: Telling, event: Event[T], hint: object, limit: int, in_flight: int = 1
) -> None:
    """Tells the agent what a trigger found; waits while ``in_flight`` aren't stored (M5.2h2).

    With 1 (the default here), it returns once the master has it.

    Raises:
        CodecError: For data that can't be stored, or is over ``limit`` bytes.
        TypeError: For an event with no key.
    """
    if not event.key:
        raise TypeError("an event's key must be a non-empty string, got ''")
    data = encode(event.data, hint)
    check_size(data, limit)
    message = {"type": "trigger_event", "key": event.key, "data": data}
    if event.at is not None:
        message["at"] = event.at.isoformat()
    # Waiting in the loop: the others on it go on.
    await channel.tell_event(message, in_flight)


def _raised(error: BaseException) -> Message:
    """The message for what a task's function raised."""
    if isinstance(error, SecretError):
        return failure(error, error.kind)
    if isinstance(error, ConnectionNotFoundError):
        return failure(error, CONNECTION_NOT_FOUND)
    if isinstance(error, ConnectionDeniedError):
        return failure(error, CONNECTION_DENIED)
    return failure(error, None)


def _finish(request: Stored, begun: _Begun, result: object) -> Message:
    """The message for what a task's function returned."""
    task, hints = _task(request), begun.hints
    sensing = task.get("kind") == "sensor"
    if request.get("cleanup") is not None:
        return {"type": "task_succeeded", "output": encode(None, type(None)), "outcome": SUCCESS}
    if sensing:
        return _sensed_result(result, hints, task)
    if not task.get("branch"):
        return _stored(result, hints.get("return", Any), task)
    try:
        chosen = chosen_ids(result, begun.downstream, text(request, "dag_id"))
    except CodecError as e:
        return failure(e, "output_not_serializable", f"`{text(task, 'id')}`: {e.reason}")
    return _stored(chosen, list[str], task)


def _sensed_result(result: object, hints: Mapping[str, object], task: Stored) -> Message:
    """The message for what a wait step's function returned."""
    if isinstance(result, TimedOut):
        return _stored(result, TimedOut, task, TIMED_OUT_NAME)
    if result is NOT_YET and task.get("wait_for") is not None:
        # Not the run it was given: the master waits for a later one (decision 353).
        return {"type": "task_succeeded", "output": None, "outcome": NOT_YET_OUTCOME}
    return _stored(result, output_hint(hints.get("return", Any)), task)


def execute(
    request: Stored,
    imported: Callable[[], None] = lambda: None,
    secrets: SecretReader | None = None,
    channel: Channel | None = None,
) -> Message:
    """Runs a task request and returns the message to send.

    Args:
        request: The ``task`` message.
        imported: Called once the DAG file is imported and the arguments are built, just
            before the task's function (not when that fails).
        secrets: Reads a secret for ``task_ctx.secret`` (through the agent); ``None``: the
            task reads none.
        channel: The channel to the agent, for the connections it opens; ``None``: none.

    Returns:
        ``task_succeeded`` with the encoded output, or ``task_failed``.
    """
    if request.get("connection_test"):
        return run_connection_test(request, imported, channel)
    begun = _begin(request, imported, secrets, channel)
    if isinstance(begun, dict):
        return begun
    try:
        with running(begun.values, begun.own):
            task = _task(request)
            function, arguments = begun.function, begun.arguments
            sensing = task.get("kind") == "sensor"
            result = _sensed(function, arguments, task) if sensing else function(**arguments)
            # Asked of the result, not the function: a decorator can wrap an `async def`.
            if inspect.iscoroutine(result):
                from schedr.runner.coroutines import run_coroutine  # noqa: PLC0415  # see there

                result = run_coroutine(result)
    except (Exception, SystemExit) as e:
        return _raised(e)
    return _finish(request, begun, result)


async def execute_watcher(
    request: Stored,
    channel: Asking,
    entries: EntriesOf,
    moving: asyncio.Event | None = None,
    go: Callable[[], Awaitable[int | None]] | None = None,
) -> Message:
    """Runs a wait step in the watcher host's loop, its file imported at most once there.

    Args:
        request: The task, as for :func:`execute`.
        channel: The watcher's channel to the agent.
        entries: Gives a DAG file's DAGs.
        moving: Set when a poke is to stop between calls, for new code.
        go: For a poke moving here: everything is ready, and this waits for the old one's stop.
            Gives what is left of its ``max_wait``, in milliseconds.

    Returns:
        ``task_succeeded`` with the encoded output, ``task_failed``, or ``watcher_moved``.
    """
    begun = _begin(
        request,
        lambda: channel.send({"type": "task_imported"}),
        secret_reader(channel),
        channel,
        entries,
        binding=go is not None,
    )
    if isinstance(begun, dict):
        return begun
    task = _task(request)
    if go is not None:
        task = {**task, "max_wait_ms": await go()}
    # Slow to import; most tasks aren't wait steps.
    from schedr.runner.waiting import WatcherMovedError  # noqa: PLC0415

    try:
        with running(begun.values, begun.own):
            result = await _sensed(begun.function, begun.arguments, task, moving)
    except WatcherMovedError:
        return {"type": "watcher_moved"}
    except (Exception, SystemExit) as e:
        return _raised(e)
    return _finish(request, begun, result)


def cleanup_call(
    entry: registry.DefinedDag,
    function: Callable[..., object],
    task_id: str,
    arguments: Mapping[str, object],
    handles: Sequence[Wire],
) -> tuple[Callable[..., object], dict[str, object]]:
    """The cleanup of a task, with the task's inputs it names and the attempt's handles.

    Args:
        entry: The task's DAG.
        function: The task's function, for the defaults of parameters no call gives.
        task_id: The task's ID.
        arguments: The task's arguments, built.
        handles: The attempt's handles, in registration order.

    Returns:
        The cleanup function and its arguments.

    Raises:
        TaskSetupError: If the task has no cleanup function (any more).
        TaskInputError: If a handle's type isn't one the cleanup takes.
    """
    cleanup = entry.cleanups.get(task_id)
    if cleanup is None:
        raise TaskSetupError(f"task `{task_id}` has no `cleanup=` function (any more?)")
    defaults = inspect.signature(function).parameters
    given: dict[str, object] = {}
    for name in inspect.signature(cleanup).parameters:
        if name == HANDLES_PARAMETER:
            continue
        if name in arguments:
            given[name] = arguments[name]
        elif name in defaults and defaults[name].default is not inspect.Parameter.empty:
            given[name] = defaults[name].default
    hints = type_hints(cleanup, f"the cleanup of task `{task_id}`")
    if HANDLES_PARAMETER in inspect.signature(cleanup).parameters:
        classes = {cls.__name__: cls for cls in handle_classes(hints.get(HANDLES_PARAMETER))}
        given[HANDLES_PARAMETER] = [_handle(obj(h, "a handle"), classes, task_id) for h in handles]
    return cleanup, given


def _handle(handle: Stored, classes: Mapping[str, type], task_id: str) -> object:
    """A registered handle as its class's instance."""
    name = str(handle["handle_type"])
    if name not in classes:
        raise TaskInputError(
            f"the cleanup of task `{task_id}` doesn't take handles of type `{name}` (any more?)"
        )
    try:
        return decode(handle["value"], classes[name])
    except CodecError as e:
        raise TaskInputError(f"a `{name}` handle of task `{task_id}`: {e.reason}") from None


_told: set[str] = set()


def tell_note(note: object) -> None:
    """Prints what the agent says of an API request to the task's log (stderr), once per note.

    Args:
        note: The answer's ``note``: a secret in the request masked; ``None``: nothing.
    """
    if isinstance(note, str) and note not in _told:
        _told.add(note)
        print(note, file=sys.stderr)


def api_caller(channel: Asking) -> ApiCaller:
    """Calls schedr's REST API through the agent (``api_call``), one request at a time.

    Args:
        channel: The channel to the agent.

    Returns:
        The caller ``schedr.api`` uses.
    """

    def call(method: str, path: str, body: object, key: str | None) -> tuple[int, object]:
        request: Message = {"type": "api_call", "method": method, "path": path}
        if body is not None:
            request["body"] = as_wire(body, "schedr.api: the body")
        if key is not None:
            request["idempotency_key"] = key
        answer = channel.ask(request)
        if answer.get("type") != "api_answer":
            raise RuntimeError(f"schedr.api: unexpected answer {answer.get('type')!r}")
        tell_note(answer.get("note"))
        return whole(answer, "status"), answer.get("body")

    return call


def handle_adder(channel: Asking) -> HandleAdder:
    """Registers cleanup handles with the agent (``handle_add``), one request at a time.

    Args:
        channel: The channel to the agent.

    Returns:
        The adder ``cleanup_handle`` calls.
    """

    def add(_handle: object, handle_type: str, value: object) -> None:
        handle: Message = {"handle_type": handle_type, "value": as_wire(value, "a handle")}
        answer = channel.ask({"type": "handle_add", "handle": handle})
        if answer.get("type") == "handle_refused":
            raise RuntimeError(f"cleanup_handle: {answer.get('message')}")
        if answer.get("type") != "handle_added":
            raise RuntimeError(f"cleanup_handle: unexpected answer {answer.get('type')!r}")

    return add


def run_connection_test(  # noqa: PLR0911 (a result for each way a test ends)
    request: Stored,
    imported: Callable[[], None] = lambda: None,
    channel: Channel | None = None,
) -> Message:
    """Runs ``schedr connections test NAME`` on this worker (decision 198).

    Imports the file defining the connection, opens it and runs its check (or opens and
    closes it, without one).

    Args:
        request: The ``task`` message, its ``connection_test`` the connection's name.
        imported: Called once the file is imported.
        channel: The channel to the agent, for the connection's values.

    Returns:
        ``task_succeeded`` with no output, or ``task_failed``.
    """
    name = text(request, "connection_test")
    root, root_dir, path = _dag_file(request)
    try:
        import_dag_entries(root, root_dir, path)
        found = defined_connection(module_name(root, path), name)
    except TaskSetupError as e:
        return failure(e, CONNECTION_NOT_FOUND)
    except (Exception, SystemExit) as e:
        return failure(e, "import_failed")
    imported()
    try:
        opener = connection_opener(channel) if channel is not None else None
        own = TaskValues(name, 1, None, None, opener)
        with running(run_values(obj(request.get("run"), "the request's `run`"), None), own):
            found.run_test()
    except SecretError as e:
        return failure(e, e.kind)
    except ConnectionNotFoundError as e:
        return failure(e, CONNECTION_NOT_FOUND)
    except ConnectionDeniedError as e:
        return failure(e, CONNECTION_DENIED)
    except (Exception, SystemExit) as e:
        return failure(e, None)
    return {"type": "task_succeeded", "output": encode(None, type(None)), "outcome": SUCCESS}


def defined_connection(module: str, name: str) -> ConnectionBase[object]:
    """The connection ``name`` the module defines at its top level.

    Args:
        module: The module's name.
        name: The connection's.

    Returns:
        The connection.

    Raises:
        TaskSetupError: When the module defines none by that name (any more).
    """
    loaded = sys.modules.get(module)
    found: dict[int, ConnectionBase[object]] = {}
    for value in vars(loaded).values() if loaded is not None else ():
        if isinstance(value, ConnectionBase) and value.name == name:
            found[id(value)] = value
    if len(found) > 1:
        # Which one the parse recorded can't be told: testing either could pass for the other.
        raise TaskSetupError(f"the file holds {len(found)} connections named `{name}`: keep one")
    if not found:
        raise TaskSetupError(f"the file doesn't define connection `{name}` (any more?)")
    return next(iter(found.values()))


def outcome_of(result: object, annotation: object) -> tuple[str, object, object]:
    """The outcome a returned value stands for, with the output to store and its type.

    Args:
        result: What the task's function returned.
        annotation: Its return annotation.

    Returns:
        ``(outcome, output, type)``: an ``Outcome`` member's name with no output, an outcome
        union member's class name (``"None"`` for ``None``) with the value, or ``SUCCESS``.

    Raises:
        CodecError: For a value that isn't one of the declared outcomes.
    """
    spec = from_return_type(annotation, "the task")
    if outcome_enum(annotation) is not None:
        # The member's name is what is stored: checked as `encode` checks it (B129).
        return encode(result, spec.enum), None, None
    if spec.is_union:
        member = type(result)
        if member not in spec.classes:
            raise CodecError("", f"`{member.__name__}` isn't one of its outcomes")
        return ("None" if result is None else member.__name__), result, member
    return SUCCESS, result, annotation


def _sensed(
    function: Callable[..., object],
    arguments: Mapping[str, object],
    task: Stored,
    moving: asyncio.Event | None = None,
) -> Coroutine[object, object, object]:
    """A wait step's coroutine, waiting for its value or its deadline."""
    # Slow to import; most tasks aren't wait steps.
    from schedr.runner.waiting import waited  # noqa: PLC0415

    return waited(function, arguments, task, moving)


def _stored(result: object, annotation: object, task: Stored, named: str | None = None) -> Message:
    try:
        outcome, result, annotation = (
            outcome_of(result, annotation) if named is None else (named, result, annotation)
        )
        output = encode(result, annotation)
    except CodecError as e:
        return failure(
            e, "output_not_serializable", f"the output of `{task['id']}` can't be stored, {e}"
        )
    except Exception as e:  # a model's serializer, a recursive value (B67)
        return failure(
            e,
            "output_not_serializable",
            f"the output of `{task['id']}` can't be stored, {type(e).__name__}: {e}",
        )
    try:
        check_size(output, _max_output_size(task))
    except CodecError as e:
        return failure(e, "output_too_large", f"the output of `{task['id']}`: {e.reason}")
    except Exception as e:  # JSON or UTF-8 conversion (B67)
        return failure(
            e,
            "output_not_serializable",
            f"the output of `{task['id']}` can't be stored, {type(e).__name__}: {e}",
        )
    return {"type": "task_succeeded", "output": output, "outcome": outcome}


def run_task(channel: Channel, request: Stored) -> int:
    """Handles a ``task`` request: runs it and reports the result.

    Args:
        channel: The channel to the agent.
        request: The ``task`` message.

    Returns:
        The exit code: 0 if the task succeeded, 1 otherwise.
    """
    extend_sys_path(texts(request, "sys_path"))
    # Its `logging` as structured lines (decision 514), unless it configures its own.
    install_structured_logging()
    # The agent times the start from it: the interpreter's, then the DAG file's import.
    result = execute(
        request,
        lambda: channel.send({"type": "task_imported"}),
        secret_reader(channel),
        channel,
    )
    channel.send(result)
    return 0 if result["type"] == "task_succeeded" else 1
