"""Task mode: run one Python task attempt.

The runner imports the DAG file (the agent's copy of the root's latest code) under the same
module name as parse mode, finds the task's function, builds its arguments from the stored
values, calls it with ``dag_ctx`` giving values, and sends back the encoded output or an error
summary. It doesn't enforce timeouts or watch memory: the agent does (see PROJECT.md,
Rust/Python integration).
"""

import contextlib
import dataclasses
import sys
import types
from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, Union, get_args, get_origin, is_typeddict
from urllib.parse import quote

from schedr._model import SecretRef
from schedr._serialization import CodecError, check_size, decode, decode_any, encode, models
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
from schedr.connections import Connection, ConnectionDeniedError, ConnectionNotFoundError
from schedr.context import (
    ConnectionEnter,
    ConnectionOpener,
    RunValues,
    SecretError,
    SecretReader,
    TaskValues,
    running,
)
from schedr.outcomes import from_return_type, outcome_enum
from schedr.runner.parse import extend_sys_path, import_dag_entries, import_error, module_name
from schedr.runner.protocol import Channel

SUCCESS = "SUCCESS"
"""The outcome of a task without outcomes of its own (``DefaultOutcome.SUCCESS``)."""


class TaskInputError(Exception):
    """A stored input or argument that can't be given to the task's function."""


class TaskSetupError(Exception):
    """The task can't be started: its DAG or function isn't in the DAG file."""


def resolve(
    reference: Mapping[str, Any],
    inputs: Mapping[str, Any],
    run: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None = None,
) -> Any:
    """The stored value a reference stands for.

    Args:
        reference: A reference as stored (``{"kind": "output", "task": ..., "path": [...],
            "on": [...]}``, ``{"kind": "outcome", "task": ...}`` or ``{"kind": "dag_ctx",
            "field": ...}``).
        inputs: The upstream outputs, by task ID, as stored JSON.
        run: The request's run information.
        outcomes: The upstream tasks' outcomes, by task ID: a name, or ``None`` for one that
            was skipped.

    Returns:
        The stored value (``planned_for`` / ``since`` tagged as ``$datetime``); ``None`` for
        a reference whose edge wasn't taken (its upstream skipped, or narrowed by ``.on()``
        to outcomes that didn't happen); the outcome's name for an ``outcome`` reference.

    Raises:
        TaskInputError: For a missing input or path, or a truncated stream.
    """
    outcomes = outcomes or {}
    if reference["kind"] == "secret":
        name = secret_key(reference)
        secrets = run.get(SECRETS, {})
        if name not in secrets:
            raise TaskInputError(f"the secret `{_label(reference)}` wasn't fetched")
        value = secrets[name]
        return escaped(value) if isinstance(value, dict) else value
    if reference["kind"] == "param":
        return _follow("dag_ctx.params", run.get("params", {}), reference.get("path", []))
    if reference["kind"] == "dag_ctx":
        field = reference["field"]
        values = {
            "dag_id": run["dag_id"],
            "run_id": run["run_id"],
            "run_kind": run["kind"],
            "planned_for": {"$datetime": run["planned_for"]},
            "since": {"$datetime": run["since"]},
        }
        return values[field]
    task = reference["task"]
    if reference["kind"] == "outcome":
        if task not in outcomes:
            raise TaskInputError(f"the outcome of `{task}` isn't among the task's inputs")
        return outcomes[task]
    if task in outcomes:
        outcome = outcomes[task]
        on = reference.get("on", [])
        # Skipped, or another outcome than the edge is taken on: the data rule's `None`.
        if outcome is None or (on and outcome not in on):
            return None
    if task not in inputs:
        raise TaskInputError(f"the output of `{task}` isn't among the task's inputs")
    return _follow(task, inputs[task], reference.get("path", []))


SECRETS = "$secrets"
"""Where :func:`execute` puts the request's fetched secrets in the run mapping."""

CONNECTION_NOT_FOUND = "connection_not_found"
"""The error kind of a connection the agent doesn't know (not in the DAG folder's registry)."""

CONNECTION_DENIED = "connection_denied"
"""The error kind of a connection the task doesn't declare in ``connections=``."""


def secret_key(
    name: Mapping[str, Any],
) -> tuple[str | None, str, str | None, str | None]:
    """A secret's name as stored, as a key.

    ``{"storage": ..., "path": ..., "key": ..., "default": ...}``.

    Args:
        name: The stored name (or a secret reference).

    Returns:
        ``(storage, path, key, default)``.
    """
    return (name.get("storage"), name["path"], name.get("key"), name.get("default"))


def _label(name: Mapping[str, Any]) -> str:
    return SecretRef(
        name["path"], name.get("key"), name.get("storage"), name.get("default")
    ).label()


def secret_reader(channel: Channel) -> SecretReader:
    """Reads a secret through the agent (``secret_get``), one request at a time.

    Args:
        channel: The channel to the agent.

    Returns:
        The reader ``task_ctx.secret`` calls.
    """

    def read(ref: SecretRef) -> str | dict[str, str]:
        name = {k: v for k, v in ref.to_json().items() if k != "kind"}
        answer = channel.ask({"type": "secret_get", "secret": name})
        if answer.get("type") == "secret":
            value: str | dict[str, str] = answer["value"]
            return value
        if answer.get("type") == "secret_failed":
            raise SecretError(str(answer["kind"]), str(answer["message"]))
        raise SecretError("secret_unavailable", f"unexpected answer {answer.get('type')!r}")

    return read


def connection_opener(channel: Channel) -> ConnectionOpener:
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
            values: Mapping[str, object] = answer["values"]
            return values
        if answer.get("type") == "connection_failed":
            kind, message = str(answer["kind"]), str(answer["message"])
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


def _is_process_result(value: Mapping[str, Any]) -> bool:
    return value.keys() == PROCESS_RESULT


def _follow(task: str, value: Any, path: list[Any]) -> Any:
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
        else:
            items = value.get("$tuple") if isinstance(value, dict) else value
            if not isinstance(items, list) or step >= len(items):
                raise TaskInputError(f"`{task}`: index {step} doesn't exist in the output")
            value = items[step]
    return value


def render(
    parts: list[Any],
    inputs: Mapping[str, Any],
    run: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None = None,
) -> str:
    """Renders a stored template to text, the way the agent renders command lines.

    Args:
        parts: The template's parts (``{"text": ...}`` and ``{"ref": {...}}``).
        inputs: The upstream outputs.
        run: The run information.
        outcomes: The upstream tasks' outcomes (see :func:`resolve`).

    Returns:
        The text.

    Raises:
        TaskInputError: For a reference that can't be resolved or written as text.
    """
    out: list[str] = []
    for part in parts:
        if "text" in part:
            out.append(part["text"])
            continue
        reference = part["ref"]
        value = decode_any(resolve(reference, inputs, run, outcomes))
        spec = reference.get("format")
        if value is None or isinstance(value, list | dict | tuple):
            raise TaskInputError(f"a {type(value).__name__} can't be rendered into text")
        written = value.strftime(spec) if spec and "%" in spec else as_text(value)
        # A typed connection's URL (decision 188): only the text, never the URL's structure.
        out.append(quote(written, safe="") if reference.get("encode") == "url" else written)
    return "".join(out)


def build(
    value: Mapping[str, Any],
    annotation: object,
    inputs: Mapping[str, Any],
    run: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None = None,
) -> Any:
    """An argument's Python value from its stored form, decoded against the parameter type.

    Args:
        value: The stored value (``literal``, ``ref``, ``template``, ``list`` or ``map``).
        annotation: The parameter's annotation.
        inputs: The upstream outputs.
        run: The run information.
        outcomes: The upstream tasks' outcomes (see :func:`resolve`).

    Returns:
        The value to pass.

    Raises:
        TaskInputError: If a stored value doesn't fit the parameter.
    """
    try:
        if "literal" in value:
            return decode(value["literal"], annotation)
        if "ref" in value:
            return decode(
                resolve(value["ref"], inputs, run, outcomes),
                _narrowed(value["ref"], annotation, outcomes),
            )
        if "template" in value:
            # Text, which must fit the place it fills (B124: a union member taking it).
            return decode(render(value["template"], inputs, run, outcomes), annotation)
        return _built(value, unwrap(annotation), inputs, run, outcomes)
    except CodecError as e:
        raise TaskInputError(f"a stored value doesn't fit the parameter's type, {e}") from e


def _built(
    value: Mapping[str, Any],
    target: object,
    inputs: Mapping[str, Any],
    run: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None,
) -> Any:
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
                return _built_list(value["list"], member, inputs, run, outcomes)
            return _built_map(value["map"], member, inputs, run, outcomes)
        except (TaskInputError, CodecError) as e:
            error = e
    raise error


def _built_list(
    items: list[Any],
    target: object,
    inputs: Mapping[str, Any],
    run: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None,
) -> Any:
    """A list written with references inside, as the parameter's type.

    A tuple for a tuple parameter (B65), else a list.
    """
    origin, args = get_origin(target), get_args(target)
    if origin is tuple:
        types_ = tuple_items(args)
        if len(types_) != len(items):
            raise TaskInputError(f"expected a tuple of {len(types_)}, got {len(items)} values")
        return tuple(build(v, t, inputs, run, outcomes) for v, t in zip(items, types_, strict=True))
    item = args[0] if origin in SEQUENCE_ORIGINS and args else Any
    return [build(v, item, inputs, run, outcomes) for v in items]


def _built_map(
    fields: Mapping[str, Any],
    target: object,
    inputs: Mapping[str, Any],
    run: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None,
) -> Any:
    """A dict written with references inside, as the parameter's type.

    A dataclass or a Pydantic model is built from its fields (B65); a ``TypedDict`` or a
    ``dict`` is a dict, each value of its field's or item's type.
    """
    origin, args = get_origin(target), get_args(target)
    if origin in MAPPING_ORIGINS or target in MAPPING_ORIGINS or not isinstance(target, type):
        item = args[1] if origin in MAPPING_ORIGINS and args else Any
        return {k: build(v, item, inputs, run, outcomes) for k, v in fields.items()}
    hints = type_hints(target, f"`{target.__name__}`")
    if is_typeddict(target):
        # Its keys, as a dataclass or a model checks its fields when built (B124).
        if unknown := sorted(set(fields) - set(hints)):
            raise TaskInputError(f"`{target.__name__}` has no field `{unknown[0]}`")
        if missing := sorted(set(getattr(target, "__required_keys__", ())) - set(fields)):
            raise TaskInputError(f"`{target.__name__}` needs `{missing[0]}`")
    built = {k: build(v, hints.get(k, Any), inputs, run, outcomes) for k, v in fields.items()}
    try:
        if dataclasses.is_dataclass(target):
            return target(**built)
        if models.is_model(target):
            return models.construct(target, built)
    except (TypeError, ValueError) as e:  # a field missing or refused
        raise TaskInputError(f"`{target.__name__}` can't be built: {e}") from e
    return built


def _narrowed(
    reference: Mapping[str, Any], annotation: object, outcomes: Mapping[str, Any] | None
) -> object:
    """The member of a union annotation the upstream's outcome names, else the annotation.

    A plain JSON object can fit several dataclasses of an outcome union: the stored outcome
    says which one it is (only a whole output: a path reaches into one member already).
    """
    outcome = (outcomes or {}).get(reference.get("task", ""))
    if reference.get("kind") != "output" or reference.get("path"):
        return annotation
    return member_named(annotation, outcome)


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
    for member in get_args(unwrap(annotation)):
        target = unwrap(member)
        if getattr(get_origin(target) or target, "__name__", None) == outcome:
            return member
    return annotation


def mapped_arguments(
    inputs: Mapping[str, Any],
    hints: Mapping[str, Any],
    run: Mapping[str, Any] | None = None,
    upstream_outcomes: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
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

    Returns:
        The arguments by parameter name (none for a task that isn't mapped).

    Raises:
        TaskInputError: If an element doesn't fit its parameter.
    """
    outcomes = inputs.get("$expand_outcomes", {})
    arguments = {}
    for name, value in inputs.get("$expand", {}).items():
        annotation = member_named(hints.get(name, Any), outcomes.get(name))
        try:
            arguments[name] = decode(value, annotation)
        except CodecError as e:
            raise TaskInputError(
                f"mapped argument `{name}`: the element doesn't fit the parameter's type, {e}"
            ) from e
    for name, value in inputs.get("$expand_values", {}).items():
        try:
            arguments[name] = build(
                value, hints.get(name, Any), inputs, run or {}, upstream_outcomes
            )
        except TaskInputError as e:
            raise TaskInputError(f"mapped argument `{name}`: {e}") from e
    return arguments


def run_values(run: Mapping[str, Any], params_class: type | None = None) -> RunValues:
    """The ``dag_ctx`` values for a request's run information.

    Args:
        run: The run information.
        params_class: The class the DAG declares its parameters with.

    Returns:
        The values.
    """
    return RunValues(
        dag_id=run["dag_id"],
        run_id=run["run_id"],
        kind=run["kind"],
        planned_for=datetime.fromisoformat(run["planned_for"]),
        since=datetime.fromisoformat(run["since"]),
        params=run.get("params", {}),
        params_class=params_class,
    )


def find_function(request: Mapping[str, Any]) -> Callable[..., Any]:
    """Imports the DAG file and finds the task's function.

    Args:
        request: The ``task`` message.

    Returns:
        The task's function.
    """
    return find_task(request)[0]


def find_task(request: Mapping[str, Any]) -> tuple[Callable[..., Any], frozenset[str]]:
    """Imports the DAG file and finds the task's function and its direct downstream tasks.

    Args:
        request: The ``task`` message.

    Returns:
        The function, and the IDs of the tasks right after it (a branch chooses among them).
    """
    function, downstream, _ = find_task_of_dag(request)
    return function, downstream


def find_task_of_dag(
    request: Mapping[str, Any],
) -> tuple[Callable[..., Any], frozenset[str], type | None]:
    """As :func:`find_task`, with the class the DAG declares its parameters with.

    Args:
        request: The ``task`` message.

    Returns:
        The function, the IDs of the tasks right after it, and the params class.

    Raises:
        TaskSetupError: If the DAG or the task isn't in the file.
    """
    dag_file = request["dag_file"]
    entries = import_dag_entries(dag_file["root"], Path(dag_file["root_dir"]), dag_file["path"])
    task_id = request["task"]["id"]
    for entry in entries:
        if entry.model.id == request["dag_id"]:
            if task_id not in entry.functions:
                raise TaskSetupError(
                    f"DAG `{request['dag_id']}` has no Python task `{task_id}` (any more?)"
                )
            downstream = frozenset(e.to for e in entry.model.edges if e.from_ == task_id)
            return entry.functions[task_id], downstream, entry.params_class
    raise TaskSetupError(
        f"`{dag_file['path']}` doesn't define DAG `{request['dag_id']}` (any more?)"
    )


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


def failure(error: BaseException, kind: str | None, message: str | None = None) -> dict[str, Any]:
    """A ``task_failed`` message.

    Args:
        error: The exception.
        kind: The failure kind schedr recognises, or ``None`` for the task's own exception.
        message: A message replacing the exception's, if given.

    Returns:
        The message.
    """
    summary = import_error(error)
    if message is not None:
        summary["message"] = message
    return {"type": "task_failed", "error": summary, "kind": kind}


def execute(  # noqa: PLR0911 (a result for each way a task ends)
    request: Mapping[str, Any],
    imported: Callable[[], None] = lambda: None,
    secrets: SecretReader | None = None,
    channel: Channel | None = None,
) -> dict[str, Any]:
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
    task, inputs, run = request["task"], request["inputs"], request["run"]
    if request.get("secrets"):
        fetched = {secret_key(s["name"]): s["value"] for s in request["secrets"]}
        run = {**run, SECRETS: fetched}
    outcomes = request.get("outcomes", {})
    task_id = task["id"]
    try:
        function, downstream, params_class = find_task_of_dag(request)
        hints = type_hints(function, f"task `{task_id}`")
        arguments = {
            name: build(value, hints.get(name, Any), inputs, run, outcomes)
            for name, value in task.get("arguments", {}).items()
        }
        arguments.update(mapped_arguments(inputs, hints, run, outcomes))
    except TaskSetupError as e:
        return failure(e, "task_not_found")
    except (TaskInputError, TypeModelError) as e:
        return failure(e, "input_mismatch")
    except (Exception, SystemExit) as e:
        return failure(e, "import_failed")
    imported()
    try:
        opener = connection_opener(channel) if channel is not None else None
        own = TaskValues(
            task_id, int(run.get("try_number", 1)), run.get("map_index"), secrets, opener
        )
        with running(run_values(run, params_class), own):
            result = function(**arguments)
    except SecretError as e:
        return failure(e, e.kind)
    except ConnectionNotFoundError as e:
        return failure(e, CONNECTION_NOT_FOUND)
    except ConnectionDeniedError as e:
        return failure(e, CONNECTION_DENIED)
    except (Exception, SystemExit) as e:
        return failure(e, None)
    if not task.get("branch"):
        return _stored(result, hints.get("return", Any), task)
    try:
        chosen = chosen_ids(result, downstream, request["dag_id"])
    except CodecError as e:
        return failure(e, "output_not_serializable", f"`{task_id}`: {e.reason}")
    return _stored(chosen, list[str], task)


def run_connection_test(  # noqa: PLR0911 (a result for each way a test ends)
    request: Mapping[str, Any],
    imported: Callable[[], None] = lambda: None,
    channel: Channel | None = None,
) -> dict[str, Any]:
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
    name = str(request["connection_test"])
    dag_file = request["dag_file"]
    try:
        import_dag_entries(dag_file["root"], Path(dag_file["root_dir"]), dag_file["path"])
        found = defined_connection(module_name(dag_file["root"], dag_file["path"]), name)
    except TaskSetupError as e:
        return failure(e, CONNECTION_NOT_FOUND)
    except (Exception, SystemExit) as e:
        return failure(e, "import_failed")
    imported()
    try:
        opener = connection_opener(channel) if channel is not None else None
        own = TaskValues(name, 1, None, None, opener)
        with running(run_values(request["run"], None), own):
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


def defined_connection(module: str, name: str) -> Connection[object]:
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
    found: dict[int, Connection[object]] = {}
    for value in vars(loaded).values() if loaded is not None else ():
        if isinstance(value, Connection) and value.name == name:
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


def _stored(result: object, annotation: object, task: Mapping[str, Any]) -> dict[str, Any]:
    try:
        outcome, result, annotation = outcome_of(result, annotation)
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
        check_size(output, task["settings"]["max_output_size"])
    except CodecError as e:
        return failure(e, "output_too_large", f"the output of `{task['id']}`: {e.reason}")
    except Exception as e:  # JSON or UTF-8 conversion (B67)
        return failure(
            e,
            "output_not_serializable",
            f"the output of `{task['id']}` can't be stored, {type(e).__name__}: {e}",
        )
    return {"type": "task_succeeded", "output": output, "outcome": outcome}


def run_task(channel: Channel, request: Mapping[str, Any]) -> int:
    """Handles a ``task`` request: runs it and reports the result.

    Args:
        channel: The channel to the agent.
        request: The ``task`` message.

    Returns:
        The exit code: 0 if the task succeeded, 1 otherwise.
    """
    extend_sys_path(request["sys_path"])
    # The agent times the start from it: the interpreter's, then the DAG file's import.
    result = execute(
        request,
        lambda: channel.send({"type": "task_imported"}),
        secret_reader(channel),
        channel,
    )
    channel.send(result)
    return 0 if result["type"] == "task_succeeded" else 1
