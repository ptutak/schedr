"""Task mode: run one Python task attempt.

The runner imports the DAG file (the agent's copy of the root's latest code) under the same
module name as parse mode, finds the task's function, builds its arguments from the stored
values, calls it with ``dag_ctx`` giving values, and sends back the encoded output or an error
summary. It doesn't enforce timeouts or watch memory: the agent does (see PROJECT.md,
Rust/Python integration).
"""

import types
from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, Union, get_args, get_origin

from schedr._serialization import CodecError, check_size, decode, decode_any, encode
from schedr._serialization.annotations import (
    MAPPING_ORIGINS,
    SEQUENCE_ORIGINS,
    TypeModelError,
    type_hints,
    unwrap,
)
from schedr._templates import as_text
from schedr.context import RunValues, TaskValues, running
from schedr.outcomes import from_return_type, outcome_enum
from schedr.runner.parse import extend_sys_path, import_dag_entries, import_error
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
    value = inputs[task]
    for step in reference.get("path", []):
        if isinstance(step, str):
            if not isinstance(value, dict) or step not in value:
                raise TaskInputError(f"`{task}`: `{step}` doesn't exist in the output")
            if value.get(f"{step}_truncated") is True:
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
        if spec and "%" in spec:
            out.append(value.strftime(spec))
        else:
            out.append(as_text(value))
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
            return render(value["template"], inputs, run, outcomes)
        target = unwrap(annotation)
        args = get_args(target)
        if "list" in value:
            item = args[0] if get_origin(target) in SEQUENCE_ORIGINS and args else Any
            return [build(v, item, inputs, run, outcomes) for v in value["list"]]
        item = args[1] if get_origin(target) in MAPPING_ORIGINS and args else Any
        return {k: build(v, item, inputs, run, outcomes) for k, v in value["map"].items()}
    except CodecError as e:
        raise TaskInputError(f"a stored value doesn't fit the parameter's type, {e}") from e


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
    return _member_named(annotation, outcome)


def _member_named(annotation: object, outcome: object) -> object:
    """The member of a union annotation named `outcome`, else the annotation."""
    if not isinstance(outcome, str) or get_origin(unwrap(annotation)) not in {
        Union,
        types.UnionType,
    }:
        return annotation
    for member in get_args(unwrap(annotation)):
        if getattr(get_origin(member) or member, "__name__", None) == outcome:
            return member
    return annotation


def mapped_arguments(inputs: Mapping[str, Any], hints: Mapping[str, Any]) -> dict[str, Any]:
    """A mapped task instance's own arguments: the elements the master resolved for it.

    They come with the inputs under ``$expand`` (decision 76); one taken element by element
    from a mapped upstream task has that instance's outcome under ``$expand_outcomes``, which
    picks the member of a union to decode it as.

    Args:
        inputs: The instance's inputs.
        hints: The task function's type hints.

    Returns:
        The arguments by parameter name (none for a task that isn't mapped).

    Raises:
        TaskInputError: If an element doesn't fit its parameter.
    """
    outcomes = inputs.get("$expand_outcomes", {})
    arguments = {}
    for name, value in inputs.get("$expand", {}).items():
        annotation = _member_named(hints.get(name, Any), outcomes.get(name))
        try:
            arguments[name] = decode(value, annotation)
        except CodecError as e:
            raise TaskInputError(
                f"mapped argument `{name}`: the element doesn't fit the parameter's type, {e}"
            ) from e
    return arguments


def run_values(run: Mapping[str, Any]) -> RunValues:
    """The ``dag_ctx`` values for a request's run information.

    Args:
        run: The run information.

    Returns:
        The values.
    """
    return RunValues(
        dag_id=run["dag_id"],
        run_id=run["run_id"],
        kind=run["kind"],
        planned_for=datetime.fromisoformat(run["planned_for"]),
        since=datetime.fromisoformat(run["since"]),
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
            return entry.functions[task_id], downstream
    raise TaskSetupError(
        f"`{dag_file['path']}` doesn't define DAG `{request['dag_id']}` (any more?)"
    )


def chosen_ids(result: object, downstream: frozenset[str]) -> list[str]:
    """The task IDs a ``@branch`` function's result chose.

    Args:
        result: What it returned: a task handle, or a list of them.
        downstream: Its direct downstream tasks, the ones it may choose.

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
    request: Mapping[str, Any], imported: Callable[[], None] = lambda: None
) -> dict[str, Any]:
    """Runs a task request and returns the message to send.

    Args:
        request: The ``task`` message.
        imported: Called once the DAG file is imported and the arguments are built, just
            before the task's function (not when that fails).

    Returns:
        ``task_succeeded`` with the encoded output, or ``task_failed``.
    """
    task, inputs, run = request["task"], request["inputs"], request["run"]
    outcomes = request.get("outcomes", {})
    task_id = task["id"]
    try:
        function, downstream = find_task(request)
        hints = type_hints(function, f"task `{task_id}`")
        arguments = {
            name: build(value, hints.get(name, Any), inputs, run, outcomes)
            for name, value in task.get("arguments", {}).items()
        }
        arguments.update(mapped_arguments(inputs, hints))
    except TaskSetupError as e:
        return failure(e, "task_not_found")
    except (TaskInputError, TypeModelError) as e:
        return failure(e, "input_mismatch")
    except (Exception, SystemExit) as e:
        return failure(e, "import_failed")
    imported()
    try:
        own = TaskValues(task_id, int(run.get("try_number", 1)), run.get("map_index"))
        with running(run_values(run), own):
            result = function(**arguments)
    except (Exception, SystemExit) as e:
        return failure(e, None)
    if not task.get("branch"):
        return _stored(result, hints.get("return", Any), task)
    try:
        chosen = chosen_ids(result, downstream)
    except CodecError as e:
        return failure(e, "output_not_serializable", f"`{task_id}`: {e.reason}")
    return _stored(chosen, list[str], task)


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
        if not isinstance(result, spec.enum):
            raise CodecError("", f"expected a member of {spec.enum.__name__}, got {result!r}")
        return result.name, None, None
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
    try:
        check_size(output, task["settings"]["max_output_size"])
    except CodecError as e:
        return failure(e, "output_too_large", f"the output of `{task['id']}`: {e.reason}")
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
    result = execute(request, lambda: channel.send({"type": "task_imported"}))
    channel.send(result)
    return 0 if result["type"] == "task_succeeded" else 1
