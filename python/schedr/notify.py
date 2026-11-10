"""Notifications (decisions 408, 409, 417-423): which channels a DAG tells of which events.

A DAG names channels in ``notify=``: a string names a channel of the server's configuration,
a :class:`WebhookChannel` is the DAG's own. A list gives each the server's default events;
a dict chooses them::

    from schedr import Dag, Notify, WebhookChannel, notify

    relay = WebhookChannel(
        "relay",
        url="https://relay.example.com/schedr",
        body={"title": t"{notify.dag} {notify.event}", "tasks": notify.failed_tasks},
    )

    with Dag("etl", notify={relay: [Notify.RUN_FAILED], "ops-slack": [Notify.RUN_RECOVERED]}):
        ...

A body is JSON whose values may be placeholders (``notify.dag``) or t-strings over them;
the master fills them in when it queues a notification, without running any code. Plain
values in a t-string are written in when the file loads. A DAG's webhook URL is in the DAG
file, so it must hold no secret: a Slack or Teams webhook URL is one, and belongs to a server
webhook (decision 425).

A task sends a message of its own to a channel of its DAG with :meth:`WebhookChannel.send`
or :func:`schedr.api.notify` (``schedr task-ctx notify`` from a command).
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from enum import StrEnum
from string.templatelib import Interpolation
from string.templatelib import Template as TString
from typing import TYPE_CHECKING, Final, NoReturn

from schedr._model.notify import NotifyEntry, ServerChannel, WebhookDef
from schedr._templates import as_text

if TYPE_CHECKING:
    from schedr.types import Json

__all__ = [
    "Notify",
    "Placeholder",
    "WebhookChannel",
    "attempt",
    "dag",
    "data",
    "event",
    "failed_tasks",
    "link",
    "planned_for",
    "run",
    "state",
    "task",
    "text",
    "time",
    "zone",
]


class Notify(StrEnum):
    """An event a channel is told of (decision 408)."""

    RUN_FAILED = "RUN_FAILED"
    """A run ended ``failed`` (a cancel with a failure included). The default."""
    TASK_FAILED = "TASK_FAILED"
    """A task failed after its last retry."""
    TASK_RETRY = "TASK_RETRY"
    """An attempt failed and will be retried."""
    RUN_CANCELLED = "RUN_CANCELLED"
    """A run was cancelled, with no failure."""
    RUN_SUCCEEDED = "RUN_SUCCEEDED"
    """A run ended ``success``."""
    RUN_RECOVERED = "RUN_RECOVERED"
    """A run succeeded after the DAG's previous run failed."""
    TRIGGER_FAILING = "TRIGGER_FAILING"
    """The DAG's trigger kept failing, or its generator returned."""
    LATE_CLEANUP_FAILED = "LATE_CLEANUP_FAILED"
    """A cleanup that ran after its run had ended failed."""


class Placeholder:
    """A value the master fills in a message body: ``notify.dag``, ``notify.data["rows"]``.

    It has no value in the DAG file: use it in a body, bare (keeping the value's JSON type)
    or in a t-string (as text).
    """

    __slots__ = ("_name", "_path")

    def __init__(self, name: str, path: tuple[str | int, ...] = ()) -> None:
        """A placeholder; ``schedr.notify`` has one of each name."""
        self._name = name
        self._path = path

    def __getitem__(self, key: str | int) -> Placeholder:
        """A key or an index into the value: ``notify.data["rows"]``.

        Raises:
            TypeError: For a key that is neither a string nor an int.
        """
        if isinstance(key, bool) or not isinstance(key, str | int):
            raise TypeError(f"{self!r}[{key!r}]: a key is a string or an int")
        if isinstance(key, int) and key < 0:
            raise TypeError(f"{self!r}[{key}]: an index counts from 0")
        return Placeholder(self._name, (*self._path, key))

    def __repr__(self) -> str:
        """``notify.data['rows']``."""
        return "notify." + self._name + "".join(f"[{key!r}]" for key in self._path)

    def _refuse(self, *_: object) -> NoReturn:
        raise TypeError(
            f"{self!r} has no value in the DAG file: use it in a notification body, bare or in "
            f'a t-string (t"{{{self!r}}}"), not in an f-string or as a value'
        )

    __str__ = _refuse
    __format__ = _refuse
    __bool__ = _refuse
    __iter__ = _refuse

    def to_json(self) -> dict[str, object]:
        """The reference as DAG JSON.

        Returns:
            ``{"name": ...}``, with ``"path"`` when it has one.
        """
        result: dict[str, object] = {"name": self._name}
        if self._path:
            result["path"] = list(self._path)
        return result


event: Final = Placeholder("event")
"""The event's name (``RUN_FAILED``; ``MESSAGE`` for a task's own ``send``)."""
dag: Final = Placeholder("dag")
"""The DAG's ID."""
run: Final = Placeholder("run")
"""The run's ID."""
planned_for: Final = Placeholder("planned_for")
"""The run's planned time (UTC, RFC 3339), or null."""
state: Final = Placeholder("state")
"""The run's or the task's state."""
text: Final = Placeholder("text")
"""schedr's one-line summary; for a task's own ``send``, its ``text``."""
link: Final = Placeholder("link")
"""The link to the run in the UI (``[notifications] ui_url``), or null."""
failed_tasks: Final = Placeholder("failed_tasks")
"""The failed tasks: a list of ``{"task": ..., "error": ...}``."""
task: Final = Placeholder("task")
"""The task's ID, for task events and ``send``; null otherwise."""
attempt: Final = Placeholder("attempt")
"""The attempt's number, for task events and ``send``; null otherwise."""
time: Final = Placeholder("time")
"""When the event happened (UTC, RFC 3339)."""
zone: Final = Placeholder("zone")
"""The DAG's time zone."""
data: Final = Placeholder("data")
"""A task's own ``send``: its ``data``; null in event notifications."""

_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")


class WebhookChannel:
    """A webhook the DAG defines: ``WebhookChannel("relay", url=..., body=...)``.

    Its URL must start with one of the server's ``[notifications] dag_urls``
    (checked by the master), and it is written in the DAG file: it must hold no secret
    (decision 425). A secret webhook (Slack's, Teams') is a server webhook, named by a string.

    Args:
        name: Its name, unique among the DAG's channels; ``send`` and ``schedr task-ctx
            notify`` name it.
        url: Where it posts (``https://`` or ``http://``).
        body: The JSON it posts, with placeholders (``notify.dag``) and t-strings over them;
            ``None``: schedr's fixed JSON, with a top-level ``text``.

    Raises:
        ValueError: For a bad name or URL.
        TypeError: For a body value that isn't JSON, a placeholder or a t-string over them.
    """

    def __init__(self, name: str, *, url: str, body: object = None) -> None:
        """Checks the channel and converts its body."""
        if not isinstance(name, str) or not _NAME.fullmatch(name):
            raise ValueError(
                f"WebhookChannel name {name!r}: letters, digits, `_`, `.` and `-`, starting "
                "with a letter or a digit"
            )
        if not isinstance(url, str) or not url.startswith(("https://", "http://")):
            raise ValueError(f"WebhookChannel `{name}`: url must start with https:// or http://")
        where = f"WebhookChannel `{name}`"
        self.name = name
        self.url = url
        self._model = WebhookDef(
            name, url, None if body is None else body_json(body, where=f"{where}: body")
        )

    def __repr__(self) -> str:
        """``WebhookChannel('relay')``."""
        return f"WebhookChannel({self.name!r})"

    def send(self, text: str | None = None, *, data: Json = None) -> None:
        """Sends a message to this channel, from a running task (as :func:`schedr.api.notify`).

        Args:
            text: Its text (the ``text`` placeholder).
            data: Its data, JSON (the ``data`` placeholder).
        """
        from schedr import api  # noqa: PLC0415 - api imports the task context, which imports this

        api.notify(self.name, text=text, data=data)


type _Channel = WebhookChannel | str


def notify_entries(
    value: Sequence[_Channel] | Mapping[_Channel, Sequence[Notify]] | None, dag_id: str
) -> list[NotifyEntry]:
    """``Dag(notify=...)`` as DAG JSON entries.

    Args:
        value: A list of channels (the server's default events), or a dict of channel to
            events.
        dag_id: The DAG's ID, for messages.

    Returns:
        The entries, in the order given.

    Raises:
        TypeError: For a channel that is neither a string nor a ``WebhookChannel``, a string
            given as ``notify=``, or events that aren't ``Notify`` members.
        ValueError: For a channel given twice, two DAG channels of one name, or no events.
    """
    if value is None:
        return []
    where = f"DAG `{dag_id}`: notify"
    if isinstance(value, str | WebhookChannel):
        raise TypeError(f"{where}: a list of channels or a dict, e.g. notify=[{value!r}]")
    pairs: list[tuple[object, object]]
    if isinstance(value, Mapping):
        pairs = list(value.items())
    elif isinstance(value, Sequence):
        pairs = [(channel, None) for channel in value]
    else:
        raise TypeError(f"{where}: a list of channels or a dict, got {type(value).__name__}")
    entries: list[NotifyEntry] = []
    names: dict[str, object] = {}
    for channel, events in pairs:
        model = _channel(channel, where)
        earlier = names.get(model.name)
        if earlier is not None:
            same = earlier is channel or (isinstance(channel, str) and earlier == channel)
            raise ValueError(
                f"{where}: channel `{model.name}` is given twice"
                if same
                else f"{where}: two channels are named `{model.name}`"
            )
        names[model.name] = channel
        entries.append(NotifyEntry(model, None if events is None else _events(events, where)))
    return entries


def _channel(channel: object, where: str) -> ServerChannel | WebhookDef:
    if isinstance(channel, WebhookChannel):
        return channel._model
    if isinstance(channel, str):
        if not _NAME.fullmatch(channel):
            raise ValueError(f"{where}: {channel!r} isn't a channel name")
        return ServerChannel(channel)
    raise TypeError(
        f"{where}: a channel is a server channel's name (a string) or a WebhookChannel, got "
        f"{type(channel).__name__}"
    )


def _events(events: object, where: str) -> list[str]:
    if isinstance(events, Notify):
        raise TypeError(f"{where}: events are a list, e.g. [Notify.{events.name}]")
    if not isinstance(events, Sequence) or isinstance(events, str):
        raise TypeError(f"{where}: events are a list of Notify members, e.g. [Notify.RUN_FAILED]")
    if not events:
        raise ValueError(f"{where}: no events; leave the channel out instead")
    chosen: list[str] = []
    for item in events:
        if not isinstance(item, Notify):
            raise TypeError(f"{where}: {item!r} isn't a Notify member (Notify.RUN_FAILED, ...)")
        if item.value not in chosen:
            chosen.append(item.value)
    return chosen


def body_json(value: object, *, where: str) -> dict[str, object]:
    """A message body as DAG JSON: ``{"object": ...}``, ``{"template": [...]}``, ...

    Args:
        value: JSON values (dicts with string keys, lists, strings, numbers, booleans, None),
            placeholders, and t-strings over placeholders and plain values.
        where: Where it is used, for messages.

    Returns:
        The body.

    Raises:
        TypeError: For anything else, a conversion or a format spec on a placeholder.
    """
    if isinstance(value, Placeholder):
        return {"ref": value.to_json()}
    if isinstance(value, TString):
        return {"template": _template(value, where)}
    if value is None or isinstance(value, bool | int | float | str):
        return {"value": value}
    if isinstance(value, Mapping):
        out: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{where}: keys are strings, got {key!r}")
            out[key] = body_json(item, where=f"{where}[{key!r}]")
        return {"object": out}
    if isinstance(value, list | tuple):
        return {"array": [body_json(item, where=f"{where}[{i}]") for i, item in enumerate(value)]}
    raise TypeError(
        f"{where}: {type(value).__name__} isn't JSON; a body takes JSON values, "
        "schedr.notify placeholders, and t-strings over them"
    )


def _template(tstring: TString, where: str) -> list[dict[str, object]]:
    parts: list[dict[str, object]] = []

    def add(piece: str) -> None:
        if not piece:
            return
        last = parts[-1].get("text") if parts else None
        if isinstance(last, str):
            parts[-1] = {"text": last + piece}
        else:
            parts.append({"text": piece})

    for item in tstring:
        if isinstance(item, str):
            add(item)
        elif isinstance(item.value, Placeholder):
            _refuse_formatting(item, where)
            parts.append({"ref": item.value.to_json()})
        else:
            add(_literal(item, where))
    return parts


def _refuse_formatting(item: Interpolation[object], where: str) -> None:
    if item.conversion is not None or item.format_spec:
        conversion = f"!{item.conversion}" if item.conversion else ""
        spec = f":{item.format_spec}" if item.format_spec else ""
        raise TypeError(
            f"{where}: `{{{item.expression}{conversion}{spec}}}`: a placeholder takes no "
            "conversion or format spec (the master fills it in, without Python)"
        )


def _literal(item: Interpolation[object], where: str) -> str:
    value = item.value
    if callable(getattr(value, "_schedr_reference", None)):
        raise TypeError(
            f"{where}: `{{{item.expression}}}` is a task output, a secret or `dag_ctx`; a "
            "notification body takes schedr.notify placeholders (the master sends it, and "
            "never sees a secret)"
        )
    if isinstance(value, TString):
        raise TypeError(f"{where}: `{{{item.expression}}}` is a t-string; nesting isn't supported")
    if value is None or not isinstance(value, str | int | float | bool):
        raise TypeError(
            f"{where}: `{{{item.expression}}}` is {type(value).__name__}; a t-string here takes "
            "placeholders, strings, numbers and booleans"
        )
    if item.conversion == "r":
        value = repr(value)
    elif item.conversion == "a":
        value = ascii(value)
    return format(value, item.format_spec) if item.format_spec else as_text(value)
