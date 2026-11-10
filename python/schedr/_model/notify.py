"""Notifications in DAG JSON: the channels a DAG notifies, and of which events.

Mirrors ``schedr-model`` (``notify.rs``). A body arrives here already as its JSON
(``{"object": ...}``, ``{"template": [...]}``, ...), built by ``schedr.notify``.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ServerChannel:
    """A channel of the server's configuration, named by a string.

    Attributes:
        name: Its name in ``[notifications.channels]``.
    """

    name: str

    def to_json(self) -> dict[str, object]:
        """The channel as DAG JSON.

        Returns:
            ``{"kind": "server", "name": ...}``.
        """
        return {"kind": "server", "name": self.name}


@dataclass(frozen=True, slots=True)
class WebhookDef:
    """A webhook a DAG defines.

    Attributes:
        name: Its name, unique among the DAG's channels.
        url: Where it posts.
        body: Its body as DAG JSON; ``None``: schedr's fixed JSON.
    """

    name: str
    url: str
    body: Mapping[str, object] | None = None

    def to_json(self) -> dict[str, object]:
        """The channel as DAG JSON.

        Returns:
            ``{"kind": "webhook", "name": ..., "url": ...}``, with ``"body"`` when set.
        """
        result: dict[str, object] = {"kind": "webhook", "name": self.name, "url": self.url}
        if self.body is not None:
            result["body"] = self.body
        return result


@dataclass(frozen=True, slots=True)
class NotifyEntry:
    """One channel a DAG notifies.

    Attributes:
        channel: The channel.
        events: The events' names (``RUN_FAILED``); ``None``: the server's default events.
    """

    channel: ServerChannel | WebhookDef
    events: Sequence[str] | None = None

    def to_json(self) -> dict[str, object]:
        """The entry as DAG JSON.

        Returns:
            ``{"channel": ...}``, with ``"events"`` when chosen.
        """
        result: dict[str, object] = {"channel": self.channel.to_json()}
        if self.events is not None:
            result["events"] = list(self.events)
        return result
