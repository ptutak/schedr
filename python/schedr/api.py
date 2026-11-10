"""schedr's REST API from Python (M4.11h): events posted and listed.

Inside a running task or trigger, nothing is configured: requests go through the task's agent,
which has its master make them as the task (``task:<dag>/<run>/<task>`` is who posted). A
command task's Python script does the same over its context channel. Elsewhere (a script of
one's own), the client goes over HTTP (the ``http`` extra), to ``server`` or
``SCHEDR_SERVER``, as the ``schedr`` CLI does::

    from schedr import api

    api.post_event("payment-completed", key=order, data={"amount": 12})
    for event in api.events(name="payment-completed", limit=10):
        ...

Tasks may call only the routes open to them (posting and listing events, for now); a route
outside them is refused (403). The async functions (``apost_event``, ``aevents``) run the call
in a thread, so a trigger's loop goes on meanwhile.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

from schedr.context import ApiCaller, current_task

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "ApiError",
    "Client",
    "PostedEvent",
    "aevents",
    "apost_event",
    "events",
    "post_event",
]

_EVENTS = "/api/v1/events"
_NOTIFICATIONS = "/api/v1/notifications"
_CTX_FD_ENV = "SCHEDR_CTX_FD"
_TIMEOUT = 100.0
"""Seconds an HTTP request may take: as long as a task's call through its agent."""


class ApiError(Exception):
    """The API answered an error status.

    Attributes:
        status: The HTTP status (503: the master couldn't be reached in time; the call may
            still be made, so send it again with the same ``idempotency_key``).
        message: What the API said.
        body: Its whole JSON answer.
    """

    def __init__(self, status: int, body: object) -> None:
        """An error from the API's answer.

        Args:
            status: The HTTP status.
            body: The JSON answer.
        """
        error = body.get("error") if isinstance(body, dict) else None
        self.status = status
        self.message = str(error) if error is not None else str(body)
        self.body = body
        super().__init__(f"schedr's API answered {status}: {self.message}")


@dataclass(frozen=True, slots=True)
class PostedEvent:
    """An event posted through the API (``EventNamed``).

    Attributes:
        id: Its ID.
        name: Its name.
        key: Its key.
        data: Its data, JSON (masked where it holds a secret's value).
        at: When it happened.
        posted_at: When it was posted.
        posted_by: Who posted it: a user, or ``task:<dag>/<run>/<task>``.
    """

    id: str
    name: str
    key: str
    data: object
    at: datetime
    posted_at: datetime
    posted_by: str

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> PostedEvent:
        """The event in the API's answer.

        Args:
            value: Its JSON.

        Returns:
            The event.
        """
        return cls(
            id=str(value["id"]),
            name=str(value["name"]),
            key=str(value["key"]),
            data=value.get("data"),
            at=datetime.fromisoformat(str(value["at"])),
            posted_at=datetime.fromisoformat(str(value["posted_at"])),
            posted_by=str(value["posted_by"]),
        )


def _http_caller(
    server: str, token: str | None, user: str | None, password: str | None
) -> ApiCaller:
    try:
        import httpx  # noqa: PLC0415  # an extra, imported when a client is made
    except ImportError:
        raise ImportError(
            "schedr.api outside a task goes over HTTP, with httpx: pip install 'schedr[http]'"
        ) from None
    headers = {"Authorization": f"Bearer {token}"} if token is not None else {}
    auth = (user, password or "") if user is not None else None

    def call(method: str, path: str, body: object, key: str | None) -> tuple[int, object]:
        sent = dict(headers)
        if key is not None:
            sent["Idempotency-Key"] = key
        response = httpx.request(
            method,
            server.rstrip("/") + path,
            json=body,
            headers=sent,
            auth=auth,
            timeout=_TIMEOUT,
        )
        try:
            answer: object = response.json()
        except ValueError:
            answer = {"error": response.text}
        return response.status_code, answer

    return call


def _script_caller() -> ApiCaller:
    from schedr import script  # noqa: PLC0415  # its import sets up `dag_ctx` for scripts

    return script.call_api


class Client:
    """schedr's REST API: through the task's agent inside a task, over HTTP elsewhere."""

    def __init__(
        self,
        server: str | None = None,
        *,
        token: str | None = None,
        user: str | None = None,
        password: str | None = None,
        caller: ApiCaller | None = None,
    ) -> None:
        """A client.

        With no ``server``, inside a running task or trigger (or a command task's script) it
        goes through the task's agent; elsewhere over HTTP to ``SCHEDR_SERVER``, with
        ``SCHEDR_TOKEN``, or ``SCHEDR_USER`` and ``SCHEDR_PASSWORD``, as the CLI does.

        Args:
            server: The webserver's URL, for HTTP.
            token: A bearer token, for HTTP.
            user: A user for HTTP Basic, with ``password``.
            password: Its password.
            caller: Makes the requests instead (a test's double).

        Raises:
            RuntimeError: Outside a task, with no server given or in ``SCHEDR_SERVER``.
        """
        if caller is not None:
            self._call = caller
            return
        task = current_task()
        if server is None and task is not None and task.api is not None:
            self._call = task.api
        elif server is None and os.environ.get(_CTX_FD_ENV):
            self._call = _script_caller()
        else:
            url = server or os.environ.get("SCHEDR_SERVER")
            if not url:
                raise RuntimeError(
                    "schedr.api outside a task: give Client a server, or set SCHEDR_SERVER"
                )
            env = os.environ.get
            self._call = _http_caller(
                url,
                token if token is not None else env("SCHEDR_TOKEN"),
                user if user is not None else env("SCHEDR_USER"),
                password if password is not None else env("SCHEDR_PASSWORD"),
            )

    def request(
        self,
        method: str,
        path: str,
        *,
        json: object = None,
        idempotency_key: str | None = None,
    ) -> object:
        """A request to the API.

        Args:
            method: ``GET``, ``POST``, ...
            path: The path and its query, e.g. ``/api/v1/events?name=paid``.
            json: The body; ``None`` for none.
            idempotency_key: Sent again with the same key, the request is the one made first.

        Returns:
            The JSON answer.

        Raises:
            ApiError: For an error status.
            ValueError: For a path not starting with ``/``: joined to the server's URL, it could
                name another host or port, and the credentials would go there.
        """
        if not path.startswith("/") or path.startswith("//"):
            raise ValueError(f"a path starting with one `/` is needed, got {path!r}")
        status, body = self._call(method.upper(), path, json, idempotency_key)
        if status >= 400:  # noqa: PLR2004 - HTTP's error statuses
            raise ApiError(status, body)
        return body

    def post_event(
        self,
        name: str,
        *,
        key: str,
        data: object = None,
        at: datetime | None = None,
        idempotency_key: str | None = None,
    ) -> PostedEvent:
        """Posts an event.

        A trigger on its name starts a run per key; a step waiting for it goes on.

        Args:
            name: Its name, written as a DAG ID is.
            key: Its key.
            data: Its data, JSON.
            at: When it happened (aware); now by default.
            idempotency_key: Sent again with the same key (a retried task), it is the event
                stored the first time.

        Returns:
            The event as stored.

        Raises:
            ValueError: For a naive ``at``.
        """
        body: dict[str, object] = {"name": name, "key": key, "data": data}
        if at is not None:
            if at.tzinfo is None or at.utcoffset() is None:
                raise ValueError("`at` must be aware (it has no time zone)")
            body["at"] = at.isoformat()
        answer = self.request("POST", _EVENTS, json=body, idempotency_key=idempotency_key)
        return PostedEvent.from_json(_object(answer))

    def notify(self, channel: str, *, text: str | None = None, data: object = None) -> None:
        """Sends a message to a notification channel, from a running task (decision 421).

        The channel is one of the task's DAG (``Dag(notify=...)``) or a server channel; the
        master fills its body (``text`` and ``data`` among the values) and delivers it later:
        this returns once it is queued.

        Args:
            channel: The channel's name.
            text: Its text (the ``text`` placeholder; schedr's summary otherwise).
            data: Its data, JSON (the ``data`` placeholder).

        Raises:
            ApiError: For a channel the DAG doesn't have, a field of ``data`` the channel's
                body uses and ``data`` lacks, or more than ``[notifications] max_per_attempt``
                sends (429). Outside a task, the server has no such route (404).
        """
        self.request("POST", _NOTIFICATIONS, json={"channel": channel, "text": text, "data": data})

    def events(self, *, name: str | None = None, limit: int = 50) -> list[PostedEvent]:
        """The events posted, newest first.

        Args:
            name: Only those of this name.
            limit: How many at most (500 at most).

        Returns:
            The events.
        """
        query: dict[str, object] = {"limit": limit}
        if name is not None:
            query["name"] = name
        answer = _object(self.request("GET", f"{_EVENTS}?{urlencode(query)}"))
        listed = answer.get("events", [])
        return [PostedEvent.from_json(e) for e in listed] if isinstance(listed, list) else []


def _object(answer: object) -> Mapping[str, Any]:
    if not isinstance(answer, dict):
        raise ApiError(502, {"error": f"the API's answer isn't an object: {answer!r}"})
    return answer


def post_event(
    name: str,
    *,
    key: str,
    data: object = None,
    at: datetime | None = None,
    idempotency_key: str | None = None,
) -> PostedEvent:
    """Posts an event as :meth:`Client.post_event`, with a client made here.

    Args:
        name: Its name.
        key: Its key.
        data: Its data, JSON.
        at: When it happened (aware); now by default.
        idempotency_key: Sent again with the same key, it is the event stored the first time.

    Returns:
        The event as stored.
    """
    return Client().post_event(name, key=key, data=data, at=at, idempotency_key=idempotency_key)


def notify(channel: str, *, text: str | None = None, data: object = None) -> None:
    """Sends a message to a notification channel as :meth:`Client.notify`, from a task.

    Args:
        channel: The channel's name.
        text: Its text.
        data: Its data, JSON.
    """
    Client().notify(channel, text=text, data=data)


async def anotify(channel: str, *, text: str | None = None, data: object = None) -> None:
    """:func:`notify` from a coroutine, run in a thread.

    Args:
        channel: The channel's name.
        text: Its text.
        data: Its data, JSON.
    """
    client = Client()
    await asyncio.to_thread(client.notify, channel, text=text, data=data)


def events(*, name: str | None = None, limit: int = 50) -> list[PostedEvent]:
    """The events posted, newest first, as :meth:`Client.events`.

    Args:
        name: Only those of this name.
        limit: How many at most.

    Returns:
        The events.
    """
    return Client().events(name=name, limit=limit)


async def apost_event(
    name: str,
    *,
    key: str,
    data: object = None,
    at: datetime | None = None,
    idempotency_key: str | None = None,
) -> PostedEvent:
    """:func:`post_event` from a coroutine: run in a thread, the loop goes on meanwhile.

    Args:
        name: Its name.
        key: Its key.
        data: Its data, JSON.
        at: When it happened (aware); now by default.
        idempotency_key: Sent again with the same key, it is the event stored the first time.

    Returns:
        The event as stored.
    """
    client = Client()
    return await asyncio.to_thread(
        client.post_event, name, key=key, data=data, at=at, idempotency_key=idempotency_key
    )


async def aevents(*, name: str | None = None, limit: int = 50) -> list[PostedEvent]:
    """:func:`events` from a coroutine, run in a thread.

    Args:
        name: Only those of this name.
        limit: How many at most.

    Returns:
        The events.
    """
    client = Client()
    return await asyncio.to_thread(client.events, name=name, limit=limit)
