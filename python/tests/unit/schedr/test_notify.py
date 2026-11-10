"""``schedr.notify``: channels, events and bodies in ``Dag(notify=...)``.

Decisions 409, 417-423.
"""

import json
from collections.abc import Callable

import pytest

from schedr import Dag, Notify, WebhookChannel, api, notify, task
from schedr.notify import body_json
from schedr.testing import task_context
from schedr.types import Json

URL = "https://relay.example.com/schedr"


@task
def work() -> None:
    """Does nothing."""


def notify_json(value: object) -> list[dict[str, object]]:
    """The DAG JSON of a DAG notifying ``value``."""
    with Dag("etl", notify=value) as dag:  # type: ignore[arg-type] - wrong types are tested
        work()
    result = dag.model.to_json().get("notify", [])
    assert isinstance(result, list)
    return result


def test_a_t_string_becomes_text_and_references_plain_values_baked_in() -> None:
    team = "data"
    body = body_json(
        {"title": t"[{team}] {notify.dag} {notify.data['rows']} rows", "n": 3},
        where="body",
    )
    assert body == {
        "object": {
            "title": {
                "template": [
                    {"text": "[data] "},
                    {"ref": {"name": "dag"}},
                    {"text": " "},
                    {"ref": {"name": "data", "path": ["rows"]}},
                    {"text": " rows"},
                ]
            },
            "n": {"value": 3},
        }
    }


def test_a_bare_placeholder_keeps_its_value_and_lists_are_arrays() -> None:
    body = body_json([notify.failed_tasks, notify.data["items"][0], None], where="body")
    assert body == {
        "array": [
            {"ref": {"name": "failed_tasks"}},
            {"ref": {"name": "data", "path": ["items", 0]}},
            {"value": None},
        ]
    }


@pytest.mark.parametrize(
    ("body", "says"),
    [
        (t"{notify.dag!r}", "takes no conversion or format spec"),
        (t"{notify.time:%Y}", "takes no conversion or format spec"),
        ({1: "x"}, "keys are strings"),
        ({"x": object()}, "isn't JSON"),
        (t"{[1]}", "is list"),
        (t"{t'x'}", "nesting isn't supported"),
    ],
)
def test_what_a_body_can_t_hold_is_refused_when_the_file_loads(body: object, says: str) -> None:
    with pytest.raises(TypeError, match=says):
        body_json(body, where="body")


def test_a_placeholder_has_no_value_in_the_dag_file() -> None:
    with pytest.raises(TypeError, match="has no value in the DAG file"):
        f"{notify.dag}"
    with pytest.raises(TypeError, match="has no value in the DAG file"):
        bool(notify.data)
    with pytest.raises(TypeError, match="a key is a string or an int"):
        notify.data[1.5]  # type: ignore[index] - a wrong key is tested


def test_a_list_names_channels_for_the_default_events_and_a_dict_chooses_them() -> None:
    relay = WebhookChannel("relay", url=URL)
    assert notify_json([relay, "ops-slack"]) == [
        {"channel": {"kind": "webhook", "name": "relay", "url": URL}},
        {"channel": {"kind": "server", "name": "ops-slack"}},
    ]
    assert notify_json({relay: [Notify.TASK_RETRY, Notify.RUN_FAILED, Notify.TASK_RETRY]}) == [
        {
            "channel": {"kind": "webhook", "name": "relay", "url": URL},
            "events": ["TASK_RETRY", "RUN_FAILED"],
        }
    ]
    assert notify_json(None) == []


def test_the_dag_json_with_notify_reads_as_the_schema_says(
    validate: Callable[[str, object], None],
) -> None:
    relay = WebhookChannel(
        "relay",
        url=URL,
        body={"text": t"{notify.dag} failed", "rows": notify.data["rows"], "list": [1, None]},
    )
    with Dag("etl", notify={relay: [Notify.RUN_FAILED], "ops": [Notify.RUN_RECOVERED]}) as dag:
        work()
    validate("Dag", json.loads(json.dumps(dag.model.to_json())))
    entries = notify_json({relay: [Notify.RUN_FAILED]})
    assert entries[0]["channel"] == {
        "kind": "webhook",
        "name": "relay",
        "url": URL,
        "body": {
            "object": {
                "text": {"template": [{"ref": {"name": "dag"}}, {"text": " failed"}]},
                "rows": {"ref": {"name": "data", "path": ["rows"]}},
                "list": {"array": [{"value": 1}, {"value": None}]},
            }
        },
    }


@pytest.mark.parametrize(
    ("value", "error", "says"),
    [
        ("ops", TypeError, "a list of channels or a dict"),
        (["ops", "ops"], ValueError, "channel `ops` is given twice"),
        (
            [WebhookChannel("x", url=URL), WebhookChannel("x", url=URL)],
            ValueError,
            "two channels are named `x`",
        ),
        ([3], TypeError, "a channel is a server channel's name"),
        ({"ops": Notify.RUN_FAILED}, TypeError, "events are a list"),
        ({"ops": ["RUN_FAILED"]}, TypeError, "isn't a Notify member"),
        ({"ops": []}, ValueError, "no events"),
        (["no spaces"], ValueError, "isn't a channel name"),
    ],
)
def test_a_wrong_notify_is_refused(value: object, error: type[Exception], says: str) -> None:
    with pytest.raises(error, match=says):
        notify_json(value)


def test_a_webhook_channel_checks_its_name_and_url() -> None:
    with pytest.raises(ValueError, match="letters, digits"):
        WebhookChannel("-x", url=URL)
    with pytest.raises(ValueError, match="must start with https:// or http://"):
        WebhookChannel("x", url="ftp://relay")
    assert repr(WebhookChannel("x", url=URL)) == "WebhookChannel('x')"


def test_send_and_api_notify_record_the_message_in_a_task_context() -> None:
    relay = WebhookChannel("relay", url=URL)
    data: Json = {"rows": 12}
    with task_context(dag_id="etl", task_id="load") as values:
        relay.send("loaded", data=data)
        api.notify("ops-slack", text="done")
    assert values.notifications == [
        {"channel": "relay", "text": "loaded", "data": {"rows": 12}},
        {"channel": "ops-slack", "text": "done", "data": None},
    ]
