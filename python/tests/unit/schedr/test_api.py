"""``schedr.api``: requests through the agent inside a task, a fake one in ``task_context``."""

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest

from schedr import api
from schedr.runner.task import api_caller
from schedr.testing import task_context


class _Agent:
    def __init__(self, answer: dict[str, Any]) -> None:
        self.answer = answer
        self.asked: list[dict[str, Any]] = []

    def ask(self, message: dict[str, Any]) -> dict[str, Any]:
        self.asked.append(message)
        return self.answer

    def send(self, message: dict[str, Any]) -> None:
        del message


_STORED = {
    "id": "e1",
    "name": "paid",
    "key": "o-1",
    "data": {"amount": 12},
    "at": "2026-11-08T10:00:00Z",
    "posted_at": "2026-11-08T10:00:01Z",
    "posted_by": "task:shop/r1/pay",
}


def test_a_call_goes_to_the_agent_and_its_answer_comes_back() -> None:
    agent = _Agent({"type": "api_answer", "status": 201, "body": _STORED})
    client = api.Client(caller=api_caller(agent))
    event = client.post_event("paid", key="o-1", data={"amount": 12}, idempotency_key="k1")
    assert agent.asked == [
        {
            "type": "api_call",
            "method": "POST",
            "path": "/api/v1/events",
            "body": {"name": "paid", "key": "o-1", "data": {"amount": 12}},
            "idempotency_key": "k1",
        }
    ]
    assert event.id == "e1"
    assert event.posted_by == "task:shop/r1/pay"
    assert event.at == datetime(2026, 11, 8, 10, tzinfo=UTC)


def test_an_error_status_is_an_api_error_with_its_message() -> None:
    answer = {"type": "api_answer", "status": 403, "body": {"error": "not open to tasks"}}
    client = api.Client(caller=api_caller(_Agent(answer)))
    with pytest.raises(api.ApiError, match="403: not open to tasks") as raised:
        client.request("DELETE", "/api/v1/dags/x")
    assert raised.value.status == 403


def test_a_naive_at_is_refused_before_anything_is_sent() -> None:
    agent = _Agent({"type": "api_answer", "status": 201, "body": _STORED})
    client = api.Client(caller=api_caller(agent))
    with pytest.raises(ValueError, match="aware"):
        client.post_event("paid", key="o-1", at=datetime(2026, 11, 8))
    assert agent.asked == []


def test_a_list_asks_with_its_query() -> None:
    agent = _Agent({"type": "api_answer", "status": 200, "body": {"events": [_STORED]}})
    listed = api.Client(caller=api_caller(agent)).events(name="paid", limit=5)
    assert agent.asked[0]["path"] == "/api/v1/events?limit=5&name=paid"
    assert [e.key for e in listed] == ["o-1"]


def test_in_a_task_context_events_are_posted_to_its_values() -> None:
    with task_context(dag_id="shop", task_id="pay") as values:
        first = api.post_event("paid", key="o-1", data=1, idempotency_key="k")
        again = api.post_event("paid", key="o-1", data=1, idempotency_key="k")
        api.post_event("refunded", key="o-2")
        listed = api.events(name="paid")
        with pytest.raises(api.ApiError, match="403"):
            api.Client().request("GET", "/api/v1/dags")
    assert first == again
    assert [e["name"] for e in values.posted_events] == ["paid", "refunded"]
    assert values.posted_events[0]["posted_by"] == f"task:shop/{values.run_id}/pay"
    assert [e.id for e in listed] == [first.id]


def test_the_async_calls_run_in_the_task_context() -> None:
    async def post() -> api.PostedEvent:
        await api.apost_event("paid", key="o-1")
        return (await api.aevents())[0]

    with task_context() as values:
        event = asyncio.run(post())
    assert event.id == values.posted_events[0]["id"]


def test_outside_a_task_without_a_server_it_says_what_to_give(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SCHEDR_SERVER", raising=False)
    monkeypatch.delenv("SCHEDR_CTX_FD", raising=False)
    with pytest.raises(RuntimeError, match="SCHEDR_SERVER"):
        api.Client()


def test_the_fake_refuses_a_key_sent_again_with_another_event() -> None:
    with task_context():
        api.post_event("paid", key="o-1", data=1, idempotency_key="k")
        with pytest.raises(api.ApiError, match="422"):
            api.post_event("paid", key="o-2", data=2, idempotency_key="k")


def test_a_note_of_the_agent_is_told_once_on_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    answer = {"type": "api_answer", "status": 201, "body": _STORED, "note": "schedr: masked"}
    client = api.Client(caller=api_caller(_Agent(answer)))
    client.post_event("paid", key="o-1")
    client.post_event("paid", key="o-1")
    assert capsys.readouterr().err == "schedr: masked\n"


@pytest.mark.parametrize("path", [":9999/steal", ".evil.example/x", "@evil.example/x", "//evil/x"])
def test_a_path_that_could_name_another_host_is_refused(path: str) -> None:
    agent = _Agent({"type": "api_answer", "status": 200, "body": {}})
    with pytest.raises(ValueError, match="one `/`"):
        api.Client(caller=api_caller(agent)).request("GET", path)
    assert agent.asked == []
