"""``delay`` and ``until``: plain wait steps (M4.11g)."""

import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from schedr import Dag, TimedOut, dag_ctx, task
from schedr._model import registry
from schedr.runner.waiting import waited
from schedr.sensors import _delay, _until, delay, until
from schedr.testing import task_context


@task
def report(at: datetime) -> str:
    """Reads the moment."""
    return at.isoformat()


def _tasks() -> dict[str, dict[str, Any]]:
    (dag,) = registry.take_defined()
    return {t["id"]: t for t in dag.to_json()["tasks"]}


def test_each_is_a_wait_step_numbered_from_the_second_on() -> None:
    with Dag("waits"):
        first = delay(timedelta(minutes=5))
        report(first.on(datetime))
        delay(timedelta(minutes=1))
        until(timedelta(hours=2))
        until(datetime(2026, 11, 9, 9, tzinfo=UTC), task_id="at_nine")
    tasks = _tasks()
    assert sorted(tasks) == ["at_nine", "delay", "delay_2", "report", "until"]
    assert {tasks[t]["kind"] for t in ("delay", "delay_2", "until", "at_nine")} == {"sensor"}
    assert tasks["delay"]["arguments"] == {"wait": {"literal": {"$timedelta": 300_000_000}}}
    assert [o["name"] for o in tasks["delay"]["outcomes"]] == ["SUCCESS", "TimedOut"]


def test_a_delay_waits_a_minute_past_its_wait_unless_told_and_until_as_any_step() -> None:
    with Dag("waits"):
        delay(timedelta(hours=30))
        delay(timedelta(hours=30), max_wait=timedelta(hours=1), task_id="cut")
        until(timedelta(hours=30))
    tasks = _tasks()
    assert tasks["delay"]["max_wait_ms"] == (30 * 60 + 1) * 60_000
    assert tasks["cut"]["max_wait_ms"] == 3_600_000
    assert tasks["until"].get("max_wait_ms") is None


@dataclass(frozen=True)
class Pause:
    """How long."""

    pause: timedelta


def test_a_run_parameter_can_say_how_long() -> None:
    with Dag("waits", params=Pause):
        delay(dag_ctx.params.pause)
    tasks = _tasks()
    assert tasks["delay"]["arguments"]["wait"] == {"ref": {"kind": "param", "path": ["pause"]}}
    assert tasks["delay"].get("max_wait_ms") is None


@pytest.mark.parametrize(
    ("make", "match"),
    [
        (lambda: delay(timedelta(seconds=-1)), "negative"),
        (lambda: until(datetime(2026, 11, 9, 9)), "time zone"),
    ],
)
def test_what_can_t_be_waited_for_is_refused(make: Any, match: str) -> None:
    with pytest.raises(TypeError, match=match), Dag("waits"):
        make()


def test_a_delay_waits_its_time_on_its_first_try() -> None:
    with task_context():
        started = time.monotonic()
        ended = asyncio.run(_delay(timedelta(milliseconds=50)))
        took = time.monotonic() - started
    assert took >= 0.05
    assert abs(datetime.now(UTC) - ended) < timedelta(seconds=1)


def test_a_delay_tried_again_waits_only_what_is_left() -> None:
    first = datetime.now(UTC) - timedelta(minutes=10)
    with task_context(first_started=first):
        started = time.monotonic()
        ended = asyncio.run(_delay(timedelta(minutes=10, milliseconds=300)))
        took = time.monotonic() - started
    assert 0.25 <= took < 5, "only what is left"
    assert ended >= first + timedelta(minutes=10, milliseconds=300), "when it ended"


def test_until_counts_from_the_run_s_moment_and_one_past_ends_at_once() -> None:
    planned = datetime.now(UTC) - timedelta(hours=1)
    with task_context(planned_for=planned):
        started = time.monotonic()
        ended = asyncio.run(_until(timedelta(minutes=30)))
        assert ended >= datetime.now(UTC) - timedelta(seconds=5), "when it ended, not its moment"
        assert time.monotonic() - started < 5
        soon = datetime.now(UTC) + timedelta(milliseconds=50)
        assert asyncio.run(_until(soon)) >= soon
        assert datetime.now(UTC) >= soon


def test_a_negative_wait_from_a_run_parameter_fails_when_it_runs() -> None:
    # Codex's review: only a literal was refused.
    with task_context(), pytest.raises(ValueError, match="negative"):
        asyncio.run(_delay(timedelta(seconds=-1)))


@pytest.mark.parametrize(
    ("step", "argument"),
    [(_delay, timedelta(days=3_000_000)), (_until, timedelta(days=3_000_000))],
)
def test_a_wait_past_what_a_datetime_holds_ends_at_its_max_wait(step: Any, argument: Any) -> None:
    # Grok's review: it raised OverflowError, failing the attempt.
    async def run() -> object:
        name = "wait" if step is _delay else "at"
        return await waited(step, {name: argument}, {"max_wait_ms": 20})

    with task_context():
        assert asyncio.run(run()) == TimedOut()


def test_until_one_before_year_one_ends_at_once() -> None:
    with task_context():
        assert asyncio.run(_until(timedelta(days=-3_000_000))) <= datetime.now(UTC)


def test_past_its_max_wait_it_times_out() -> None:
    async def run() -> object:
        task = {"max_wait_ms": 20}
        return await waited(_delay, {"wait": timedelta(hours=1)}, task)

    with task_context():
        assert asyncio.run(run()) == TimedOut()
