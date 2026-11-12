"""One run per hot device: diagnose it, wait for an operator, then maintain it or escalate.

An `anomaly` event (posted by `readings_ingest`, keyed `dev-a1@<time>`: one run per alarm, not
per device) starts a run; its data names the device. Two probes
race and `first_of` takes whichever answers first. Then the run waits for the operator's `ack`
for that device (`schedr events post ack --key dev-a1 --data '{"by": "ana"}'`), for at most
`ACK_WAIT`: acknowledged, it starts `device_maintenance` and waits for that run; not, it pages.
"""

import asyncio
import random
from datetime import timedelta

from greenhouse import pager, wait

from schedr import (
    Dag,
    Event,
    EventNamed,
    Failed,
    Notify,
    RunIf,
    TimedOut,
    TriggerDagOperator,
    dag_ctx,
    first_of,
    task,
    wait_for,
)
from schedr.types import Json

ACK_WAIT = wait(minutes=2)
"""How long an operator has to acknowledge; then the pager is told."""


async def _ping(device: str, delay: float) -> float:
    """Stands in for asking the device: a round trip of `delay` seconds."""
    del device  # what a real client would address
    await asyncio.sleep(delay)
    return round(delay * 1000)


@task
def device_of(alarm: Json) -> str:
    """The alarm's device, from its data."""
    if not (isinstance(alarm, dict) and isinstance(device := alarm.get("device"), str)):
        raise ValueError(f"an anomaly event's data names its device: {alarm!r}")
    return device


@task
async def probe_radio(device: str) -> str:
    """Pings the device three times at once, over the radio link."""
    times = await asyncio.gather(*(_ping(device, random.uniform(0.1, 0.6)) for _ in range(3)))
    return f"{device} answers over radio in {min(times):.0f} ms"


@task
async def probe_gateway(device: str) -> str:
    """Asks the zone's gateway for the device's last heartbeat."""
    ago = await _ping(device, random.uniform(0.1, 0.6))
    return f"{device} last seen by its gateway {ago:.0f} ms ago"


@task
def diagnose(finding: str, winner: str, anomaly: Json) -> str:
    """Whichever probe answered first, with the reading that raised the alarm."""
    text = f"{finding} (via {winner}); reading: {anomaly}"
    print(text)
    return text


@task
def requested(ack: Event[Json]) -> str:
    """What the operator asked for: a recalibration unless they said otherwise."""
    said = ack.data if isinstance(ack.data, dict) else {}
    print(f"acknowledged by {said.get('by', 'someone')}")
    return str(said.get("action", "recalibrate"))


# Skipped, not run with `None`, when the acknowledgement came (the edge from `ack` not taken).
@task(run_if=RunIf.ALL_SUCCEEDED)
def escalate(timed_out: TimedOut, diagnosis: str) -> None:
    """No one acknowledged in time: the pager hears of it."""
    del timed_out
    pager.send(
        f"{dag_ctx.event.key}: no acknowledgement in {ACK_WAIT}", data={"diagnosis": diagnosis}
    )


@task
def done(run_id: str) -> str:
    return f"maintenance run {run_id} succeeded"


@task
def maintenance_failed(result: Failed) -> None:
    pager.send(
        f"maintenance of {dag_ctx.event.key} ended {result.state}", data={"run": result.run_id}
    )


# `pager` among the DAG's channels: its tasks' own `pager.send` posts there too.
with Dag(
    "anomaly_response",
    description="Diagnoses a hot device, waits for an operator, then maintains it or escalates.",
    trigger=EventNamed("anomaly"),
    tags=["showcase"],
    notify={pager: [Notify.RUN_FAILED]},
):
    alarm: Json = dag_ctx.event.data  # pyright: ignore[reportAny]
    # An output: the wait below takes it as its key, so an operator acks by device.
    device = device_of(alarm)
    finding, winner = first_of(probe_radio(device), probe_gateway(device), with_winner=True)
    diagnosis = diagnose(finding, winner, alarm)

    ack = wait_for(EventNamed("ack", key=device), max_wait=ACK_WAIT)
    diagnosis >> ack
    escalate(ack.on(TimedOut), diagnosis)

    started = TriggerDagOperator(
        "start_maintenance",
        dag="device_maintenance",
        params={"device": device, "action": requested(ack.on(Event[Json]))},
        max_wait=timedelta(minutes=5),
        # Skipped when no one acknowledged: `requested` was, though `device_of` ran.
        run_if=RunIf.ALL_SUCCEEDED,
    )
    done(started.on(str))
    maintenance_failed(started.on(Failed))
