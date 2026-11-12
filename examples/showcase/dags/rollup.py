"""Every five minutes, each zone's readings summed up; a warm zone gets its vents opened.

A run covers `since` to `planned_for`. Its poke sensor waits until the window has readings at
all, for at most a minute: an empty window ends `TimedOut` and the run records that, without
failing. Each zone is a group of its own, with its own retries. A branch then picks what follows
from the summaries. `schedr runs backfill greenhouse_rollup --from ... --to ...` fills windows
already past, oldest first.
"""

from datetime import datetime, timedelta
from typing import cast

from greenhouse import FAST, ZONES, ZoneSummary, readings_db, wait

from schedr import (
    Dag,
    Every,
    RunIf,
    StepDefaults,
    Task,
    TimedOut,
    branch,
    dag_ctx,
    group,
    sensor,
    task,
)
from schedr.sensors import NOT_YET, NotYet

WARM_CELSIUS = 28.0


def _count(since: datetime, until: datetime) -> int:
    with readings_db() as db:
        row = cast(
            "tuple[int]",
            db.execute(
                "select count(*) from readings where at >= ? and at < ?",
                (since.isoformat(), until.isoformat()),
            ).fetchone(),
        )
    return row[0]


@sensor.poke(
    every=timedelta(seconds=1 if FAST else 10), max_wait=wait(minutes=1), connections=[readings_db]
)
async def has_readings(since: datetime, until: datetime) -> int | NotYet:
    """The window's readings once there are any; a late device still counts."""
    found = _count(since, until)
    return found if found else NOT_YET


@task(connections=[readings_db])
def summarize(zone: str, since: datetime, until: datetime) -> ZoneSummary:
    with readings_db() as db:
        count, mean, top = cast(
            "tuple[int, float | None, float | None]",
            db.execute(
                """select count(*), avg(celsius), max(celsius) from readings
                where zone = ? and at >= ? and at < ?""",
                (zone, since.isoformat(), until.isoformat()),
            ).fetchone(),
        )
    return ZoneSummary(
        zone=zone, readings=count, mean_celsius=mean, max_celsius=top, since=since, until=until
    )


# Both skipped for a window without readings, where the zones' summaries were.
@task(run_if=RunIf.ALL_SUCCEEDED)
def ventilate(summaries: list[ZoneSummary]) -> list[str]:
    opened = [s.zone for s in summaries if s.mean_celsius and s.mean_celsius > WARM_CELSIUS]
    print(f"opening the vents of {', '.join(opened)}")
    return opened


@task
def all_normal() -> None:
    print("every zone within range")


@task
def quiet(timed_out: TimedOut, since: datetime, until: datetime) -> str:
    del timed_out
    return f"no readings from {since:%H:%M} to {until:%H:%M}"


with Dag(
    "greenhouse_rollup",
    description="Sums up each zone's readings every 5 minutes, and opens a warm zone's vents.",
    trigger=Every(timedelta(minutes=5)),
    tags=["showcase"],
):
    since: datetime = dag_ctx.run.since
    until: datetime = dag_ctx.run.planned_for
    readings = has_readings(since, until)
    quiet(readings.on(TimedOut), since, until)

    summaries: list[ZoneSummary] = []
    for zone in ZONES:
        # The group's tasks are `zone_a.summarize`, ...: retried on their own.
        with group(
            zone, defaults=StepDefaults(retries=2, retry_delay=timedelta(seconds=10))
        ) as per_zone:
            summaries.append(summarize(zone, since, until))
        readings.on(int) >> per_zone

    vents = ventilate(summaries)
    normal = all_normal()

    @branch
    def act(summaries: list[ZoneSummary]) -> list[Task]:
        """Decides at run time, from the summaries, which path runs."""
        warm = any(s.mean_celsius and s.mean_celsius > WARM_CELSIUS for s in summaries)
        return [vents] if warm else [normal]

    act.override(run_if=RunIf.ALL_SUCCEEDED)(summaries) >> [vents, normal]
