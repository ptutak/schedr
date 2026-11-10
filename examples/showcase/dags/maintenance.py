"""Maintaining one device, started by `anomaly_response` once an operator acknowledged.

The command task talks to the device with a token from schedr's secrets (`greenhouse/devices`,
put in by `make standalone`), registers a cleanup handle before it starts changing the device,
and says how it went by its exit code: 0 fixed remotely, 3 a site visit is needed. The
outcome routes to the closing step.
"""

from dataclasses import dataclass
from datetime import timedelta

from schedr import (
    Chained,
    CommandOperator,
    Dag,
    EmptyOperator,
    Outcome,
    StepDefaults,
    dag_ctx,
    secrets,
    task,
)


@dataclass
class MaintenanceParams:
    """What a maintenance run is given; the defaults let it run on its own."""

    device: str = "dev-a1"
    action: str = "recalibrate"


class Result(Outcome):
    """What the device said, by the command's exit code."""

    FIXED = 0
    NEEDS_VISIT = 3


# A device whose name ends in 2 "needs a visit": the scenario's way to show both paths.
SCRIPT = r"""
set -eu
test -n "$DEVICE_TOKEN"
echo "$ACTION on $DEVICE with a token of ${#DEVICE_TOKEN} characters"
schedr task-ctx cleanup add Session device="$DEVICE"
sleep 2
case "$DEVICE" in *2) echo "sensor drift too large: replace the probe"; exit 3 ;; esac
echo "calibrated"
"""

CLEANUP = r"""
schedr task-ctx cleanup list Session --field device | while read -r d; do
  echo "closing the maintenance session on $d"
done
"""


@task
def visit_ticket(device: str) -> str:
    text = f"ticket: send someone to {device}"
    print(text)
    return text


@task
def fixed_report(device: str, action: str) -> str:
    return f"{device}: {action} done remotely"


with Dag(
    "device_maintenance",
    trigger=Chained(by=["anomaly_response"]),
    params=MaintenanceParams,
    defaults=StepDefaults(retries=1, retry_delay=timedelta(seconds=5)),
    tags=["showcase"],
):
    device: str = dag_ctx.params.device  # pyright: ignore[reportAny]
    action: str = dag_ctx.params.action  # pyright: ignore[reportAny]
    work = CommandOperator(
        "maintain",
        ["sh", "-c", SCRIPT],
        env={
            "DEVICE": device,
            "ACTION": action,
            "DEVICE_TOKEN": secrets["greenhouse/devices"]["token"],
        },
        outcomes=Result,
        cleanup=CLEANUP,
        cleanup_handles=["Session"],
        cleanup_timeout=timedelta(seconds=20),
        retries=0,
    )
    report = fixed_report(device, action)
    ticket = visit_ticket(device)
    work.on(Result.FIXED) >> report
    work.on(Result.NEEDS_VISIT) >> ticket
    [report, ticket] >> EmptyOperator("closed")
