"""A load to watch on the Metrics page: every minute a burst of short tasks, CPU and memory.

Unpause it on `make standalone-separate-ui`'s `main` (the separate roles), then open the Metrics
page on :8082: `schedr dags unpause metrics_load`. Each run maps 40 short tasks (throughput,
start latency), 8 that keep a CPU busy for 2-6 s, 8 that hold 40-140 MiB for 3-6 s against a
256 MiB budget of their own (each task's memory against its limit), two commands and a wait step.
"""

import time
from datetime import timedelta

from schedr import CommandOperator, Dag, Every, MiB, task
from schedr.sensors import delay


@task
def items() -> list[int]:
    return list(range(40))


@task
def few() -> list[int]:
    return list(range(8))


@task
def tick(n: int) -> int:
    """A short task: many of them make the throughput and start-latency lines."""
    return n * n


@task
def crunch(n: int) -> int:
    """Keeps one CPU busy for 2-6 s."""
    deadline = time.monotonic() + 2 + n % 5
    spins = 0
    while time.monotonic() < deadline:
        spins += sum(i * i for i in range(1000)) % 7
    return spins


@task(memory=256 * MiB)
def hold(n: int) -> int:
    """Holds 40-140 MiB for 3-6 s, every page touched so it counts as resident."""
    size = (40 + 20 * (n % 6)) * MiB
    block = bytearray(size)
    for at in range(0, size, 4096):
        block[at] = 1
    time.sleep(3 + n % 4)
    return len(block) // MiB


@task
def total(squares: list[int], spins: list[int], held: list[int]) -> str:
    return f"{len(squares)} ticks, {sum(spins)} spins, {sum(held)} MiB held"


with Dag(
    "metrics_load",
    description="Every minute: 40 short tasks, 8 CPU-bound, 8 holding memory, two commands.",
    trigger=Every(timedelta(minutes=1), start="2026-01-01"),
    max_active_runs=2,
):
    squares = tick.expand(n=items())
    eight = few()
    spins = crunch.expand(n=eight)
    held = hold.expand(n=eight)
    total(squares, spins, held)
    CommandOperator(
        "compress",
        command=["sh", "-c", "head -c 100000000 /dev/urandom | gzip -1 > /dev/null"],
    )
    CommandOperator("pause", command=["sleep", "5"])
    delay(timedelta(seconds=10))
