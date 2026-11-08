"""Wait steps, run for real: ``schedr dags test``, a process per task (M4.11a1)."""

from pathlib import Path

from schedr.testing import run_dag

HEADER = """
import asyncio
from datetime import timedelta
from pathlib import Path
from schedr import Dag, NOT_YET, NotYet, TimedOut, sensor, task
from schedr.tasks import on

STATE = Path(STATE_DIR)
MARK = STATE / "mark"
"""


def test_a_poke_is_called_until_it_has_its_value(tmp_path: Path) -> None:
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "calls").write_text("")
    result = run_dag(
        _write(
            tmp_path,
            """
CALLS = STATE / "calls"

@sensor.poke(every=timedelta(milliseconds=50), max_wait=timedelta(seconds=30))
async def ready() -> str | NotYet:
    with CALLS.open("a") as f:
        f.write("x")
    return "here" if len(CALLS.read_text()) == 3 else NOT_YET

@task
def load(value: str) -> str:
    return value.upper()

with Dag("d"):
    load(ready().on(str))
""",
        ),
        "d",
    )
    assert result.state == "success", result
    assert result.tasks["ready"].output == "here"
    assert result.tasks["ready"].outcome == "SUCCESS"
    assert result.tasks["load"].output == "HERE"
    assert (tmp_path / "state" / "calls").read_text() == "xxx"


def test_a_sensor_that_runs_out_of_time_ends_timed_out_and_its_edges_route(tmp_path: Path) -> None:
    result = run_dag(
        _write(
            tmp_path,
            """
@sensor(max_wait=timedelta(milliseconds=300))
async def never(n: int) -> int:
    try:
        await asyncio.sleep(60)
    finally:
        await asyncio.sleep(0)
        MARK.write_text("cleaned")
    return n

@task
def load(value: int) -> None:
    raise AssertionError("not on a timeout")

@task
def alert() -> str:
    return "alerted"

@task
def after() -> None:
    raise AssertionError("not on a timeout")

@task
def either() -> str:
    return "ran"

@task
def expects(value: int | TimedOut) -> str:
    return type(value).__name__

with Dag("d"):
    waited = never(1)
    load(waited.on(int))
    waited.on(TimedOut) >> alert()
    waited >> after()
    waited.on(int, TimedOut) >> either()
    expects(waited)
""",
        ),
        "d",
    )
    assert result.state == "success", result
    assert result.tasks["never"].state == "success"
    assert result.tasks["never"].outcome == "TimedOut"
    assert result.tasks["load"].state == "skipped"
    assert result.tasks["alert"].output == "alerted"
    assert result.tasks["after"].state == "skipped"
    assert result.tasks["either"].output == "ran"
    assert result.tasks["expects"].output == "TimedOut"
    # Its `finally` ran, on the cancel.
    assert (tmp_path / "state" / "mark").read_text() == "cleaned"


def test_a_poke_that_never_has_it_times_out(tmp_path: Path) -> None:
    result = run_dag(
        _write(
            tmp_path,
            """
@sensor.poke(every=timedelta(milliseconds=50), max_wait=timedelta(milliseconds=300))
async def never() -> str | NotYet:
    return NOT_YET

with Dag("d"):
    never()
""",
        ),
        "d",
    )
    assert result.state == "success", result
    assert result.tasks["never"].outcome == "TimedOut"


def test_a_poke_call_longer_than_every_is_a_failure(tmp_path: Path) -> None:
    result = run_dag(
        _write(
            tmp_path,
            """
@sensor.poke(every=timedelta(milliseconds=100), max_wait=timedelta(seconds=30), retries=0)
async def slow() -> str | NotYet:
    await asyncio.sleep(60)
    return "never"

with Dag("d"):
    slow()
""",
        ),
        "d",
    )
    assert result.state == "failed", result
    attempt = result.tasks["slow"].attempts[-1]
    assert "took longer than `every`" in str(attempt.error), attempt


def test_a_poke_that_raises_fails_with_the_usual_retries(tmp_path: Path) -> None:
    result = run_dag(
        _write(
            tmp_path,
            """
@sensor.poke(every=timedelta(milliseconds=50), retries=1, retry_delay=timedelta(0))
async def flaky() -> str | NotYet:
    if MARK.exists():
        return "second try"
    MARK.write_text("x")
    raise RuntimeError("flaky source")

with Dag("d"):
    flaky()
""",
        ),
        "d",
    )
    assert result.state == "success", result
    assert result.tasks["flaky"].output == "second try"
    assert len(result.tasks["flaky"].attempts) == 2


def test_an_outcome_union_keeps_its_members_and_gains_timed_out(tmp_path: Path) -> None:
    result = run_dag(
        _write(
            tmp_path,
            """
from schedr import Data

class Found(Data, frozen=True):
    name: str

class Gone(Data, frozen=True):
    pass

@sensor(max_wait=timedelta(seconds=30))
async def look() -> Found | Gone:
    return Found("f")

@task
def use(found: Found) -> str:
    return found.name

@task
def missing() -> None:
    pass

@task
def late() -> None:
    pass

with Dag("d"):
    looked = look()
    use(looked.on(Found))
    looked.on(Gone) >> missing()
    looked.on(TimedOut) >> late()
""",
        ),
        "d",
    )
    assert result.state == "success", result
    assert result.tasks["look"].outcome == "Found"
    assert result.tasks["use"].output == "f"
    assert result.tasks["missing"].state == "skipped"
    assert result.tasks["late"].state == "skipped"


def _write(tmp_path: Path, source: str) -> Path:
    # Outside the root: a task's code is a read-only copy of it.
    (tmp_path / "state").mkdir(exist_ok=True)
    root = tmp_path / "root"
    root.mkdir()
    path = root / "dag.py"
    path.write_text(HEADER.replace("STATE_DIR", repr(str(tmp_path / "state"))) + source)
    return path


def test_a_task_with_another_upstream_still_runs_after_a_timeout_unless_it_asks_not_to(
    tmp_path: Path,
) -> None:
    result = run_dag(
        _write(
            tmp_path,
            """
from schedr import RunIf

@sensor(max_wait=timedelta(milliseconds=100))
async def never() -> int:
    await asyncio.sleep(60)
    return 1

@task
def other() -> None:
    pass

@task
def either() -> None:
    pass

@task(run_if=RunIf.ALL_SUCCEEDED)
def both() -> None:
    pass

@task
def takes(value: int | None) -> str:
    return repr(value)

with Dag("d"):
    waited = never()
    done = other()
    any_of = either()
    all_of = both()
    waited >> any_of
    done >> any_of
    waited >> all_of
    done >> all_of
    done >> takes(waited.on(int))
""",
        ),
        "d",
    )
    assert result.state == "success", result
    assert result.tasks["never"].outcome == "TimedOut"
    # As for any edge not taken: run on the other upstream, unless it asks for all of them.
    assert result.tasks["either"].state == "success"
    assert result.tasks["both"].state == "skipped"
    assert result.tasks["takes"].output == "None"


def test_a_task_expecting_either_gets_the_value_decoded_when_there_is_one(tmp_path: Path) -> None:
    result = run_dag(
        _write(
            tmp_path,
            """
from schedr import Data

class Found(Data, frozen=True):
    name: str = "default"

@sensor
async def number() -> int:
    return 7

@sensor
async def found() -> Found:
    return Found("f")

@sensor
async def nothing() -> None:
    return None

@task
def of_number(value: int | TimedOut) -> str:
    return repr(value)

@task
def of_found(value: Found | TimedOut) -> str:
    return repr(value)

@task
def of_nothing(value: None | TimedOut) -> str:
    return repr(value)

with Dag("d"):
    of_number(number())
    of_found(found())
    of_nothing(nothing())
""",
        ),
        "d",
    )
    assert result.state == "success", result
    assert result.tasks["of_number"].output == "7"
    assert result.tasks["of_found"].output == "Found(name='f')"
    assert result.tasks["of_nothing"].output == "None"
