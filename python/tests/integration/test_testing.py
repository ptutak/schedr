"""``schedr.testing.run_dag`` against the real binary: ``schedr dags test`` as a subprocess.

The binary is found as ``run_dag`` finds it (``SCHEDR_BIN``, else the one installed with
this Python's ``schedr``); ``make test-integration`` and CI install the current build.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from schedr.testing import RunDagError, run_dag

FOR = datetime(2026, 10, 15, 3, tzinfo=UTC)

FAILING = """
from datetime import timedelta

from schedr import Dag, TaskDefaults, task

with Dag("fails", defaults=TaskDefaults(retries=1, retry_delay=timedelta(0))):

    @task
    def extract() -> int:
        raise ValueError("source is down")

    @task
    def load(n: int) -> int:
        return n

    load(extract())
"""

TYPED = """
from dataclasses import dataclass

from schedr import Dag, task

@dataclass
class Batch:
    name: str
    size: int

with Dag("typed"):

    @task
    def make() -> Batch:
        return Batch("b-1", 3)

    make()
"""


def test_a_failed_run_shows_each_attempt_and_the_tasks_downstream(tmp_path: Path) -> None:
    (tmp_path / "fails.py").write_text(FAILING)
    result = run_dag(tmp_path / "fails.py", "fails", for_=FOR)
    assert result.state == "failed"
    extract = result.tasks["extract"]
    assert extract.state == "failed"
    assert [a.status for a in extract.attempts] == ["failed", "failed"]
    error = extract.attempts[-1].error
    assert error is not None
    assert (error["exception"], error["message"]) == ("ValueError", "source is down")
    assert all(a.duration is not None for a in extract.attempts)
    assert result.tasks["load"].state == "upstream_failed"
    assert result.tasks["load"].attempts == ()


def test_a_directory_is_a_root_and_outputs_are_decoded_from_its_file(tmp_path: Path) -> None:
    (tmp_path / "typed.py").write_text(TYPED)
    (tmp_path / "helpers.py").write_text("X = 1\n")
    batch = run_dag(tmp_path, "typed", for_=FOR).tasks["make"].output
    assert type(batch).__name__ == "Batch"
    assert (batch.name, batch.size) == ("b-1", 3)


def test_an_unknown_dag_is_an_error(tmp_path: Path) -> None:
    (tmp_path / "typed.py").write_text(TYPED)
    with pytest.raises(RunDagError, match="no DAG `nope`"):
        run_dag(tmp_path, "nope")


MAPPED = """
from schedr import Dag, task, task_ctx

@task
def files() -> list[str]:
    return ["a", "bb", "ccc"]

@task
def size(path: str) -> int:
    print(f"instance {task_ctx.map_index} of {task_ctx.task_id}, try {task_ctx.try_number}")
    return len(path)

@task
def total(sizes: list[int]) -> int:
    return sum(sizes)

with Dag("mapped"):
    total(size.expand(path=files()))
"""


def test_a_mapped_task_has_a_result_per_instance_and_knows_its_map_index(tmp_path: Path) -> None:
    (tmp_path / "mapped.py").write_text(MAPPED)
    result = run_dag(tmp_path / "mapped.py", "mapped", for_=FOR)
    assert result.state == "success"
    size = result.tasks["size"]
    # The task as a whole: its output the list, its instances each a result of its own.
    assert (size.state, size.output, size.attempts) == ("success", [1, 2, 3], ())
    assert size.instances is not None
    assert [i.map_index for i in size.instances] == [0, 1, 2]
    assert (size[2].output, size[2].state) == (3, "success")
    assert "instance 1 of size, try 1" in size[1].log
    assert result.tasks["total"].output == 6
    with pytest.raises(TypeError, match="isn't mapped"):
        result.tasks["total"][0]


OUTCOMES = """
from schedr import Dag, Data, task


class A(Data):
    pass


class B(Data):
    pass


@task
def produce() -> A | B:
    return B()


@task
def pick(n: int) -> A | B:
    return A() if n == 0 else B()


with Dag("outcomes"):
    produce()
    pick.expand(n=[0, 1])
"""


def test_an_output_is_decoded_as_its_recorded_outcome(tmp_path: Path) -> None:
    # B117: `A()` and `B()` store the same JSON; the outcome says which it is.
    (tmp_path / "outcomes.py").write_text(OUTCOMES)
    result = run_dag(tmp_path / "outcomes.py", "outcomes", for_=FOR)
    assert result.tasks["produce"].outcome == "B"
    assert type(result.tasks["produce"].output).__name__ == "B"
    assert [type(item).__name__ for item in result.tasks["pick"].output] == ["A", "B"]


def test_a_file_is_tested_alone_its_siblings_importable(tmp_path: Path) -> None:
    # B118: every file beside it was parsed, so imported, side effects and all.
    (tmp_path / "helpers.py").write_text("NAME = 'one'\n")
    (tmp_path / "target.py").write_text(
        "from helpers import NAME\nfrom schedr import Dag, EmptyOperator\n"
        "with Dag('file_scope'):\n    EmptyOperator(NAME)\n"
    )
    # An absolute path: the worker imports from its read-only copy of the root.
    marker = tmp_path / "marker"
    (tmp_path / "sibling.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('imported')\n"
    )
    result = run_dag(tmp_path / "target.py", "file_scope", for_=FOR)
    assert result.state == "success"
    assert list(result.tasks) == ["one"]
    assert not marker.exists()


ELEMENTS = """
from schedr import Dag, dag_ctx, task


@task
def suffix() -> str:
    return "2026"


@task
def region(name: str) -> str:
    return name


@task
def day(on: str) -> str:
    return on


@task
def kind(of: str) -> str:
    return of


@task
def pair(left: str, right: int) -> str:
    return f"{left}{right}"


with Dag("elements"):
    year = suffix()
    region.expand(name=[t"eu-{year}", "us"])
    day.expand(on=[t"{dag_ctx.run.planned_for:%Y-%m-%d}", "2026-01-01"])
    kind.expand(of=[dag_ctx.run.kind, "other"])
    pair.expand_kwargs([{"left": t"x-{year}", "right": 1}, {"left": "y", "right": 2}])
"""


def test_a_t_string_or_dag_ctx_element_of_a_mapped_list_is_built_per_instance(
    tmp_path: Path,
) -> None:
    # B81 (decision 118): the master couldn't resolve such an element, and the task failed
    # when it expanded.
    (tmp_path / "elements.py").write_text(ELEMENTS)
    result = run_dag(tmp_path / "elements.py", "elements", for_=FOR)
    assert result.state == "success"
    assert result.tasks["region"].output == ["eu-2026", "us"]
    assert result.tasks["day"].output == [f"{FOR:%Y-%m-%d}", "2026-01-01"]
    assert result.tasks["kind"].output == ["manual", "other"]
    assert result.tasks["pair"].output == ["x-20261", "y2"]


NARROWED = """
from schedr import Dag, Data, task


class Rows(Data):
    count: int


class Missing(Data):
    reason: str


@task
def fetch(path: str) -> Rows | Missing:
    if path.startswith("missing"):
        return Missing(reason=path)
    return Rows(count=len(path))


@task
def count(rows: Rows, everything: list[Rows | Missing]) -> int:
    return rows.count * 10 + len(everything)


with Dag("narrowed"):
    results = fetch.expand(path=["ab", "missing", "abc"])
    count.partial(everything=results).expand(rows=results.on(Rows))
"""


def test_a_narrowed_element_is_skipped_when_its_list_is_also_used_whole(tmp_path: Path) -> None:
    # B80 (decision 119): with the mapped output used again, `count` waits for the whole of
    # `fetch`, and `.on(Rows)` still goes element by element: the instance for `Missing` is
    # skipped. The expansion compared `Rows` with the whole task's outcome and failed.
    (tmp_path / "narrowed.py").write_text(NARROWED)
    result = run_dag(tmp_path / "narrowed.py", "narrowed", for_=FOR)
    assert result.state == "success", result
    assert result.tasks["count"].output == [23, None, 33]


MAPPED_OUTCOME = """
from schedr import Dag, task


@task
def number(n: int) -> int:
    return n


@task
def take(s: str) -> str:
    return s


with Dag("mapped_outcome"):
    r = number.expand(n=[1, 2])
    take.expand(s=[r.outcome])
"""


def test_a_mapped_tasks_outcome_is_still_read_as_data(tmp_path: Path) -> None:
    # Codex's review of B80: leaving a whole mapped task's outcome out of what the master
    # resolves against made `r.outcome` resolve to `None`.
    (tmp_path / "mapped_outcome.py").write_text(MAPPED_OUTCOME)
    result = run_dag(tmp_path / "mapped_outcome.py", "mapped_outcome", for_=FOR)
    assert result.state == "success", result
    assert result.tasks["take"].output == ["SUCCESS"]


AMBIGUOUS = """
from schedr import Dag, Data, task


class A(Data):
    pass


class B(Data):
    pass


class C(Data):
    pass


@task
def source(n: int) -> A | B | C:
    return (A(), B(), C())[n]


@task
def take(x: A | B, all: list[A | B | C]) -> str:
    return type(x).__name__


with Dag("ambiguous"):
    r = source.expand(n=[0, 1, 2])
    take.partial(all=r).expand(x=r.on(A, B))
"""


def test_an_element_of_a_list_also_used_whole_keeps_its_member(tmp_path: Path) -> None:
    # Codex's review of B80: an element taken from a mapped output used whole came without
    # its instance's outcome, and `B()` was built as `A`, the first member it fits.
    (tmp_path / "ambiguous.py").write_text(AMBIGUOUS)
    result = run_dag(tmp_path / "ambiguous.py", "ambiguous", for_=FOR)
    assert result.state == "success", result
    assert result.tasks["take"].output == ["A", "B", None]


ORDERED = """
from schedr import Dag, Data, RunIf, task


class Rows(Data):
    n: int


class Missing(Data):
    r: str


@task
def fetch(path: str) -> Rows | Missing:
    return Missing(r=path) if path.startswith("missing") else Rows(n=1)


@task
def any_rows() -> str:
    return "ran"


@task(run_if=RunIf.ALL_SUCCEEDED)
def all_rows() -> str:
    return "ran"


with Dag("ordered"):
    results = fetch.expand(path=["a", "missing", "c"])
    results.on(Rows) >> any_rows()
    results.on(Rows) >> all_rows()
"""


def test_an_order_edge_narrowed_from_a_mapped_task_follows_the_rule(tmp_path: Path) -> None:
    # B132 (decision 120): the edge's `Rows` was compared with the whole mapped task's own
    # outcome, and both were always skipped. Per instance now: `NONE_FAILED` (the default)
    # runs when any instance had `Rows`, `ALL_SUCCEEDED` only when every one had.
    (tmp_path / "ordered.py").write_text(ORDERED)
    result = run_dag(tmp_path / "ordered.py", "ordered", for_=FOR)
    assert result.state == "success", result
    assert result.tasks["any_rows"].state == "success"
    assert result.tasks["all_rows"].state == "skipped"


LIMITED = """
import time
from pathlib import Path

from schedr import Dag, task

LOG = Path({log!r})


def ran(name: str) -> None:
    start = time.time()
    time.sleep(0.4)
    with LOG.open("a") as log:
        log.write(f"{{name}} {{start}} {{time.time()}}\\n")


with Dag("limited", max_active_tasks=2):

    @task(max_active_per_run=2)
    def fetch(n: int) -> int:
        ran("fetch")
        return n

    @task(max_active=1)
    def other(n: int) -> int:
        ran("other")
        return n

    fetch.expand(n=[0, 1, 2, 3, 4, 5])
    other.expand(n=[0, 1, 2, 3])
"""


def most_at_once(spans: list[tuple[float, float]]) -> int:
    """The most of ``spans`` (start, end) that overlap at one moment."""
    events = sorted([(start, 1) for start, _ in spans] + [(end, -1) for _, end in spans])
    most = running = 0
    for _, change in events:
        running += change
        most = max(most, running)
    return most


def test_concurrency_limits_hold_with_slots_to_spare(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # M4.4 (decision 92B): 6 slots, so without the limits 6 would run at once.
    monkeypatch.setenv("SCHEDR_CONFIG__WORKER__SLOTS", "6")
    log = tmp_path / "ran.log"
    (tmp_path / "limited.py").write_text(LIMITED.format(log=str(log)))
    result = run_dag(tmp_path / "limited.py", "limited", for_=FOR)
    assert result.state == "success", result
    spans: dict[str, list[tuple[float, float]]] = {"fetch": [], "other": []}
    for line in log.read_text().splitlines():
        name, start, end = line.split()
        spans[name].append((float(start), float(end)))
    assert [len(spans["fetch"]), len(spans["other"])] == [6, 4]
    assert most_at_once(spans["fetch"]) <= 2  # max_active_per_run
    assert most_at_once(spans["other"]) <= 1  # max_active
    together = most_at_once(spans["fetch"] + spans["other"])
    # max_active_tasks, binding on its own (the tasks' limits would let 3 run), and tasks did
    # run side by side.
    assert together == 2, together


SECRETS = """
from schedr import Dag, SecretError, task, task_ctx, secrets

@task
def connect(password: str) -> str:
    print("connecting with", password)
    return f"u:{password}@db"

@task(secrets=["etl/db#user"])
def who() -> str:
    try:
        task_ctx.secret("etl/db#port")
    except SecretError as e:
        return f"{e.kind}: {task_ctx.secret('etl/db#user')}"
    return "read an undeclared secret"

with Dag("secrets"):
    connect(secrets["etl/db#password"])
    who()
"""


def test_secrets_are_given_and_masked(tmp_path: Path) -> None:
    (tmp_path / "s.py").write_text(SECRETS)
    given = {"etl/db#password": "hunter22-pw", "etl/db#user": "etl-user"}
    result = run_dag(tmp_path / "s.py", "secrets", for_=FOR, secrets=given)
    assert result.state == "success", result
    connect = result.tasks["connect"]
    assert connect.output == "u:****@db"
    assert "connecting with ****" in connect.log
    assert "hunter22-pw" not in connect.log
    assert result.tasks["who"].output == "secret_denied: ****"
