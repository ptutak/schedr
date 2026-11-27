"""scripts/kind_chaos.py's schedule and checks, on records made here: no cluster."""

from __future__ import annotations

import importlib.util
import json
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def chaos() -> ModuleType:
    """Load scripts/kind_chaos.py."""
    path = ROOT / "scripts" / "kind_chaos.py"
    spec = importlib.util.spec_from_file_location("kind_chaos", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def mark(
    what: str, task: str, number: int, at: int, host: str = "w0", pid: int = 1
) -> dict[str, object]:
    return {
        "what": what,
        "run": "r1",
        "task": task,
        "try": number,
        "host": host,
        "pid": pid,
        "at_ns": at,
    }


def test_a_seed_picks_the_same_schedule_each_kind_once_first() -> None:
    first = chaos().schedule(7)
    assert first == chaos().schedule(7)
    assert sorted(f.kind for f in first) == sorted(chaos().KINDS)
    assert [f.at for f in first] == sorted(f.at for f in first)
    assert all(chaos().FIRST_FAULT <= f.at <= chaos().LAST_FAULT for f in first)
    assert chaos().schedule(8) != first
    assert len(chaos().schedule(7, count=6)) == 6


def test_two_tries_at_once_break_the_rule_unless_the_earlier_was_lost() -> None:
    marks = [
        mark("start", "t00", 1, 10),
        mark("start", "t00", 2, 20, "w1"),
        mark("end", "t00", 2, 30, "w1"),
    ]
    found = chaos().tries(marks)
    assert chaos().overlaps(found, {("r1", "t00", 1): "success"})
    assert not chaos().overlaps(found, {("r1", "t00", 1): "lost"})


def test_tries_one_after_another_are_fine() -> None:
    marks = [mark("start", "t01", 1, 10), mark("end", "t01", 1, 15), mark("start", "t01", 2, 20)]
    assert not chaos().overlaps(chaos().tries(marks), {})


def test_a_task_whose_cleanup_may_move_never_runs_twice_at_once() -> None:
    marks = [
        mark("start", "t00", 1, 10),
        mark("start", "t00", 2, 20, "w1"),
        mark("end", "t00", 2, 30, "w1"),
    ]
    # Lost excuses an ordinary task's overlap, not this one's: its cut-off worker stops it.
    lost = {("r1", "t00", 1): "lost"}
    assert chaos().overlaps(chaos().tries(marks), lost, {"t00"})
    assert not chaos().overlaps(chaos().tries(marks), lost, set())


def test_a_node_death_ends_only_the_tries_started_before_it() -> None:
    # Agy's review of M5.3f: a pod name once on a dead node was excused for the whole run, though
    # a StatefulSet's pod comes back under it.
    died = {"w0": [15]}
    before = chaos().tries([mark("start", "t00", 1, 10)])
    chaos().settle(before, {}, died)
    assert before[0].end == 15
    after = chaos().tries([mark("start", "t00", 1, 20)])
    chaos().settle(after, {}, died)
    assert after[0].end is None, "started after the death: it runs on"
    # The worker's report comes first.
    reported = chaos().tries([mark("start", "t00", 1, 10)])
    chaos().settle(reported, {("r1", "t00", 1): 12}, died)
    assert reported[0].end == 12


def test_a_cleanup_beside_a_try_of_its_task_is_reported_either_begun_first() -> None:
    ran = chaos().tries([mark("start", "t00", 1, 10), mark("end", "t00", 1, 30)])
    beside = chaos().cleanups(
        [mark("cstart", "t00", 1, 20, "w1"), mark("cend", "t00", 1, 25, "w1")], {}
    )
    assert chaos().cleanups_beside(ran, beside) == [
        "r1 t00: the cleanup of try 1 on w1 ran beside try 1 on w0"
    ]
    after = chaos().cleanups([mark("cstart", "t00", 1, 31, "w1"), mark("cend", "t00", 1, 35)], {})
    assert not chaos().cleanups_beside(ran, after)
    # Agy's review of M5.3f: a try begun while the cleanup ran was missed.
    cleanup = chaos().cleanups(
        [mark("cstart", "t00", 1, 40, "w1"), mark("cend", "t00", 1, 60, "w1")], {}
    )
    retried = chaos().tries([mark("start", "t00", 2, 50, "w2"), mark("end", "t00", 2, 55, "w2")])
    assert chaos().cleanups_beside(retried, cleanup)


def test_each_tries_cleanup_is_checked_on_its_own() -> None:
    # Agy's review of M5.3f: try 2's cleanup stood for try 1's missing one.
    attempts = [
        {
            "run": "r1",
            "task": "t00",
            "try": 1,
            "status": "lost",
            "anywhere": True,
            "started_at": "x",
        },
        {
            "run": "r1",
            "task": "t00",
            "try": 2,
            "status": "success",
            "anywhere": True,
            "started_at": "x",
        },
        {
            "run": "r1",
            "task": "t01",
            "try": 1,
            "status": "success",
            "anywhere": False,
            "started_at": "x",
        },
        # Never started: no cleanup owed.
        {
            "run": "r1",
            "task": "t04",
            "try": 1,
            "status": "assignment_expired",
            "anywhere": True,
            "started_at": None,
        },
    ]
    marks = [mark("cend", "t00", 2, 5)]
    assert chaos().cleanups_missing(attempts, marks) == ["r1 t00: try 1's cleanup never ran"]


def test_a_record_that_can_show_nothing_fails(tmp_path: Path) -> None:
    (tmp_path / "faults.json").write_text("[]")
    unread = {"runs": "not read: Postgres gone", "attempts": [], "marks": []}
    (tmp_path / "database.json").write_text(json.dumps(unread))
    assert chaos().check(tmp_path) == ["runs: not read: Postgres gone"]
    (tmp_path / "database.json").write_text(json.dumps({"runs": [], "attempts": [], "marks": []}))
    assert chaos().check(tmp_path) == ["no run ran: the load never started"]
    # Agy's review of M5.3f: runs with no attempt or mark passed.
    runs = [{"id": "r1", "state": "success"}]
    (tmp_path / "database.json").write_text(json.dumps({"runs": runs, "attempts": [], "marks": []}))
    assert chaos().check(tmp_path) == [
        "no attempt or no mark recorded: the load did nothing to check"
    ]


def record(tmp_path: Path, data: dict[str, object], faults: list[dict[str, object]]) -> None:
    (tmp_path / "database.json").write_text(json.dumps(data))
    (tmp_path / "faults.json").write_text(json.dumps(faults))


def test_a_record_is_checked_whole(tmp_path: Path) -> None:
    started = 1_800_000_000 * 10**9
    attempts = [
        {
            "run": "r1",
            "task": "t00",
            "try": 1,
            "status": "success",
            "anywhere": False,
            "started_at": "2027-01-15T08:00:00+00:00",
            "ended_at": "2027-01-15T08:00:01+00:00",
        },
    ]
    marks = [mark("start", "t00", 1, started), mark("end", "t00", 1, started + 10**9)]
    runs = [{"id": "r1", "state": "success"}, {"id": "r2", "state": "running"}]
    faults = [
        {
            "kind": "node_death",
            "at": 60,
            "applied": True,
            "pods": ["w9"],
            "begun": "2027-01-15T08:00:05+00:00",
        }
    ]
    record(tmp_path, {"runs": runs, "attempts": attempts, "marks": marks}, faults)
    assert chaos().check(tmp_path) == ["run r2 is running"]
    # Agy's review of M5.3f: a fault that hit nothing, or one of the schedule not executed.
    (tmp_path / "schedule.json").write_text(json.dumps([{"kind": "node_death"}, {"kind": "x"}]))
    faults[0]["applied"] = False
    record(tmp_path, {"runs": runs[:1], "attempts": attempts, "marks": marks}, faults)
    assert chaos().check(tmp_path) == [
        "1 of the schedule's 2 faults executed",
        "fault node_death at 60 s hit nothing",
    ]


def test_a_try_that_succeeded_unmarked_fails_the_record() -> None:
    attempts = [{"run": "r1", "task": "t00", "try": 1, "status": "success"}]
    assert chaos().marks_missing(attempts, [mark("start", "t00", 1, 1)]) == [
        "r1 t00: try 1 succeeded unmarked"
    ]


def test_a_try_without_its_end_mark_ends_when_the_worker_reported_it(tmp_path: Path) -> None:
    started = 1_800_000_000 * 10**9
    attempts = [
        {
            "run": "r1",
            "task": "t06",
            "try": 1,
            "status": "failed",
            "anywhere": False,
            "started_at": "x",
            "ended_at": "2027-01-15T08:00:01+00:00",
        },
        {
            "run": "r1",
            "task": "t06",
            "try": 2,
            "status": "success",
            "anywhere": False,
            "started_at": "x",
            "ended_at": "2027-01-15T08:00:09+00:00",
        },
    ]
    marks = [
        mark("start", "t06", 1, started),
        mark("start", "t06", 2, started + 5 * 10**9, "w1"),
        mark("end", "t06", 2, started + 8 * 10**9, "w1"),
    ]
    data = {"runs": [{"id": "r1", "state": "success"}], "attempts": attempts, "marks": marks}
    record(tmp_path, data, [])
    # Try 1 failed before marking its end (at 08:00:01), before try 2 began: no overlap.
    assert chaos().check(tmp_path) == []


@pytest.mark.parametrize(
    ("server", "local"),
    [
        ("https://127.0.0.1:41234", True),
        ("https://localhost:6443/", True),
        ("https://127.0.0.1:1@prod.example", False),
        ("https://prod.example:443", False),
        ("", False),
    ],
)
def test_only_a_cluster_on_this_machine_is_talked_to(server: str, local: bool) -> None:
    assert chaos().local_server(server) is local


def test_an_address_from_the_cluster_is_checked_before_iptables() -> None:
    # Agy's review of M5.3f: a value read from the cluster went to a command unchecked.
    assert chaos().address("10.244.1.5") == "10.244.1.5"
    with pytest.raises(ValueError, match="octets"):
        chaos().address("--flush")


def test_the_schedule_command_prints_without_a_cluster(capsys: pytest.CaptureFixture[str]) -> None:
    assert chaos().main(["schedule", "--seed", "3"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert [f["kind"] for f in printed] == [f.kind for f in chaos().schedule(3)]


def test_the_load_dag_builds() -> None:
    # Built as the parse builds it: a refused option raises here, not only on a cluster (its
    # first run: `cleanup_anywhere` without a cleanup, on each other task).
    namespace: dict[str, object] = {}
    exec(compile(chaos().DAG, "chaos.py", "exec"), namespace)


def test_a_statement_waits_for_the_marks_postgres_to_answer(tmp_path: Path) -> None:
    # Its image's first start restarts the server after init: the readiness probe passes
    # between the two, a `psql` there fails (chaos seed 7 on 701a8882's setup).
    module = chaos()
    run = module.Run(tmp_path, "podman")
    answers = iter([2, 2, 0])
    calls: list[tuple[str, ...]] = []

    def k(*args: str, check: bool = True, stdin: str | None = None) -> object:
        assert not check, "a refusal is waited out, not raised"
        calls.append(args)
        code = next(answers)
        return type("Done", (), {"returncode": code, "stdout": "ok\n", "stderr": "no socket"})()

    run.k = k  # type: ignore[method-assign]
    slept: list[float] = []
    assert run.psql_when_up("SELECT 1", "chaos-marks-0", sleep=slept.append) == "ok\n"
    assert len(calls) == 3
    assert slept == [1.0, 1.0]


def test_a_statement_still_refused_after_the_wait_fails_with_its_error(tmp_path: Path) -> None:
    module = chaos()
    run = module.Run(tmp_path, "podman")

    def k(*args: str, check: bool = True, stdin: str | None = None) -> object:
        return type("Done", (), {"returncode": 2, "stdout": "", "stderr": "no socket"})()

    run.k = k  # type: ignore[method-assign]
    times = iter([0.0, 0.0, 30.0, 61.0, 61.0])
    with pytest.raises(RuntimeError, match="no socket"):
        run.psql_when_up(
            "SELECT 1", "chaos-marks-0", wait=60.0, sleep=lambda _: None, clock=lambda: next(times)
        )
