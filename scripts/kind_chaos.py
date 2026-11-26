#!/usr/bin/env python3
"""Chaos on kind (M5.3f): seeded faults on a cluster of several nodes, checked on guarantees.

Checked against schedr's guarantees only, never against how long something took.

    scripts/kind_chaos.py run --seed 7            the whole run, on the LOCAL cluster schedr-chaos
    scripts/kind_chaos.py schedule --seed 7       the faults the seed picks, without running them
    scripts/kind_chaos.py check RECORD_DIR        the checks again, on a run's record

A run sets up its own kind cluster, `schedr-chaos` (one control plane and three nodes; its context
is refused unless its server is on this machine), installs the chart (two masters, three workers,
Postgres in the cluster), starts a load whose tasks write marks into Postgres, applies the faults
at their times, and keeps a record in target/kind-chaos/<seed>/: every fault as it was executed
(its command, output and exit code, its targets by identity, the UTC moments before and after),
the schedule beside it, Kubernetes's logs of every container (`kind export logs`, dead ones too),
the pods and events, and the database's rows and marks. The checks then read only that record:
each run ended; no two tries of a task ran at once unless the earlier one ended `lost` (at least
once, decision 252); every `cleanup_anywhere` task's cleanup ran; no try left without its end
unless its node died. The seed repeats the schedule, not the cluster's timing: a failure is read
from its record.

The images: IMAGE and SERVER_IMAGE (a release's, or ones built here); CONTAINER the container
tool (podman by default, docker on CI).
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import ipaddress
import itertools
import json
import os
import random
import re
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from collections.abc import Set as AbstractSet
from pathlib import Path
from typing import cast

ROOT = Path(__file__).resolve().parents[1]
CLUSTER = "schedr-chaos"
CONTEXT = f"kind-{CLUSTER}"
NAMESPACE = "schedr"
# The marks' own database, on the control plane, which no fault touches: a fault on schedr's
# Postgres (or a node's death) can't change what is measured (a side agent's note on M5.3f).
MARKS_URL = "postgresql://postgres:schedr@chaos-marks.schedr.svc:5432/postgres"
# The kinds of fault built so far, of the sixteen in M5.3f's scope.
KINDS = ("node_death", "packet_drop", "postgres_kill", "rolling_upgrade")
# How long a run's load lasts, and the faults' window inside it (seconds).
LOAD = 420
FIRST_FAULT = 60
LAST_FAULT = 300


@dataclasses.dataclass(frozen=True)
class Fault:
    """One fault of a schedule.

    Its kind, when (seconds into the load), how long, and which of the candidates it picks (an
    index taken modulo how many there are when it runs).
    """

    kind: str
    at: int
    length: int
    pick: int


def schedule(seed: int, count: int = 4, kinds: tuple[str, ...] = KINDS) -> list[Fault]:
    """The faults `seed` picks, in order.

    `count` of them, each kind once before any repeats, at times spread over the window.
    """
    rng = random.Random(seed)
    picked = list(kinds)
    rng.shuffle(picked)
    while len(picked) < count:
        picked.append(rng.choice(kinds))
    picked = picked[:count]
    times = sorted(rng.randint(FIRST_FAULT, LAST_FAULT) for _ in picked)
    # Dropped packets on both sides of the lease (60 s): shorter loses nothing, longer moves a
    # cleanup once, after the worker cut its task off (M5.3f's scope).
    return [
        Fault(
            kind,
            at,
            rng.randint(15, 120) if kind == "packet_drop" else rng.randint(15, 45),
            rng.randint(0, 1_000_000),
        )
        for kind, at in zip(picked, times, strict=True)
    ]


# --- the checks, on a record ---------------------------------------------------------------


@dataclasses.dataclass
class Try:
    """One try of a task as its marks tell: its process's start, and its end when marked."""

    run: str
    task: str
    number: int
    host: str
    start: int
    end: int | None = None


def tries(marks: list[dict[str, object]]) -> list[Try]:
    """The tries the marks tell of: a `start` and its `end` (or none) for each process."""
    by_process: dict[tuple[str, str, int, str, int], Try] = {}
    for mark in marks:
        what = str(mark["what"])
        if what not in ("start", "end"):
            continue
        key = (
            str(mark["run"]),
            str(mark["task"]),
            int(str(mark["try"])),
            str(mark["host"]),
            int(str(mark["pid"])),
        )
        at = int(str(mark["at_ns"]))
        one = by_process.setdefault(key, Try(key[0], key[1], key[2], key[3], at))
        if what == "start":
            one.start = at
        else:
            one.end = at
    return sorted(by_process.values(), key=lambda t: (t.run, t.task, t.start))


def to_ns(utc: str) -> int:
    """An ISO 8601 moment (the record's UTC), in ns since the epoch, to the microsecond."""
    at = dt.datetime.fromisoformat(utc)
    return int(at.timestamp() * 1_000_000) * 1_000


def deaths(faults: list[dict[str, object]]) -> dict[str, list[int]]:
    """For each pod a node's death took, when each such death began, in ns.

    Only a try started before one ended with it: StatefulSet pods come back under their names
    (agy's review of M5.3f).
    """
    found: dict[str, list[int]] = {}
    for fault in faults:
        if fault.get("kind") != "node_death":
            continue
        pods = fault.get("pods")
        names = [str(name) for name in pods] if isinstance(pods, list) else []
        for name in names:
            found.setdefault(name, []).append(to_ns(str(fault["begun"])))
    return found


def settle(
    found: list[Try], ends: dict[tuple[str, str, int], int], died: dict[str, list[int]]
) -> None:
    """Each try without its end mark given the end the record shows.

    The worker's report of it (`ended_by_worker`), else the first death of its node after it
    started; none, it may run on.
    """
    for one in found:
        if one.end is not None:
            continue
        reported = ends.get((one.run, one.task, one.number))
        if reported is not None:
            one.end = reported
            continue
        after = [at for at in died.get(one.host, []) if at >= one.start]
        if after:
            one.end = min(after)


def overlaps(
    found: list[Try],
    status: dict[tuple[str, str, int], str | None],
    gated: AbstractSet[str] = frozenset(),
) -> list[str]:
    """Two tries of one task at once, not by design.

    By design for an ordinary task only when the earlier ended `lost` (its worker lost, which may
    run on: at least once, decisions 252 and 280). Never for a task whose cleanup may move
    (`gated`): its cut-off worker stops it before the master moves its cleanup (decision 252), as
    the clock simulator's `check_alone`. A try with no end ([`settle`]d) runs on.
    """
    problems = []
    by_task: dict[tuple[str, str], list[Try]] = {}
    for one in found:
        by_task.setdefault((one.run, one.task), []).append(one)
    for (run, task), list_ in by_task.items():
        for earlier, later in itertools.pairwise(list_):
            end = earlier.end
            if end is not None and end <= later.start:
                continue
            ended = status.get((run, task, earlier.number))
            if ended == "lost" and task not in gated:
                continue
            problems.append(
                f"{run} {task}: try {later.number} on {later.host} started while try "
                f"{earlier.number} on {earlier.host} ran (ended {ended})"
            )
    return problems


@dataclasses.dataclass
class Cleanup:
    """One cleanup's process as its marks tell: its `cstart`, and its `cend` when marked."""

    run: str
    task: str
    number: int
    host: str
    start: int
    end: int | None = None


def cleanups(marks: list[dict[str, object]], died: dict[str, list[int]]) -> list[Cleanup]:
    """The cleanups the marks tell of, each with its end (its `cend`, its node's death)."""
    by_process: dict[tuple[str, str, int, str, int], Cleanup] = {}
    for mark in marks:
        what = str(mark["what"])
        if what not in ("cstart", "cend"):
            continue
        key = (
            str(mark["run"]),
            str(mark["task"]),
            int(str(mark["try"])),
            str(mark["host"]),
            int(str(mark["pid"])),
        )
        at = int(str(mark["at_ns"]))
        one = by_process.setdefault(key, Cleanup(key[0], key[1], key[2], key[3], at))
        if what == "cstart":
            one.start = at
        else:
            one.end = at
    for one in by_process.values():
        after = [at for at in died.get(one.host, []) if at >= one.start]
        if one.end is None and after:
            one.end = min(after)
    return list(by_process.values())


def cleanups_beside(found: list[Try], cleaned: list[Cleanup]) -> list[str]:
    """A cleanup and a try of its task at once, either begun first.

    A cleanup runs after its attempt ended, wherever it moved, and nothing of its task runs
    beside it (decision 252). Either without an end runs on.
    """
    problems = []
    for cleanup in cleaned:
        for one in found:
            if (one.run, one.task) != (cleanup.run, cleanup.task):
                continue
            try_end = one.end if one.end is not None else float("inf")
            cleanup_end = cleanup.end if cleanup.end is not None else float("inf")
            if one.start < cleanup_end and cleanup.start < try_end:
                problems.append(
                    f"{cleanup.run} {cleanup.task}: the cleanup of try {cleanup.number} on "
                    f"{cleanup.host} ran beside try {one.number} on {one.host}"
                )
    return problems


def cleanups_missing(
    attempts: list[dict[str, object]], marks: list[dict[str, object]]
) -> list[str]:
    """A started `cleanup_anywhere` try that ended without its cleanup ever ending.

    Per try: a later try's cleanup stands for no earlier one's (agy's review of M5.3f).
    """
    cleaned = {
        (str(m["run"]), str(m["task"]), int(str(m["try"]))) for m in marks if m["what"] == "cend"
    }
    problems = []
    for attempt in attempts:
        started = attempt.get("started_at") is not None
        if not attempt.get("anywhere") or attempt.get("status") is None or not started:
            continue
        key = (str(attempt["run"]), str(attempt["task"]), int(str(attempt["try"])))
        if key not in cleaned:
            problems.append(f"{key[0]} {key[1]}: try {key[2]}'s cleanup never ran")
    return problems


def marks_missing(attempts: list[dict[str, object]], marks: list[dict[str, object]]) -> list[str]:
    """A try that succeeded without both its marks: the load's marks are lost, the checks blind."""
    marked: dict[tuple[str, str, int], set[str]] = {}
    for mark in marks:
        key = (str(mark["run"]), str(mark["task"]), int(str(mark["try"])))
        marked.setdefault(key, set()).add(str(mark["what"]))
    problems = []
    for attempt in attempts:
        if attempt.get("status") != "success":
            continue
        key = (str(attempt["run"]), str(attempt["task"]), int(str(attempt["try"])))
        if not {"start", "end"} <= marked.get(key, set()):
            problems.append(f"{key[0]} {key[1]}: try {key[2]} succeeded unmarked")
    return problems


def faults_missing(
    faults: list[dict[str, object]], planned: list[dict[str, object]] | None
) -> list[str]:
    """A fault of the schedule never executed, or executed against nothing."""
    problems = []
    if planned is not None and len(faults) != len(planned):
        problems.append(f"{len(faults)} of the schedule's {len(planned)} faults executed")
    problems += [
        f"fault {f.get('kind')} at {f.get('at')} s hit nothing"
        for f in faults
        if not f.get("applied")
    ]
    return problems


def unfinished(runs: list[dict[str, object]]) -> list[str]:
    """Runs that didn't end."""
    return [
        f"run {r['id']} is {r['state']}" for r in runs if r["state"] not in ("success", "failed")
    ]


def ended_by_worker(attempts: list[dict[str, object]]) -> dict[tuple[str, str, int], int]:
    """When each try the worker reported ended (`success`, `failed`), in ns.

    A try's process that failed before marking its end ended then. Not a `lost` one: its process
    may run on.
    """
    ends = {}
    for attempt in attempts:
        if attempt.get("status") in ("success", "failed") and attempt.get("ended_at"):
            key = (str(attempt["run"]), str(attempt["task"]), int(str(attempt["try"])))
            ends[key] = to_ns(str(attempt["ended_at"]))
    return ends


def check(record: Path) -> list[str]:
    """Every guarantee checked on a run's record: what broke, empty when nothing did.

    A record that can't show anything fails too: a part not read, no run, no attempt or no mark,
    a fault of the schedule not executed or executed against nothing (agy's review of M5.3f).
    """
    data = json.loads((record / "database.json").read_text())
    faults = json.loads((record / "faults.json").read_text())
    schedule_file = record / "schedule.json"
    planned = json.loads(schedule_file.read_text()) if schedule_file.exists() else None
    unread = [f"{part}: {value}" for part, value in data.items() if isinstance(value, str)]
    if unread:
        return unread
    if not data["runs"]:
        return ["no run ran: the load never started"]
    if not data["attempts"] or not data["marks"]:
        return ["no attempt or no mark recorded: the load did nothing to check"]
    died = deaths(faults)
    status = {(a["run"], a["task"], int(a["try"])): a["status"] for a in data["attempts"]}
    gated = {str(a["task"]) for a in data["attempts"] if a.get("anywhere")}
    problems = faults_missing(faults, planned)
    problems += unfinished(data["runs"])
    problems += marks_missing(data["attempts"], data["marks"])
    found = tries(data["marks"])
    settle(found, ended_by_worker(data["attempts"]), died)
    problems += overlaps(found, status, gated)
    problems += cleanups_beside(found, cleanups(data["marks"], died))
    problems += cleanups_missing(data["attempts"], data["marks"])
    return problems


# --- the run ---------------------------------------------------------------------------------


def now() -> str:
    """UTC now, to the millisecond."""
    return dt.datetime.now(dt.UTC).isoformat(timespec="milliseconds")


class Run:
    """A run's commands and its record."""

    def __init__(self, record: Path, tool: str) -> None:
        """Commands logged to `record`'s harness.log; `tool` the container tool."""
        self.record = record
        self.tool = tool
        self.log = (record / "harness.log").open("a")

    def sh(
        self, *command: str, check: bool = True, stdin: str | None = None
    ) -> subprocess.CompletedProcess[str]:
        """A command, logged with its output, its exit code and when."""
        started = now()
        done = subprocess.run(command, input=stdin, capture_output=True, text=True, check=False)
        self.log.write(
            json.dumps(
                {
                    "command": command,
                    "started": started,
                    "ended": now(),
                    "code": done.returncode,
                    "stdout": done.stdout[-4000:],
                    "stderr": done.stderr[-4000:],
                }
            )
            + "\n"
        )
        self.log.flush()
        if check and done.returncode != 0:
            raise RuntimeError(
                f"{' '.join(command)} failed ({done.returncode}): {done.stderr.strip()}"
            )
        return done

    def k(
        self, *args: str, check: bool = True, stdin: str | None = None
    ) -> subprocess.CompletedProcess[str]:
        """A kubectl command in the cluster's namespace."""
        return self.sh(
            "kubectl", "--context", CONTEXT, "-n", NAMESPACE, *args, check=check, stdin=stdin
        )

    def psql(self, sql: str, pod: str = "postgres-0") -> str:
        """A statement in the cluster's Postgres, its rows as text.

        Schedr's (`postgres-0`), or the marks' (`chaos-marks-0`).
        """
        return self.k("exec", pod, "--", "psql", "-U", "postgres", "-tAc", sql).stdout


def require_local_context() -> None:
    """The context's server on this machine, or no command at all (as kind-lib.sh's)."""
    view = json.loads(
        subprocess.run(
            ["kubectl", "config", "view", "-o", "json"], capture_output=True, text=True, check=True
        ).stdout
    )
    # `null` lists from an empty kubeconfig read as none (agy's review of M5.3f).
    contexts = view.get("contexts") or []
    clusters = view.get("clusters") or []
    cluster = next((c["context"]["cluster"] for c in contexts if c["name"] == CONTEXT), None)
    entry: dict[str, object] = next((c["cluster"] for c in clusters if c["name"] == cluster), {})
    server = str(entry.get("server", ""))
    if not local_server(server) or entry.get("proxy-url"):
        sys.exit(
            f"FAILED: context {CONTEXT} has server {server or '<none>'}; "
            "this only talks to local kind"
        )


def local_server(server: str) -> bool:
    """A bare loopback address and port, no user info or path."""
    return re.fullmatch(r"https://(127\.0\.0\.1|localhost|\[::1\]):[0-9]+/?", server) is not None


# A try's mark, written by its task's own process (in the worker image, beside the DAG): the kind
# (start, end, cstart, cend), the run, the task, the try, the host, the process and when.
MARK = """\
import os
import socket
import sys
import time

import psycopg

# When it happened, taken once: written late (Postgres down, restarting), still the moment.
row = (sys.argv[1], os.environ["SCHEDR_RUN_ID"], os.environ["SCHEDR_TASK_ID"],
       int(os.environ["SCHEDR_TRY_NUMBER"]), socket.gethostname(), os.getppid(), time.time_ns())
# Tried again for two minutes: a mark lost with Postgres down blinds the checks (agy's review).
for attempt in range(120):
    try:
        with psycopg.connect(os.environ["CHAOS_PG"], connect_timeout=5) as conn:
            conn.execute("INSERT INTO chaos_marks VALUES (%s, %s, %s, %s, %s, %s, %s)", row)
        break
    except psycopg.OperationalError:
        time.sleep(1)
else:
    sys.exit("the mark was never written")
"""

# The load: tasks of 3 s and 20 s; each fourth one's cleanup allowed to move, the first of those
# running 150 s, past the lease and the move wait (100 s), so that its cleanup moving finds it
# still running unless its cut-off worker stopped it (one a run: all three overloaded the workers).
DAG = f"""\
from datetime import timedelta

from schedr import CommandOperator, Dag

# A mark failing for good fails no task: a try that succeeded unmarked fails the record instead.
M = "CHAOS_PG={MARKS_URL} python3 /dags/current/mark.py"

with Dag("chaos"):
    for k in range(12):
        anywhere = k % 4 == 0
        CommandOperator(
            f"t{{k:02}}",
            # Stopped (SIGTERM: its worker cut off, a cancel), it marks its end then: the sleep
            # in the background so the trap runs at once.
            f"trap '{{M}} end; exit 143' TERM; {{M}} start || true; "
            f"sleep {{150 if k == 0 else 20 if k % 3 == 0 else 3}} & wait $!; {{M}} end || true",
            retries=3,
            retry_delay=timedelta(0),
            # A cleanup that may move, on each fourth task only: `cleanup_anywhere` without a
            # cleanup is refused.
            **(
                {{"cleanup": f"{{M}} cstart || true; {{M}} cend || true", "cleanup_anywhere": True}}
                if anywhere
                else {{}}
            ),
        )
"""


# --- the cluster: setting it up, the load, the faults as executed, the record ------------

# A packet drop is both ways half the time.
HALF = 0.5

KIND_CONFIG = """\
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
"""

POSTGRES = """\
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: postgres}
spec:
  serviceName: postgres
  replicas: 1
  selector: {matchLabels: {app: postgres}}
  template:
    metadata: {labels: {app: postgres}}
    spec:
      containers:
        - name: postgres
          image: {image}
          env:
            - {name: POSTGRES_PASSWORD, value: schedr}
            # A folder of the volume: its root holds lost+found on some provisioners.
            - {name: PGDATA, value: /var/lib/postgresql/data/pgdata}
          ports: [{containerPort: 5432}]
          readinessProbe: {exec: {command: [pg_isready, -U, postgres]}, periodSeconds: 2}
          volumeMounts: [{name: data, mountPath: /var/lib/postgresql/data}]
  # Its data outlives the pod: a killed Postgres comes back with it, as a deployment's would
  # (without, the first run's kill wiped schedr's whole database).
  volumeClaimTemplates:
    - metadata: {name: data}
      spec:
        accessModes: [ReadWriteOnce]
        resources: {requests: {storage: 1Gi}}
---
apiVersion: v1
kind: Service
metadata: {name: postgres}
spec: {selector: {app: postgres}, ports: [{name: tcp-postgres, port: 5432}]}
"""

MARKS_TABLE = (
    "CREATE TABLE IF NOT EXISTS chaos_marks (what text, run text, task text, try int, "
    "host text, pid int, at_ns bigint)"
)


def run_all(seed: int, count: int) -> int:
    """The whole run: the cluster, the load, the faults, the record, the checks."""
    record = ROOT / "target" / "kind-chaos" / str(seed)
    record.mkdir(parents=True, exist_ok=True)
    tool = os.environ.get("CONTAINER", "podman")
    os.environ["KIND_EXPERIMENTAL_PROVIDER"] = tool
    r = Run(record, tool)
    planned = schedule(seed, count)
    (record / "schedule.json").write_text(
        json.dumps([dataclasses.asdict(f) for f in planned], indent=2)
    )
    (record / "faults.json").write_text("[]")
    # Its own cluster, afresh (agy's review of M5.3f: a kept one mixed runs' rows), and checked
    # local before anything talks to it: the record's `finally` talks to it too.
    if CLUSTER in r.sh("kind", "get", "clusters").stdout.split():
        require_local_context()
        r.sh("kind", "delete", "cluster", "--name", CLUSTER)
    with tempfile.TemporaryDirectory() as config_dir:
        config = Path(config_dir) / "kind.yaml"
        config.write_text(KIND_CONFIG)
        r.sh("kind", "create", "cluster", "--name", CLUSTER, "--config", str(config))
    require_local_context()
    try:
        load_and_faults(r, planned, seed)
    finally:
        # Kept whatever happened: a harness error is read from the record too.
        keep(r)
    problems = check(record)
    (record / "result.txt").write_text("\n".join(problems) or "ok: every guarantee held")
    print("\n".join(problems) or "ok: every guarantee held")
    return 1 if problems else 0


def load_and_faults(r: Run, planned: list[Fault], seed: int) -> None:
    """The cluster set up, the load with the faults at their times, until every run ended.

    Each fault is written to the record as it is executed.
    """
    record = r.record
    setup(r)
    started = time.monotonic()
    executed = []
    rng = random.Random(seed)
    # A run each 60 s: about four slots' work, the three workers' six.
    trigger_every = 60
    next_trigger = 0.0
    queue = list(planned)
    # Until every fault ran too (agy's review of M5.3f: a late one was dropped).
    while time.monotonic() - started < LOAD or queue:
        elapsed = time.monotonic() - started
        if elapsed >= next_trigger and elapsed < LAST_FAULT + 30:
            trigger(r)
            next_trigger = elapsed + trigger_every
        if queue and elapsed >= queue[0].at:
            executed.append(apply(r, queue.pop(0), rng))
            (record / "faults.json").write_text(json.dumps(executed, indent=2))
        time.sleep(1)
    wait_for_runs(r, 600)


def setup(r: Run) -> None:
    """On the cluster `run_all` made: the images, Postgres with the marks' table, the chart."""
    r.sh("kubectl", "--context", CONTEXT, "create", "namespace", NAMESPACE, check=False)
    image, server = os.environ["IMAGE"], os.environ["SERVER_IMAGE"]
    work = Path(tempfile.mkdtemp(prefix="schedr-kind-chaos-"))
    (work / "chaos.py").write_text(DAG)
    (work / "mark.py").write_text(MARK)
    (work / "Dockerfile").write_text(f"FROM {image}\nCOPY chaos.py mark.py /dags/current/\n")
    # Tagged by what it adds and its base: a changed DAG rolls the pods (one tag kept them on
    # the old image, the chart unchanged).
    added = hashlib.sha256(f"{image}\n{DAG}\n{MARK}".encode()).hexdigest()[:12]
    worker_image = f"localhost/schedr-kind-chaos:{added}"
    r.sh(r.tool, "build", "-q", "-t", worker_image, str(work))
    for one in (worker_image, server):
        archive = work / (one.replace("/", "_").replace(":", "_") + ".tar")
        r.sh(r.tool, "save", "-o", str(archive), one)
        r.sh("kind", "load", "image-archive", "--name", CLUSTER, str(archive))
    r.k("apply", "-f", "-", stdin=kc_postgres())
    r.k("rollout", "status", "statefulset/postgres", "--timeout=180s")
    r.k("apply", "-f", "-", stdin=marks_postgres())
    r.k("rollout", "status", "statefulset/chaos-marks", "--timeout=180s")
    r.psql(MARKS_TABLE, MARKS_POD)
    r.k(
        "create",
        "secret",
        "generic",
        "schedr-pg",
        "--from-literal=username=postgres",
        "--from-literal=password=schedr",
        check=False,
    )
    repository, tag = worker_image.rsplit(":", 1)
    server_repository, server_tag = server.rsplit(":", 1)
    r.sh(
        "helm",
        "--kube-context",
        CONTEXT,
        "-n",
        NAMESPACE,
        "upgrade",
        "--install",
        "schedr",
        str(ROOT / "charts" / "schedr"),
        "--set",
        "mode=distributed",
        "--set",
        "tls.mode=app",
        "--set",
        "master.replicas=2",
        "--set",
        "worker.replicas=3",
        "--set",
        "ui.enabled=false",
        "--set",
        "database.postgres.host=postgres",
        "--set",
        "database.postgres.name=postgres",
        "--set",
        "database.postgres.existingSecret=schedr-pg",
        "--set",
        f"image.repository={repository}",
        "--set",
        f"image.tag={tag}",
        "--set",
        "image.pullPolicy=Never",
        *[
            item
            for role in ("master", "webserver")
            for item in (
                "--set",
                f"{role}.image.repository={server_repository}",
                "--set",
                f"{role}.image.tag={server_tag}",
                "--set",
                f"{role}.image.pullPolicy=Never",
            )
        ],
        "--set",
        "master.resources.requests.cpu=100m",
        "--set",
        "worker.resources.requests.cpu=100m",
        "--set",
        "persistence.size=1Gi",
        "--set",
        "worker.persistence.size=1Gi",
        "--set",
        "worker.resources.requests.memory=512Mi",
        "--set",
        "worker.resources.limits.memory=1Gi",
    )
    r.k("rollout", "status", "statefulset/schedr-worker", "--timeout=600s")
    for _ in range(90):
        listed = cli(r, "dags", "list", check=False).stdout
        if "chaos" in listed:
            break
        time.sleep(2)
    else:
        # What the parse said of the file, from the database (the CLI lists DAGs only).
        said = r.psql("SELECT path || ': ' || coalesce(error::text, '') FROM parse_files")
        raise RuntimeError(f"the DAG `chaos` never appeared; the parse said: {said.strip()}")
    # No trigger of its own: started only by the load's `runs trigger`, nothing to unpause.


# As the stress suite's and kind-lib.sh's.
PG_IMAGE = (
    "docker.io/library/postgres:17.6-alpine"
    "@sha256:ef257d85f76e48da1c64832459b59fcaba1a4dac97bf5d7450c77753542eee94"
)


def kc_postgres() -> str:
    """Postgres's manifests, its image pinned."""
    return POSTGRES.replace("{image}", PG_IMAGE)


MARKS_POD = "chaos-marks-0"


def marks_postgres() -> str:
    """The marks' Postgres, on the control plane.

    Never stopped, no fault aims at it; its data in the pod, which lives as long as the run.
    """
    return f"""\
apiVersion: apps/v1
kind: StatefulSet
metadata: {{name: chaos-marks}}
spec:
  serviceName: chaos-marks
  replicas: 1
  selector: {{matchLabels: {{app: chaos-marks}}}}
  template:
    metadata: {{labels: {{app: chaos-marks}}}}
    spec:
      nodeSelector: {{node-role.kubernetes.io/control-plane: ""}}
      tolerations:
        - {{key: node-role.kubernetes.io/control-plane, operator: Exists, effect: NoSchedule}}
      containers:
        - name: postgres
          image: {PG_IMAGE}
          env: [{{name: POSTGRES_PASSWORD, value: schedr}}]
          ports: [{{containerPort: 5432}}]
          readinessProbe: {{exec: {{command: [pg_isready, -U, postgres]}}, periodSeconds: 2}}
---
apiVersion: v1
kind: Service
metadata: {{name: chaos-marks}}
spec: {{selector: {{app: chaos-marks}}, ports: [{{name: tcp-postgres, port: 5432}}]}}
"""


def cli(r: Run, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """The schedr CLI in the webserver's pod."""
    return r.k(
        "exec",
        "deploy/schedr-webserver",
        "-c",
        "schedr",
        "--",
        "schedr",
        *args,
        "--server",
        "http://127.0.0.1:8080",
        check=check,
    )


def trigger(r: Run) -> None:
    """A run of the load's DAG."""
    cli(r, "runs", "trigger", "chaos", check=False)


def pods(r: Run) -> list[dict[str, object]]:
    """The namespace's pods: name, UID, node, IP, labels."""
    items = json.loads(r.k("get", "pods", "-o", "json").stdout)["items"]
    return [
        {
            "name": p["metadata"]["name"],
            "uid": p["metadata"]["uid"],
            "node": p["spec"].get("nodeName"),
            "ip": p["status"].get("podIP"),
            "component": p["metadata"]["labels"].get("app.kubernetes.io/component"),
        }
        for p in items
    ]


def worker_nodes(r: Run) -> set[str]:
    """This cluster's worker nodes, as kind names their containers.

    The only ones a fault stops or changes (agy's review of M5.3f: a node name read from a pod
    went to the container tool).
    """
    named = r.sh("kind", "get", "nodes", "--name", CLUSTER).stdout.split()
    return {node for node in named if node.startswith(f"{CLUSTER}-worker")}


def address(value: object) -> str:
    """A pod's IPv4 address, checked as one before it reaches iptables."""
    return str(ipaddress.IPv4Address(str(value)))


def targets(r: Run, want: Callable[[list[dict[str, object]]], bool]) -> list[dict[str, object]]:
    """The pods once `want` holds of them, at most a minute: a fault aimed at none hits nothing."""
    for _ in range(60):
        seen = pods(r)
        if want(seen):
            return seen
        time.sleep(1)
    return pods(r)


def node_death(r: Run, fault: Fault, done: dict[str, object], nodes: set[str]) -> None:
    """A node of a master or a worker stopped for the fault's length, started again whatever."""
    seen = targets(r, lambda p: any(x["component"] == "worker" and x["node"] for x in p))
    done["pods_before"] = seen
    candidates = sorted(
        {str(p["node"]) for p in seen if p["component"] in ("master", "worker")} & nodes
    )
    if not candidates:
        return
    node = candidates[fault.pick % len(candidates)]
    done["node"] = node
    done["pods"] = [p["name"] for p in seen if p["node"] == node]
    done["begun"] = now()
    r.sh(r.tool, "stop", "-t", "0", "--", node)
    try:
        done["applied"] = True
        time.sleep(fault.length)
    finally:
        r.sh(r.tool, "start", "--", node, check=False)


def has_worker_and_master(seen: list[dict[str, object]]) -> bool:
    """Whether a worker and a master both have an address."""
    return all(
        any(x["component"] == kind and x["ip"] for x in seen) for kind in ("worker", "master")
    )


def running_long_task(r: Run) -> str | None:
    """The worker running the long task whose cleanup may move (`t00`), begun in the last 30 s.

    Dropped packets aimed at it test the cutoff (it runs past the lease and the move wait), as
    the simulator's aimed scenarios do. Waits up to 90 s for one (a run a minute).
    """
    for _ in range(90):
        try:
            worker = r.psql(
                "SELECT a.worker_id FROM attempts a JOIN task_instances i "
                "ON i.id = a.task_instance_id WHERE i.task_id = 't00' AND a.status IS NULL "
                "AND a.started_at > now() - interval '30 seconds' "
                "ORDER BY a.started_at DESC LIMIT 1"
            ).strip()
        except RuntimeError:
            worker = ""
        if re.fullmatch(r"schedr-worker-[0-9]+", worker):
            return worker
        time.sleep(1)
    return None


def packet_drop(
    r: Run, fault: Fault, done: dict[str, object], nodes: set[str], rng: random.Random
) -> None:
    """A worker's packets to the masters dropped, or both ways, for the fault's length.

    On every worker node; every rule put in is taken out again whatever happens.
    """
    seen = targets(r, has_worker_and_master)
    done["pods_before"] = seen
    workers = [p for p in seen if p["component"] == "worker" and p["ip"]]
    masters = [p for p in seen if p["component"] == "master" and p["ip"]]
    if not workers or not masters:
        return
    aimed = running_long_task(r)
    by_name = {str(p["name"]): p for p in workers}
    worker = by_name.get(aimed or "") or workers[fault.pick % len(workers)]
    done["aimed"] = aimed is not None and aimed in by_name
    both = rng.random() < HALF
    done.update(
        {
            "worker": worker["name"],
            "worker_uid": worker["uid"],
            "both_ways": both,
            "masters": [m["ip"] for m in masters],
        }
    )
    rules = []
    for master in masters:
        rules.append(["-s", address(worker["ip"]), "-d", address(master["ip"]), "-j", "DROP"])
        if both:
            rules.append(["-s", address(master["ip"]), "-d", address(worker["ip"]), "-j", "DROP"])
    applied: list[tuple[str, list[str]]] = []
    try:
        for node in sorted(nodes):
            for rule in rules:
                r.sh(r.tool, "exec", node, "iptables", "-I", "FORWARD", *rule)
                applied.append((node, rule))
        done["applied"] = bool(applied)
        time.sleep(fault.length)
    finally:
        for node, rule in applied:
            r.sh(r.tool, "exec", node, "iptables", "-D", "FORWARD", *rule, check=False)


def postgres_kill(r: Run, done: dict[str, object]) -> None:
    """Postgres's pod killed at once, its volume kept."""
    seen = targets(r, lambda p: any(x["name"] == "postgres-0" and x["uid"] for x in p))
    done["pods_before"] = seen
    pg = next((p for p in seen if p["name"] == "postgres-0"), None)
    if pg is None:
        return
    done.update({"pod": "postgres-0", "uid": pg["uid"], "node": pg["node"]})
    r.k("delete", "pod", "postgres-0", "--force", "--grace-period=0")
    done["applied"] = True


def rolling_upgrade(r: Run, fault: Fault, done: dict[str, object]) -> None:
    """The chart upgraded with a changed pod annotation: every master and worker replaced."""
    done["pods_before"] = pods(r)
    r.sh(
        "helm",
        "--kube-context",
        CONTEXT,
        "-n",
        NAMESPACE,
        "upgrade",
        "schedr",
        str(ROOT / "charts" / "schedr"),
        "--reuse-values",
        "--set-string",
        f"master.podAnnotations.chaos={fault.pick}",
        "--set-string",
        f"worker.podAnnotations.chaos={fault.pick}",
    )
    done["applied"] = True


def apply(r: Run, fault: Fault, rng: random.Random) -> dict[str, object]:
    """One fault, as executed, undone whatever happens meanwhile (agy's review of M5.3f).

    Its targets by identity, its commands' results in harness.log, the UTC moments it began and
    was undone, and whether it hit anything (`applied`).
    """
    done: dict[str, object] = {**dataclasses.asdict(fault), "begun": now(), "applied": False}
    nodes = worker_nodes(r)
    if fault.kind == "node_death":
        node_death(r, fault, done, nodes)
    elif fault.kind == "packet_drop":
        packet_drop(r, fault, done, nodes, rng)
    elif fault.kind == "postgres_kill":
        postgres_kill(r, done)
    elif fault.kind == "rolling_upgrade":
        rolling_upgrade(r, fault, done)
    done["undone"] = now()
    done["pods_after"] = pods(r)
    return done


def wait_for_runs(r: Run, limit: int) -> None:
    """Every run ended, or `limit` seconds: a run left over is a check's to say.

    A failed read (Postgres restarting) is tried again (agy's review of M5.3f).
    """
    started = time.monotonic()
    while time.monotonic() - started < limit:
        try:
            left = r.psql(
                "SELECT count(*) FROM runs WHERE state NOT IN ('success', 'failed')"
            ).strip()
        except RuntimeError:
            left = ""
        if left == "0":
            return
        time.sleep(5)


def keep(r: Run) -> None:
    """The record: the database's rows and the marks, Kubernetes's logs, the pods and events.

    Each part on its own: one failing (the database gone, say) costs that part, written into the
    record as what went wrong, not the rest.
    """

    def rows(sql: str, pod: str = "postgres-0") -> list[dict[str, object]] | str:
        try:
            out = r.psql(f"SELECT coalesce(json_agg(t), '[]') FROM ({sql}) t", pod)
        except RuntimeError as error:
            return f"not read: {error}"
        # json_agg of rows: a list of objects, as the statement builds it.
        return cast("list[dict[str, object]]", json.loads(out or "[]"))

    data = {
        "runs": rows("SELECT id, state FROM runs"),
        "attempts": rows(
            "SELECT i.run_id AS run, i.task_id AS task, a.try_number AS try, a.status, "
            "a.worker_id, a.started_at, a.ended_at, i.task_id IN ('t00', 't04', 't08') AS anywhere "
            "FROM attempts a JOIN task_instances i ON i.id = a.task_instance_id"
        ),
        "workers": rows("SELECT * FROM workers"),
        "marks": rows("SELECT * FROM chaos_marks ORDER BY at_ns", MARKS_POD),
    }
    (r.record / "database.json").write_text(json.dumps(data, indent=2, default=str))
    pods = r.k("get", "pods,events", "-o", "wide", check=False)
    (r.record / "pods-and-events.txt").write_text(pods.stdout + pods.stderr)
    r.sh("kind", "export", "logs", "--name", CLUSTER, str(r.record / "kind-logs"), check=False)


def main(argv: list[str]) -> int:
    """The command line: run, schedule or check."""
    parser = argparse.ArgumentParser(
        description="Chaos on kind: seeded faults, checked on guarantees"
    )
    sub = parser.add_subparsers(dest="action", required=True)
    for name in ("run", "schedule"):
        one = sub.add_parser(name)
        one.add_argument("--seed", type=int, required=True)
        one.add_argument("--faults", type=int, default=4)
    sub.add_parser("check").add_argument("record", type=Path)
    args = parser.parse_args(argv)
    if args.action == "schedule":
        print(
            json.dumps([dataclasses.asdict(f) for f in schedule(args.seed, args.faults)], indent=2)
        )
        return 0
    if args.action == "check":
        problems = check(args.record)
        print("\n".join(problems) or "ok: every guarantee held")
        return 1 if problems else 0
    return run_all(args.seed, args.faults)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
