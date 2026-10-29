"""Routing by outcome: a union of dataclasses, a command's exit codes, and a branch."""

from schedr import (
    CommandOperator,
    Dag,
    Data,
    EmptyOperator,
    Outcome,
    RunIf,
    Task,
    branch,
    task,
)


class Rows(Data, frozen=True):
    """Rows fetched from one source."""

    rows: list[dict[str, str]]


class Unavailable(Data, frozen=True):
    """The source could not be reached."""

    reason: str


class Check(Outcome):
    """What the probe found: each member is the exit code that means it."""

    CHANGED = 0
    UNCHANGED = 1


with Dag("route_by_outcome", trigger=None):

    @task
    def fetch() -> Rows | Unavailable:
        return Rows([{"id": "1", "name": "Ada"}])

    @task
    def fetch_mirror() -> Rows | Unavailable:
        return Unavailable("mirror down")

    @task
    def load(primary: Rows | None, mirror: Rows | None) -> int:
        batch = primary or mirror
        assert batch is not None
        print(f"loading {len(batch.rows)}")
        return len(batch.rows)

    @task
    def report_outage(outage: Unavailable) -> None:
        print(outage.reason)

    @task
    def react(outcome: Check) -> str:
        print(outcome.name)
        return outcome.name

    @task
    def to_s3() -> None:
        print("s3")

    @task
    def to_gcs() -> None:
        print("gcs")

    primary = fetch()
    mirror = fetch_mirror()
    count = load(primary.on(Rows), mirror.on(Rows))
    report_outage(mirror.on(Unavailable))

    probe = CommandOperator("probe", command="exit 0", outcomes=Check)
    probe.on(Check.CHANGED) >> EmptyOperator("changed")
    probe.on(Check.UNCHANGED) >> EmptyOperator("unchanged")
    react(probe.outcome)

    s3 = to_s3()
    gcs = to_gcs()

    @branch
    def dest() -> list[Task]:
        return [s3]

    chosen = dest()
    count >> chosen
    chosen >> [s3, gcs]
    done = EmptyOperator("done")
    [s3, gcs] >> done
    all_ok = EmptyOperator("all_ok", run_if=RunIf.ALL_SUCCEEDED)
    [s3, gcs] >> all_ok
