"""How type checkers read a DAG body: calls as their values, outcomes from `Data`."""

from datetime import datetime

from schedr import (
    Chosen,
    CommandOperator,
    Dag,
    Data,
    Done,
    Outcome,
    RunIf,
    Task,
    branch,
    dag_ctx,
    task,
)


class Found(Data, frozen=True):
    """Rows were found."""

    rows: list[str]


class Empty(Data, frozen=True):
    """Nothing to load."""

    reason: str


class Disk(Outcome):
    """What `df` found: each member is the exit code that means it."""

    ROOM = 0
    FULL = 1


with Dag("typed_flow", trigger=None):

    @task
    def find(day: datetime) -> Found | Empty:
        return Found([f"{day:%Y-%m-%d}.csv"])

    @task
    def load(found: Found) -> int:
        return len(found.rows)

    @task
    def count_files(rows: list[str]) -> int:
        return len(rows)

    @task(run_if=RunIf.ALL_SUCCEEDED)  # skipped with `load`, so never given None
    def report(loaded: int, files: int) -> None:
        print(f"loaded {loaded} of {files}")

    @task
    def note(state: Disk) -> None:
        print(state.name)

    @task
    def archive() -> None:
        print("archived")

    found = find(dag_ctx.run.planned_for)  # a `datetime` for type checkers
    rows = found.on(Found)  # `Found`: `.on` comes from `Data`
    loaded = load(rows)  # `int`
    files = count_files(rows.rows)  # `rows.rows` is `list[str]`
    done: Done = report(loaded, files)  # a task returning None: `Done`

    disk = CommandOperator("disk", "exit 0", outcomes=Disk)  # `CommandOperator[Disk]`
    note(disk.outcome)  # `Disk`
    stored = archive()

    @branch
    def keep() -> list[Task]:
        return [stored]

    chosen: Chosen = keep()
    done >> chosen >> stored  # `>>` orders Done and Chosen
    disk.on(Disk.ROOM) >> stored
