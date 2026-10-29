"""How type checkers read a DAG body (decision 57): checked by mypy and basedpyright, not run.

Every line must type-check, except those marked with an ignore comment: each of those must
be reported (mypy's `warn_unused_ignores`, basedpyright's unnecessary-ignore rule), so a
check that stops catching a mistake fails `make lint`.
"""

# The tasks here are signatures only: their bodies don't use their parameters.
# pyright: reportUnusedParameter=false

from dataclasses import dataclass
from datetime import datetime

from schedr import (
    Chosen,
    CommandOperator,
    Dag,
    Data,
    DefaultOutcome,
    Done,
    EmptyOperator,
    Outcome,
    Task,
    branch,
    chain,
    dag_ctx,
    on,
    task,
)
from schedr.types import ProcessResult


@dataclass
class Row:
    id: str


class Rows(Data, frozen=True):
    rows: list[Row]


class Unavailable(Data, frozen=True):
    reason: str


@dataclass
class Plain:
    n: int


@dataclass
class PlainOther:
    n: int


class Check(Outcome):
    CHANGED = 0
    UNCHANGED = 3


class Other(Outcome):
    X = 0


@task
def extract() -> list[Row]:
    return []


@task
def load(rows: list[Row]) -> int:
    return len(rows)


@task
def fetch() -> Rows | Unavailable:
    return Unavailable("down")


@task
def use_rows(rows: Rows | None) -> None: ...


@task
def outage(down: Unavailable) -> None: ...


@task
def plain_fetch() -> Plain | PlainOther:
    return Plain(1)


@task
def plain_use(p: Plain) -> None: ...


@task
def check() -> Check:
    return Check.CHANGED


@task
def react(c: Check) -> None: ...


@task
def wants_int(n: int) -> None: ...


@task
def wants_str(s: str) -> None: ...


@task
def wants_time(t: datetime) -> None: ...


@task
def side_effect() -> None: ...


with Dag("typed"):
    rows = extract()
    count = load(rows)
    wants_int(count)
    wants_str(rows[0].id)
    wants_int(rows)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]

    fetched = fetch()
    use_rows(fetched.on(Rows))
    use_rows(fetched.on(Rows, Unavailable))  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
    outage(fetched.on(Unavailable))
    outage(fetched.on(Rows))  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
    gate: Done = fetched.on(None)
    fetched.on(Rows) >> side_effect()

    plain_use(on(plain_fetch(), Plain))

    react(check().outcome)
    react(check())
    finished: Done = check().on(Check.CHANGED)
    finished >> EmptyOperator("after")

    probe = CommandOperator("probe", "exit 0", outcomes=Check)
    react(probe.outcome)
    probe.on(Check.CHANGED) >> EmptyOperator("changed")
    probe.on(Other.X)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
    result: ProcessResult = probe.on(Check.UNCHANGED)
    wants_str(probe.stdout)
    plain = CommandOperator("plain", "true")
    default: DefaultOutcome = plain.outcome
    react(plain.outcome)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]

    done: Done = side_effect()
    done >> side_effect()
    side_effect() >> done
    rows >> done
    count >> plain
    chain(rows, count)

    @branch
    def pick() -> list[Task]:
        return [done]

    chosen: Chosen = pick()
    count >> chosen
    chosen >> [done, plain]

    wants_time(dag_ctx.run.planned_for)
    wants_str(dag_ctx.run.id)
    wants_str(dag_ctx.dag_id)
    wants_int(dag_ctx.run.since)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]

    Rows(["x"])  # type: ignore[list-item]  # pyright: ignore[reportArgumentType]
