"""How type checkers read a DAG body (decision 57): checked by mypy and basedpyright, not run.

Every line must type-check, except those marked with an ignore comment: each of those must
be reported (mypy's `warn_unused_ignores`, basedpyright's unnecessary-ignore rule), so a
check that stops catching a mistake fails `make lint`.
"""

# The tasks here are signatures only: their bodies don't use their parameters.
# pyright: reportUnusedParameter=false

from collections.abc import Iterator
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
    connection,
    dag_ctx,
    group,
    on,
    secrets,
    task,
    task_ctx,
    text,
)
from schedr.connections import HTTP, SQL
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


@task
def check_n(n: int) -> Check:
    return Check.CHANGED


@task
def fetch_n(n: int) -> Rows | Unavailable:
    return Rows([])


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

    # A mapped task's output: each element narrowed like a task's output.
    fetched_each = fetch_n.expand(n=[1, 2])
    each_rows: list[Rows] = fetched_each.on(Rows)
    each_either: list[Rows | Unavailable] = fetched_each.on(Rows, Unavailable)
    each_gate: list[Done] = fetched_each.on(None)
    checked_each = check_n.expand(n=[1, 2])
    each_changed: list[Done] = checked_each.on(Check.CHANGED)
    checked_each.on(Other.X)  # type: ignore[call-overload]  # pyright: ignore[reportArgumentType,reportCallIssue]

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

with Dag("grouped"):
    first = side_effect()
    with group("loading") as loading:
        rows_in_group = extract()
        load(rows_in_group)
    first >> loading >> side_effect()
    rows_in_group >> loading
    loading >> EmptyOperator("after_load")
    with group("more") as more:
        side_effect()
    loading >> more


@task(retries=1)
def side_with_options() -> None: ...


@task(retries=1)
def counted() -> int:
    return 1


with Dag("decorated"):
    marked: Done = side_with_options()
    wants_int(counted())
    wants_str(counted())  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]


@dataclass
class TypedParams:
    region: str = "eu"


with Dag("with_params", params=TypedParams):
    # A field is untyped (as an output's path, so strict checkers flag it as Any); the call is
    # typed by the class passed.
    wants_str(dag_ctx.params.region)  # pyright: ignore[reportAny]
    wants_str(dag_ctx.params(TypedParams).region)
    wants_int(dag_ctx.params(TypedParams).region)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]


@task
def connect(url: str, password: str, settings: dict[str, str]) -> None: ...


with Dag("with_secrets"):
    # A placeholder is typed as the value the task receives (decision 176).
    connect(
        text(t"postgresql://etl:{secrets['etl/db']['password']}@db/etl"),
        secrets.value("etl/db#password"),
        secrets["etl/db"],
    )
    connect(
        secrets.vault["cfg"].get("url", ""),
        secrets.value("etl/db#password", default=""),
        secrets.storage("vault-2")["cfg"],
    )
    CommandOperator(
        "dump",
        ["pg_dump"],
        env={"PGPASSWORD": secrets["etl/db"]["password"]},
        secrets=["etl/api#token", secrets["etl/api"]["token"], secrets["etl/cfg"]],
    )
    # The kinds don't mix: a whole secret isn't one value, a value isn't a whole secret.
    connect(
        secrets["etl/db"],  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
        secrets["etl/db"]["password"],
        secrets.value("etl/db#password"),  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
    )
    # A t-string passed whole is a `Template`: `text(...)` marks it.
    connect(
        t"{secrets['etl/db']['password']}",  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
        "x",
        {},
    )


@task(secrets=["etl/api#token"])
def reads_inside() -> int:
    token: str = task_ctx.secret("etl/api", "token")
    whole: dict[str, str] = task_ctx.secret("etl/api")
    value: str = task_ctx.secret_value("etl/api#token")
    wrong: dict[str, str] = task_ctx.secret("etl/api", "token")  # type: ignore[assignment]  # pyright: ignore[reportAssignmentType]
    return len(token) + len(whole) + len(value) + len(wrong)


@connection(base_url="https://api.example.com", token=secrets["etl/api"]["token"])
def orders_api(base_url: str, token: str) -> Iterator[dict[str, str]]:
    yield {"base_url": base_url, "token": token}


# A connection yielding a client of its own declares as any other, by itself or by name.
@task(connections=[orders_api, "warehouse"])
def opens() -> str:
    with orders_api() as api:
        url: str = api["base_url"]
        wrong: int = api["base_url"]  # type: ignore[assignment]  # pyright: ignore[reportAssignmentType]
        return url + str(wrong)


CommandOperator("export", ["true"], connections=[orders_api])


# A check takes what its connection yields, or something wider.
@orders_api.check
def reachable(api: dict[str, str]) -> None:
    assert api["base_url"]


@orders_api.check
def anything(api: object) -> None:
    assert api


@orders_api.check  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
def numbers(api: int) -> None:
    assert api > 0


billing = HTTP("billing", base_url="https://billing.example.com", token=secrets["billing"]["token"])
warehouse_db = SQL(
    "warehouse_db", url=text(t"postgresql://etl:{secrets['etl/db']['password']}@db/etl")
)
HTTP("no_name")  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]


# A typed connection gives its client's own type.
@task(connections=[billing, warehouse_db])
def typed_clients() -> int:
    with billing() as client:
        status: int = client.get("/invoices").status_code
        wrong: str = client.get("/invoices").status_code  # type: ignore[assignment]  # pyright: ignore[reportAssignmentType]
    with warehouse_db() as db:
        db.cursor().execute("SELECT 1")
    return status + len(wrong)


# A command gets connections as variables by prefix: functions, typed ones, or names.
CommandOperator(
    "dump",
    ["sh", "-c", 'psql "$DB_URL"'],
    connections={"DB": warehouse_db, "API": billing, "ORDERS": orders_api, "W": "warehouse"},
)
CommandOperator("bad", ["true"], connections={"DB": 3})  # type: ignore[dict-item]  # pyright: ignore[reportArgumentType]
