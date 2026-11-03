import socket
import sys
import threading
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from schedr import (
    CommandOperator,
    Connection,
    ConnectionDeniedError,
    ConnectionNotFoundError,
    Dag,
    connection,
    secrets,
    task,
)
from schedr._model import registry
from schedr.connections import take_connections
from schedr.runner.parse import file_result
from schedr.runner.protocol import Channel
from schedr.runner.task import execute
from schedr.testing import task_context

DAG = """
from collections.abc import Iterator
from datetime import date

from schedr import Dag, connection, secrets, task

@connection(
    uri=t"mongodb://{secrets['mongo/default']['user']}@db",
    database="orders",
    since=date(2026, 1, 2),
)
def mongodb_default(uri: str, database: str, since: date, port: int = 27017) -> Iterator[str]:
    yield f"{uri}/{database}:{port} since {since.isoformat()}"

@task(connections=[mongodb_default])
def count() -> str:
    with mongodb_default() as client:
        return client

@task
def undeclared() -> str:
    with mongodb_default() as client:
        return client

with Dag("d"):
    count()
    undeclared()
"""

RUN = {
    "dag_id": "d",
    "run_id": "0190a3e5-0000-7000-8000-000000000000",
    "kind": "manual",
    "planned_for": "2026-03-04T05:06:07Z",
    "since": "2026-03-03T05:06:07Z",
    "try_number": 1,
    "map_index": None,
}


@pytest.fixture
def root(tmp_path: Path) -> Iterator[Path]:
    (tmp_path / "d.py").write_text(DAG)
    saved = list(sys.path)
    sys.path.insert(0, str(tmp_path))
    yield tmp_path
    sys.path[:] = saved


def request_for(root: Path, task_id: str = "count") -> dict[str, Any]:
    message = file_result("etl", root, "d.py")
    (task,) = [t for t in message["dags"][0]["tasks"] if t["id"] == task_id]
    return {
        "type": "task",
        "dag_file": {"root": "etl", "root_dir": str(root), "path": "d.py"},
        "dag_id": "d",
        "task": task,
        "inputs": {},
        "run": RUN,
        "sys_path": [str(root)],
    }


def test_the_parse_records_a_connections_values_never_a_secrets(root: Path) -> None:
    message = file_result("etl", root, "d.py")
    assert message["error"] is None
    assert message["connections"] == [
        {
            "name": "mongodb_default",
            "params": {
                "uri": {
                    "template": [
                        {"text": "mongodb://"},
                        {"ref": {"kind": "secret", "path": "mongo/default", "key": "user"}},
                        {"text": "@db"},
                    ]
                },
                "database": {"literal": "orders"},
                "since": {"literal": {"$date": "2026-01-02"}},
                "port": {"literal": 27017},
            },
        }
    ]


def test_a_default_left_out_is_recorded_a_secret_one_to_be_filled_in(tmp_path: Path) -> None:
    # A command reads every value; a secret default reaches the function only filled in.
    (tmp_path / "defaults.py").write_text(
        "from collections.abc import Iterator\n"
        "from schedr import connection, secrets\n"
        "@connection(host='db')\n"
        "def defaults(\n"
        "    host: str,\n"
        "    pool: int = 3,\n"
        "    password: str = secrets['etl/db']['password'],\n"
        "    hook: object = object(),\n"
        ") -> Iterator[str]:\n"
        "    yield host\n"
    )
    assert file_result("etl", tmp_path, "defaults.py")["connections"] == [
        {
            "name": "defaults",
            "params": {
                "host": {"literal": "db"},
                "pool": {"literal": 3},
                "password": {"ref": {"kind": "secret", "path": "etl/db", "key": "password"}},
            },
        }
    ]


def test_a_root_named_local_records_its_connections(root: Path) -> None:
    # `local` is also the name given to DAGs outside any root: not a reason to drop them.
    assert [c["name"] for c in file_result("local", root, "d.py")["connections"]] == [
        "mongodb_default"
    ]


def test_a_connection_imported_from_another_file_is_that_files(tmp_path: Path) -> None:
    (tmp_path / "shared.py").write_text(
        "from collections.abc import Iterator\n"
        "from schedr import connection\n"
        "@connection(n=1)\n"
        "def shared(n: int) -> Iterator[int]:\n"
        "    yield n\n"
    )
    (tmp_path / "user.py").write_text("import shared\n")
    saved = list(sys.path)
    sys.path.insert(0, str(tmp_path))
    try:
        assert "connections" not in file_result("etl", tmp_path, "user.py")
        sys.modules.pop("shared", None)
        assert file_result("etl", tmp_path, "shared.py")["connections"] == [
            {"name": "shared", "params": {"n": {"literal": 1}}}
        ]
    finally:
        sys.path[:] = saved
        sys.modules.pop("shared", None)


def test_two_connections_of_one_name_in_a_file_fail_its_import(tmp_path: Path) -> None:
    body = "@connection()\ndef twice() -> Iterator[int]:\n    yield 1\n"
    (tmp_path / "x.py").write_text(
        "from collections.abc import Iterator\nfrom schedr import connection\n" + body + body
    )
    message = file_result("etl", tmp_path, "x.py")
    assert message["error"]["message"] == "connection `twice` is defined twice in x.py"
    assert "connections" not in message
    assert take_connections() == []


def test_a_task_opens_a_connection_with_the_agents_values(root: Path) -> None:
    ours, theirs = socket.socketpair()
    asked: list[dict[str, Any]] = []
    with ours, theirs:
        agent = Channel(theirs)

        def serve() -> None:
            asked.append(agent.receive())
            agent.send(
                {
                    "type": "connection",
                    "values": {
                        "uri": "mongodb://etl@db",
                        "database": "orders",
                        "since": {"$date": "2026-01-02"},
                    },
                }
            )

        server = threading.Thread(target=serve)
        server.start()
        result = execute(request_for(root), channel=Channel(ours))
        server.join()
    assert asked == [{"type": "connection_get", "name": "mongodb_default"}]
    assert result["type"] == "task_succeeded", result
    assert result["output"] == "mongodb://etl@db/orders:27017 since 2026-01-02"


@pytest.mark.parametrize(
    ("kind", "exception"),
    [("connection_not_found", "ConnectionNotFoundError"), ("secret_not_found", "SecretError")],
)
def test_a_connection_the_agent_cant_open_fails_the_attempt_with_its_kind(
    root: Path, kind: str, exception: str
) -> None:
    ours, theirs = socket.socketpair()
    with ours, theirs:
        agent = Channel(theirs)

        def serve() -> None:
            agent.receive()
            agent.send({"type": "connection_failed", "kind": kind, "message": "no: x"})

        server = threading.Thread(target=serve)
        server.start()
        result = execute(request_for(root), channel=Channel(ours))
        server.join()
    assert result["type"] == "task_failed"
    assert result["kind"] == kind
    assert result["error"]["exception"] == exception
    assert result["error"]["message"] == "no: x"


def test_without_the_agent_a_connection_is_unknown(root: Path) -> None:
    result = execute(request_for(root))
    assert result["kind"] == "connection_not_found"
    assert "task_context(connections=" in result["error"]["message"]


def define_local() -> Connection[str]:
    @connection(uri=secrets["mongo/default"]["uri"])
    def local(uri: str) -> Iterator[str]:
        yield f"real {uri}"

    return local


def test_task_context_gives_doubles() -> None:
    local = define_local()
    assert isinstance(local, Connection)
    assert local.name == "local"
    fake = object()
    with task_context(connections={"local": fake}), local() as client:
        assert client is fake
    with (
        task_context(connections={}),
        pytest.raises(ConnectionNotFoundError, match="no double"),
    ):
        local().__enter__()
    with (
        task_context(),
        pytest.raises(ConnectionNotFoundError, match=r"task_context\(connections="),
    ):
        local().__enter__()


def test_outside_a_task_the_function_is_called_through_fn() -> None:
    local = define_local()
    with pytest.raises(RuntimeError, match=r"local\.fn\(\.\.\.\)"):
        local().__enter__()
    with local.fn(uri="u") as client:
        assert client == "real u"


def test_a_connection_is_a_generator_function() -> None:
    with pytest.raises(TypeError, match="generator function"):

        @connection()  # type: ignore[arg-type]  # the mistake tested
        def plain() -> int:
            return 1


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"uri": "u", "extra": 1}, "no parameter `extra`"),
        ({}, "no value for `uri`"),
        ({"uri": 3}, "uri"),
    ],
)
def test_a_connections_values_are_checked_against_its_parameters(
    values: dict[str, object], message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):

        @connection(**values)
        def checked(uri: str, port: int = 1) -> Iterator[str]:
            yield f"{uri}:{port}"


def test_a_connection_over_1_mib_fails_at_import() -> None:
    # Its values travel with each assignment of a task opening it.
    with pytest.raises(ValueError, match=r"its values take 10486\d\d bytes, over the 1 MiB"):

        @connection(host="x" * 1024 * 1024)
        def huge(host: str) -> Iterator[str]:
            yield host


def test_a_container_must_fit_one_form_of_a_union() -> None:
    # Each element fits one form or the other; the dict as a whole fits neither.
    with pytest.raises(TypeError, match="options"):

        @connection(options={"port": 5432, "password": secrets["db"]["password"]})
        def mixed(options: dict[str, int] | dict[str, str]) -> Iterator[object]:
            yield options


def test_a_connections_parameters_are_named() -> None:
    with pytest.raises(TypeError, match="positional-only"):

        @connection(a=1)
        def positional(a: int, /) -> Iterator[int]:
            yield a

    with pytest.raises(TypeError, match="are named"):

        @connection()
        def variadic(*a: int) -> Iterator[int]:
            yield len(a)


def test_a_connections_name_is_an_identifier() -> None:
    def generator() -> Iterator[int]:
        yield 1

    generator.__name__ = "not valid"
    with pytest.raises(ValueError, match="connection `not valid`"):
        connection()(generator)


def test_a_date_value_is_stored_tagged() -> None:
    @connection(day=date(2026, 1, 2))
    def dated(day: date) -> Iterator[date]:
        yield day

    with dated.fn(day=date(2026, 1, 2)) as day:
        assert day == date(2026, 1, 2)


def test_the_declared_connections_are_in_the_dag_json(root: Path) -> None:
    tasks = {t["id"]: t for t in file_result("etl", root, "d.py")["dags"][0]["tasks"]}
    assert tasks["count"]["connections"] == ["mongodb_default"]
    assert "connections" not in tasks["undeclared"]


def test_the_agents_refusal_of_an_undeclared_one_fails_the_attempt(root: Path) -> None:
    ours, theirs = socket.socketpair()
    with ours, theirs:
        agent = Channel(theirs)

        def serve() -> None:
            agent.receive()
            agent.send(
                {
                    "type": "connection_failed",
                    "kind": "connection_denied",
                    "message": "connection `mongodb_default` isn't declared",
                }
            )

        server = threading.Thread(target=serve)
        server.start()
        result = execute(request_for(root, "undeclared"), channel=Channel(ours))
        server.join()
    assert result["kind"] == "connection_denied"
    assert result["error"]["exception"] == "ConnectionDeniedError"


def test_a_task_function_in_a_test_opens_only_what_it_declares() -> None:
    local = define_local()

    @task(connections=[local])
    def declared() -> object:
        with local() as client:
            return client

    @task(connections=["local"])
    def by_name() -> object:
        with local() as client:
            return client

    @task
    def plain() -> object:
        with local() as client:
            return client

    def not_a_task() -> object:
        with local() as client:
            return client

    fake = object()
    with task_context(connections={"local": fake}):
        assert declared() is fake
        assert by_name() is fake
        assert declared.override(connections=[])  # a new function; this one is unchanged
        with pytest.raises(ConnectionDeniedError, match="isn't declared in the task's"):
            plain()
        with pytest.raises(ConnectionDeniedError):
            declared.override(connections=[])()
        # A plain function isn't checked: it declares nothing.
        assert not_a_task() is fake


@pytest.mark.parametrize(
    ("given", "error", "message"),
    [
        ([3], TypeError, "takes `@connection` functions or their names, not int"),
        (["no good"], ValueError, "connections=: "),
    ],
)
def test_connections_takes_functions_or_names(
    given: list[object], error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        task(connections=given)  # type: ignore[arg-type]  # the mistake tested


def test_a_command_declares_connections_too() -> None:
    local = define_local()
    with Dag("c"):
        CommandOperator(
            "show",
            ["schedr", "task-ctx", "connection", "local", "--json"],
            connections=[local, "local", "other"],
        )
    (dag,) = registry.take_defined()
    (show,) = dag.tasks
    assert show.connections == ("local", "other")
    assert show.to_json()["connections"] == ["local", "other"]


def test_a_command_given_connections_by_prefix_declares_and_names_them() -> None:
    local = define_local()
    with Dag("c"):
        CommandOperator(
            "show",
            ["sh", "-c", 'psql "$DB_URL"'],
            connections={"DB": local, "OTHER": "other", "AGAIN": "local"},
        )
    (dag,) = registry.take_defined()
    (show,) = dag.tasks
    assert show.connections == ("local", "other")
    assert show.to_json()["connection_env"] == {"AGAIN": "local", "DB": "local", "OTHER": "other"}


def test_a_command_without_prefixes_writes_no_connection_env() -> None:
    with Dag("c"):
        CommandOperator("show", "true", connections=["other"])
    (dag,) = registry.take_defined()
    assert "connection_env" not in dag.tasks[0].to_json()


@pytest.mark.parametrize(
    ("given", "env", "error", "message"),
    [
        ({"1DB": "other"}, {}, ValueError, "`1DB`: a prefix is letters"),
        ({"DB-X": "other"}, {}, ValueError, "`DB-X`: a prefix is letters"),
        ({"Schedr_db": "other"}, {}, ValueError, "SCHEDR are schedr's own"),
        ({"DB": "other"}, {"DB_URL": "x"}, ValueError, "`DB_URL` in env= starts with"),
        ({"DB": 3}, {}, TypeError, "takes `@connection` functions"),
        ({"DB": "not a name"}, {}, ValueError, "connections="),
        ({1: "other"}, {}, TypeError, "a prefix is a string"),
    ],
)
def test_a_prefix_a_variable_cant_start_with_is_refused(
    given: dict[object, object], env: dict[str, str], error: type[Exception], message: str
) -> None:
    with Dag("c"), pytest.raises(error, match=message):
        CommandOperator("show", "true", connections=given, env=env)  # type: ignore[arg-type]


def test_a_test_refuses_a_file_holding_two_connections_by_one_name() -> None:
    """A test runs the connection the parse recorded: of two by one name it can't tell."""
    from types import ModuleType  # noqa: PLC0415

    from schedr.runner.task import TaskSetupError, defined_connection  # noqa: PLC0415

    def opens() -> Iterator[str]:
        yield "x"

    real = connection()(opens)
    module = ModuleType("m47c_two_connections")
    module.real = real  # type: ignore[attr-defined]
    module.alias = real  # type: ignore[attr-defined]
    sys.modules[module.__name__] = module
    try:
        assert defined_connection(module.__name__, "opens") is real
        module.decoy = Connection(opens, {}, name="opens")  # type: ignore[attr-defined]
        with pytest.raises(TaskSetupError, match="2 connections named `opens`"):
            defined_connection(module.__name__, "opens")
    finally:
        del sys.modules[module.__name__]
