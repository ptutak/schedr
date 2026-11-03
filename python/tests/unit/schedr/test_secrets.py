"""Secrets in the DAG body (M4.6a): placeholders, ``secrets=`` and what is refused."""

import json
from collections.abc import Callable
from typing import Any

import pytest

from schedr import CommandOperator, Dag, SecretPrefix, secrets, task, text
from schedr._model import SecretPrefixDecl, SecretRef, registry

Validate = Callable[[str, Any], None]


@task
def load(dsn: str, conn: dict[str, str]) -> None: ...


@task
def count() -> list[int]:
    return [1]


@task
def one(n: int) -> int:
    return n


def only_dag() -> Any:
    (dag,) = registry.take_defined()
    return dag


def test_placeholders_become_references(validate: Validate) -> None:
    with Dag("d"):
        load(t"postgres://u:{secrets['etl/db']['password']}@h", secrets.other["connections/db"])
    (task_json,) = [t.to_json() for t in only_dag().tasks]
    assert task_json["arguments"] == {
        "conn": {"ref": {"kind": "secret", "path": "connections/db", "storage": "other"}},
        "dsn": {
            "template": [
                {"text": "postgres://u:"},
                {"ref": {"kind": "secret", "path": "etl/db", "key": "password"}},
                {"text": "@h"},
            ]
        },
    }
    validate("Task", task_json)


def test_a_command_gets_secrets_in_env_and_declares_others(validate: Validate) -> None:
    with Dag("d"):
        CommandOperator(
            "dump",
            ["pg_dump"],
            env={
                "PGPASSWORD": secrets["etl/db"]["password"],
                "URL": t"https://{secrets['a']['b']}",
            },
            secrets=[
                "ci/deploy#token",
                secrets.other["ci/deploy"]["token"],
                "connections/db_eu",
                SecretPrefix("tenants"),
                SecretPrefix("x/", storage="other"),
                "ci/deploy#token",
            ],
        )
    task_obj = only_dag().tasks[0]
    assert task_obj.secrets == (
        SecretRef("ci/deploy", "token"),
        SecretRef("ci/deploy", "token", "other"),
        SecretRef("connections/db_eu"),
        SecretPrefixDecl("tenants/"),
        SecretPrefixDecl("x/", "other"),
    )
    task_json = task_obj.to_json()
    assert task_json["env"]["PGPASSWORD"] == [
        {"ref": {"kind": "secret", "path": "etl/db", "key": "password"}}
    ]
    validate("Task", task_json)
    for declared in task_json["secrets"]:
        validate("SecretDecl", declared)


def test_one_declaration_needs_no_list() -> None:
    with Dag("d"):
        CommandOperator("a", ["true"], secrets="ci/x#y")
        CommandOperator("b", ["true"], secrets=SecretPrefix("t/"))
    a, b = only_dag().tasks
    assert a.secrets == (SecretRef("ci/x", "y"),)
    assert b.secrets == (SecretPrefixDecl("t/"),)


def test_a_python_task_declares_secrets_and_override_replaces_them() -> None:
    @task(secrets=["etl/api#token"])
    def call() -> None: ...

    with Dag("d"):
        call()
        call.override(task_id="call_eu", secrets=[SecretPrefix("eu/")])()
        call.override(task_id="call_us")()
    first, eu, us = only_dag().tasks
    assert first.secrets == (SecretRef("etl/api", "token"),)
    assert eu.secrets == (SecretPrefixDecl("eu/"),)
    assert us.secrets == first.secrets
    assert "secrets" not in only_task_json_without_secrets()


def only_task_json_without_secrets() -> dict[str, Any]:
    with Dag("e"):
        one(1)
    return only_dag().tasks[0].to_json()


@pytest.mark.parametrize(
    ("make", "message"),
    [
        (lambda: secrets["etl/"], r"declare `SecretPrefix\('etl/'\)` in `secrets=`"),
        (lambda: secrets["/etl"]["k"], "no empty part"),
        (lambda: secrets["etl"][""], "the key after `#` is empty"),
        (lambda: secrets["etl"]["a#b"], "a key contains neither"),
        (
            lambda: secrets["etl#k"],
            r"`secrets\['etl'\]\['k'\]`, or write `secrets.value\('etl#k'\)`",
        ),
        (lambda: secrets.value("etl"), r"the whole secret is `secrets\['etl'\]`"),
        (lambda: secrets.value("etl#"), "the key after `#` is empty"),
        (lambda: secrets[""], "the path is empty"),
        (lambda: secrets.storage("a b"), "only letters, digits"),
        (lambda: secrets.storage("default"), "`default` names"),
        (lambda: SecretPrefix(""), "a prefix ends with `/`"),
        (lambda: SecretPrefix("a//"), "no empty part"),
    ],
)
def test_a_bad_name_is_refused(make: Callable[[], object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        make()


def test_a_name_is_text() -> None:
    with pytest.raises(TypeError, match="a secret is named by its path"):
        secrets[1]  # type: ignore[index]
    with pytest.raises(TypeError, match="a key is text"):
        secrets["etl"][1]  # type: ignore[index]
    with pytest.raises(TypeError, match="a secret is named by text"):
        secrets.value(1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="a prefix is text"):
        SecretPrefix(1)  # type: ignore[arg-type]


def test_storages_by_attribute_and_by_name() -> None:
    assert repr(secrets.other["a"]["b"]) == "secrets.other['a']['b']"
    assert repr(secrets.storage("vault-2")["a"]) == "secrets.storage('vault-2')['a']"
    assert repr(secrets) == "secrets"
    assert repr(secrets.other) == "secrets.other"
    assert repr(SecretPrefix("t", storage="o")) == "SecretPrefix('t/', storage='o')"
    with pytest.raises(AttributeError):
        _ = secrets._private


def test_a_placeholder_isnt_a_value() -> None:
    secret = secrets["etl/db"]["password"]
    for use in (str, bool, lambda s: s == "x"):
        with pytest.raises(TypeError, match=r"task_ctx\.secret"):
            use(secret)
    assert secret == secrets["etl/db"]["password"]
    assert len({secret, secrets["etl/db"]["password"], secrets.o["etl/db"]["password"]}) == 2
    assert SecretPrefix("a") == SecretPrefix("a/")
    assert SecretPrefix("a") != SecretPrefix("b")
    assert len({SecretPrefix("a"), SecretPrefix("a/")}) == 1


def test_a_secret_in_the_command_line_is_refused() -> None:
    with Dag("d"), pytest.raises(TypeError, match=r"in the command line.*process list"):
        CommandOperator("dump", ["pg_dump", t"--password={secrets['etl/db']['password']}"])
    with Dag("e"), pytest.raises(TypeError, match="in the command line"):
        CommandOperator("dump", ["echo", secrets["etl/db"]["password"]])


def test_a_whole_secret_in_text_is_refused() -> None:
    with Dag("d"), pytest.raises(TypeError, match="is a whole secret"):
        CommandOperator("dump", ["true"], env={"X": secrets["etl/db"]})
    with Dag("e"), pytest.raises(TypeError, match="is a whole secret"):
        load(t"{secrets['etl/db']}", secrets["etl/db"])


def test_a_secret_has_a_type() -> None:
    with Dag("d"), pytest.raises(TypeError, match=r"expects `int`.*is `str`"):
        one(secrets["etl/db"]["port"])  # type: ignore[arg-type]
    with Dag("e"), pytest.raises(TypeError, match=r"expects `str`.*is `dict\[str, str\]`"):
        load(secrets["etl/db"], secrets["etl/db"])  # type: ignore[arg-type]


def test_a_task_cant_be_mapped_over_a_secret() -> None:
    # A secret is never a list, so its type refuses it first (the master refuses it too).
    with Dag("d"), pytest.raises(TypeError, match=r"expects `list\[int\]`.*is `str`"):
        one.expand(n=secrets["etl/list"]["x"])


@pytest.mark.parametrize(
    ("value", "error", "message"),
    [
        (["etl/"], ValueError, "secrets=:.*SecretPrefix"),
        ([1], TypeError, "secrets= takes"),
        ({"a#b"}, TypeError, "secrets= takes a list"),
        ([object()], TypeError, "secrets= takes"),
    ],
)
def test_bad_declarations_are_refused(value: object, error: type[Exception], message: str) -> None:
    with Dag("d"), pytest.raises(error, match=message):
        CommandOperator("x", ["true"], secrets=value)  # type: ignore[arg-type]
    with pytest.raises(error, match=message):
        task(secrets=value)  # type: ignore[arg-type]
    registry.take_entries()


@task
def connect(host: str, password: str = secrets["etl/db"]["password"], *, n: int = 1) -> str:
    return f"{host}:{password}:{n}"


def test_a_secret_default_is_recorded_as_an_argument() -> None:
    # The runner calls the function with the recorded arguments only (review of M4.6a).
    ref = {"ref": {"kind": "secret", "path": "etl/db", "key": "password"}}
    with Dag("d"):
        connect("h")
        connect.override(task_id="given")("h", "given", n=2)
        connect.override(task_id="each").partial().expand(host=["a", "b"])
    tasks = only_dag().to_json()["tasks"]
    assert tasks[0]["arguments"] == {"host": {"literal": "h"}, "password": ref}
    assert tasks[1]["arguments"]["password"] == {"literal": "given"}
    assert tasks[2]["arguments"] == {"password": ref}


def test_a_t_string_default_is_recorded_as_an_argument() -> None:
    # Left out, it reached the function as a `Template` (Codex, review of M4.6e).
    @task
    def region(value: str = text(t"region={secrets['cfg']['region']}")) -> str:
        return value

    with Dag("d"):
        region()
        region.override(task_id="given")(text(t"region={secrets['cfg']['region']}"))
    tasks = only_dag().to_json()["tasks"]
    assert tasks[0]["arguments"]["value"] == tasks[1]["arguments"]["value"]
    assert "template" in json.dumps(tasks[0]["arguments"]["value"])


def test_a_secret_in_a_shell_command_says_to_use_env() -> None:
    with Dag("d"), pytest.raises(TypeError, match=r"in a shell command.*env=\{\"PGPASSWORD\""):
        CommandOperator("dump", t"pg_dump --password={secrets['etl/db']['password']}")


@pytest.mark.parametrize(
    ("storage", "label"),
    [
        ("other", "secrets.other['a']['k']"),
        ("vault-2", "secrets.storage('vault-2')['a']['k']"),
        ("storage", "secrets.storage('storage')['a']['k']"),
        ("_a", "secrets.storage('_a')['a']['k']"),
        ("class", "secrets.storage('class')['a']['k']"),
        ("value", "secrets.storage('value')['a']['k']"),
    ],
)
def test_a_storage_label_can_be_written_back(storage: str, label: str) -> None:
    assert SecretRef("a", "k", storage).label() == label
    # As Rust's `Display` writes it (crates/schedr-model/src/secret/tests.rs).
    assert repr(eval(label, {"secrets": secrets})) == label


def test_a_key_is_indexed_written_with_hash_or_given_a_default(validate: Validate) -> None:
    with Dag("d"):
        load(
            text(t"{secrets['etl/db']['password']}|{secrets.value('etl/db#user')}"),
            {"region": secrets.vault["cfg"].get("region", "eu"), "x": secrets.value("a#b", "")},
        )
    (task_json,) = [t.to_json() for t in only_dag().tasks]
    assert task_json["arguments"]["dsn"] == {
        "template": [
            {"ref": {"kind": "secret", "path": "etl/db", "key": "password"}},
            {"text": "|"},
            {"ref": {"kind": "secret", "path": "etl/db", "key": "user"}},
        ]
    }
    conn = task_json["arguments"]["conn"]
    assert conn["map"]["region"]["ref"] == {
        "kind": "secret",
        "path": "cfg",
        "key": "region",
        "storage": "vault",
        "default": "eu",
    }
    assert conn["map"]["x"]["ref"]["default"] == ""
    validate("Task", task_json)


def test_a_whole_secret_isnt_a_value_in_the_dag_body() -> None:
    whole = secrets["etl/db"]
    for use in (lambda: list(whole), lambda: len(whole), lambda: str(whole), lambda: bool(whole)):
        with pytest.raises(TypeError, match=r"secrets\['etl/db'\]` is a placeholder"):
            use()
    with pytest.raises(TypeError, match=r"give the default as text, `.get\('k', ''\)`"):
        whole.get("k")  # type: ignore[call-arg]


def test_a_default_is_never_declared() -> None:
    with Dag("d"), pytest.raises(ValueError, match="a default is for a value passed to a task"):
        CommandOperator("c", ["true"], secrets=[secrets["a"].get("k", "x")])


def test_text_takes_only_a_t_string() -> None:
    template = t"{secrets['a']['b']}"
    assert text(template) is template
    with pytest.raises(TypeError, match=r"text\(\) takes a t-string"):
        text("plain")  # type: ignore[arg-type]
