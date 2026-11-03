"""The built-in connection types (decisions 174, 183): how they are defined and recorded.

Their clients against real servers are in ``tests/integration/test_connection_types.py``.
"""

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from schedr import secrets
from schedr.connection_types import _known_host, _private_file
from schedr.connections import HTTP, SQL, SSH, Generic, Mongo, connection
from schedr.runner.parse import file_result
from schedr.testing import task_context

TYPED = """
from schedr import secrets
from schedr.connections import HTTP, Generic

billing = HTTP("billing", base_url="https://billing.example.com", token=secrets["billing"]["token"])
flags = Generic("flags", region="eu", retries=3)
"""


def test_a_typed_connection_is_recorded_with_its_type_and_every_value(tmp_path: Path) -> None:
    (tmp_path / "typed.py").write_text(TYPED)
    sys.path.insert(0, str(tmp_path))
    try:
        recorded = {c["name"]: c for c in file_result("etl", tmp_path, "typed.py")["connections"]}
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("typed", None)
    billing = recorded["billing"]
    assert billing["type"] == "http"
    assert billing["params"]["token"] == {
        "ref": {"kind": "secret", "path": "billing", "key": "token"}
    }
    # Its defaults too: a command reads every value.
    assert billing["params"]["timeout"] == {"literal": 30.0}
    assert billing["params"]["user"] == {"literal": None}
    assert recorded["flags"] == {
        "name": "flags",
        "type": "generic",
        "params": {"region": {"literal": "eu"}, "retries": {"literal": 3}},
    }


@pytest.mark.parametrize(
    ("define", "message"),
    [
        (
            lambda: HTTP("a", base_url="u", token="t", user="me"),
            "a token or a user, not both",
        ),
        (lambda: HTTP("a", base_url="u", client_key="k"), "`client_key` without `client_cert`"),
        (lambda: SSH("a", host="h", user="u"), "`known_hosts` .* is needed"),
        (
            lambda: SSH("a", host="h", user="u", known_hosts="k", insecure_skip_host_check=True),
            "not both",
        ),
        (lambda: SQL("a", url="sqlite:///x.db"), r"postgresql:// or mysql://, not sqlite://"),
        (
            lambda: SQL("a", url=t"oracle://u:{secrets['db']['password']}@h/x"),
            "not oracle://",
        ),
        (lambda: HTTP("not a name", base_url="u"), "connection `not a name`"),
        (lambda: Mongo("a", uri="mongodb://h", database=3), "database"),
        # PyMySQL would ignore any other key, a TLS setting among them (decision 187).
        (
            lambda: SQL("a", url="mysql://u@h/x?ssl_verify_cert=true"),
            "takes only `sslmode`, not `ssl_verify_cert`",
        ),
        (
            lambda: SQL("a", url=t"mysql://u:{secrets['db']['password']}@h/x?ssl_ca=/x"),
            "not `ssl_ca`",
        ),
        (lambda: SQL("a", url="postgresql://h/x?sslmode=verify"), "`sslmode=verify`: one of"),
        # Given once (decision 188).
        (
            lambda: SQL("a", url="postgresql://etl:pw@h/x", password=secrets["db"]["password"]),
            "a password in the URL and `password=` both",
        ),
        (
            lambda: SQL("a", url=t"mysql://etl:{secrets['db']['pw']}@h/x", password="p"),
            "a password in the URL",
        ),
        (lambda: Mongo("a", uri="mongodb://me@h/", database="d", user="me"), "a user in the URL"),
        # The token goes to `base_url` only.
        (
            lambda: HTTP("a", base_url="https://api.example.com", check_path="https://x.example/"),
            "`check_path` is a path under `base_url`, not a URL",
        ),
        (
            lambda: HTTP("a", base_url="https://api.example.com", check_path="//x.example/"),
            "not a URL",
        ),  # type: ignore[arg-type]  # the mistake tested
    ],
)
def test_a_typed_connections_mistakes_are_import_errors(define: object, message: str) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        define()  # type: ignore[operator]  # each a lambda


def test_without_its_extra_opening_one_names_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    # `import schedr` needs none of them; the error says what to install.
    monkeypatch.setitem(sys.modules, "httpx", None)
    api = HTTP("api", base_url="https://api.example.com")
    with (
        pytest.raises(ImportError, match=r"needs `httpx`: install `schedr\[http\]`"),
        api.fn(base_url="https://api.example.com"),
    ):
        pass


def test_generic_gives_its_keys_as_a_dict() -> None:
    flags = Generic("flags", region="eu", retries=3)
    with flags.fn(region="eu", retries=3) as values:
        assert values == {"region": "eu", "retries": 3}


def test_a_check_of_ones_own_replaces_none_and_runs_on_the_client() -> None:
    @connection(n=1)
    def counter(n: int) -> Iterator[int]:
        yield n

    with pytest.raises(ValueError, match=r"no check: give it one with `@counter.check`"):
        counter.run_check()

    checked: list[int] = []

    @counter.check
    def positive(n: int) -> None:
        checked.append(n)
        assert n > 0

    assert positive(1) is None  # the decorator gives the function back
    with task_context(connections={"counter": -5}), pytest.raises(AssertionError):
        counter.run_check()
    assert checked == [1, -5]


def test_sql_takes_libpqs_sslmode_for_mysql_too() -> None:
    # Every mode, on either database; a t-string's query is read without its values.
    for mode in ("disable", "allow", "prefer", "require", "verify-ca", "verify-full"):
        SQL("a", url=f"mysql://u@h/x?sslmode={mode}")
        SQL("b", url=f"postgresql://u@h/x?sslmode={mode}&application_name=etl")
    SQL("c", url=t"mysql://u@h/x?sslmode={secrets['db']['mode']}")


def test_known_hosts_takes_comments_and_refuses_what_it_cant_read() -> None:
    line = (
        "example.com ssh-ed25519"
        " AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
    )
    assert _known_host(1, "# ops") == []
    assert _known_host(2, "   ") == []
    [(host, kind, _key)] = _known_host(3, line)
    assert (host, kind) == ("example.com", "ssh-ed25519")
    with pytest.raises(ValueError, match=r"line 4: markers \(`@revoked`\) aren't supported"):
        _known_host(4, f"@revoked {line}")
    with pytest.raises(ValueError, match="line 5 can't be read"):
        _known_host(5, "example.com ssh-ed25519 not-base64!")


def test_key_material_is_never_on_a_disk() -> None:
    with _private_file("-----BEGIN KEY-----") as path:
        assert str(path).startswith("/proc/self/fd/")
        assert path.read_text() == "-----BEGIN KEY-----"
    assert not path.exists()  # its descriptor closed


def test_a_value_in_a_types_url_is_recorded_url_encoded(tmp_path: Path) -> None:
    # Decision 188: a secret filled into the URL is only the text it stands for.
    (tmp_path / "urls.py").write_text(
        "from schedr import secrets\n"
        "from schedr.connections import SQL, Mongo\n"
        "pg = SQL('pg', url=t\"postgresql://etl:{secrets['db']['pw']}@h/x\")\n"
        "orders = Mongo('orders', uri=t\"mongodb://{secrets['m']['host']}/\", database='o')\n"
        "whole = SQL('whole', url=secrets['db']['url'], password=secrets['db']['pw'])\n"
    )
    sys.path.insert(0, str(tmp_path))
    try:
        recorded = {c["name"]: c for c in file_result("etl", tmp_path, "urls.py")["connections"]}
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("urls", None)
    assert recorded["pg"]["params"]["url"] == {
        "template": [
            {"text": "postgresql://etl:"},
            {"ref": {"kind": "secret", "path": "db", "key": "pw", "encode": "url"}},
            {"text": "@h/x"},
        ]
    }
    [_, ref] = recorded["orders"]["params"]["uri"]["template"][:2]
    assert ref["ref"]["encode"] == "url"
    # A whole secret is the URL as stored; a password of its own is never in it.
    assert recorded["whole"]["params"]["url"] == {
        "ref": {"kind": "secret", "path": "db", "key": "url"}
    }
