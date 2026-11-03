"""The built-in connection types against real servers (decision 185), through a run: the
agent renders each connection (its secrets fetched and masked), the task gets the type's
client and runs its check.

HTTP is served here; the rest come from ``make test-integration`` (Postgres, MySQL, Mongo and
an SSH server in containers; MySQL's CA), found by ``SCHEDR_TEST_*`` variables.
"""

import os
import subprocess
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from schedr.testing import run_dag

FOR = datetime(2026, 10, 15, 3, tzinfo=UTC)
# A password a URL would misread: `@` ends the user part, `/` the host, `?` and `#` the path.
ODD = "p@ss/w#rd?x&y=1"


def _env(name: str) -> str:
    value = os.environ.get(name)
    assert value, f"{name}: started by `make test-integration`"
    return value


def _run(tmp_path: Path, source: str, secrets: dict[str, str]) -> dict[str, object]:
    """Runs DAG `c` of ``source``; its tasks' outputs, each task having succeeded."""
    (tmp_path / "c.py").write_text(source)
    result = run_dag(tmp_path / "c.py", "c", for_=FOR, secrets=secrets)
    for task in result.tasks.values():
        assert task.state == "success", (task.log, [a.error for a in task.attempts])
    return {task_id: task.output for task_id, task in result.tasks.items()}


class _Api(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = f"{self.path} {self.headers.get('Authorization')}".encode()
        self.send_response(200 if self.path != "/broken" else 500)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def api() -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Api)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


def test_http_sends_its_token_and_checks_its_path(tmp_path: Path, api: str) -> None:
    outputs = _run(
        tmp_path,
        f"""
from schedr import Dag, secrets, task
from schedr.connections import HTTP

billing = HTTP("billing", base_url={api!r}, token=secrets["billing"]["token"], check_path="/health")
broken = HTTP("broken", base_url={api!r}, check_path="/broken")

@task(connections=[billing, broken])
def fetch() -> str:
    billing.run_check()
    try:
        broken.run_check()
    except Exception as e:
        failed = type(e).__name__
    with billing() as client:
        return client.get("/invoices").text + " / " + failed

with Dag("c"):
    fetch()
""",
        {"billing#token": "s3cret-bearer"},
    )
    # The token reached the server, and was masked in what the task stored.
    assert outputs["fetch"] == "/invoices Bearer **** / HTTPStatusError"


def test_sql_on_postgres_and_mysql_verifies_tls_unless_told_not_to(tmp_path: Path) -> None:
    pg = urlsplit(_env("SCHEDR_TEST_POSTGRES_URL"))
    my = urlsplit(_env("SCHEDR_TEST_MYSQL_URL"))
    # MySQL writes the CA it makes with a NUL at the end.
    ca = Path(_env("SCHEDR_TEST_MYSQL_CA")).read_text().rstrip("\0")
    pg_url = (
        f"postgresql://{pg.username}:{{secrets['pg']['password']}}@{pg.hostname}:{pg.port}/postgres"
    )
    my_url = f"mysql://{my.username}:{{secrets['my']['password']}}@{my.hostname}:{my.port}/mysql"
    outputs = _run(
        tmp_path,
        f"""
from schedr import Dag, secrets, task
from schedr.connections import SQL

# The test servers: Postgres without TLS, MySQL with a certificate of its own CA.
pg = SQL("pg", url=t"{pg_url}?sslmode=disable")
my = SQL("my", url=t"{my_url}?sslmode=disable")
# verify-full by default: Postgres has no TLS; MySQL's certificate names no host.
pg_default = SQL("pg_default", url=t"{pg_url}")
my_default = SQL("my_default", url=t"{my_url}")
# MySQL's certificate checked against its CA.
my_tls = SQL("my_tls", url=t"{my_url}?sslmode=verify-ca", ca_cert=secrets["my"]["ca"])
# A password with a URL's structure in it, both ways (decision 188): filled into the URL
# (encoded), and as a value of its own.
pg_odd = SQL(
    "pg_odd",
    url=t"postgresql://odd:{{secrets['odd']['pw']}}@{pg.hostname}:{pg.port}/postgres?sslmode=disable",
)
my_odd = SQL(
    "my_odd",
    # No TLS: MySQL 8's first login sends the password RSA-encrypted, the path PyMySQL 1.2.3
    # breaks. WORKAROUND(pymysql==1.2.3): passes through `_CachingSha2Password`.
    url="mysql://{my.hostname}:{my.port}/?sslmode=disable",
    user="odd",
    password=secrets["odd"]["pw"],
)

@task(connections=[pg, my, pg_default, my_default, my_tls, pg_odd, my_odd])
def add() -> str:
    with pg() as connection:
        cursor = connection.cursor()
        cursor.execute("DROP ROLE IF EXISTS odd")
        cursor.execute("CREATE ROLE odd LOGIN PASSWORD '{ODD}'")
    with my() as connection:
        cursor = connection.cursor()
        cursor.execute("DROP USER IF EXISTS 'odd'@'%'")
        cursor.execute("CREATE USER 'odd'@'%' IDENTIFIED BY '{ODD}'")
    users = []
    for db in (pg_odd, my_odd):
        with db() as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT current_user" if db is pg_odd else "SELECT CURRENT_USER()")
            users.append(cursor.fetchone()[0])
    sums = []
    for db in (pg, my):
        db.run_check()
        with db() as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT 1 + 1")
            sums.append(cursor.fetchone()[0])
    refused = []
    for db in (pg_default, my_default):
        try:
            db.run_check()
            refused.append("no")
        except Exception as e:
            refused.append(type(e).__name__)
    with my_tls() as connection:
        cursor = connection.cursor()
        cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_version'")
        tls = cursor.fetchone()[1]
    return f"{{sums}} {{refused}} {{tls}} {{users}}"

with Dag("c"):
    add()
""",
        {
            "pg#password": pg.password or "",
            "my#password": my.password or "",
            "my#ca": ca,
            "odd#pw": ODD,
        },
    )
    assert outputs["add"] == (
        "[2, 2] ['OperationalError', 'OperationalError'] TLSv1.3 ['odd', 'odd@%']"
    )


def test_mongo_gives_its_database(tmp_path: Path) -> None:
    uri = _env("SCHEDR_TEST_MONGO_URI")
    outputs = _run(
        tmp_path,
        f"""
from schedr import Dag, task
from schedr.connections import Mongo

# The test server has no TLS: said in the URI. Without it, TLS is required (decision 189).
orders = Mongo("orders", uri={uri + "?tls=false"!r}, database="schedr_test")
verified = Mongo("verified", uri={uri!r}, database="schedr_test", timeout=2)

@task(connections=[orders, verified])
def count() -> int:
    orders.run_check()
    try:
        verified.run_check()
    except Exception as e:
        assert type(e).__name__ == "ServerSelectionTimeoutError", e
        assert "SSL" in str(e), e
    else:
        raise AssertionError("connected without TLS")
    with orders() as db:
        db.orders.delete_many({{}})
        db.orders.insert_many([{{"n": 1}}, {{"n": 2}}])
        return db.orders.count_documents({{}})

with Dag("c"):
    count()
""",
        {},
    )
    assert outputs["count"] == 2


def test_ssh_logs_in_with_its_key_and_trusts_only_known_hosts(tmp_path: Path) -> None:
    host, port = _env("SCHEDR_TEST_SSH").rsplit(":", 1)
    user = _env("SCHEDR_TEST_SSH_USER")
    key = Path(_env("SCHEDR_TEST_SSH_KEY_FILE")).read_text()
    # As OpenSSH writes them: a comment, a blank line.
    known_hosts = "# the test server\n\n" + Path(_env("SCHEDR_TEST_SSH_KNOWN_HOSTS")).read_text()
    # Another host key, trusted for that host instead of its own.
    other = tmp_path / "other"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", other], check=True)
    kind, encoded = Path(f"{other}.pub").read_text().split()[:2]
    host_names = Path(_env("SCHEDR_TEST_SSH_KNOWN_HOSTS")).read_text().split()[0]
    impostor_hosts = f"{host_names} {kind} {encoded}\n"
    outputs = _run(
        tmp_path,
        f"""
from schedr import Dag, secrets, task
from schedr.connections import SSH

router = SSH(
    "router", host={host!r}, port={port}, user={user!r},
    private_key=secrets["router"]["key"], known_hosts={known_hosts!r},
)
# The same host, but another key trusted: refused.
impostor = SSH(
    "impostor", host={host!r}, port={port}, user={user!r},
    private_key=secrets["router"]["key"], known_hosts={impostor_hosts!r},
)

@task(connections=[router, impostor])
def version() -> str:
    router.run_check()
    try:
        with impostor():
            refused = "no"
    except Exception as e:
        refused = type(e).__name__
    with router() as client:
        _, out, _ = client.exec_command("echo hello from $(whoami)")
        return out.read().decode().strip() + " / " + refused

with Dag("c"):
    version()
""",
        {"router#key": key},
    )
    assert outputs["version"] == f"hello from {user} / BadHostKeyException"
