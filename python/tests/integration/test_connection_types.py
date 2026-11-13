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
from schedr.types import ProcessResult

# One worker runs them all, one after another (`make test-integration` runs the others on
# several): they share the containers' role, user, table and database names.
pytestmark = pytest.mark.xdist_group("containers")

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
    for task_id, task in result.tasks.items():
        assert task.state == "success", (task_id, task.log, [a.error for a in task.attempts])
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
    # The host field of its first key line: OpenSSH 10's ssh-keyscan writes a comment
    # (`# host:port SSH-2.0-...`) before each key, on standard output.
    host_names = next(
        line.split()[0]
        for line in Path(_env("SCHEDR_TEST_SSH_KNOWN_HOSTS")).read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )
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
        refused = f"{{type(e).__name__}}: {{e}}"
    with router() as client:
        _, out, _ = client.exec_command("echo hello from $(whoami)")
        return out.read().decode().strip() + " / " + refused

with Dag("c"):
    version()
""",
        {"router#key": key},
    )
    # Its message with it, to see why when another exception comes.
    assert outputs["version"].startswith(f"hello from {user} / BadHostKeyException: "), outputs


def test_commands_get_connections_as_their_tools_files(tmp_path: Path, api: str) -> None:
    """Each tool logs in from the files written for it, no password on a command line
    (decisions 184, 190): psql's `.pgpass`, mysql's option file, ssh's config, curl's."""
    pg = urlsplit(_env("SCHEDR_TEST_POSTGRES_URL"))
    my = urlsplit(_env("SCHEDR_TEST_MYSQL_URL"))
    ca = Path(_env("SCHEDR_TEST_MYSQL_CA")).read_text().rstrip("\0")
    host, port = _env("SCHEDR_TEST_SSH").rsplit(":", 1)
    user = _env("SCHEDR_TEST_SSH_USER")
    outputs = _run(
        tmp_path,
        f"""
from schedr import CommandOperator, Dag, secrets
from schedr.connections import HTTP, SQL, SSH

pg = SQL(
    "pg", url="postgresql://{pg.hostname}:{pg.port}/postgres?sslmode=disable",
    user={pg.username!r}, password=secrets["pg"]["password"],
)
my = SQL(
    "my", url="mysql://{my.hostname}:{my.port}/mysql?sslmode=verify-ca",
    user={my.username!r}, password=secrets["my"]["password"], ca_cert=secrets["my"]["ca"],
)
router = SSH(
    "router", host={host!r}, port={port}, user={user!r},
    private_key=secrets["router"]["key"],
    known_hosts={Path(_env("SCHEDR_TEST_SSH_KNOWN_HOSTS")).read_text()!r},
)
billing = HTTP("billing", base_url={api!r}, token=secrets["billing"]["token"])

with Dag("c"):
    CommandOperator(
        "psql", ["sh", "-c", 'psql "$DB_URL" -tAc "select current_user"'], connections={{"DB": pg}}
    )
    CommandOperator(
        "mysql",
        ["sh", "-c", 'mysql --defaults-file="$DB_MYSQL_CNF" -N -e "select current_user()"'],
        connections={{"DB": my}},
    )
    CommandOperator(
        "ssh", ["sh", "-c", 'ssh -F "$R_SSH_CONFIG" target "echo hello from \\\\$(whoami)"'],
        connections={{"R": router}},
    )
    CommandOperator(
        "curl", ["sh", "-c", 'curl -q -sS -K "$API_CURL_CONFIG" "$API_BASE_URL/invoices"'],
        connections={{"API": billing}},
    )
    # The same, read by the command itself.
    CommandOperator(
        "env",
        [
            "sh", "-c",
            'vars="$(schedr task-ctx connection pg --env P)" && eval "$vars"'
            ' && psql "$P_URL" -tAc "select 1"',
        ],
        connections=[pg],
    )
""",
        {
            "pg#password": pg.password or "",
            "my#password": my.password or "",
            "my#ca": ca,
            "router#key": Path(_env("SCHEDR_TEST_SSH_KEY_FILE")).read_text(),
            "billing#token": "s3cret-bearer",
        },
    )
    stdout = {
        task_id: output.stdout.strip()
        for task_id, output in outputs.items()
        if isinstance(output, ProcessResult)
    }
    assert stdout == {
        "psql": pg.username,
        "mysql": f"{my.username}@%",
        "ssh": f"hello from {user}",
        # The token reached the server, and was masked in the output.
        "curl": "/invoices Bearer ****",
        "env": "1",
    }


def test_mariadb_through_python_and_both_vendors_clients(tmp_path: Path) -> None:
    """MariaDB as MySQL (decision 196): logins, TLS modes, and one option file read by
    MySQL's client and MariaDB's (decision 195)."""
    maria = urlsplit(_env("SCHEDR_TEST_MARIADB_URL"))
    where = f"{maria.hostname}:{maria.port}"
    outputs = _run(
        tmp_path,
        f"""
from schedr import CommandOperator, Dag, secrets, task
from schedr.connections import SQL

root = SQL(
    "root", url="mariadb://{where}/mysql?sslmode=disable",
    user={maria.username!r}, password=secrets["maria"]["password"],
)
# MariaDB 11.4+ serves TLS with a certificate it made itself: encrypted, unchecked.
encrypted = SQL(
    "encrypted", url="mariadb://{where}/?sslmode=require",
    user={maria.username!r}, password=secrets["maria"]["password"],
)
# That certificate can't pass verify-full (the default) from PyMySQL.
verified = SQL(
    "verified", url="mysql://{where}/", user={maria.username!r},
    password=secrets["maria"]["password"],
)
# A password with a URL's structure in it, both ways (decision 188).
odd_url = SQL(
    "odd_url",
    url=t"mariadb://odd:{{secrets['odd']['pw']}}@{where}/?sslmode=require",
)
odd_apart = SQL(
    "odd_apart", url="mariadb://{where}/?sslmode=require", user="odd",
    password=secrets["odd"]["pw"],
)

def cipher(connection) -> str:
    cursor = connection.cursor()
    cursor.execute("SHOW STATUS LIKE 'Ssl_cipher'")
    return cursor.fetchone()[1]

@task(connections=[root, encrypted, verified, odd_url, odd_apart])
def logins() -> str:
    with root() as connection:
        cursor = connection.cursor()
        cursor.execute("DROP USER IF EXISTS 'odd'@'%'")
        # With values, PyMySQL formats the query: `%%` is a `%`.
        cursor.execute("CREATE USER 'odd'@'%%' IDENTIFIED BY %s", ({ODD!r},))
        plain = cipher(connection)
    with encrypted() as connection:
        tls = cipher(connection)
    try:
        with verified():
            refused = "no"
    except Exception as e:
        refused = type(e).__name__
    users = []
    for odd in (odd_url, odd_apart):
        with odd() as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT CURRENT_USER()")
            users.append(cursor.fetchone()[0])
    with root() as connection:
        connection.cursor().execute("DROP USER 'odd'@'%'")
    return f"{{plain!r}} {{bool(tls)}} {{refused}} {{users}}"

with Dag("c"):
    CommandOperator(
        "mysql_client",
        [
            "sh", "-c",
            'cp "$DB_MYSQL_CNF" "$OUT/my.cnf" && mysql --defaults-file="$DB_MYSQL_CNF" -N'
            ' -e "SHOW STATUS LIKE \\'Ssl_cipher\\'"',
        ],
        env={{"OUT": {str(tmp_path)!r}}},
        connections={{"DB": encrypted}},
    )
    logins()
""",
        {"maria#password": maria.password or "", "odd#pw": ODD},
    )
    assert outputs["logins"] == f"'' True OperationalError {['odd@%', 'odd@%']}"
    mysql_client = outputs["mysql_client"]
    assert isinstance(mysql_client, ProcessResult)
    assert mysql_client.stdout.startswith("Ssl_cipher\tTLS"), mysql_client
    # The same file, read by MariaDB's client in its container (its port the server's own
    # there): `[client-mariadb]` gives it TLS; MySQL's `loose-ssl-mode` only a warning.
    exec_ = _env("SCHEDR_TEST_MARIADB_EXEC").split()
    script = (
        "cat > /tmp/schedr-test.cnf && mariadb --defaults-file=/tmp/schedr-test.cnf"
        " --host=127.0.0.1 --port=3306 -N -e \"SHOW STATUS LIKE 'Ssl_cipher'\""
    )
    read = subprocess.run(
        [*exec_, "sh", "-c", script],
        input=(tmp_path / "my.cnf").read_bytes(),
        capture_output=True,
        check=False,
    )
    assert read.returncode == 0, read.stderr
    assert read.stdout.decode().startswith("Ssl_cipher\tTLS"), read
    assert read.stderr.decode().strip() == (
        "Warning: mariadb: unknown variable 'loose-ssl-mode=REQUIRED'"
    )


def test_http_async_with_gives_the_async_client(tmp_path: Path, api: str) -> None:
    outputs = _run(
        tmp_path,
        f"""
from schedr import Dag, secrets, task
from schedr.connections import HTTP

billing = HTTP("billing", base_url={api!r}, token=secrets["billing"]["token"])

with Dag("c"):
    @task(connections=[billing])
    async def fetch() -> str:
        async with billing() as client:
            assert type(client).__name__ == "AsyncClient"
            answer = await client.get("/invoices")
        return answer.text

    fetch()
""",
        {"billing#token": "s3cret"},
    )
    assert "/invoices" in str(outputs["fetch"])


def test_generic_opens_either_way_and_ssh_has_no_async_side(tmp_path: Path) -> None:
    outputs = _run(
        tmp_path,
        """
from schedr import Dag, task
from schedr.connections import SSH, Generic

flags = Generic("flags", region="eu")
box = SSH("box", host="localhost", user="u", insecure_skip_host_check=True)

with Dag("c"):
    @task(connections=[flags, box])
    async def reads() -> str:
        async with flags() as values:
            region = values["region"]
        with flags() as values:
            assert values["region"] == region
        try:
            async with box():
                raise AssertionError("opened")
        except TypeError as e:
            return f"{region}: {e}"

    reads()
""",
        {},
    )
    assert str(outputs["reads"]).startswith("eu: "), outputs
    assert "asynchronous context manager" in str(outputs["reads"]), outputs


def test_mongo_async_with_gives_its_async_database(tmp_path: Path) -> None:
    uri = _env("SCHEDR_TEST_MONGO_URI")
    outputs = _run(
        tmp_path,
        f"""
from schedr import Dag, task
from schedr.connections import Mongo

orders = Mongo("orders", uri={uri + "?tls=false"!r}, database="schedr_test")

with Dag("c"):
    @task(connections=[orders])
    async def count() -> int:
        async with orders() as db:
            assert type(db).__name__ == "AsyncDatabase"
            await db.async_orders.delete_many({{}})
            await db.async_orders.insert_one({{"n": 1}})
            return await db.async_orders.count_documents({{}})

    count()
""",
        {},
    )
    assert outputs["count"] == 1


def test_postgres_async_with_commits_and_rolls_back(tmp_path: Path) -> None:
    url = _env("SCHEDR_TEST_POSTGRES_URL")
    joined = "&" if "?" in url else "?"
    outputs = _run(
        tmp_path,
        f"""
from schedr import Dag, task
from schedr.connections import SQL

base = SQL("base", url={url + joined + "sslmode=disable"!r})

with Dag("c"):
    @task(connections=[base])
    async def writes() -> int:
        async with base() as db:
            assert type(db).__name__ == "AsyncConnection"
            await db.execute("CREATE TABLE IF NOT EXISTS async_rows (n int)")
            await db.execute("DELETE FROM async_rows")
            await db.execute("INSERT INTO async_rows VALUES (1)")
        try:
            async with base() as db:
                await db.execute("INSERT INTO async_rows VALUES (2)")
                raise ValueError("undone")
        except ValueError:
            pass
        async with base() as db:
            found = await db.execute("SELECT count(*) FROM async_rows")
            row = await found.fetchone()
            await db.execute("DROP TABLE async_rows")
        assert row is not None
        return int(row[0])

    writes()
""",
        {},
    )
    assert outputs["writes"] == 1


def test_mysql_async_with_says_to_use_with(tmp_path: Path) -> None:
    outputs = _run(
        tmp_path,
        """
from schedr import Dag, task
from schedr.connections import SQL

base = SQL("base", url="mysql://u:p@localhost/db?sslmode=disable")

with Dag("c"):
    @task(connections=[base])
    async def refused() -> str:
        try:
            async with base():
                raise AssertionError("opened")
        except TypeError as e:
            return str(e)

    refused()
""",
        {},
    )
    assert "use `with`" in str(outputs["refused"])
