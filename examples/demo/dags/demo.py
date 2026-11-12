"""Each connection type opened from Python and from a command (`make demo`).

Every task names its deployment: run in `dev` and in `prod`, the same code reads each one's
own database, user, password, key and token from that deployment's secrets.
"""

from typing import cast

from connections import api, box, deployment, events, ledger, orders_db, warehouse

from schedr import CommandOperator, Dag, task
from schedr.connections import DbApiConnection


def greeting(db: DbApiConnection) -> str:
    """The one row of the deployment's `greetings` table."""
    cursor = db.cursor()
    cursor.execute("SELECT greeting FROM greetings")
    row = cursor.fetchone()
    if row is None:
        raise LookupError("no greeting: the server's first start didn't run its script")
    return str(row[0])


@task(connections=[warehouse])
def postgres_from_python() -> str:
    with warehouse() as db:
        return greeting(db)


@task(connections=[orders_db])
def mysql_from_python() -> str:
    with orders_db() as db:
        return greeting(db)


@task(connections=[ledger])
def mariadb_from_python() -> str:
    with ledger() as db:
        return greeting(db)


@task(connections=[events])
def mongo_from_python() -> str:
    with events() as db:
        found = db["greetings"].find_one({}, {"_id": 0, "greeting": 1})
        if found is None:
            raise LookupError("no greeting: the server's first start didn't run its script")
        return str(found["greeting"])


@task(connections=[api])
def http_from_python() -> bool:
    with api() as client:
        # go-httpbin's /bearer answers 200 for any bearer token, 401 without one.
        answer = client.get("/bearer")
        answer.raise_for_status()
        return cast("bool", answer.json()["authenticated"])


@task(connections=[box])
def ssh_from_python() -> str:
    with box() as client:
        _, out, err = client.exec_command("echo logged in as $(whoami)")
        said = out.read().decode().strip()
        if (status := out.channel.recv_exit_status()) != 0:
            raise RuntimeError(f"the command ended {status}: {err.read().decode().strip()}")
        return said


@task(connections=[deployment])
def which_deployment() -> str:
    with deployment() as values:
        return str(values["name"])


with Dag(
    "connections_demo",
    description=(
        "Opens every kind of connection, from Python and from a command: each deployment "
        "reads its own database, user, password, key and token from its own secrets."
    ),
):
    which_deployment()
    postgres_from_python()
    mysql_from_python()
    mariadb_from_python()
    mongo_from_python()
    http_from_python()
    ssh_from_python()
    # The same connections in commands: variables and the files each tool reads.
    CommandOperator(
        "postgres_from_psql",
        ["sh", "-c", 'psql "$PG_URL" -tAc "SELECT greeting FROM greetings"'],
        connections={"PG": warehouse},
    )
    CommandOperator(
        "mysql_from_its_client",
        # MariaDB's client (the image's only one), logging in to MySQL.
        [
            "sh",
            "-c",
            'mariadb --defaults-file="$MY_MYSQL_CNF" -Ne "SELECT greeting FROM greetings"',
        ],
        connections={"MY": orders_db},
    )
    CommandOperator(
        "mariadb_from_its_client",
        [
            "sh",
            "-c",
            'mariadb --defaults-file="$MA_MYSQL_CNF" -Ne "SELECT greeting FROM greetings"',
        ],
        connections={"MA": ledger},
    )
    CommandOperator(
        "mongo_from_mongoexport",
        ["sh", "-c", 'mongoexport --config "$MO_TOOLS_CONFIG" --collection greetings --quiet'],
        connections={"MO": events},
    )
    CommandOperator(
        "http_from_curl",
        ["sh", "-c", 'curl -q -sS -f -K "$API_CURL_CONFIG" "$API_BASE_URL/bearer"'],
        connections={"API": api},
    )
    CommandOperator(
        "ssh_from_ssh",
        ["sh", "-c", 'ssh -F "$BOX_SSH_CONFIG" target "echo logged in as \\$(whoami)"'],
        connections={"BOX": box},
    )
