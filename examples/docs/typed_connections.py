"""Built-in connection types: one definition, the type's own client."""

from typing import cast

from schedr import CommandOperator, Dag, secrets, task
from schedr.connections import HTTP, SQL, SSH, Generic, Mongo

# Usually in a module of its own (`connections.py`), imported by the DAGs using them. The
# first argument is the name tasks declare and commands read.
billing_api = HTTP(
    "billing_api",
    base_url="https://billing.example.com",
    token=secrets["billing"]["token"],
    check_path="/health",
)
# TLS verified by default (`?sslmode=...` for another mode); the password apart from the URL.
warehouse = SQL(
    "warehouse",
    url="postgresql://db.example.com:5432/etl",
    user="etl",
    password=secrets["etl/db"]["password"],
)
orders = Mongo("orders", uri=secrets["mongo"]["uri"], database="orders")
router = SSH(
    "router",
    host="10.0.0.1",
    user="netops",
    private_key=secrets["router"]["key"],
    known_hosts="10.0.0.1 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEXAMPLEexampleEXAMPLEexampleEXAMPLE",
)
region = Generic("region", name="eu-west", buckets=["raw", "clean"])


@task(connections=[region])
def bucket_names() -> list[str]:
    # Keys of one's own: a dict. The other types give their clients: `httpx.Client`, a
    # DB-API connection, a pymongo `Database`, a paramiko `SSHClient`.
    with region() as values:
        buckets = cast("list[str]", values["buckets"])  # a dict's values are `object`
        return [f"{values['name']}/{bucket}" for bucket in buckets]


with Dag("typed_connections"):
    bucket_names()
    # A command gets each connection as variables (and the files its tools read), by
    # prefix: `{"DB": warehouse}` would give `psql "$DB_URL"` its URL and password file.
    CommandOperator(
        "list_buckets",
        ["sh", "-c", 'echo "$REGION_NAME: $REGION_BUCKETS"'],
        connections={"REGION": region},
    )
