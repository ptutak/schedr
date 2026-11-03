"""The demo's connections: one of each type, every value from the deployment's own secrets.

The same file runs in both deployments (`dev` and `prod`); each one's secret storage points
it at its own database, user, password, key and token. No TLS here: the servers have none
(Postgres, Mongo) or a certificate they made themselves (MySQL, MariaDB: `require` encrypts
without checking it), and they are on the demo's own network. A real deployment keeps the
default `verify-full`.
"""

from schedr import secrets, text
from schedr.connections import HTTP, SQL, SSH, Generic, Mongo

pg = secrets["demo/postgres"]
warehouse = SQL(
    "warehouse",
    url=text(t"postgresql://postgres:5432/{pg['database']}?sslmode=disable"),
    user=pg["user"],
    password=pg["password"],
)

my = secrets["demo/mysql"]
orders_db = SQL(
    "orders_db",
    url=text(t"mysql://mysql:3306/{my['database']}?sslmode=require"),
    user=my["user"],
    password=my["password"],
)

ma = secrets["demo/mariadb"]
ledger = SQL(
    "ledger",
    url=text(t"mariadb://mariadb:3306/{ma['database']}?sslmode=require"),
    user=ma["user"],
    password=ma["password"],
)

mo = secrets["demo/mongo"]
events = Mongo(
    "events",
    uri="mongodb://mongo:27017/?tls=false",
    database=mo["database"],
    user=mo["user"],
    password=mo["password"],
)

api = HTTP(
    "api",
    base_url="http://httpbin:8080",
    token=secrets["demo/api"]["token"],
    check_path="/bearer",
)

box = SSH(
    "box",
    host="ssh",
    port=2222,
    user="demo",
    private_key=secrets["demo/ssh"]["key"],
    known_hosts=secrets["demo/ssh"]["known_hosts"],
)

deployment = Generic("deployment", name=secrets["demo/deployment"]["name"])
