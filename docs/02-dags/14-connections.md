# Connections

A connection is what a task connects to: a database, an API, a host. It is written once, as
a generator function, and the tasks that use it name it. Its values come from where the DAG
runs: the same DAG reads a development database in development and production's in
production, because each deployment has its own [secret storage](../03-guide/13-secrets.md).

```{literalinclude} ../../examples/docs/connections.py
:language: python
:caption: examples/docs/connections.py
```

## Defining one

`@connection(...)` goes on a generator function, as `contextlib.contextmanager` does. The
function's parameters are the connection's shape, and the decorator's keyword arguments are
their values. The code up to `yield` opens it; what it yields is what the task gets; the code
after `yield` runs when the task's `with` block ends, on errors too.

A value is a plain value (`"orders"`, `5432`, a date) or a secret (`secrets['etl/api']['token']`,
`secrets['etl/db'].get('port', '5432')`), or a t-string of them
(`t"postgresql://etl:{secrets['etl/db']['password']}@db/etl"`). A parameter with a default
can be left out. A run's values (`dag_ctx`) are not connection values: a connection is defined
when the module loads, outside any DAG. Pass them to the task, or read `dag_ctx` inside the
function, which runs in the task.

The checks happen when the file loads, so a mistake is an import error, shown on the
[import errors](../03-guide/12-ui.md) page:

| Written | Refused because |
|---|---|
| a plain function (no `yield`) | a connection is a generator function |
| `@connection(uri=..., extra=1)` | the function has no parameter `extra` |
| `@connection()` on `def db(uri: str)` | `uri` has no value |
| `@connection(port="x")` on `port: int` | the value doesn't fit the type |
| `*args`, `**kwargs`, or a positional-only parameter | every value is given by name |
| two connections of one name in one file | the name must be unique |
| values over 1 MiB as JSON | they travel with each task that opens it: give bigger data to the task |
| `SSH(...)` without `known_hosts` or `insecure_skip_host_check=True` | the host can't be trusted |
| `HTTP(...)` with both `token` and `user` | one way to authenticate |
| `SQL(url="sqlite://...")` | the URL is `postgresql://`, `mysql://` or `mariadb://` |
| `SQL(url="mysql://...?ssl_ca=...")` | a `mysql://` or `mariadb://` URL takes only `sslmode` |
| `SQL(url="postgresql://etl:pw@db/etl", password=...)` | a password is given once |
| `HTTP(..., check_path="https://other.example/health")` | `check_path` is a path under `base_url`: the token goes nowhere else |

The name is the function's. It must be unique in the DAG folder: one defined in two files
can't be opened, and the tasks naming it fail, saying which two files.

## Opening one

A task names the connections it opens in `connections=`: the functions themselves, or their
names. Inside, `with name() as client:` opens it:

```python
@task(connections=[orders_api])
def fetch_orders() -> str:
    with orders_api() as api:
        return api.get("/orders")
```

Before the task starts, the worker renders each connection it declares: it fetches the
secrets from its storage, masks their values in the task's log and output from then on, and
fills in the t-strings. A connection the folder doesn't define, or a secret that can't be
read, fails the attempt before any of the task's code runs. Entering `with`, the function
runs with those values, decoded to its parameters' types.

A task opens only what it declares, as it reads at run time only the secrets its `secrets=`
declares. Opening another fails the attempt as `connection_denied`, even when the folder
defines it.

### In a command

A command gets a connection as variables and files: `connections=` takes a dict, each key the
prefix of that connection's variables.

```python
CommandOperator(
    "export",
    ["sh", "-c", 'psql "$DB_URL" -f export.sql'],
    connections={"DB": warehouse},
)
```

Before the command starts, the worker renders the connection (its secrets fetched and
masked, as for Python), writes the files the connection's tools read into a folder of the
attempt's own (mode 0700, each file 0600), and sets the variables: `DB_URL`, `DB_PGPASSFILE`,
`DB_JSON`, ... The files are in memory, under `/dev/shm`, where the machine has it, else in
the worker's data directory (`[worker] connection_dir` puts them elsewhere). The folder is removed when the attempt ends, however
it ends; a worker that restarts removes what an attempt before it left.

- **A password goes into its tool's file**, and into a variable for scripts (`DB_PASSWORD`),
  masked in the log, in every form the tool's file writes it. A URL variable never holds
  one, so `psql "$DB_URL"` puts no password on the command line, where anyone on the machine
  could read it in the process list: one written in the URL (`postgresql://u:pw@db`,
  `?password=`, `https://u:pw@api`) is taken out of it, and one the tool's file can't take
  (`?sslpassword=`, MySQL's `?password=`) fails the attempt.
- **TLS is verified as in Python:** `DB_URL` carries `sslmode=verify-full` unless the URL says
  otherwise, Mongo's `tls=true`, SSH's config trusts only the connection's `known_hosts`.
- **Only prefixed names:** `DB_URL`, never the tools' own (`PGHOST`, `MYSQL_PWD`), so two
  connections of one type in a task never clash. A prefix is letters, digits and `_`, not
  starting with a digit, and not `SCHEDR...`. A variable both in `env=` and given by a
  connection fails the attempt (an `env=` name starting with a connection's prefix is refused
  when the file loads).

What each type gives (the prefix `DB`; `?` when the value is given):

| Type | Variables | Use it as |
|---|---|---|
| `SQL`, Postgres | `DB_URL` (the user, `sslmode`, `sslrootcert`, and `passfile` naming the `.pgpass` file), `DB_USER`?, `DB_PASSWORD`?, `DB_PGPASSFILE`?, `DB_CA_FILE`? | `psql "$DB_URL"` (libpq reads the password file from the URL) |
| `SQL`, MySQL or MariaDB | `DB_URL` (no password), `DB_HOST`, `DB_PORT`, `DB_USER`?, `DB_DATABASE`?, `DB_PASSWORD`?, `DB_MYSQL_CNF`, `DB_CA_FILE`? | `mysql --defaults-file="$DB_MYSQL_CNF"` (first on the command line; over TCP, `localhost` too), `mysqldump` and `mariadb-dump` too; not `--defaults-extra-file`, after which the clients read `~/.my.cnf`, whose options would win |
| `Mongo` | `DB_URI` (the database in its path, `authSource`; no user or password; `tls=true` and `tlsCAFile` unless written), `DB_DATABASE`, `DB_AUTH_SOURCE`, `DB_USER`?, `DB_PASSWORD`?, `DB_TOOLS_CONFIG`, `DB_CA_FILE`? | `mongodump --config "$DB_TOOLS_CONFIG"` (and `mongorestore`, `mongoexport`, `mongoimport`); mongosh below |
| `HTTP` | `DB_BASE_URL` (no user or password), `DB_TOKEN`? or `DB_USER`?/`DB_PASSWORD`?, `DB_CURL_CONFIG`, `DB_CA_FILE`?, `DB_CERT_FILE`?, `DB_KEY_FILE`? | `curl -q -K "$DB_CURL_CONFIG" "$DB_BASE_URL/orders"` (the token, headers, certificates and timeout in the config; `-q` first, or curl reads `~/.curlrc` too, which could turn off the TLS check or send the token through a proxy) |
| `SSH` | `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_SSH_CONFIG`, `DB_KEY_FILE`?, `DB_KNOWN_HOSTS`, `DB_PASSWORD`?, `DB_KEY_PASSPHRASE`? | `ssh -F "$DB_SSH_CONFIG" target uptime`, `scp -F "$DB_SSH_CONFIG" file target:` |
| `Generic`, a `@connection` | one per parameter, `DB_` and its name in capitals: text as it is, anything else as JSON | `"$DB_DSN"` |

Every connection also gets `DB_JSON`, a file with all its values as one object of JSON, for
`jq` or a program of your own. A parameter whose name can't be a variable's (`région`), or
two that would be one (`host` and `HOST`), fails the attempt: read such a connection with
`schedr task-ctx connection NAME --json` instead. A value curl's config or `.pgpass` can't
hold (a line break in a header, a token or a Postgres password) fails it too.

SSH's config has one host, `target`, with the connection's host, port, user, key and
`known_hosts`, `StrictHostKeyChecking yes`, and `BatchMode yes` unless a password is given
(a prompt would hang the task). With `insecure_skip_host_check=True` it takes any host key.

**The MySQL option file serves both vendors' clients.** MySQL's client and MariaDB's read
different TLS options, and MariaDB's refuses MySQL's `ssl-mode`. So the login and MySQL's TLS
(written `loose-ssl-mode`, which MariaDB's client skips with one warning line) are in
`[client]`, read by every program of both, and MariaDB's TLS is in `[client-mariadb]`, which
only MariaDB's programs read:

```ini
[client]
host = "db.example.com"
port = 3306
user = "etl"
password = "..."
loose-ssl-mode = VERIFY_IDENTITY
ssl-capath = "/etc/ssl/certs"

[client-mariadb]
ssl
ssl-verify-server-cert

[mysql]
database = "shop"
```

MariaDB's client has no "CA only" check and no "TLS if offered": `verify-ca` checks the name
too, and `allow` and `prefer` require TLS.

**mongosh reads no password from a file**, and `--password "$DB_PASSWORD"` would put it on
the command line. Log in from the variables instead, on the database that holds the user
(`DB_AUTH_SOURCE`: the URI's `authSource`, else the database in its path, else `admin`),
then run your script in the same session, on the connection's database:

```sh
mongosh "$DB_URI" --quiet --norc \
  --eval 'db.getSiblingDB(process.env.DB_AUTH_SOURCE).auth(process.env.DB_USER, process.env.DB_PASSWORD)' \
  --file job.js
```

`DB_URI` names no user: with one and no password, mongosh would prompt for it and the task
would hang.

A command can also read a connection itself, over the same channel as `schedr task-ctx secret
get`: `schedr task-ctx connection NAME --env PREFIX` writes the same files and prints the same
variables as `export` lines for the shell to `eval`, each value single-quoted. The connection
is declared in the list form:

```python
CommandOperator(
    "export",
    [
        "sh", "-c",
        'vars="$(schedr task-ctx connection warehouse --env DB)" && eval "$vars"'
        ' && psql "$DB_URL" -f export.sql',
    ],
    connections=["warehouse"],
)
```

Read it into a variable first, as above: `eval "$(...)"` alone succeeds when the read fails
(it evaluates nothing), and the command would go on without the values.

Without `--env`, or with `--json`, it prints every value as one line of JSON
(`{"dsn": "...", "pool": 3}`), and writes no file. The function doesn't run for a command: a
command gets the values only.

## Built-in types

For a common kind of server you don't write the function: a built-in type takes the values and
gives its client. Its name is the first argument, as an operator's task ID is:

```{literalinclude} ../../examples/docs/typed_connections.py
:language: python
:caption: examples/docs/typed_connections.py
```

| Type | Values (besides the name) | `with` gives | Its check |
|---|---|---|---|
| `HTTP` | `base_url`; `token` (a bearer token) or `user` and `password`; `headers`, `ca_cert`, `client_cert`, `client_key` (PEM text), `timeout`, `check_path` | an `httpx.Client` on `base_url` | a request to `check_path`, which must succeed; else any answer to `/` |
| `SSH` | `host`, `user`, `port`; `private_key` (and its `passphrase`) or `password`; `known_hosts`; `timeout` | a connected paramiko `SSHClient` | connecting |
| `Mongo` | `uri`, `database`; `user`, `password`; `ca_cert` (PEM text), `timeout` | a pymongo `Database` | `ping` |
| `SQL` | `url` (`postgresql://`, `mysql://` or `mariadb://`, `?sslmode=...`); `user`, `password`; `ca_cert` (PEM text), `timeout` | a DB-API connection (psycopg 3, PyMySQL); the block's work committed when it ends, rolled back on an error | `SELECT 1` |
| `Generic` | keys of your own | a `dict` | none |

Each client's library is an extra, installed where tasks run: `schedr[http]`,
`schedr[ssh]`, `schedr[mongo]`, `schedr[postgres]`, `schedr[mysql]`. It is imported only when
a connection of its type is opened; without it, the attempt fails saying which extra to
install.

**SSH trusts only the host keys it is given.** `known_hosts` holds them as `known_hosts`
lines (`ssh-keyscan HOST` prints them; `[host]:port` for another port than 22), and any other
key is refused. Without them, `insecure_skip_host_check=True` accepts whatever key the host
shows, an attacker's too, and says so to anyone reading the definition.

**A password is a value of its own, or encoded in the URL.** `SQL` and `Mongo` take `user`
and `password` apart from the URL, given to the driver as they are:

```python
warehouse = SQL(
    "warehouse",
    url="postgresql://db.example.com:5432/etl",
    user="etl",
    password=secrets["etl/db"]["password"],
)
```

A t-string URL works too (`url=text(t"postgresql://etl:{secrets['etl/db']['password']}@db/etl")`,
`text()` telling a type checker the `str` it becomes, as in [Secrets](13-secrets.md)):
each value it fills in is percent-encoded, so a password with `@`, `/`, `?` or `#` in it is
only a password, never another host or query. A value that is part of the URL's structure
(`host:port`, a whole URL) is given whole instead, `url=secrets["etl/db"]["url"]`, as
written in the storage. A user or a password both in the URL and given apart is refused.

**SQL verifies TLS unless told not to.** Both databases take libpq's `sslmode` in the URL:
`verify-full`, the default, needs TLS and checks the server's certificate and name, against
`ca_cert` or the system's CAs; `verify-ca` checks the certificate only; `require` needs TLS,
unchecked; `prefer` and `allow` use it when offered; `disable` never. A server without TLS
(a local one) needs `?sslmode=disable`, which says so in the definition. A `mysql://` URL
takes no other query key, nor does a `mariadb://` one: PyMySQL would ignore it.

**MariaDB** is reached as MySQL, through PyMySQL: `mariadb://` or `mysql://`, the same
rules. MariaDB 11.4 and later serve TLS with a certificate they make in memory when none is
configured; nothing but MariaDB's own client can check it, so the default `verify-full` fails
with Python and with MySQL's client. Give the server a certificate of your own, or connect
with `?sslmode=require` (encrypted, the certificate unchecked). Its `ed25519` login plugin
isn't supported (PyMySQL needs PyNaCl for it): use `mysql_native_password`, its default.

A MySQL server left with the certificate it makes itself (no certificate of your own
configured) can't pass `verify-full`: that certificate names no host. Give it one that names
its host, or connect with `?sslmode=verify-ca` and its CA as `ca_cert` (the CA MySQL made is
`ca.pem` in its data directory), which checks the certificate but not the name.

**Mongo verifies TLS too**, unless the URI says `tls=false`: the server's certificate and
name, against `ca_cert` or the system's CAs. The URI's own TLS options
(`tlsAllowInvalidHostnames=true`, ...) apply as written. A local server without TLS needs
`?tls=false`.

**In a service mesh** (Istio, Linkerd) the sidecars carry a database's TLS as opaque bytes:
Postgres and MySQL start TLS inside their own protocol, so the mesh can neither add it nor
take it off. What to write depends on what protects the hop:

| The database | Protected by | Write |
|---|---|---|
| outside the cluster (a managed database, a VM), reached through a `ServiceEntry` | its own TLS only: the mesh can't originate TLS for these protocols | the default, `verify-full`; its provider's CA as `ca_cert` when the system's CAs don't cover it |
| in the mesh, without TLS of its own, under `STRICT` mutual TLS | the mesh's mutual TLS (the leg to the sidecar stays inside the pod) | `?sslmode=disable` (Mongo: `?tls=false`) |
| in the mesh, with TLS of its own | both | `verify-full`, its certificate naming the host the task dials (`db.ns.svc.cluster.local`); else `verify-ca` with its CA |

`?sslmode=disable` there means "no TLS from the task", not "no encryption", and schedr can't
see the mesh's policy: it stays right only while the database stays in the mesh and the
policy stays `STRICT` (under `PERMISSIVE`, plaintext is accepted). A comment beside the
definition saying so helps the next reader.

`known_hosts` lines are read as OpenSSH writes them, comments and blank lines too; a line
that can't be read, or a marker (`@cert-authority`, `@revoked`), fails the attempt rather
than being skipped.

A connection's check (`name.run_check()` in a task; `schedr connections test NAME` on a
worker, below)
opens it and raises when it doesn't work. A `@connection` of your own gets one with
`@name.check`:

```python
@orders_api.check
def reachable(api: ApiClient) -> None:
    api.get("/health")
```

## Checking them on a worker

A connection's values come from the worker's secret storages, so whether it works depends on
where it runs. Two commands ask a worker, through the master:

```console
$ schedr connections check etl
ROOT  CONNECTION  WORKER    STATE
etl   orders_api  worker-1  failed
etl   warehouse   worker-1  ok
orders_api: secret `secrets['etl/api']['token']` missing
orders_api: connection `orders_api`: `secrets['etl/api']['token']`: no such secret
$ schedr connections test warehouse
ROOT  CONNECTION  WORKER    STATE
etl   warehouse   worker-1  failed (0.3 s)
warehouse: OperationalError: password authentication failed for user "etl"
```

- **`check [ROOT]`** renders every connection of the root (of every root, without one; one
  connection with `--name`): each secret it names is present, missing, falling back to its
  default, denied or unavailable, and never shown. Nothing is opened.
- **`test NAME`** opens one connection on a worker and runs its check (a type's own, or
  `@name.check`); one without a check is opened and closed, and a `Generic` only rendered.
  It runs as a one-off attempt of the connection's file beside the tasks, taking no slot,
  stopped after 60 s (`--timeout 2m`). A failure says why, with its secrets masked.

A worker runs one of these at a time; the time a job waits for its turn is part of its own
(a check has 30 s). It runs on the code the root's DAGs were last parsed from, so a worker
still on older code, or on newer code not parsed yet, isn't picked.

Either goes to a worker having the root's current code (the connected one with the fewest
attempts), or the one `--worker NAME` names. `--json` prints the command's result as it is.
Both exit 1 when a connection failed, or when no answer came (`lost`: the worker went
away or restarted, the master restarted, or the answer didn't come in time). The API: `POST /api/v1/connections/check` and
`POST /api/v1/connections/{name}/test`, each returning the command, its result filled in as
the workers answer (`GET /api/v1/commands/{id}`).

**To try it all:** the repository's demo, `examples/demo` (`make demo`, podman and
podman-compose), runs a server of every type and two deployments, `dev` and `prod`, over one
DAG folder with secrets of their own, each type opened from Python and from a command.

## When a connection can't be opened

| What happened | Error kind | Retried |
|---|---|---|
| The folder doesn't define it, or defines it in two files | `connection_not_found` | yes, while the task's `retries` last (new code can fix it) |
| The task doesn't declare it | `connection_denied` | no |
| A secret it names | the secret's own kind ([Secrets](13-secrets.md#when-a-secret-cant-be-read)) | as for the secret |

A task naming a connection its folder doesn't define shows a warning on its DAG's page as
soon as the folder loads, naming the task. The DAG still loads: the folder's other DAGs, and
the tasks that don't use it, run. `schedr dags check DIR` reports the same over a folder on
your machine, as an error.

## Testing

`schedr.testing.task_context(connections={...})` gives a stand-in for each connection by
name: `with orders_api() as api:` in the block gives the object, and the function isn't
called. A `@task` function called in the block opens only what it declares, as on a worker.

```python
def test_fetch_orders() -> None:
    fake = FakeApi()
    with task_context(connections={"orders_api": fake}):
        assert fetch_orders() == fake.answer
```

`run_dag(..., connections={"orders_api": {"token": "t", "base_url": "http://localhost:8000"}})`
runs the real function with those values instead of its own, which are never rendered (`schedr
dags test --connection 'orders_api={"token": "t", ...}'`). To call the function yourself,
`orders_api.fn(token=..., base_url=...)` is it as a context manager.
