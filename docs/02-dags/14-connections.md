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
| `SQL(url="sqlite://...")` | the URL is `postgresql://` or `mysql://` |
| `SQL(url="mysql://...?ssl_ca=...")` | a `mysql://` URL takes only `sslmode` |
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

A command reads a connection's values with `schedr task-ctx connection NAME`, over the same
channel as `schedr task-ctx secret get`. With `--env PREFIX` it prints them as `export` lines
for the shell to `eval`, one per parameter (`PREFIX_` and the parameter's name in capitals),
each value single-quoted:

```python
CommandOperator(
    "export",
    [
        "sh", "-c",
        'vars="$(schedr task-ctx connection warehouse --env DB)" && eval "$vars"'
        ' && psql "$DB_DSN" -f export.sql',
    ],
    connections=["warehouse"],
)
```

Read it into a variable first, as above: `eval "$(...)"` alone succeeds when the read fails
(it evaluates nothing), and the command would go on without the values.

Without `--env`, or with `--json`, it prints every value as one line of JSON
(`{"dsn": "...", "pool": 3}`), for `jq` or a program of your own. The function doesn't run
for a command: a command gets the values only.

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
| `SQL` | `url` (`postgresql://` or `mysql://`, `?sslmode=...`); `user`, `password`; `ca_cert` (PEM text), `timeout` | a DB-API connection (psycopg 3, PyMySQL); the block's work committed when it ends, rolled back on an error | `SELECT 1` |
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

A t-string URL works too (`url=t"postgresql://etl:{secrets['etl/db']['password']}@db/etl"`):
each value it fills in is percent-encoded, so a password with `@`, `/`, `?` or `#` in it is
only a password, never another host or query. A value that is part of the URL's structure
(`host:port`, a whole URL) is given whole instead, `url=secrets["etl/db"]["url"]`, as
written in the storage. A user or a password both in the URL and given apart is refused.

**SQL verifies TLS unless told not to.** Both databases take libpq's `sslmode` in the URL:
`verify-full`, the default, needs TLS and checks the server's certificate and name, against
`ca_cert` or the system's CAs; `verify-ca` checks the certificate only; `require` needs TLS,
unchecked; `prefer` and `allow` use it when offered; `disable` never. A server without TLS
(a local one) needs `?sslmode=disable`, which says so in the definition. A `mysql://` URL
takes no other query key: PyMySQL would ignore it.

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

A connection's check (`name.run_check()` in a task; `schedr connections test NAME` to come)
opens it and raises when it doesn't work. A `@connection` of your own gets one with
`@name.check`:

```python
@orders_api.check
def reachable(api: ApiClient) -> None:
    api.get("/health")
```

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
