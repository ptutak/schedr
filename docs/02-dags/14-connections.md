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
