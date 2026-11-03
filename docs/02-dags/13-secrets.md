# Secrets

A DAG file names its secrets; it never holds their values. `secrets['etl/db#password']`
is a placeholder: the worker that runs the task fetches the value from secret storage and
fills it in. The master never sees a value, and the DAG JSON holds only the name. Where
the values live (schedr's own database or Vault) is the deployment's choice
([Secret storage](../03-guide/13-secrets.md)). The same DAG runs against either.

```{literalinclude} ../../examples/docs/secrets.py
:language: python
:caption: examples/docs/secrets.py
```

## Naming a secret

A secret is `path#key`: a path like a Vault KV path (`etl/db`) and one of its keys
(`password`). `secrets['etl/db']`, without `#key`, is the whole secret, a `dict` of its keys
and values. That form works only as a Python task's argument; in text it is refused.

`secrets[...]` is the **default** storage. Another storage is named after `secrets.`:
`secrets.vault['etl/db#password']`. A name that isn't a Python attribute (`vault-2`) is
`secrets.storage('vault-2')[...]`. Adding a second storage to a deployment never changes
which one `secrets[...]` means: with several, the configuration names the default.

Each worker reports the storages it has. A DAG naming one its worker doesn't have is an
import error on the master: ``task `load`: no secret storage `vault` on the worker that
parsed it (configured: `db`; `[secret_storage.vault]` in the workers' configuration)``.

A placeholder has no value in the DAG body. Printing it, comparing it with text, or using
it in an `if` raises `TypeError`, with the ways to use it in the message.

## Passing a secret to a task

**A Python task** takes a placeholder as an argument, alone or inside a t-string:
`connect(t"postgresql://etl:{secrets['etl/db#password']}@db/etl")`. The agent fills it in
before the task's process starts, so the function receives the text with the value. A
parameter's default may be one too: `def load(password: str = secrets['etl/db#password'])`.
Type checkers don't accept this form yet: `mypy --strict` reads the placeholder (or the
t-string) as another type than the parameter's `str`. Reading the secret inside the task
with `task_ctx.secret(...)`, as `connect` does above, type-checks.

**A command** gets a secret through `env=`, never its command line (any process on the host
can read a command line): `env={"PGPASSWORD": secrets["etl/db#password"]}`, or a t-string
there. A placeholder in the command itself is a `TypeError` when the file is imported.

Every secret a task is given this way is fetched **before its process starts**. A secret
that can't be fetched fails the attempt without running anything
([When a secret can't be read](#when-a-secret-cant-be-read)).

## Reading a secret inside a task

An environment variable is inherited by every child process and readable in
`/proc/<pid>/environ`. A task can instead **declare** what it reads, in `secrets=`, and read it
when it needs it:

| Where | Declare | Read |
|---|---|---|
| A Python task | `@task(secrets=["etl/api#token"])` | `task_ctx.secret("etl/api#token")` |
| A command | `CommandOperator(..., secrets=["etl/api#token"])` | `schedr task-ctx secret get 'etl/api#token'` |

`secrets=` takes a placeholder (`secrets.vault['etl/api#token']`), a string for the default
storage (`"etl/api#token"`), a whole secret (`"etl/api"`), or a `SecretPrefix`. It never
sets an environment variable. `task_ctx.secret(name, storage=...)` and `schedr task-ctx
secret get [--storage NAME] SECRET` name the storage separately, never inside the string.
A key's value is returned as it is (the CLI prints it with no trailing newline); a whole
secret is a `dict` (the CLI prints one line of JSON). `task_ctx.secret` is typed `Any`, as
it can be either: `cast("str", task_ctx.secret(...))` gives a strict checker the type, as in
the example. `.override(secrets=[...])` changes
the list for one use of a task.

Only declared secrets can be read. Another one raises `SecretError` with the kind
`secret_denied`, as a typo does: ``secret_denied: `etl/api#tokn` isn't declared in the
task's `secrets=` ``.

### Secrets chosen at run time

`SecretPrefix("tenants/")` in `secrets=` covers every secret under `tenants/`, at any depth
(`SecretPrefix("tenants/", storage="vault")` in another storage). A missing last `/` is
added. A task can then read `tenants/{tenant}#api_key` for a tenant it learns while running.

These are **fetched when read**, not before the start, so a storage outage can fail the
task while it runs. A plain string ending in `/` is refused: widening a task's access takes
`SecretPrefix`, visible in review, never a stray character. There are no globs.

## Masking

The agent replaces every secret value the task was given or read with `****`:

- in the **task's log**, as it is written;
- in its **output** and its error summary, before they leave the worker. A downstream task
  receives `****` too: a task that needs the secret declares it itself.

Besides the value itself it masks its JSON form (also with non-ASCII characters escaped as
`\uXXXX`), Python's `repr` of it, and each line of a value of several lines. A number in an
output equal to a value is masked too (it becomes the text `"****"`). Masking guards against
a value shown by accident, not against the task: a value encoded, or printed half to stdout
and half to stderr, is not masked.

Masking matches text, so a secret whose value is a common word also changes unrelated text
that contains it. Whenever a value is masked, the log ends with a note naming the secret
(never its value) and how often: ``schedr: `etl/db#password` was masked once in this task's
log and output (`****`)``. Keep secret values long and unique.

A value shorter than `[secret_storage] min_mask_length` (5 by default) is not masked:
masking `1` or `true` would mangle the log. The log has a warning naming that secret: at the
start, or at the end for one read inside the task.

## When a secret can't be read

The attempt fails with one of three error kinds:

| Cause | Kind | Retried? |
|---|---|---|
| The storage is unreachable, times out, or answers with an error | `secret_unavailable` | yes, by the task's `retries` |
| The storage refuses: Vault's permissions, the `db` storage without `SCHEDR_SECRET_KEY`, an undeclared secret | `secret_denied` | no |
| No such path or key | `secret_not_found` | no |

A secret given to the task fails the attempt before the process starts. A secret read
inside the task raises `SecretError` (its `kind` is one of the three), and `schedr task-ctx
secret get` exits non-zero. The task may handle that, as `call_api` does above for a tenant
without a key, and succeed. A `SecretError` raised out of the task fails the attempt with its
kind; a task that fails otherwise, with no kind of its own, after a failed read, takes the
secret's kind too, so the retry rules above apply.

## Testing

Tests never reach a real storage. Values are given by `path#key`, and answer for **every**
storage name, so a DAG naming `secrets.vault[...]` runs without configuring one:

- `schedr dags test DAG --secret 'etl/db#password=…'` (repeatable);
- `run_dag(path, dag_id, secrets={"etl/db#password": "…"})`, which passes `--secret`;
- `with task_context(secrets={...}):` for one task called as a function. A `@task` function
  called in the block reads only what its `secrets=` declares, as on a worker.

A secret the test doesn't give is `secret_not_found`. Values given this way are masked as
on a worker ([Testing a DAG](07-testing.md)).
