# Secret storage

Where the values behind a DAG's `secrets['path']['key']` live, and who reads them. How a DAG
names and uses a secret is [Secrets](../02-dags/13-secrets.md).

A DAG may give a fallback for a missing key (`.get(key, fallback)`). It is written in the
DAG, shown in the DAG's JSON, the API and the UI, and never masked: **never put a real secret
in a fallback** ([Fallbacks are not secret](../02-dags/13-secrets.md#fallbacks-are-not-secret)).
Put each environment's real values in its storage.

The **worker agent** that runs a task fetches its secrets, never the master or the webserver,
and never the task's own code. Values stay in the agent's memory (never on disk), are given
to the task, and are masked in its log and output before anything leaves the worker. The
master, the database's run tables, the API and the UI only ever see names.

Two kinds of storage exist:

| Kind | Where the values are | Where |
|---|---|---|
| `db` | schedr's own database, each value encrypted | standalone only |
| `vault` | HashiCorp Vault, a KV v2 mount | standalone and workers |

## Configuration

Each storage is a table of its own, named by its key; DAGs write that name
(`secrets.vault['...']`):

```toml
[secret_storage]
default = "vault"                       # which storage secrets['...'] means
min_mask_length = 5                     # shorter values are not masked

[secret_storage.vault]
type = "vault"
address = "https://vault.example.com:8200"
mount = "secret"                        # the KV v2 mount (default "secret")
namespace = "team-x"                    # Vault Enterprise, optional
ca_cert = "/etc/schedr/vault-ca.pem"    # only these CAs, optional
auth = { method = "kubernetes", role = "schedr-worker" }
cache_ttl = "60s"                       # how long a value is kept in memory; "0s": never
unmasked_prefixes = ["variables/"]      # values under these aren't masked
```

- **The default storage** is `default`; without it, the only configured storage; with none,
  standalone's `db`. Several storages without `default` are a startup error ("set
  [secret_storage] default"), so adding a storage never moves the secrets DAGs read with
  `secrets[...]`.
- **A name** is letters, digits, `_` and `-`, not `default`, and without `__` (environment
  variables split there). Environment variables lowercase it, so a storage configured only
  through them has a lowercase name.
- **Only workers use it** (and standalone, which has one). Each worker reports its storage
  names, and the master refuses a DAG naming one that the worker which parsed it lacks.
- **`unmasked_prefixes`** marks parts of a storage as settings, not credentials: values under
  them are not masked (a bucket name would otherwise be `****` on every log line). Each ends
  with `/`. Keep nothing secret there.
- Every key is also `SCHEDR_CONFIG__SECRET_STORAGE__<NAME>__<KEY>`. The full list is in the
  [Configuration reference](../_generated/04-reference/01-configuration.md).

## The `db` storage (standalone)

Standalone always has a storage named `db`, in its own database (SQLite or Postgres). With
no other storage configured it is the default, so `secrets['etl/db']['password']` works with no
`[secret_storage]` at all. With Vault configured too, `db` stays reachable as
`secrets.db['etl/db']['password']`.

A secret is a path with keys, as in Vault. Each value is encrypted with XChaCha20-Poly1305
under **`SCHEDR_SECRET_KEY`** (32 bytes, base64), bound to its `path#key`, so a database
dump, a backup or a copy of the SQLite file shows paths and key names but no value. Make a
key with `schedr secrets new-key` and keep it as you keep the database's password: a lost key
means setting every secret again. Without the key, standalone still starts; reading the `db`
storage then fails as `secret_denied`, naming the missing variable. This protects against
leaks of the database, not against someone who can read the process's environment.

The `schedr secrets` commands write the table directly, as `schedr db migrate` does: the
database is `[database] url`, else standalone's SQLite file in its data directory, and its
schema must be up to date. In Kubernetes, run them with `kubectl exec` in the pod.

| Command | Does |
|---|---|
| `schedr secrets set etl/db password=- user=etl` | Sets keys; the secret's other keys are kept. `key=@FILE` is a file's content exactly; `key=-` reads standard input, one trailing newline dropped, so a value stays out of the shell's history. |
| `schedr secrets list [etl/]` | Paths and key names, never values. |
| `schedr secrets get etl/db#password` | Prints one value; `get etl/db` prints the whole secret as JSON. The only way to see a value. |
| `schedr secrets delete etl/db` | Deletes the secret, all its keys. |
| `schedr secrets export --to DIR` | Writes each secret as `DIR/<path>.json` (mode 0600), for Vault. |
| `schedr secrets new-key` | Prints a new key for `SCHEDR_SECRET_KEY`. |

`set`, `get` and `export` need `SCHEDR_SECRET_KEY`; `list` and `delete` don't. The API and the
UI never show a value.

**Moving to Vault**, e.g. before moving from standalone to separate roles (workers have no
database, so they can't read `db`): `schedr secrets export --to DIR` refuses a `DIR` that isn't
empty and prints nothing. Then load each file with `vault kv put secret/<path> @DIR/<path>.json`,
configure the Vault storage as the default, and delete `DIR`.

## Vault

A `vault` storage reads **KV v2** (KV v1 isn't supported): `secrets['etl/db']['password']` is
the key `password` of the secret `etl/db` under `mount`. A value that isn't a string is read
as its JSON text, as `vault kv get -field=` prints it: `5432` is `"5432"`, `true` is
`"true"`. A deleted latest version is not found.

**Logging in:**

- `auth = { method = "kubernetes", role = "...", mount = "kubernetes" }`: Vault's Kubernetes
  auth with the pod's service-account token (`token_file`, by default
  `/var/run/secrets/kubernetes.io/serviceaccount/token`; the chart mounts its own). The
  Vault role binds the service account. On OpenShift the auth mount is often `openshift`.
- `auth = { method = "token" }`: a token from `SCHEDR_CONFIG__SECRET_STORAGE__<NAME>__TOKEN`,
  for development. A token in the configuration file is a startup error.

Nothing is sent to Vault before the first read, so a worker starts while Vault is down. The
agent then keeps its token, renews it at two thirds of its time to live, and logs in again
when renewal fails. A read refused with Kubernetes auth logs in again once and retries.
Reads never wait for a renewal while the token is still good.

**Answers:** a 404 on a secret is `secret_not_found`; 400, 401 and 403, and a 404 on a login
(a wrong auth mount), are `secret_denied`, with Vault's own message; anything else, an
answer that isn't a KV v2 secret, a timeout (30 s a request) or no connection is
`secret_unavailable`, which is retried. The client follows no redirect: the next host would
get the token.

Values are kept in the agent's memory for `cache_ttl` (60 s by default), so many short
tasks don't each call Vault. A changed secret is seen after at most that long.

## In the chart

| Value | Sets |
|---|---|
| `secretStorage.db.existingSecret`, `secretKey` | standalone's `SCHEDR_SECRET_KEY`, from an existing Secret (`secret-key` by default). The chart never makes one. |
| `secretStorage.vault.enabled` | A Vault storage on standalone or the workers (never the master or the webserver). |
| `secretStorage.vault.name` | Its name, as DAGs write it (`vault`). |
| `secretStorage.vault.address`, `mount`, `namespace` | As in the configuration. |
| `secretStorage.vault.auth.role`, `auth.mount` | The Vault role and the Kubernetes auth mount. |
| `secretStorage.vault.auth.tokenExpirationSeconds`, `auth.audience` | The projected token Vault's login uses. |
| `secretStorage.vault.caCert.configMap`, `key` | A ConfigMap with the CA certificates Vault's certificate is checked against. |

The pods mount no service-account token otherwise (`serviceAccount.automountToken: false`);
with Vault on, the chart mounts a **projected token** of its own at
`/var/run/secrets/schedr/vault/token`, for Vault's login only. Under Istio's `REGISTRY_ONLY`,
a Vault outside the mesh needs a ServiceEntry (`istio.serviceEntries`,
[Deployment](10-deployment.md)).

With another storage in `config` too, set `SECRET_STORAGE__DEFAULT` there.

## What a task can reach

Every task on a worker runs as the agent's user. So a task can read what the agent can:
its environment (standalone's `SCHEDR_SECRET_KEY`), its files, and the token Vault's login
uses. With that token it can log in as its worker and read **whatever the worker's Vault
role allows**, also secrets it never declared, and the UI won't list them. This reaches no
more than a DAG could declare (any path can be declared); it skips only the declaration.

So a worker is one trust domain: its tasks, its agent and its Vault role. **Teams that must
be kept apart run separate deployments**, each with its own master, database, workers,
service accounts and Vault role; queues don't separate them, since a release's workers share
one role. One [UI](12-ui.md) can still show them all, as a federation of deployments.
