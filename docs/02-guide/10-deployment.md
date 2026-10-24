# Deployment

## Configuration

Every role reads the same configuration: one TOML file (`--config`, else
`SCHEDR_CONFIG_FILE`, else `/etc/schedr/config.toml`) and environment variables mirroring
it, e.g. `SCHEDR_CONFIG__MASTER__GRPC_PORT` for `[master] grpc_port`. Flags win over the
environment, which wins over the file. Unknown keys are an error. Each role uses only its own
sections. The [configuration reference](../_generated/03-reference/01-configuration.md) lists
every key.

## The roles in a cluster

| Role | Replicas | Needs |
|---|---|---|
| `schedr master` | **exactly one** | `[database] url`, a migrated schema; its gRPC port behind a Service the workers reach |
| `schedr webserver` | one or more | `[database] url`, a migrated schema; its REST port behind a Service |
| `schedr worker` | as many as the load needs | `[worker] master_url`, the DAG code (e.g. a git-sync sidecar), a disk for its journal and logs; a StatefulSet with a headless Service, each pod's `log_address` set to its DNS name |

- **Postgres** is the database for anything spread over several hosts. Run
  `schedr db migrate` before starting the master and the webserver; both refuse a schema that
  doesn't match their version. It holds the master's lease while it migrates, so it is
  refused while a master runs (`--wait` waits for the lease instead). SQLite works for roles
  on one host with a local disk.
- **Upgrade the master first**, then the webservers and workers (see
  [Parsing and code versions](06-parsing.md#upgrades)).
- **Workers' `terminationGracePeriodSeconds`** should match `[worker]
  termination_grace_period`, so a draining worker has the time it expects.
- **Probes** go to the ops port: liveness on `/healthz` for every role, readiness on `/readyz`
  for the master and the webserver. Workers need no readiness probe: nothing connects to them
  through a Service.
- **Memory:** a worker pod's memory limit is split between its slots (see
  [Workers](05-workers.md#memory)); size the pod for its slots.

A Helm chart for this layout comes with step 2.11 (`mode: distributed`).

## One pod: `schedr standalone`, the production start

Production starts with **standalone in one pod**: first on SQLite, then on Postgres, and the
separate roles only if the load needs them (a full switch: the history stays in Postgres).

**The image.** `images/schedr/Dockerfile` builds the base image (`make image`): Alpine with
Python 3.14 and the `schedr` package, `tini` as PID 1, a data directory writable by group 0
for OpenShift's arbitrary UID, and **libraries for tasks** (about 410 MB in all, Polars the
largest):

| For | Libraries |
|---|---|
| API calls | `requests`, `httpx`, `aiohttp`, `tenacity` |
| Data | `polars`, `orjson`, `pyyaml`, `python-dateutil` |
| Networking | `paramiko` (SSH, SFTP), `netmiko` (device CLIs), `ncclient` (NETCONF), `dnspython`, `netaddr` |
| MongoDB | `pymongo` (compiled in the image's builder stage, with its C extensions: it has no wheel for Alpine) |

They are listed in `images/schedr/requirements.in` and pinned with their dependencies in
`requirements.txt` (`make image-requirements`, no version newer than two weeks). Async
libraries work inside a task with `asyncio.run(...)` until async task functions come (M4). Your DAGs' libraries go in an image of your own built `FROM` it:
`examples/kubernetes/Dockerfile` does it in two stages, compiling in a builder stage what has
no musl wheel, so no compiler ends up in your image.

**The chart.** `charts/schedr/` with `mode: standalone`:

- one replica (a StatefulSet) with a ReadWriteOnce volume on block storage for the data
  directory (task logs, the worker's journal, and on SQLite the database);
- **the database**, one of three: SQLite on the volume (nothing set: the start);
  **`database.postgres`**, the host, port, database name and URL parameters in values and the
  user and password from a Secret (`existingSecret`, keys `userKey` / `passwordKey`), put
  into the URL by Kubernetes (`$(SCHEDR_DB_USER)`), so they must not contain `@ : / ? # %` or
  a space; or `database.existingSecret`, a Secret holding the whole URL. Any connection pooler
  can sit in between. Behind one in transaction mode the master finds it
  and polls for new commands (see [the master](04-master.md)); `database.notificationsUrlKey`
  names a key of the same Secret with a URL straight to Postgres, for listening only
  (`database.postgres.notificationsHost` with `database.postgres`);
- **what the database user needs:** its own database only. It owns the database (so it can
  create the schema in `public`, which Postgres 15 no longer lets everyone do), and can use
  PL/pgSQL (`DO` blocks: a master's lease check), which Postgres allows everyone by default; a
  server that revokes it refuses a master's writes;
- **`migration`**, how the schema is migrated (both hold the master's lease, and change
  nothing when the schema is up to date):
  - `initContainer` (the default): an init container runs `schedr db migrate --wait` before
    schedr starts, so a failed migration shows as a failed init container, and schedr itself
    refuses a schema that isn't up to date;
  - `onStart`: schedr migrates at start (`--migrate`). Use it **under Istio with classic
    sidecars**: an init container runs before the pod's proxy exists, so it can't reach the
    database through the mesh. Native sidecars (Kubernetes 1.29 on, Istio's
    `ENABLE_NATIVE_SIDECARS`) start before init containers and don't have this problem;
- **`vault`**: Secrets kept in sync from Vault by the Vault Secrets Operator, e.g. the one
  `database.postgres.existingSecret` names: a VaultAuth (`auth.role`, `mount`, the Vault
  `namespace`) for the pod's ServiceAccount, and a VaultStaticSecret per `staticSecrets` item
  (each key of the Vault entry a key of the Secret). A change in Vault restarts schedr
  (`restartOnChange`, after a drain), since it reads its Secrets at start. Off by default:
  create the Secrets by hand;
- **`serviceAccount`**: the chart creates one by default, holding no permissions;
  `create: false` with a `name` runs the pod as an existing one (e.g. the account a namespace
  in the mesh already has). No token is mounted (`automountToken: false`): schedr never
  calls the Kubernetes API;
- the pod's `fsGroup` is 0, so the volume is writable by the image's group, except on
  OpenShift (found by its API), which sets its own;
- `terminationGracePeriodSeconds` (600) is passed to schedr as `[worker]
  termination_grace_period`, so a drain always fits the pod's grace period;
- probes on the ops port, a Service with ports named for Istio (`http-api`, `http-ops`), an
  optional `ServiceMonitor`;
- more settings as `config` (`WORKER__SLOTS: "4"` becomes `SCHEDR_CONFIG__WORKER__SLOTS`).

**The DAGs** reach the pod in three ways: `dags.files` is a root of its own, beside
`dagsPath`'s, which holds either git-sync's checkout or the image's DAGs (git-sync's volume
covers the image's folder), so at most two roots:

| Way | Values | A change reaches schedr | For |
|---|---|---|---|
| **A flat folder through `helm upgrade`** | `dags.files`, made by `charts/schedr/dags-values.sh <folder>` | within about a minute, **without a restart** | trying DAGs, a handful of files (up to 512 KiB together) |
| **git-sync** | `gitSync` (`repo`, `ref`, `period`, `credentials`) | within `period` (30 s) of a push, without a restart | the real way: a repository with history and review |
| **Baked into the image** | `dagsPath` (`/dags/current`) | with a new image (a restart, which drains) | DAGs that change with their libraries |

- **From a folder:** the script takes the folder's `*.py` files and its `.schedrignore`
  (hidden files, `__pycache__` and hidden folders left out) and prints values with each
  file's content in base64 (no file can break the YAML; the same folder always gives the same
  values). **The folder must be flat**: Python files in a subfolder are refused, since a
  ConfigMap's files can be put in subfolders only by the pod's spec, which would restart
  schedr whenever a file is added; helpers are top-level modules (`import helpers`), and
  packages go through git-sync. It needs only bash and coreutils, so it runs in
  any CI job with Helm; it checks everything before printing anything:

  ```bash
  charts/schedr/dags-values.sh dags/ > dags-values.yaml
  helm upgrade --install schedr charts/schedr -f my-values.yaml -f dags-values.yaml
  ```

  The chart makes a ConfigMap of them, mounted at `/dag-files` as the root **`files`**. The
  root is the ConfigMap's `..data` link, which the kubelet switches to each new version, as
  git-sync switches its link. The first upgrade with files adds a volume to the pod (a
  restart); adding, changing or removing files later only changes the ConfigMap.
- **git-sync** keeps a repository in `dagsPath` (root `dagsRoot`, `dags`). Credentials come
  from a Secret, `gitSync.credentials`: `type: token` for an HTTPS repository (a user and a
  token, e.g. a GitLab deploy token, keys `username` and `password`), `type: ssh` for an SSH
  one (keys `ssh-privatekey` and `known_hosts`, the line `ssh-keyscan <host>` prints). Under
  Istio with `REGISTRY_ONLY`, a repository outside the mesh needs a ServiceEntry
  (`istio.serviceEntries`: the host on 443, or 22).
- `dagsPath: ""` leaves out the image's root, for a deployment whose DAGs come only from
  `dags.files`.

**From SQLite to Postgres** is one `helm upgrade` with `database.postgres` (or
`database.existingSecret`) set. The pod drains and restarts; schedr starts afresh on the empty
database, migrated as `migration` says, and parses the DAGs again; the volume keeps the task
logs and the journal, and the SQLite file with the old history stays behind on it. Emptying
the values goes back to SQLite.

```yaml
database:
  postgres:
    host: postgres.schedr.svc
    name: schedr
    params: sslmode=require
    existingSecret: schedr-pg     # keys username and password
```

Anything else built from a Secret goes in `env`, whose values may refer to earlier variables
as `$(NAME)`, and any configuration key in `config`.

What to know:

- **One master per database:** a second pod during a rollout waits as a standby (see [The
  master](04-master.md#one-master-per-database-the-lease)); on SQLite the volume lets only
  one pod in anyway. If the master part fails (another took its lease over), the whole pod's
  process exits and Kubernetes restarts it.
- **Memory:** the pod's limit is shared by tasks and schedr itself; task budgets are (limit −
  25 %) ÷ slots in standalone (`[worker] memory_reserve` to change it). schedr's own process
  used about 20–25 MiB in measurements.
- **Upgrades restart every role at once:** running tasks get the grace period, then are
  interrupted and run again.
- **The REST API has no authentication** until the UI: keep the Service internal, or put it
  behind the mesh (below).
- `scripts/kind-standalone.sh` installs the chart on a local kind cluster on SQLite, switches
  it to Postgres with `database.postgres`, then to a URL Secret, then adds DAGs from a folder
  (a second upgrade without a restart) and from git-sync (a git server in the cluster), and
  runs a DAG each time.

### Inside an Istio mesh

In a namespace that is part of the mesh, nothing reaches schedr or leaves it without Istio's
objects. The chart's `istio` values create them, each optional and off by default:

| Value | Creates | For |
|---|---|---|
| `istio.enabled` | the sidecar label and annotation, and the proxy's settings | schedr starts once its proxy is ready (it reaches the database through it), and the proxy stays until schedr's connections close. Without that, the proxy stops 5 s after SIGTERM and a drain's results never reach the database |
| `istio.virtualService` | a VirtualService on an existing Gateway (`gateways`, `hosts`, `prefix`) | the API from outside the namespace. A `prefix` other than `/` (e.g. `/service/schedr/`) is removed before the request reaches schedr and named to it in `X-Forwarded-Prefix` |
| `istio.requestAuthentication` | a RequestAuthentication (`issuer`, and `jwksUri` or the keys inline in `jwks`; `forwardOriginalToken`) | tokens (JWT, e.g. Keycloak's) checked at schedr's pod. It refuses only invalid tokens: a request without one passes, unless the policy below requires one |
| `istio.authorizationPolicy` | an ALLOW AuthorizationPolicy for schedr's pod | who may call it: anything from the same namespace (`sameNamespace`, on by default); the API with a valid token (`api.requireJwt`), carrying `api.claims` (every claim listed, each with any of its values); callers without a token by mesh identity (`api.principals`) or namespace (`api.namespaces`); the ops port from `ops.namespaces` (empty: anyone). `/api/v1/docs` and `/api/v1/openapi.json` are always open to everyone |
| `istio.peerAuthentication` | a STRICT PeerAuthentication | mTLS required, for a namespace that doesn't already require it |
| `istio.serviceEntries` | ServiceEntries (a name and a spec each) | destinations outside the mesh under `outboundTrafficPolicy: REGISTRY_ONLY`: a Postgres or a git server elsewhere |

- **An ALLOW policy denies whatever it doesn't list**, so the chart's policy lists every port
  schedr serves; adding a policy of your own for the API alone would cut off the ops port.
- The kubelet's probes pass whatever the policy says: Istio sends them from inside the pod.
- The objects use Istio's `v1` API where the cluster serves it (Istio 1.22 on), else
  `v1beta1` (older Istio, OpenShift Service Mesh 2).
- From outside, the CLI passes the prefix and the token: `schedr dags list --server
  https://<host>/service/schedr --token "$TOKEN"`. Swagger UI is at
  `https://<host>/service/schedr/api/v1/docs/`; paste a token into its **Authorize** button to
  call the API from it. Its page refers to the description by a relative URL, and the
  description names the prefix from `X-Forwarded-Prefix` as its server, so its calls go
  through the same prefix.
- **With an existing ServiceAccount** shared by other workloads, a policy can tell schedr's
  callers apart only by namespace or token, since Istio identifies a caller by its account.

Recent Istio (1.31, for one) injects **native sidecars**: the proxy starts before init
containers and stops after schedr, so neither the migration nor a drain needs anything there.
The chart's settings are for **classic sidecars** (older Istio, OpenShift Service Mesh 2), and
harmless otherwise. Which your cluster runs shows in the pod: `istio-proxy` among the init
containers is native, among the containers classic.

`scripts/kind-mesh.sh` installs all of this on a local kind cluster with Istio
(`REGISTRY_ONLY`; `SIDECARS=native` or `classic`) and checks it: the docs open without a token, the API refused without one,
with a forged one and without the required group, the CLI through the gateway with
`--token`, a DAG run on Postgres migrated on start, and a pod stopped while a task runs, whose
result still reaches the database through the sidecar.

On a laptop, `schedr standalone --migrate` runs everything on localhost, with SQLite in the
data directory (`[worker] data_dir`, else `~/.local/share/schedr`), migrating the database at
start.

In this repository, **`make standalone`** does the same for trying things by hand (development
only): the debug binary just built, the Python environment of `uv sync` for DAG files and
tasks, and everything in `.schedr/` in the repository (git-ignored): the SQLite database
(`.schedr/schedr.db`, to open in an IDE), task logs and the journal. It runs the DAGs in
`examples/` unless `DAGS=<directory>` says otherwise, with the API on `0.0.0.0:8080`
(`LISTEN`, all addresses by default, so it can be reached from outside WSL, e.g. Swagger UI at
`http://<address>:8080/api/v1/docs/`; `schedr dags list` needs no `--server` with
`0.0.0.0` or `127.0.0.1`) and the ops
port on 8090. The API has no authentication: on a network you don't trust, run
`make standalone LISTEN=127.0.0.1`. Ctrl-C stops it; `rm -rf
.schedr` starts afresh.
