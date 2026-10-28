# Deployment

## Configuration

Every role reads the same configuration: one TOML file (`--config`, else
`SCHEDR_CONFIG_FILE`, else `/etc/schedr/config.toml`) and environment variables mirroring
it, e.g. `SCHEDR_CONFIG__MASTER__GRPC_PORT` for `[master] grpc_port`. Flags win over the
environment, which wins over the file. Unknown keys are an error. Each role uses only its own
sections. The [configuration reference](../_generated/04-reference/01-configuration.md) lists
every key.

## The roles in a cluster

| Role | Replicas | Needs |
|---|---|---|
| `schedr master` | **exactly one** | `[database] url`, a migrated schema (or `--migrate`); its gRPC port behind a Service the workers reach |
| `schedr webserver` | one or more | `[database] url` (it waits for the schema to be migrated); its REST port behind a Service |
| `schedr worker` | as many as the load needs | `[worker] master_url`, the DAG code (e.g. a git-sync sidecar), a disk for its journal and logs; a StatefulSet with a headless Service, each pod's `log_address` set to its DNS name |
| `schedr ui` | one | `[ui] deployments` (each webserver's URL); no database. Listens on `[ui] port` (8082). A chart pod for it comes in M3.6; until then, [run it yourself](#running-schedr-ui) |

- **Postgres** is the database for anything spread over several hosts. Migrate the schema
  with `schedr db migrate` before starting the master (it holds the master's lease while it
  migrates, so it is refused while a master runs; `--wait` waits for the lease instead), or
  start the master with **`schedr master --migrate`**, which migrates a schema that isn't up
  to date once it holds the lease. Without either, the master refuses such a schema. The
  **webserver waits** for a schema it can use (`/readyz` not ready, checked every 5 s), so it
  can start before the migration. SQLite works for roles on one host with a local disk.
- **Upgrade the master first**, then the webservers and workers (see
  [Parsing and code versions](06-parsing.md#upgrades)).
- **Workers' `terminationGracePeriodSeconds`** should match `[worker]
  termination_grace_period`, so a draining worker has the time it expects.
- **Probes** go to the ops port: liveness on `/healthz` for every role, readiness on `/readyz`
  for the master, the webserver and `schedr ui` (ready once it listens). Workers need no
  readiness probe: nothing connects to them through a Service.
- **Memory:** a worker pod's memory limit is split between its slots (see
  [Workers](05-workers.md#memory)); size the pod for its slots. The chart turns the fork
  server on by default, and with its preload list the worker keeps two long-lived fork
  server processes, so size the pod for them too (see
  [Workers](05-workers.md#the-fork-server) and below).

### The chart's `mode: distributed`

`charts/schedr/` with `mode: distributed` runs this layout. Every other value means the same as
in standalone (`database`, `serviceAccount`, `istio`, `vault`, `dags.files`, `gitSync`,
`passEnv`, `config`, `env`), and the API's Service keeps its name, so **moving from standalone
is one `helm upgrade` with `mode: distributed`** on the same Postgres: the history stays,
standalone's pod goes, the roles come (a short downtime). Standalone's volume stays behind
(NOTES say how to delete it). Distributed needs Postgres: SQLite is refused.

| Role | Kubernetes | Image | Notes |
|---|---|---|---|
| master | a Deployment, **1 replica, `Recreate`** | `schedr-server` (`images/schedr-server`, 44 MB, no Python) | The old master stops (giving up the lease) before the new one starts. `migration`: an init container (`db migrate --wait`) or `onStart` (`master --migrate`). Its Service `<release>-master` (gRPC 8070). |
| webserver | a Deployment, `webserver.replicas` (1) | `schedr-server` | The API's Service `<release>` (8080). Waits for its schema, so during an upgrade the old one serves until the new one is ready. The web UI is a separate role; the chart gains it in M3.6. |
| workers | a **StatefulSet**, `worker.replicas` (2), one pod at a time on updates | `image` (the base image or yours) | A headless Service `<release>-worker`; each pod's `log_address` is its own DNS name; **a volume each** (`worker.persistence`: logs and the journal survive the pod moving); a PodDisruptionBudget (`worker.podDisruptionBudget`, at most 1 down); `terminationGracePeriodSeconds` mirrored into `[worker] termination_grace_period`; the DAGs (git-sync, `dags.files`) only here. |

Each role has its own section (`master`, `webserver`, `worker`: `replicas`, `resources`,
`image` for the server roles, `serviceAccountName`, `podAnnotations`, `nodeSelector`,
`tolerations`, `affinity`); standalone keeps the top-level ones.

**The fork server.** `worker.forkServer` turns it on for these workers and for standalone's
own worker, not for the master or the webserver. It is on by default.

- `enabled` is true.
- `path` is the image's Python, `/usr/local/bin/python3`. An image built `FROM` this one
  keeps that path; set `path` when the interpreter is somewhere else.
- `preload` is the modules the preloaded server imports. The default is the image's
  libraries except `polars`, kept in `images/schedr/preload.txt` (the image, below).
  Set it to add libraries an image of your own brings in. Leave out one that starts a
  thread: the preloaded server is then disabled, and the worker warns (see
  [Workers](05-workers.md#the-fork-server)).

The image's build imports that list and fails if a thread is still running. `make
chart-check` fails if this default differs from the file.

The chart sets that interpreter's `path`, `fork_server` and `preload`. The interpreter is
`config.DAGS__INTERPRETER`, or `python` when that is unset; the chart uppercases the name.
A renamed DAG interpreter is the one it sets. To use an interpreter of your own in
`config`, turn `worker.forkServer.enabled` off. The chart refuses to render when both set
the DAG interpreter (`config.INTERPRETERS__PYTHON`, or `INTERPRETERS__PYTHON__PATH` and
the rest of that table; the same keys with the interpreter's name in upper case). With the
fork server off, set `path`, `fork_server` and `preload` in `config`. Or leave the
interpreter out of `config` and use `worker.forkServer.path`.

With the default list the worker keeps two long-lived processes, the preloaded server and
the bare one. Size the pod for them as well as its slots.

**TLS between the roles:** `tls.mode: app` (the default) is schedr's own mTLS, with a
certificate per role in a Secret of its own (`ca.crt`, `tls.crt`, `tls.key`), from, in order:

1. `tls.existingSecrets` (`master`, `webserver`, `worker`: all three): the master's
   certificate must name its Service (`<release>-master.<namespace>.svc`), the workers' must
   name `tls.workerName` (`schedr-worker`), and both need serverAuth and clientAuth;
2. cert-manager: `tls.certManager.issuerRef` creates a `Certificate` per role;
3. **generated by the chart** (`tls.generate`, on by default): a CA and the three
   certificates at install, valid 10 years, kept on upgrades (made again when the names they
   must carry change: `tls.workerName`, `clusterDomain`). Helm reads them back from the
   cluster, which `helm template`, client dry runs and GitOps tools that render (ArgoCD)
   can't: they would make new ones on every render, so use cert-manager or your own Secrets
   there. To rotate, delete the `<release>-*-tls` Secrets and upgrade; schedr reloads the
   files without a restart.

`tls.mode: mesh` sends plain traffic for a service mesh to encrypt (Istio) and needs no
certificates.

**Accounts and Istio:** created without a `serviceAccount.name`, the ServiceAccounts are one per
role (`<release>-master`, `-webserver`, `-worker`); with a name, every role shares that one,
as standalone's pod did, so moving keeps the account a Vault role or a policy is bound to;
`serviceAccount.create: false` runs every role as one existing account (a role's
`serviceAccountName` overrides each of these). The Vault Secrets Operator authenticates as
`vault.auth.serviceAccount`, by default the pods' account (the master's when each role has its
own). In `tls.mode: app` the master's gRPC port and the workers' log port are named `tls-grpc`
and `tls-logs`, so Istio's sidecars pass schedr's own TLS through instead of parsing it. The
workers' addresses use `clusterDomain` (`cluster.local`).
The `istio.authorizationPolicy` adds a policy per role listing every port it serves: the
master's gRPC port for the workers, the workers' log endpoint for the webserver, by account
when each role has its own, else by namespace (and `sameNamespace` lets the whole namespace
in, as in standalone). With `istio.enabled` the chart also adds a DestinationRule
(`ISTIO_MUTUAL`) for the workers' headless Service: Istio sends calls to a headless Service's
pods without its automatic mTLS, so without it the webserver's log requests would carry no
identity and every policy would refuse them.

**Tasks' environment:** tasks get a clean environment, so a variable of the workers' (a
MongoDB URI from a Secret, a proxy) reaches them only when listed in **`passEnv`**:

```yaml
env:
  - name: MONGO_URI
    valueFrom: {secretKeyRef: {name: mongo, key: uri}}
passEnv: [MONGO_URI]
```

`scripts/kind-distributed.sh` checks it on a local kind cluster: standalone on Postgres with a
run, then `mode: distributed` (the run still there), two workers over mTLS with generated
certificates, a DAG run, a task's log read through the webserver from its worker, the
certificates kept by an upgrade, and a worker deleted while its task runs (the task finishes
in the drain, one attempt). `MESH=1` does the same on the mesh cluster of `kind-mesh.sh`, in
`tls.mode: mesh`, with the internal links allowed by account only. Killing the master or a
worker without a drain is a separate check, below.

## One pod: `schedr standalone`, the production start

Production starts with **standalone in one pod**: first on SQLite, then on Postgres, and the
separate roles only if the load needs them (a full switch: the history stays in Postgres).

**The image.** `images/schedr/Dockerfile` builds the base image (`make image`): Alpine with
Python 3.14 and the `schedr` package, `tini` as PID 1, a data directory writable by group 0
for OpenShift's arbitrary UID, and **libraries for tasks** (about 410 MB in all, `polars` the
largest):

| For | Libraries |
|---|---|
| API calls | `requests`, `httpx`, `aiohttp`, `tenacity` |
| Data | `polars`, `orjson`, `pyyaml`, `python-dateutil` |
| Networking | `paramiko` (SSH, SFTP), `netmiko` (device CLIs), `ncclient` (NETCONF), `dnspython`, `netaddr` |
| MongoDB | `pymongo` (compiled in the image's builder stage, with its C extensions: it has no wheel for Alpine) |

They are listed in `images/schedr/requirements.in` and pinned with their dependencies in
`requirements.txt` (`make image-requirements`, no version newer than two weeks). The
preloaded fork server imports them except `polars`, by module name, from
`images/schedr/preload.txt`: `yaml` for PyYAML, `dateutil` for python-dateutil, `dns` for
dnspython, and the others under the names in the table. `polars` starts threads on import,
so preloading it would disable that server. The image's build imports the list and fails if
a thread is still running. Async libraries work inside a task with `asyncio.run(...)` until
async task functions come (M4). Your DAGs' libraries go in an image of your own built
`FROM` it:
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
    schedr starts, so a failed migration shows as a failed init container, and the master
    (standalone's included) refuses a schema that isn't up to date;
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
- the fork server on this pod's own worker, the same `worker.forkServer` as the
  [distributed workers](#the-charts-mode-distributed) (on by default);
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
- **Memory:** the pod's limit is shared by tasks, schedr itself and, with the chart's
  default, the two long-lived fork server processes. Task budgets are (limit − 25 %) ÷ slots
  in standalone (`[worker] memory_reserve` to change it). schedr's own process used about
  20–25 MiB in measurements. Size the pod for those two processes as well as the slots.
- **Upgrades restart every role at once:** running tasks get the grace period, then are
  interrupted and run again.
- **The REST API and the web UI have no authentication of their own** until M3.5: keep the
  Service internal, or put it behind the mesh (below). On this pod the UI is on the API's
  port, so that Service is the UI too.
- **The web UI** is at `/` on the API's port. Its deployment is named `default`. `/docs`
  serves the image's documentation. What the pages show is [The web UI](12-ui.md).
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
| `istio.virtualService` | a VirtualService on an existing Gateway (`gateways`, `hosts`, `prefix`) | the API from outside the namespace. On a standalone pod that is the web UI too, on the same port. A `prefix` other than `/` (e.g. `/service/schedr/`) is removed before the request reaches schedr and named to it in `X-Forwarded-Prefix`; the UI's links follow that header |
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
(`REGISTRY_ONLY`; `SIDECARS=native` or `classic`) and checks it: Swagger UI opens without a
token, the API is refused without one, with a forged one and without the required group, the
CLI through the gateway with
`--token`, a DAG run on Postgres migrated on start, and a pod stopped while a task runs, whose
result still reaches the database through the sidecar. Like the other kind scripts, it refuses a
context whose server isn't loopback, before calling anything on that cluster.

On a laptop, `schedr standalone --migrate` runs everything on localhost, with SQLite in the
data directory (`[worker] data_dir`, else `~/.local/share/schedr`), migrating the database at
start.

In this repository, **`make standalone`** does the same for trying things by hand (development
only): the debug binary just built, the Python environment of `uv sync` for DAG files and
tasks, and everything in `.schedr/` in the repository (git-ignored): the SQLite database
(`.schedr/schedr.db`, to open in an IDE), task logs and the journal. It runs the DAGs in
`examples/` unless `DAGS=<directory>` says otherwise, with the API and the web UI on
`0.0.0.0:8080` (`LISTEN`, all addresses by default, so it can be reached from outside WSL:
the UI at `http://<address>:8080/`, Swagger UI at `http://<address>:8080/api/v1/docs/`;
`schedr dags list` needs no `--server` with `0.0.0.0` or `127.0.0.1`) and the ops port on
8090. A build tree has no documentation directory at `[ui] docs_dir`'s default, so `/docs`
appears once that key points at one (`site` after `make docs`). The API and the UI have no
authentication: on a network you don't trust, run `make standalone LISTEN=127.0.0.1`. Ctrl-C
stops it; `rm -rf .schedr` starts afresh.

## Running `schedr ui`

`schedr ui` is the web UI as its own process, over one or more webservers. It needs no
database and no DAG code. The server image is enough: it has the binary and the
documentation, and this role runs no user code. The base image has them too. The chart's
distributed mode deploys this process in M3.6. Until then, run it yourself. Standalone
already serves the same pages on its API port, and ignores `[ui] bind`, `[ui] port` and
`[ui] deployments`.

Point each deployment at that deployment's webserver. A name is letters, digits, `-` and
`_`, shown on the pages and used in their addresses, and each name once. With no list, or a
name that breaks those rules, `schedr ui` refuses to start and says what to fix.

```toml
[ui]
bind = "0.0.0.0"
port = 8082

[[ui.deployments]]
name = "etl"
url = "http://schedr-webserver.etl.svc:8080"

[[ui.deployments]]
name = "ml"
url = "http://schedr-webserver.ml.svc:8080"
```

It listens on `[ui] port` (8082), on all interfaces unless `[ui] bind` says otherwise, and
warns when that is beyond localhost: there is no authentication until M3.5. It sends no
credentials to the webservers, so run it where those webservers allow a caller without a
token (in the chart, the same namespace). Probes go to the ops port, as for the webserver:
it is ready once it listens. `/docs` is the image's documentation when `[ui] docs_dir` is
that directory. What the pages show, the themes and the times are [The web UI](12-ui.md).

## Killing the master or a worker

`kubectl delete pod` sends SIGTERM. The worker drains: a task that finishes within the grace
period ends `success` (one attempt), and one still running ends `interrupted` and runs again
without using a retry. `scripts/kind-distributed.sh` checks that with a worker deleted during
a 20 s task, and `scripts/kind-mesh.sh` checks it for standalone in the mesh.

A crash leaves no time to drain. `kubectl delete pod POD --force --grace-period=0` kills the
container at once. What schedr does then is in [State and failures](07-failures.md). With the
chart's defaults (one master, workers as a StatefulSet with a volume each, `[master]
lease_period` 30 s):

| Killed | The run | Attempts |
|---|---|---|
| The master pod | Finishes. Workers keep the task and register again with the new master. It holds the lease within `[master] lease_period` (30 s) of the old one's last renewal, after its pod has been scheduled and has started. The kind script allows 180 s for that. | The same attempt, `success`. No retry. Each worker is `disconnected` until it registers, then `active`. |
| A worker pod | Finishes when the task has a retry left. The default is `retries=0`, so a DAG that does not set `retries` fails the run here. The StatefulSet brings the pod back under the same name, with its volume, so the journal is still there. | `lost`, with `error_summary.exception` `WorkerRestarted` ("worker `<id>` restarted while the attempt ran (possibly out of memory)"), then `success`. The other worker stays `active`. |
| The standalone pod, SQLite on its volume | Finishes the same way. The volume keeps the database and the journal, so a run that already succeeded is still there. | `lost` / `WorkerRestarted`, then `success`, and the task needs a retry left. |

A worker listed `lost` after the master is killed means the downtime was counted against the
lease, which it must not be. A killed worker's attempt is `WorkerLost` instead of
`WorkerRestarted` when its new pod is not ready within `[master] lease_timeout` (60 s). That
is the rule, and the kind script prints it. The other worker has to stay `active`.

`scripts/kind-kill.sh` runs these three, in that order, on the local kind cluster `schedr-test`
(context `kind-schedr-test`; any other cluster name is refused). It is not part of `make check`.
The context's server has to be loopback; `scripts/kind-standalone.sh`,
`scripts/kind-distributed.sh` and `scripts/kind-mesh.sh` require the same, and all four
share `scripts/kind-lib.sh`.
It needs the images from `make image` and `make server-image`. `hold` sleeps 90 s with `retries=0`
(the master). `again` sleeps 30 s with `retries=1` and `retry_delay` of 0 (a worker, then
standalone). The default delay is 5 minutes, which would only postpone the same retry. `ping` is
a short run that has to still be `success` after the standalone pod is killed.

### On OpenShift, by hand

The same three checks, against a release that is already installed. Nothing in this repository
runs them: `scripts/kind-kill.sh` refuses every context except `kind-schedr-test`. A plain
`kubectl delete pod` is the drain above. The crash is the command with `--force
--grace-period=0`.

The DAGs, wherever the workers or the standalone pod read their code. `ping` is the short
run that standalone keeps:

```python
import time
from datetime import timedelta

from schedr import CommandOperator, Dag, task

with Dag("hold"):

    @task(retries=0)
    def hold() -> str:
        time.sleep(90)
        return "held"

    hold()

with Dag("again"):

    @task(retries=1, retry_delay=timedelta(0))
    def again() -> str:
        time.sleep(30)
        return "again"

    again()

with Dag("ping"):
    CommandOperator("ping", command="echo pong")
```

The call inside `with Dag` records the task. Decorating the function and not calling it
leaves a DAG with no tasks: a trigger still returns a run, and that run never starts `hold`
or `again`. `CommandOperator` records `ping` by being constructed there.

`CTX` and `NS` are the lab's context and namespace, `REL` the release name (`schedr` in the
kind scripts). Distributed, the CLI and the API are the webserver's. Standalone, the pod is
`schedr-0` and the same commands run there. The server image has no Python, so the attempts
document is read with `wget` and judged on the laptop.

```bash
CTX=<the lab context>
NS=<namespace>
REL=schedr
kubectl --context "$CTX" -n "$NS" exec "deploy/$REL-webserver" -c schedr -- \
  schedr dags unpause hold --server http://127.0.0.1:8080
kubectl --context "$CTX" -n "$NS" exec "deploy/$REL-webserver" -c schedr -- \
  schedr runs trigger hold --server http://127.0.0.1:8080
```

When `hold`'s attempt has `started_at`, kill the master. The attempts stay that one attempt,
and it ends `success`. `schedr workers list` shows both workers `active` again;
`disconnected` in between is the lease starting over.

```bash
kubectl --context "$CTX" -n "$NS" delete pod \
  -l "app.kubernetes.io/instance=$REL,app.kubernetes.io/component=master" \
  --force --grace-period=0
kubectl --context "$CTX" -n "$NS" exec "deploy/$REL-webserver" -c schedr -- \
  wget -qO- "http://127.0.0.1:8080/api/v1/runs/<run id>/tasks/hold/attempts"
```

Then `again`. Kill the pod named by that attempt's `worker_id` (`$REL-worker-0` or
`$REL-worker-1`), not a fixed pod:

```bash
kubectl --context "$CTX" -n "$NS" exec "deploy/$REL-webserver" -c schedr -- \
  schedr dags unpause again --server http://127.0.0.1:8080
kubectl --context "$CTX" -n "$NS" exec "deploy/$REL-webserver" -c schedr -- \
  schedr runs trigger again --server http://127.0.0.1:8080
kubectl --context "$CTX" -n "$NS" exec "deploy/$REL-webserver" -c schedr -- \
  wget -qO- "http://127.0.0.1:8080/api/v1/runs/<run id>/tasks/again/attempts"
kubectl --context "$CTX" -n "$NS" delete pod "<worker_id>" --force --grace-period=0
```

The attempts, oldest first, are `lost` with `WorkerRestarted`, then `success`. `WorkerLost`
instead, when the new pod was not ready within 60 s, is the same rule.

Standalone on SQLite: `ping` first, then `again`. The commands run in `$REL-0`. The attempt's
`worker_id` is that pod.

```bash
kubectl --context "$CTX" -n "$NS" exec "$REL-0" -c schedr -- \
  schedr dags unpause ping --server http://127.0.0.1:8080
kubectl --context "$CTX" -n "$NS" exec "$REL-0" -c schedr -- \
  schedr runs trigger ping --server http://127.0.0.1:8080
kubectl --context "$CTX" -n "$NS" exec "$REL-0" -c schedr -- \
  schedr dags unpause again --server http://127.0.0.1:8080
kubectl --context "$CTX" -n "$NS" exec "$REL-0" -c schedr -- \
  schedr runs trigger again --server http://127.0.0.1:8080
kubectl --context "$CTX" -n "$NS" delete pod "<worker_id>" --force --grace-period=0
```

`ping` is still `success`. `again` is `lost` / `WorkerRestarted`, then `success` (`WorkerLost`
when the pod took longer than the 60 s lease).
