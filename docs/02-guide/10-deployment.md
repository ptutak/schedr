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

A Helm chart for this layout comes with step 2.11.

## One pod: `schedr standalone`, the production start

Production starts with **standalone in one pod**: first on SQLite, then on Postgres, and the
separate roles only if the load needs them (a full switch: the history stays in Postgres).

**The image.** `images/schedr/Dockerfile` builds the base image (`make image`): Alpine with
Python 3.14 and the `schedr` package, `tini` as PID 1, a data directory writable by group 0
for OpenShift's arbitrary UID. Your DAGs' libraries go in an image of your own built `FROM` it:
`examples/kubernetes/Dockerfile` does it in two stages, compiling in a builder stage what has
no musl wheel, so no compiler ends up in your image.

**The chart.** `charts/schedr/` with `mode: standalone`:

- one replica (a StatefulSet) with a ReadWriteOnce volume on block storage for the data
  directory (task logs, the worker's journal, and on SQLite the database);
- `database.existingSecret`: a Secret with a Postgres URL; without it, SQLite on the volume.
  Any connection pooler can sit in between;
- an **init container** migrates the schema (`schedr db migrate --wait`) before schedr
  starts: an install or upgrade migrates once, and a failed migration shows as a failed init
  container. schedr itself doesn't migrate (no `--migrate`), and refuses a schema that isn't
  up to date;
- the pod's `fsGroup` is 0, so the volume is writable by the image's group, except on
  OpenShift (found by its API), which sets its own;
- `terminationGracePeriodSeconds` (600) is passed to schedr as `[worker]
  termination_grace_period`, so a drain always fits the pod's grace period;
- probes on the ops port, a Service with ports named for Istio (`http-api`, `http-ops`), an
  optional git-sync sidecar for the DAGs, an optional `ServiceMonitor`;
- more settings as `config` (`WORKER__SLOTS: "4"` becomes `SCHEDR_CONFIG__WORKER__SLOTS`).

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
- **The REST API has no authentication** until the UI: keep the Service internal.
- `scripts/kind-standalone.sh` installs the chart on a local kind cluster, on SQLite and on
  Postgres, and runs a DAG.

On a laptop, `schedr standalone --migrate` runs everything on localhost, with SQLite in the
data directory (`[worker] data_dir`, else `~/.local/share/schedr`), migrating the database at
start.
