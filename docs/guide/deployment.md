# Deployment

## Configuration

Every role reads the same configuration: one TOML file (`--config`, else
`SCHEDR_CONFIG_FILE`, else `/etc/schedr/config.toml`) and environment variables mirroring
it, e.g. `SCHEDR_CONFIG__MASTER__GRPC_PORT` for `[master] grpc_port`. Flags win over the
environment, which wins over the file. Unknown keys are an error. Each role uses only its own
sections. The [configuration reference](../_generated/reference/configuration.md) lists
every key.

## The roles in a cluster

| Role | Replicas | Needs |
|---|---|---|
| `schedr master` | **exactly one** | `[database] url`, a migrated schema; its gRPC port behind a Service the workers reach |
| `schedr webserver` | one or more | `[database] url`, a migrated schema; its REST port behind a Service |
| `schedr worker` | as many as the load needs | `[worker] master_url`, the DAG code (e.g. a git-sync sidecar), a disk for its journal and logs; a StatefulSet with a headless Service, each pod's `log_address` set to its DNS name |

- **Postgres** is the database for anything spread over several hosts. Run
  `schedr db migrate` before starting the master and the webserver; both refuse a schema that
  doesn't match their version. SQLite works for roles on one host with a local disk.
- **Upgrade the master first**, then the webservers and workers (see
  [Parsing and code versions](parsing.md#upgrades)).
- **Workers' `terminationGracePeriodSeconds`** should match `[worker]
  termination_grace_period`, so a draining worker has the time it expects.
- **Probes** go to the ops port: liveness on `/healthz` for every role, readiness on `/readyz`
  for the master and the webserver. Workers need no readiness probe: nothing connects to them
  through a Service.
- **Memory:** a worker pod's memory limit is split between its slots (see
  [Workers](workers.md#memory)); size the pod for its slots.

A Helm chart for this layout comes with step 2.10.

## One pod: `schedr standalone`

To try schedr on a cluster before the full deployment, `examples/kubernetes/` runs
`schedr standalone` in one pod. Its `Dockerfile` builds the image from the musllinux wheel on
Alpine, with `tini` as PID 1. `standalone.yaml` sets up a StatefulSet with one replica, a
Service for the REST API, probes on the ops port, a volume for the data directory, and a
git-sync sidecar for the DAG code.

- `--listen 0.0.0.0` opens the REST API and the ops port beyond localhost; the master's gRPC
  and the log endpoint stay on localhost.
- **One replica only.** A lock in the data directory refuses a second standalone on the same
  data (two would schedule every run twice).
- The volume must be block storage (ReadWriteOnce): SQLite's locks aren't reliable on network
  filesystems.
- Not for production: one process runs every role, so a restart restarts the master and the
  worker together, and tasks share the pod's memory.

On a laptop, `schedr standalone` alone runs everything on localhost, with SQLite in the data
directory (`[worker] data_dir`, else `~/.local/share/schedr`), and migrates the database at
start.
