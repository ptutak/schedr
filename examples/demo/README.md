# The connections demo

Every connection type schedr has, opened from Python and from a command, in two deployments
running the same DAG folder: `dev` and `prod`, each with secrets of its own.

```console
$ make demo          # builds the images, then runs everything; Ctrl-C stops it
```

Then open <http://127.0.0.1:8082>: one `schedr ui` over both deployments. Trigger
`connections_demo` in each: its tasks name the deployment they ran in and read its greeting
from Postgres, MySQL, MariaDB and Mongo (`hello from dev's Postgres`, `hello from prod's
Postgres`, ...), log in over SSH and call an HTTP API, once from Python and once with each
server's own client (`psql`, `mariadb` for MySQL and MariaDB, `mongoexport`, `ssh`, `curl`).

Each deployment's connections checked and tested from where its tasks run:

```console
$ podman-compose -f examples/demo/compose.yaml exec dev schedr connections check
$ podman-compose -f examples/demo/compose.yaml exec prod schedr connections test warehouse
```

`make demo-check` does all of it unattended (both runs, a check and a test of every
connection in both) and stops the demo; `DEMO_KEEP=1` leaves it running. `make demo-down`
stops it, `make demo-clean` also drops its passwords and keys.

## What runs

| Service | What |
|---|---|
| `postgres`, `mysql`, `mariadb`, `mongo` | the databases; each has a `dev` and a `prod` database, with users `dev_app` and `prod_app` |
| `ssh` | an OpenSSH server: user `demo`, logging in with either deployment's key |
| `httpbin` | [go-httpbin](https://github.com/mccutchen/go-httpbin): `/bearer` takes any bearer token |
| `dev`, `prod` | `schedr standalone --no-ui`, the DAGs of `dags/`, SQLite inside the container |
| `ui` | `schedr ui` over `dev` and `prod`, published on 127.0.0.1:8082 only |

[`dags/connections.py`](dags/connections.py) defines the connections; every value comes from
the deployment's secret storage (schedr's own database), which
[`start.sh`](start.sh) fills when a deployment starts. The values themselves are generated
once, by [`keys.sh`](keys.sh): a password per deployment (for every database), an API token,
an SSH key, and the SSH server's host key, which the deployments' `known_hosts` trusts. They
are kept, in plain text, in the container engine's volumes (`schedr-demo-dev`,
`-prod`, `-servers`, `-ssh-users`, `-ssh-host`), one per reader: `dev` mounts only its own,
never `prod`'s or the servers' administrator password. They stay until `make demo-clean`.

## Not like production

- **No TLS to the databases:** Postgres and Mongo have none here (`?sslmode=disable`,
  `tls=false`); MySQL and MariaDB encrypt with a certificate they made themselves
  (`?sslmode=require`, unchecked). A real deployment keeps the default, `verify-full`.
- **No authentication on the API:** the deployments' APIs are reachable only on the
  demo's network, and the UI on your machine's loopback.
- **The base image is built here** (`make image`, the first time, which needs Rust and uv):
  the demo needs schedr's current code, newer than any release. Once a release has
  connections, `make demo IMAGE=ghcr.io/ptutak/schedr:<version>` pulls it instead, and
  only podman (or Docker) and its compose are needed.
