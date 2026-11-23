# What schedr trusts

This page says what schedr trusts, what it guarantees against the rest, and what it won't do
by design (decision 520). It matters most when a worker runs
[extensions](21-extensions.md) that control a larger system, such as a network
orchestration service. A worker's certificate then opens more than schedr.

## Nodes are trusted

**schedr trusts the machines it runs on.** A worker's process, its plugins and its tasks run
as one user on one node. They can all read the worker's certificate and key. Someone who
controls a node, or a plugin on it, can do what that worker can do. schedr can't prevent that,
and doesn't try. So these are left out on purpose:

| Not done | Why |
|---|---|
| Encrypting the links between a worker and its plugins | Whoever could read them can read the worker's key too. |
| Running a plugin as an OS user of its own | Its tasks need the worker's files, and a plugin is the worker's code. |
| Keeping standalone's database in memory | It would lose every run at a restart, which is what a database is for. |

What schedr does instead is **limit how far one compromised worker reaches**, and keep
everything outside the nodes out.

## A worker acts only as itself

Every call a worker makes to the master is checked:

- **Its role.** In `[tls] mode = "app"` the master's gRPC port takes only a client certificate
  naming `[tls] worker_name`. The webserver's or `schedr ui`'s certificate, though signed by
  the same CA, can't connect as a worker.
- **Its own attempts.** Every message about an attempt is taken only from the worker that holds
  the attempt: starts, rejections, results, ended tasks, cleanups and their handles, reports,
  a task's calls to the API, an extension's notifications. Another worker's is ignored or
  refused. The same goes for wait steps (only the worker watching them) and connection tests
  (only the worker that was asked).
- **Its own code.** A worker uploads a parse only of the code it reported for a root, or of a
  parse job it was given. A parse defines DAGs, their commands among them, so another's upload
  would make every worker run what it chose. A worker that serves a root is still trusted with that root's
  code: it parses it, and its parse defines the root's DAGs.
- **Its own ID, with `[tls] worker_identity = "certificate"`.** By default every worker
  shares one certificate (the chart's), so any worker's key can register as any worker ID.
  With this key set on the master, a worker's certificate must also name its ID, as the DNS
  name `<id>.<worker_name>` (e.g. `schedr-worker-0.schedr-worker`). The ID must then be a
  lowercase DNS label (letters a-z, digits, `-`), as pod names are: DNS names ignore case, and
  worker IDs don't. It is checked at registration, at a parse upload and when an attempt's
  inputs are fetched. So a stolen key
  acts as one worker only, on that worker's attempts. A wildcard name (`*.schedr-worker`)
  names no worker, so it is refused; nor does a wildcard pass for `[tls] worker_name` or
  `ui_name` themselves. The chart sets it with `tls.workerIdentity: certificate`
  (see [Deployment](10-deployment.md)). **Turn it on where workers control other systems.**

In `mesh` mode schedr sees no certificates. There the chart's Istio AuthorizationPolicy
(`istio.authorizationPolicy.enabled`, with an account per role) lets only the workers'
service account reach the master's gRPC port. Its `sameNamespace` rule opens every port to the
whole namespace, so leave it off. Istio's identity is per service account, not per pod, so a
per-worker identity isn't available in `mesh` mode: schedr refuses to start with
`worker_identity = "certificate"` there.

## Links on one machine

**A worker's links to its own processes are never ports.** Each one is a socket pair, made
for one process and passed to it as an inherited descriptor:

| Link | Between |
|---|---|
| The runner's channel | the worker and a task's runner |
| A fork server's channel | the worker and its fork servers |
| `task-ctx` | a task's process and the worker |
| An extension's channel (`SCHEDR_AGENT_FD`) | the worker and a plugin it started |
| A shell plugin's script | `schedr extension shell` and its script's `task-ctx` |
| An extension's service's control socket (`SCHEDR_SERVICE_FD`) | the worker and the service it started |
| An attempt's connection to its service | the attempt's plugin and the service, the service's end passed by the worker |

Nothing listens, so no other process can connect: it would need the descriptor itself. A
plugin should make its end close-on-exec first, so processes it starts don't inherit it
(the helpers do this; see [the plugin protocol](22-plugin-protocol.md)). A long-running
process the worker didn't start can't attach to the protocol, by design.

**One link is a file:** the cleanup barrier. A task's process waits there while its cleanup
may move to another worker. It is a socket in a folder only the worker's user can open
(`schedr-barriers-<pid>-…`, mode 0700, in `$TMPDIR`). The one exposure is another container
in the pod running as the same user with the same `/tmp`. Don't share `/tmp` with such a
sidecar.

**The ops port answers anyone who reaches it.** It serves `/healthz`, `/readyz` and
`/metrics` only, never data or actions, so probes and Prometheus need no credentials. Its
metrics carry DAG IDs, and extensions' figures with the labels their tasks gave them. Keep it inside
the cluster (the chart's AuthorizationPolicy can limit who reaches it).

## Standalone

`schedr standalone` runs every role in one process, so its master's gRPC port and its
worker's log endpoint are on the same machine as everything else. They listen on 127.0.0.1
only, and since decision 520 under mTLS made in memory:

- At each start, standalone makes a CA and a certificate in the process. They are never
  written to disk. Another user of the machine, or a sidecar, has no key to connect with:
  it can't register a worker or read a log.
- The certificate lives 24 hours. A new one replaces it every 12 hours of wall time, looked at
  every minute, so standalone can run for months, and a laptop woken from sleep renews it
  within a minute.
- `[tls]` doesn't apply to these links.

A port is only one way in. Standalone's database and secrets are files, so standalone keeps
them private too:

- What it creates is its user's alone: the data directory (mode 0700) and its SQLite file
  (0600; SQLite gives the `-wal` and `-shm` files the same mode).
- At start it warns of a data directory, a database file, a configuration file, a users file
  or a UI token file that other users of the machine can read, and names the `chmod` that
  fixes it. It changes nothing of yours. Group access isn't warned of: a volume's `fsGroup`
  gives it.

**Its REST API is the way in that remains.** With `[webserver.auth] mode = "none"` (the
default), every user of the machine can reach 127.0.0.1, and through the API read runs and
task logs: the API fetches logs over the mTLS itself. Standalone says so in a warning at start.
On a machine shared with other users, set `[webserver.auth] mode = "basic"` (a users file) or
`"proxy_header"`.

A Postgres URL's password still belongs in the environment (`SCHEDR_CONFIG__DATABASE__URL`)
or a Secret, never the configuration file.

## Running a controller for a larger system

A worker whose extension drives another system holds that system's keys as well as its own.
To keep what a compromise reaches small:

1. **Give it a deployment of its own**, its workers on nodes running nothing else, or a
   standalone on a machine of its own, with the in-memory mTLS above.
2. **Use a certificate per worker** (`tls.workerIdentity: certificate`), so one worker's key
   isn't every worker's.
3. **Give the controlled system least privilege.** The extension's credentials should allow
   only what its tasks do. That system's own audit then shows what was done.
4. **Rotate the certificates.** schedr reads `[tls]` files again every `reload_interval`, so
   cert-manager or your own rotation needs no restart. The chart's per-pod certificates
   are renewed by cert-manager's CSI driver.
