# Network and security

## The links

| Link | Protocol | Opened by | Security |
|---|---|---|---|
| CLI → webserver | HTTP, REST API | the CLI | none of its own yet (see below) |
| worker → master | gRPC over HTTP/2: one bidirectional stream per worker, plus unary calls | the worker | mTLS, or the mesh |
| webserver → worker | HTTP, the log endpoint | the webserver | mTLS, or the mesh |
| master, webserver → database | Postgres or SQLite | the role | the database's own |
| probes, Prometheus → any role | HTTP, the ops port | them | none: it serves no data |

There is no link between the master and the webserver: the database carries commands one
way and state the other.

**Workers always connect to the master.** The master needs no address for its workers, and a
worker only needs `[worker] master_url`. The stream stays open, so the master can still send
work and cancels at once. Heartbeats every 10 s keep it alive through proxies and the mesh's
idle timeouts.

## `[tls] mode`: who encrypts

**`app` (the default): schedr does mTLS itself** on the master's gRPC port and on every
worker's log endpoint.

- Each role gets `[tls] ca`, `cert` and `key`: PEM files, typically mounted from a Kubernetes
  Secret. A role without them refuses to start, and says to set them or use `mode = "mesh"`.
- Servers require a client certificate signed by the CA. A worker checks that the master's
  certificate names the host of `master_url`. The webserver checks that a worker's certificate
  names `[tls] worker_name` (default `schedr-worker`), whatever address it used, since pod IPs
  and names change.
- TLS 1.3 only (rustls). The files are read again every `[tls] reload_interval` (30 s), so a
  rotated certificate is used for new connections without a restart; a broken change keeps
  the old one.
- `master_url` must be `https://` in this mode.

**`mesh`: plain traffic, for a service mesh** (e.g. Istio) that already provides mTLS between
pods. schedr adds no TLS of its own, and the mesh's policies decide who may connect.
`master_url` must be `http://`. Every role must use the same mode. A webserver that reaches a
worker in the other mode reports it.

`schedr standalone` never uses TLS: its gRPC port and log endpoint listen on localhost only.

## Reaching workers: the log address

The webserver reads logs from the worker that ran the attempt, at the address the worker
sent in `Register`: `[worker] log_address` (the host name when not set) and `log_port`. Under
Istio with `outboundTrafficPolicy: REGISTRY_ONLY`, traffic may only go to hosts in the mesh's
registry. A raw pod IP isn't one, but a pod behind a **headless Service** is. So workers run as
a StatefulSet with a headless Service, and each worker's `log_address` is set to its own pod's
DNS name (`<pod>.<service>.<namespace>.svc.cluster.local`). A headless Service doesn't
balance: the name resolves to that one pod. Istio doesn't apply its automatic mTLS to a
headless Service's pods, so the chart adds a DestinationRule with `ISTIO_MUTUAL` for it; the
calls then carry the webserver's identity, which the workers' policies check.

Requests to workers **never go through an HTTP proxy**, whatever `HTTPS_PROXY` says: a proxy
can't reach pod addresses, and in mesh mode it would bypass the mesh's policy.

## The REST API has no authentication yet

Until the web UI (milestone M3), the webserver trusts its callers. It can record who made a
command from a header (`[webserver.auth] identity_header`), but never uses it to allow or deny
anything. A webserver listening beyond localhost warns about this at startup. Keep it behind an
authenticating proxy or the mesh.

The CLI sends a bearer token for such a proxy or mesh with `--token` (or `SCHEDR_TOKEN`), e.g.
a Keycloak token that Istio checks at the gateway.

## The API's description and Swagger UI

Every webserver (and standalone) serves its OpenAPI description at `/api/v1/openapi.json` and
**Swagger UI** over it at **`/api/v1/docs/`**, always: they describe the API and do nothing.
The UI's files are built into the binary, so it needs no internet access. The description
lists an optional bearer token: schedr checks none itself, but Swagger UI's **Authorize** button
sends one, for a mesh or proxy that requires it. Behind a gateway that serves schedr under a
path prefix and removes it, setting `X-Forwarded-Prefix` (as Istio's `rewrite` with a header
does), the page still works: it refers to the description by a relative URL, and the
description names the prefix as its server. The chart's Istio policy leaves both paths
open to everyone without a token (see [Deployment](10-deployment.md#inside-an-istio-mesh)).

## Secrets

Configuration secrets (a database URL with a password) come only from environment variables,
never from the configuration file: a file with one is a startup error. They are never logged.
