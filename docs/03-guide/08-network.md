# Network and security

## The links

| Link | Protocol | Opened by | Security |
|---|---|---|---|
| browser → web UI | HTTP, the pages, and long-lived streams for live updates | the browser | none of its own yet (see below) |
| CLI → webserver | HTTP, REST API | the CLI | none of its own yet (see below) |
| web UI → webserver | HTTP, REST API, including one long-lived events stream per deployment | the UI | none of its own yet (see below) |
| worker → master | gRPC over HTTP/2: one bidirectional stream per worker, plus unary calls | the worker | mTLS, or the mesh |
| webserver → worker | HTTP, the log endpoint | the webserver | mTLS, or the mesh |
| master, webserver → database | Postgres or SQLite | the role | the database's own |
| probes, Prometheus → any role | HTTP, the ops port | them | none: it serves no data |

There is no link between the master and the webserver: the database carries commands one
way and state the other. The web UI's only link is the REST API, including one long-lived
stream of what changed per deployment (see [The events stream](#the-events-stream)). `schedr
ui` listens on `[ui] port` (8082) and calls each webserver over the network. In `schedr
standalone` the UI is on the REST API's port, and that call stays inside the process. The UI
has no link to the master, the workers or the database.

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

## The REST API and the web UI have no authentication yet

The modes that check callers come with M3.5. Until then the webserver and the web UI trust
whoever reaches them. Each warns at startup when it listens beyond localhost:

- the webserver, on `[webserver] bind` and `port` (all interfaces, 8080): keep it behind an
  authenticating proxy or the mesh, or set `[webserver] bind = "127.0.0.1"`;
- `schedr ui`, on `[ui] bind` and `port` (all interfaces, 8082): the same proxy or mesh, or
  `[ui] bind = "127.0.0.1"`;
- `schedr standalone` with `--listen` beyond localhost. The warning names the REST API, and
  the UI is on that same port. Without `--listen` it listens on 127.0.0.1, and there is no
  warning.

The webserver can record who made a command from a header (`[webserver.auth]
identity_header`), and it never uses that header to allow or deny anything. `schedr ui` sends
no credentials when it calls a webserver, so a command it sends is recorded as `anonymous`
until the auth modes pass the user's name on (M3.5).

Anyone who can open the UI can take an action. Two limits keep another site from doing that
with a visitor's browser.

**An action is taken only from the UI's own pages.** A POST from anywhere else is refused
(403), the page says "Refused: actions are taken only from schedr's own pages.", and nothing
is sent to the webserver. A browser sends `Sec-Fetch-Site`. `same-origin`, from the UI's own
page, and `none`, from an address the user typed, are taken. `cross-site` and `same-site`
are refused, and an `Origin` that names this UI doesn't change that. A browser that sends
no `Sec-Fetch-Site` is taken only when its `Origin` names the same host as the request. That
host is `X-Forwarded-Host` when the request carries it (a proxy in front puts the public
host there), and otherwise `Host`. The default ports are left out of the comparison (`:443`
for an `https` origin, `:80` for `http`), and case is ignored. A request with neither header
is refused. Opening a page is a GET, and GETs aren't checked.

**The UI's pages can't be shown in another site's frame.** Every answer from the UI carries
`X-Frame-Options: DENY` and `Content-Security-Policy: frame-ancestors 'none'`, so a page's
buttons can't be clicked through another site's frame. Standalone serves the pages on the
API's port, and those answers carry the same headers.

The CLI sends a bearer token for such a proxy or mesh with `--token` (or `SCHEDR_TOKEN`), e.g.
a Keycloak token that Istio checks at the gateway.

## The events stream

`GET /api/v1/events` is a server-sent events stream of what changed. The UI is the client. A
browser doesn't open this path. It opens the pages, and in standalone those pages are on this
same port. The stream carries:

| Event | Data | When |
|---|---|---|
| `run` | `{"run_id", "dag_id"}` | That run, one of its tasks, or one of its attempts changed. |
| `dag` | `{"dag_id"}` | That DAG changed. |
| `workers` | `{}` | A worker registered, changed state, or was seen. A heartbeat counts as seen. |
| `refresh` | `{}` | Anything may have changed: a change was missed, or the changes can't be followed. |

The [Workers](12-ui.md#workers) page lists them, and a `workers` event makes that page read
itself again. No other page lists the workers, so no other page redraws for that event. A
comment every 15 s keeps an idle stream from looking dead. The response is
`Content-Type: text/event-stream`, with `Cache-Control: no-cache` and
`X-Accel-Buffering: no`. The webserver follows the database once, from the first stream, and
hands each change to every open stream. When the changes can't be followed, each stream gets
`refresh` every 5 s (see [The master](04-master.md)). A stream more than 1,024 changes behind
gets `refresh` instead of the ones it skipped.

The UI opens one of these streams per deployment. If it drops, the UI opens it again after
1 s, doubling up to 30 s. In `schedr standalone` that stream stays inside the process. Each open
page holds another stream, to the UI: `GET /events` on the UI's port (the API's port in
standalone). While a running try's Logs tab is open, the browser holds one more,
`GET /d/<deployment>/runs/<run>/tasks/<task>/tail`. Those two are server-sent events as well,
with the same cache and buffering headers and a comment every 15 s. The page's stream sends
a ping every 15 s. What the pages do with them is [Live updates](12-ui.md#live-updates).

A proxy in front of the webserver, and a proxy in front of the UI, has to leave these
responses open and send each byte on as it is written. Buffering holds the updates until the
buffer fills. `X-Accel-Buffering: no` is nginx's switch for that. A proxy that ignores the
header can still buffer `text/event-stream`. `Cache-Control: no-cache` tells a cache not to
store the response. An idle timeout has to allow a stream whose only traffic for a while is
the comment every 15 s. When a proxy in front of the UI holds the page's stream back, the
page sees no ping. After 30 s of that it reads its live parts every 5 s instead of waiting
for the stream.

## The API's description and Swagger UI

Every webserver (and standalone) serves its OpenAPI description at `/api/v1/openapi.json` and
**Swagger UI** over it at **`/api/v1/docs/`**, always: they describe the API and do nothing.
This is separate from the web UI's documentation at `/docs` (see
[The web UI](12-ui.md#the-documentation)). The Swagger files are built into the binary, so the
page needs no internet access. The description
lists an optional bearer token: schedr checks none itself, but Swagger UI's **Authorize** button
sends one, for a mesh or proxy that requires it. Behind a gateway that serves schedr under a
path prefix and removes it, setting `X-Forwarded-Prefix` (as Istio's `rewrite` with a header
does), the page still works: it refers to the description by a relative URL, and the
description names the prefix as its server. The chart's Istio policy leaves both paths
open to everyone without a token (see [Deployment](10-deployment.md#inside-an-istio-mesh)).

## Secrets

Configuration secrets (a database URL with a password) come only from environment variables,
never from the configuration file: a file with one is a startup error. They are never logged.
