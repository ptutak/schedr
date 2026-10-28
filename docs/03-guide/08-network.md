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

## Sign-in

schedr records who took each action, on the command (its `requested_by`), and shows it in
the UI. It never uses the name to allow or deny anything: there are no roles, and whoever is
signed in can do everything. Who may reach schedr at all is decided in front of it, by the
mesh, the network or a sign-in proxy. schedr keeps no users of its own: no user database, no
Users page, no sessions (decision 39).

Three links are involved, each set on its own:

- **a browser at the web UI**, by `[ui.auth]` (a standalone's own pages: `[webserver.auth]`);
- **`schedr ui` at a webserver**, by `[tls] mode`: how the UI proves it is `schedr ui`, so
  the webserver takes the name it passes on;
- **the CLI or Swagger UI at the API**, by `[webserver.auth]`.

### `[webserver.auth]` and `[ui.auth]`

The same keys in both sections.

| Key | Values | Default | What it does |
|---|---|---|---|
| `mode` | `none`, `proxy_header`, `basic` | `none` | How the user is known (below). |
| `identity_header` | a header name | unset; `x-forwarded-user` in `proxy_header` | `none`: the user's name when the header is there (e.g. set by Istio from a Keycloak token), else `anonymous`. `proxy_header`: required, set by the sign-in proxy. |
| `users_file` | a path | unset | `basic`: `name:bcrypt-hash` lines, from a Secret (never in the configuration). Read again when it changes. |
| `behind_tls_proxy` | bool | `false` | `basic` beyond a loopback address: TLS is ended in front. Without it schedr refuses to start, as passwords would cross the network in the clear. |
| `allow_remote` | bool | `false` | `proxy_header` beyond a loopback address: only the proxy can reach schedr. Without it schedr refuses to start, as anyone reaching it around the proxy could name any user. |

| `mode` | Who signs people in | For |
|---|---|---|
| `none` | Whatever is in front (the mesh, the network). The name comes from `identity_header` if set, else it is `anonymous`. | Istio with Keycloak; a laptop. |
| `proxy_header` | A sign-in proxy in front (oauth2-proxy, OpenShift's oauth-proxy, …), which sets the header. | Kubernetes without a mesh, with any identity provider. |
| `basic` | schedr: HTTP Basic against `users_file`. | A lab, a single machine. |

A sign-in against LDAP (`mode = "ldap"`) is planned.

In every mode, a header sent twice names no one, and a name with control characters or over
256 characters isn't taken.

#### The users file

One line per user, `name:bcrypt-hash`. Blank lines and `#` comments are skipped. The
passwords themselves are never stored. `scripts/users-file.py` asks for each password and
writes the file and a Kubernetes Secret holding it:

```bash
scripts/users-file.py --out ~/schedr-users anna ben
kubectl apply -f ~/schedr-users/users-secret.yaml   # or the chart's auth.usersSecret
```

A running schedr looks at the file every 5 seconds and reads it again when it has changed.
A changed password or a removed user then takes effect, and checks remembered from the old
file are forgotten. A Secret's mounted file is updated by Kubernetes within about a minute.
A file that can't be read or doesn't parse leaves the users as they were and is logged as an
error, and it is tried again 5 seconds later. An empty file signs no one in.

bcrypt is slow on purpose (about 0.25 s at cost 12), and every request carries the password,
since there are no sessions. So a check that passed is remembered for 5 minutes, by a keyed
hash of the name and password, never the password itself. A failed check isn't remembered.
An unknown name takes as long as a known one.

### How `schedr ui` proves itself to a webserver

A webserver takes the name the UI passes on (`X-Schedr-User`) only from a caller it knows is
`schedr ui`. Anyone else's `X-Schedr-User` is ignored. A UI it can't recognise is treated as
any other caller: it is recorded as `anonymous`, or refused in `proxy_header` and `basic`.

| Proof | Webserver | `schedr ui` | Used |
|---|---|---|---|
| mTLS on the UI port | `[webserver] ui_port` (8083) in `[tls] mode = "app"`; the client certificate must name `[tls] ui_name` (`schedr-ui`). | `[[ui.deployments]] url = "https://<webserver>:8083"` and its own `[tls]` certificate. | Distributed, `app` mode. |
| The mesh on the UI port | `[webserver] ui_port` in plain HTTP with `ui_port_in_mesh = true` in `[tls] mode = "mesh"`: a mesh policy lets only `schedr ui` reach it. | `url = "http://<webserver>:8083"`. | Distributed, `mesh` mode with Istio's AuthorizationPolicy. |
| A shared token | `[webserver] ui_token_file`: at least 16 characters, read again when it changes. | `[[ui.deployments]] token_file`: the same token, sent in `X-Schedr-Ui-Token`. | Standalones in a federation; `mesh` mode without Istio's policy. |
| In-process | Nothing to set. | Nothing to set. | A standalone's own UI. |

The UI port follows `[tls] mode` as the links between the master and the workers do. In
`app` mode schedr's own mTLS guards it. In `mesh` mode the mesh does, and nothing in schedr
does. So `ui_port_in_mesh` is off by default, is refused in `app` mode, and logs a warning
when it is on.

### Which combinations are allowed

`[tls] mode` decides only the UI's proof, `[ui.auth]` only the browser's sign-in at the UI,
and `[webserver.auth]` only callers of the API directly. The UI must sign people in as the
API does, or the API's mode is `none`, kept internal, with people coming through the UI
(decision 41A). For a distributed release with its UI, the chart refuses the other pairs.
For a `schedr ui` over other releases, nothing sees both sides, so it is the rule to follow.

| `tls.mode` | `webserver.auth` | `ui.auth` | Browser → UI | UI → webserver | CLI → API | For |
|---|---|---|---|---|---|---|
| app | none | none | Open; the name from `identity_header` | mTLS | Open | Behind a mesh or network that signs people in |
| app | none | proxy_header | The sign-in proxy | mTLS | Open, kept internal | People through the UI only |
| app | none | basic | The users file | mTLS | Open, kept internal | A lab, people through the UI only |
| app | proxy_header | proxy_header | The sign-in proxy | mTLS | The sign-in proxy | Kubernetes without a mesh |
| app | basic | basic | The users file | mTLS | The users file (`--user`) | A lab, a small team |
| mesh | none | none | Istio (Keycloak's token); the name from `identity_header` | The mesh on the UI port, else the token | Istio (`--token`) | **Istio with Keycloak** |
| mesh | none | proxy_header | The sign-in proxy | The mesh, else the token | Open, kept internal | People through the UI only |
| mesh | none | basic | The users file | The mesh, else the token | Open, kept internal | A lab in a mesh |
| mesh | proxy_header | proxy_header | The sign-in proxy | The mesh, else the token | The sign-in proxy | A mesh without Istio's token check |
| mesh | basic | basic | The users file | The mesh, else the token | The users file (`--user`) | A lab in a mesh |

Refused: a UI open in front of an API that asks for a sign-in (`none` at the UI with
`proxy_header` or `basic` at the API), as anyone could act through the UI. Also refused: two
sources of people (`proxy_header` at one and `basic` at the other), which would show one
person under two names.

A standalone has no `[tls] mode`. Its pages and its API share `[webserver.auth]`. A
`schedr ui` elsewhere that shows it proves itself with the token:

| `webserver.auth` | Its pages and API | A `schedr ui` elsewhere |
|---|---|---|
| none | Open (keep it on localhost) | The token passes the name on; without it, `anonymous` |
| proxy_header | The sign-in proxy | The token; without it, refused |
| basic | The users file | The token; without it, refused |

### In the chart

| Value | Becomes |
|---|---|
| `webserver.auth.{mode, identityHeader, behindTlsProxy, allowRemote}` | `[webserver.auth]` of the webserver and the standalone |
| `webserver.auth.usersSecret.{name, key}` | The users file, mounted from that Secret |
| `ui.auth.*` (the same keys) | `[ui.auth]` of the chart's `schedr ui`; `mode` empty: `webserver.auth`'s |
| `webserver.uiTokenSecret.{name, key}` | `[webserver] ui_token_file`. In `mesh` mode with the UI and no mesh proof, the chart makes the token itself and keeps it on upgrades. |
| `ui.deployments[].{name, url, tokenSecret}` | `[[ui.deployments]]` and their token files |
| `tls.mode`, `tls.uiName`, `tls.existingSecrets.ui` | The UI's proof by mTLS, and its certificate |
| `service.webserverUiPort` (8083) | `[webserver] ui_port`: in `app` mode always, in `mesh` mode with the mesh proof |
| `istio.authorizationPolicy.ui.principals` | Other releases' `schedr ui`, let in on the mesh-proven UI port |
| `istio.requestAuthentication.outputClaimToHeaders` | Claims Istio copies from a valid token into headers, e.g. `{header: x-forwarded-user, claim: preferred_username}`, at the API's and the UI's pods |
| `standalone.ui` | `false`: `--no-ui` |

The chart uses the mesh proof by itself in `mesh` mode when `istio.enabled` and
`istio.authorizationPolicy.enabled` are on and the UI's service account is shared with no
other role. It then admits only the UI's account to the port, with a DENY policy for anyone
else, and makes no token. Otherwise it makes a token.

**With Istio and Keycloak** (mode `none`), the name reaches schedr only if Istio puts it in a
header. Istio checks the token, but copies a claim into a header only when told to. So set
both sides, or every action is recorded as `anonymous`:

```yaml
istio:
  requestAuthentication:
    enabled: true
    issuer: https://keycloak.example.com/realms/main
    outputClaimToHeaders:
      - {header: x-forwarded-user, claim: preferred_username}
  authorizationPolicy:
    enabled: true            # api.requireJwt: true, so every request carries a valid token
webserver:
  auth: {mode: none, identityHeader: x-forwarded-user}
```

Istio sets the header from the token it has checked, and with `api.requireJwt` every request
needs a valid token. A copy of the header a client sends itself can't name someone else either
way: if Istio replaces it, Istio's is the only one, and if Istio adds its own beside it, the
header is there twice and schedr takes neither.

A token the chart makes is kept on upgrades by looking up its Secret. A tool that only renders
the chart, as Argo CD does with `helm template`, can't look it up, so each sync makes a new
token. Both sides read the new one within about a minute. To avoid the rotation, use the mesh
proof, or give a Secret of your own in `webserver.uiTokenSecret.name`.

### Another site acting through a visitor's browser

A browser attaches credentials it remembers (Basic, a proxy's cookie) to any site's form, so
two limits keep another site from acting as the person signed in.

**A write is taken only from schedr's own pages.** A browser sends `Sec-Fetch-Site`.
`same-origin`, from schedr's own page, and `none`, from an address the user typed, are taken.
`cross-site` and `same-site` are refused, and an `Origin` that names this host doesn't change
that. A browser that sends no `Sec-Fetch-Site` is taken only when its `Origin` names the same
host as the request. That host is `X-Forwarded-Host` when the request carries it (a proxy in
front puts the public host there), and otherwise `Host`. The default ports are left out of
the comparison (`:443` for an `https` origin, `:80` for `http`), and case is ignored.

- **The UI's actions:** a POST failing this is refused (403). The page says "Refused: actions
  are taken only from schedr's own pages.", and nothing is sent to the webserver. A POST with
  neither header is refused.
- **The API:** a write (anything but GET, HEAD and OPTIONS) that a browser marks as from
  another site is refused (403). A request with neither header comes from no browser (the
  CLI, `schedr ui`) and passes.

Opening a page is a GET, and GETs aren't checked.

**The UI's pages can't be shown in another site's frame.** Every answer from the UI carries
`X-Frame-Options: DENY` and `Content-Security-Policy: frame-ancestors 'none'`, so a page's
buttons can't be clicked through another site's frame. Standalone serves the pages on the
API's port, and those answers carry the same headers.

### Startup warnings

With `mode = "none"`, a role listening beyond localhost warns that it signs no one in:

- the webserver, on `[webserver] bind` and `port` (all interfaces, 8080);
- `schedr ui`, on `[ui] bind` and `port` (all interfaces, 8082);
- `schedr standalone` with `--listen` beyond localhost (its pages are on the API's port).

Keep such a role behind the mesh, the network or a sign-in proxy, or bind it to `127.0.0.1`.

The CLI sends a bearer token for a proxy or mesh in front with `--token` (or `SCHEDR_TOKEN`),
e.g. a Keycloak token that Istio checks. With `basic` it sends `--user`, with the password
from `SCHEDR_PASSWORD`.

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
`refresh` every `[webserver] refresh_interval` (15 s by default; see
[Live updates](12-ui.md#live-updates)). A stream more than 1,024 changes behind gets
`refresh` instead of the ones it skipped.

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
