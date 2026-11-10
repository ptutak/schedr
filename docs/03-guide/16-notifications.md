# Notifications

The master sends notifications to webhooks and email channels. DAG authors choose events and
channels as described in [Notifications](../02-dags/24-notifications.md).

```toml
[notifications]
ui_url = "https://schedr.example.com"
default_events = ["RUN_FAILED"]
give_up_after = "24h"
timeout = "10s"
max_per_attempt = 100
dag_urls = ["https://relay.example.com/schedr"]
ca_file = "/etc/schedr/relay-ca.pem"

[notifications.channels.ops-slack]
type = "webhook"
url = "https://hooks.example.com/service/path"
headers_env = { Authorization = "OPS_WEBHOOK_AUTH" }
body = { text = "{dag}: {event} — {text}" }

[notifications.smtp]
host = "mail.example.com"
port = 587
tls = "starttls"
username = "schedr"
from = "schedr <schedr@example.com>"

[notifications.channels.oncall]
type = "email"
to = ["oncall@example.com", "ops@example.com"]
```

`ui_url` supplies links; without it they are null. `default_events` applies when a DAG
lists a channel without events. `give_up_after` limits retries, `timeout` limits one delivery,
and `max_per_attempt` limits a task's own sends. `ca_file` adds PEM CAs to the system's
trust roots for webhooks and the mail server. `dag_urls` defaults to empty; the other defaults
are shown above, except that `ui_url` and `ca_file` are unset. All server channels are available
by name to every DAG.

A server webhook posts the fixed JSON if `body` is absent. In a configured body, strings
may contain `"{name}"` placeholders from the [table](../02-dags/24-notifications.md#bodies);
`{{` writes a literal opening brace. A bare placeholder preserves its JSON type. The body is
parsed at startup. Header values come only from the environment variables named in
`headers_env`; put credentials there. A secret webhook URL can be supplied through the
configuration's environment variables: a channel named without hyphens, `ops_slack`, takes its
URL from `SCHEDR_CONFIG__NOTIFICATIONS__CHANNELS__OPS_SLACK__URL` (a shell can't set a name
holding `-`). The deployment's displayed configuration masks server webhook URLs and bodies.

## Email channels

Set `[notifications.smtp]` to the mail server's `host`, `port`, `tls`, optional `username`
and sender address `from`. The default port is 587 and the default TLS mode is `starttls`,
which requires the server to upgrade the connection. Use `tls` for TLS from the start
(usually port 465), or `none` for a plain connection to a local relay. If `username` is set,
supply its password only in `SCHEDR_CONFIG__NOTIFICATIONS__SMTP__PASSWORD`; the configuration
file refuses a password. Login with `tls = "none"` is refused. The deployment's displayed
configuration shows a password as `****`.

An email channel has `type = "email"` and `to = [...]`; it takes no webhook settings. The
master reads recipients when sending, so a changed list applies to queued emails. If a channel
is removed, its queued emails go nowhere and are retried until `give_up_after`.

Each notification produces one plain-text email with every recipient in To. Its subject is the
summary's first line, trimmed and shortened after 200 characters. The body holds the summary,
event and DAG, run and task details where available, times and a link when configured, failed
tasks and their errors, and any data sent by a task.

The master needs network access to the mail server's port. Under Istio `REGISTRY_ONLY`, add a
TCP ServiceEntry for the mail server; see [Deployment](10-deployment.md).

## DAG webhooks and network access

The master makes the POST from where it runs, with access potentially including its database,
API and cloud metadata. `dag_urls` limits where DAG files can direct it (SSRF). A URL must
match an allowed entry's scheme, host and port, and its path must equal the allowed path or
continue at a `/` boundary. Userinfo is refused. For example, `/schedr` permits
`/schedr/mail` but not `/schedr-other`. DAG webhook URLs are visible in DAG files and JSON;
keep secrets out of them.

The master checks DAG URLs when validating a DAG, when queuing and when sending. A URL no
longer allowed is logged, counted as failed and dropped. An unknown server channel is kept
and retried until `give_up_after`, so restoring it can deliver queued messages.

The HTTP client uses `HTTPS_PROXY` and `NO_PROXY` where configured. Under Istio
`REGISTRY_ONLY`, add a ServiceEntry for external webhook hosts; see
[Deployment](10-deployment.md). Redirects are not followed.

## Delivery

The outbox row is written with the event. The leading master sends it at least once: a receiver
may get a duplicate after a failure or failover. A successful delivery deletes the row. For
webhooks, a 4xx response other than 408 or 429 is logged, counted and dropped immediately. 408, 429,
5xx, connection errors and timeouts are retried with a doubling wait up to a maximum, until
`give_up_after`; then the row is logged, counted and dropped. For SMTP, a permanent refusal
(5xx) is dropped at once; transient failures are retried. `timeout` covers the whole SMTP
exchange. If the mail server takes a message but does not answer, the retry sends it again.

The hidden timings `SCHEDR_HIDDEN__MASTER__OUTBOX_POLL`,
`SCHEDR_HIDDEN__MASTER__OUTBOX_RETRY_FIRST` and `SCHEDR_HIDDEN__MASTER__OUTBOX_RETRY_MAX`
control polling and backoff. `SCHEDR_HIDDEN__MASTER__NOTIFICATION_BODY` limits a filled template body
to 1 MiB by default. The master's `schedr_notifications_sent` and
`schedr_notifications_failed` metrics count deliveries and dropped notifications by channel.
