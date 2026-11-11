# Timings

Some timings depend on the deployment: those are configuration keys that can be tuned. The
others are fixed because they are tied to one another or to correctness. All are read once
when a process starts; a change takes a restart.

## Public timing keys

These are the duration keys in the [configuration reference](../_generated/04-reference/01-configuration.md).

| Section and key | Default | What it does and when to change it |
|---|---|---|
| `[dags] poll_interval` | 30 s | Scans roots for changes. Shorten for faster discovery, lengthen for fewer scans. |
| `[dags] settle_time` | 5 s | Waits for a root to stay unchanged before hashing it. Raise it if updates arrive in pieces. |
| `[dags] parse_file_timeout` | 60 s | Limits one DAG file's import. Raise it for files that need longer to import. |
| `[master] cancel_grace` | 30 s | Time from SIGTERM to SIGKILL on cancellation. Change it to give tasks more or less time to stop. |
| `[master] code_wait` | 10 min | Waits for missing task code on an eligible worker before marking the task unschedulable. Raise it if code takes longer to arrive. |
| `[master] parse_job_timeout` | 10 min | Time a worker has for a parse job before it is reassigned. Raise it for large DAG folders; each file still has `parse_file_timeout`. |
| `[webserver] worker_request_timeout` | 30 s | Limits a whole request to a worker for a task log. Raise it for large logs that arrive slowly; a user waits this long at most for a hung worker. |
| `[webserver] connection_test_timeout` | 1 min | Default limit for a connection test. Raise it for slow connections; `schedr connections test --timeout` sets one test's limit. |
| `[worker] termination_grace_period` | 30 s | Time running tasks get on worker shutdown, including the drain's stop and report periods. Match the pod's `terminationGracePeriodSeconds`. |
| `[worker] drain_terminate_period` | 3 s | Time from SIGTERM to SIGKILL when a drain's time is up. Change it for the time tasks need to stop; it and the fixed report period must fit in the grace period. |
| `[worker] log_retention` | 30 days | Deletes task logs older than this. Change it for the retention needed. |
| `[worker] runner_start_timeout` | 30 s | Time a Python runner has to say hello. Raise it for a slow interpreter start; a failed start is noticed later. |
| `[worker] fork_server_preload_timeout` | 5 min | Time a fork server has to import preloaded libraries. Raise it for large preload lists or slow storage; tasks are spawned until it is up. |
| `[worker] cleanup_timeout` | 5 min | Limit for each per-task cleanup try when the task doesn't set its own. Raise it for slow systems, or lower it to free slots sooner. The attempt holds its slot meanwhile. |
| `[worker] resource_cleanup_timeout` | 30 min | Limit for each resource cleanup try without its own `timeout=`. Raise it for slow teardowns, or lower it to notice a hang sooner; retries each get the same limit. |
| `[worker] fork_server_import_retry` | 20 min | How long a failed preload import is retried with backoff. Change it for dependencies slow to become ready; `0s` disables retry. Threads after preloading and version mismatches are never retried. |
| `[tls] reload_interval` | 30 s | How often TLS files are read for changes. Shorten it for faster rotation, or lengthen it for fewer reads. |
| `[secret_storage.<name>] request_timeout` | 30 s | Limit for one Vault request. Raise it for a slow or distant Vault; a task waits this long at most for a hung request. |
| `[secret_storage.<name>] connect_timeout` | 10 s | Limit for connecting to Vault. Raise it on a slow network. |
| `[secret_storage.<name>] cache_ttl` | 60 s | Time a fetched secret stays in the agent's memory. Change it for freshness; `0s` fetches every time. |

A key that can't work at zero stops the start when set to `0s`: `[dags] poll_interval` and
`parse_file_timeout`, `[master] parse_job_timeout`, `[webserver] worker_request_timeout` and
`connection_test_timeout`, `[worker] log_retention`, `runner_start_timeout`,
`fork_server_preload_timeout`, `cleanup_timeout` and `resource_cleanup_timeout`, `[tls] reload_interval` (with TLS on), and
a storage's `request_timeout` and `connect_timeout`. Elsewhere zero means something: no wait, no
retry, no cache.

## Fixed timings

**Warning:** `SCHEDR_HIDDEN__*` is unsupported and used at your own risk; it is not
configuration. A wrong value can run a task twice or lose a healthy worker. It exists for
tests and experiments and can change or go without notice.

These variables work only in the environment. Setting one of these timings in a config file,
a flag or a `SCHEDR_CONFIG__*` variable fails as an unknown key. Each hidden variable set is
warned about at startup. An unknown name, a bad duration or zero stops the start.

Some timings stay in the binary: those used by the CLI, `task-ctx` and the client, which do
not load this configuration; the gauge refresh limit, half the loop stall timeout; and the
migration connection timing, which has no query timeout to tie it to.

### Master

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__MASTER__TICK_INTERVAL` | 1 s | Runs the master's scheduler, commands and dispatch loop; rounds and their work depend on it. |
| `SCHEDR_HIDDEN__MASTER__TRIGGER_RESTART_BACKOFF` | 5 s | First wait before restarting a failed event trigger; it doubles up to the maximum. |
| `SCHEDR_HIDDEN__MASTER__TRIGGER_RESTART_BACKOFF_MAX` | 5 min | Longest wait before restarting a failed event trigger. |
| `SCHEDR_HIDDEN__MASTER__HEARTBEAT_INTERVAL` | 10 s | How often workers send heartbeats, set by the master and sent to workers; worker liveness depends on messages arriving. |
| `SCHEDR_HIDDEN__MASTER__LEASE_TIMEOUT` | 60 s | A worker silent this long is lost; its attempts are ended. An unstarted assignment is taken back two heartbeat intervals after this time (decision 450), and a worker rejects one it can no longer start within it, less two heartbeat intervals; a worker also ends a stream silent this long. At least four times the heartbeat interval, or schedr refuses to start. |
| `SCHEDR_HIDDEN__MASTER__SCRIPT_ROOT_GRACE` | 5 min | Time without a report from any alive worker before a script root is omitted from the uses sent to workers, counted from master startup too. |
| `SCHEDR_HIDDEN__MASTER__LEASE_PERIOD` | 30 s | Master's database lease; renewed every third of it. A standby takes over at most this long after a crash. It must fit the query and stall limits below. |
| `SCHEDR_HIDDEN__MASTER__READ_BACK_WINDOW` | 3 s | After a commit is cut off or the connection lost, the master checks whether it was stored for this long, holding its lock; rounds and worker messages wait. If not seen, it is taken as not stored. |
| `SCHEDR_HIDDEN__MASTER__READ_BACK_INTERVAL` | 500 ms | Interval between reads within the read-back window. |
| `SCHEDR_HIDDEN__OPS__LOOP_STALL_TIMEOUT` | 120 s | Liveness fails if a role's main loop (the master's, a worker agent's) has not turned for this long; query and lease limits must be shorter. |
| `SCHEDR_HIDDEN__MASTER__SET_ASIDE` | 1 min | Time an assignment that failed on its own is left out of rounds before retry, so other tasks can proceed. |
| `SCHEDR_HIDDEN__MASTER__HANDED_CALL_MIN` | 1 min | The least an `@sensor(wait_for=...)`'s call gets once it is handed a run that ended within its `max_wait`, though little or none of that is left. |
| `SCHEDR_HIDDEN__MASTER__CLEANUP_MOVE_WAIT` | 10 s | Time a lost worker's cleanup waits before it moves to another worker, on top of the cancel grace: a worker only cut off has stopped the task by then. |
| `SCHEDR_HIDDEN__MASTER__LISTEN_RETRY` | 30 min | How often the master retries starting command listening after failure; it polls meanwhile. |
| `SCHEDR_HIDDEN__MASTER__QUIET_DAG_AFTER` | 1 min | Time after the last active visit before a formerly busy DAG becomes quiet, unless its fire time is within the wake-ahead time. An otherwise idle DAG is quiet at once. |
| `SCHEDR_HIDDEN__MASTER__QUIET_DAG_WAKE_AHEAD` | 1 min | How far ahead of its next fire time a quiet DAG wakes and becomes busy. |
| `SCHEDR_HIDDEN__MASTER__QUIET_DAG_FULL_PASS_INTERVAL` | 1 min | Every quiet DAG is visited within this time, spread over rounds, and every DAG's ID and version are checked against memory. It bounds a missed wake's delay approximately. |
| `SCHEDR_HIDDEN__MASTER__ROUND_GAP` | 50 ms | Minimum gap between extra rounds under a stream of finished tasks. |
| `SCHEDR_HIDDEN__MASTER__NOTIFICATION_RETRY` | 30 s | Wait before retrying failed notifications; polling continues meanwhile. |
| `SCHEDR_HIDDEN__MASTER__ALIVE_INTERVAL` | 5 s | How often the master's loop turns at least while waiting; it must stay shorter than the loop stall timeout, or a waiting master looks stuck. |
| `SCHEDR_HIDDEN__MASTER__GAUGE_REFRESH` | 15 s | How often the master reads gauges from the database. |
| `SCHEDR_HIDDEN__MASTER__OUTBOX_POLL` | 5 s | How often the sender looks for notifications to deliver. |
| `SCHEDR_HIDDEN__MASTER__OUTBOX_RETRY_FIRST` | 10 s | First wait before retrying a failed notification; it doubles up to the maximum. |
| `SCHEDR_HIDDEN__MASTER__OUTBOX_RETRY_MAX` | 1 h | Longest wait before retrying a failed notification. |
| `SCHEDR_HIDDEN__MASTER__RETENTION_INTERVAL` | 1 h | How often the leading master runs retention. |
| `SCHEDR_HIDDEN__MASTER__PARSE_RETRY_FIRST` | 30 s | First wait before retrying a failed parse job; it doubles up to the maximum. |
| `SCHEDR_HIDDEN__MASTER__PARSE_RETRY_MAX` | 10 min | Longest wait before retrying a failed parse job. |
| `SCHEDR_HIDDEN__MASTER__NODE_HEARTBEAT` | 10 s | How often the master writes its row of the deployment's nodes. |

### Database

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__DATABASE__QUERY_TIMEOUT` | 10 s | Limit for a query or waiting for a connection; a write transaction has a few of these. Lease renewal and stall detection depend on it. |
| `SCHEDR_HIDDEN__DATABASE__CHANGES_POLL_INTERVAL` | 1 s | How often changes are read where they are not announced, such as SQLite. |

### Webserver

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__WEBSERVER__REFRESH_INTERVAL` | 15 s | Refreshes live UI pages when database changes cannot be followed. |
| `SCHEDR_HIDDEN__WEBSERVER__WORKER_CONNECT_TIMEOUT` | 5 s | Limits connection to a worker when fetching a task log; log access depends on it. |
| `SCHEDR_HIDDEN__WEBSERVER__EVENTS_RECHECK` | 1 min | How often the webserver checks whether the database announces changes. |
| `SCHEDR_HIDDEN__WEBSERVER__EVENTS_KEEP_ALIVE` | 15 s | How often an event stream sends a comment to keep idle connections open through proxies. |
| `SCHEDR_HIDDEN__WEBSERVER__EVENTS_RETRY` | 1 s | Wait before retrying following database changes after failure. |
| `SCHEDR_HIDDEN__WEBSERVER__AUTH_RECHECK` | 5 s | How long a removed user or a changed password keeps working; raising it delays revocation. |
| `SCHEDR_HIDDEN__WEBSERVER__AUTH_REMEMBERED` | 5 min | How long a successful password check is remembered; this only saves bcrypt runs, and a change of the users file forgets them all. |
| `SCHEDR_HIDDEN__WEBSERVER__REQUESTS_GRACE` | 3 s | How long running requests get after the server stops before they are cut. |
| `SCHEDR_HIDDEN__WEBSERVER__SCHEMA_RETRY` | 5 s | How often a webserver started before the migration checks again. |

### UI

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__UI__KEEP_ALIVE` | 15 s | How often a page stream sends a ping and the log tail sends a comment. |
| `SCHEDR_HIDDEN__UI__COALESCE` | 1 s | Minimum interval between change messages on a page stream, per deployment. |
| `SCHEDR_HIDDEN__UI__TAIL_INTERVAL` | 1 s | How often the log tail reads the log. |
| `SCHEDR_HIDDEN__UI__TAIL_GIVE_UP` | 30 s | How long the log tail retries an unreachable log before reporting it and stopping. |
| `SCHEDR_HIDDEN__UI__RELAY_RETRY_FIRST` | 1 s | First wait before a relay reopens its stream; it doubles up to the maximum. |
| `SCHEDR_HIDDEN__UI__RELAY_RETRY_MAX` | 30 s | Longest wait between a relay's retries. |

### Worker

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__WORKER__DRAIN_REPORT_PERIOD` | 2 s | Time from a drain's SIGKILL until the grace period ends, to report interruptions and deregister; results may otherwise end lost. It must fit with the termination period. |
| `SCHEDR_HIDDEN__WORKER__MASTER_SAMPLE_WINDOW` | 10 min | How far back a worker looks for its tightest reading of master time (decision 447); a longer window keeps an older sample, whose drift allowance grows with its age. |
| `SCHEDR_HIDDEN__WORKER__CLOCK_SKEW_WARNING` | 2 s | How far the master's wall clock may differ from a worker's, beyond the round trip, before the worker warns, once a session (decision 449). Only for people: no deadline reads a wall clock. |
| `SCHEDR_HIDDEN__WORKER__MEMORY_CHECK_INTERVAL` | 1 s | How often the watchdog samples each task's memory; budget enforcement depends on samples. |
| `SCHEDR_HIDDEN__WORKER__OUTPUT_DRAIN_TIMEOUT` | 5 s | How long a stopped process's output may take to reach its log when another process holds its pipes open. |
| `SCHEDR_HIDDEN__WORKER__FORK_ANSWER_TIMEOUT` | 30 s | How long a fork server may take to answer; then it is restarted and the task spawned. |
| `SCHEDR_HIDDEN__WORKER__FORK_BACKOFF_FIRST` | 1 s | First wait before restarting a fork server; it doubles up to the maximum. |
| `SCHEDR_HIDDEN__WORKER__FORK_BACKOFF_MAX` | 5 min | Longest wait before restarting a fork server. |
| `SCHEDR_HIDDEN__WORKER__FORK_BACKOFF_RESET_AFTER` | 10 min | Time a fork server must stay up to reset its restart waits. |
| `SCHEDR_HIDDEN__WORKER__LOG_SWEEP_INTERVAL` | 1 h | How often old task logs are swept. |
| `SCHEDR_HIDDEN__WORKER__ORPHAN_POLL` | 50 ms | How often an orphan found at startup is checked while it stops. |
| `SCHEDR_HIDDEN__WORKER__ORPHAN_KILL_WAIT` | 5 s | How long an orphan may take to go after SIGKILL. |
| `SCHEDR_HIDDEN__WORKER__ORPHAN_GRACE` | 10 s | Time an orphan found at startup gets from SIGTERM to SIGKILL. |
| `SCHEDR_HIDDEN__WORKER__RECONNECT_BACKOFF_FIRST` | 200 ms | First wait after the master connection drops; it doubles with jitter up to the maximum. |
| `SCHEDR_HIDDEN__WORKER__RECONNECT_BACKOFF_MAX` | 30 s | Longest wait between tries to reach the master; once the worker knows its lease, each wait is also capped at half that lease. |
| `SCHEDR_HIDDEN__WORKER__ALIVE_INTERVAL` | 5 s | How often the agent's loop turns at least, even when quiet; it must stay shorter than the loop stall timeout, or a quiet agent looks stuck. |
| `SCHEDR_HIDDEN__WORKER__DEREGISTER_WAIT` | 5 s | How long a stopping worker waits for the master to accept its deregistration. |
| `SCHEDR_HIDDEN__WORKER__DRAIN_TICK` | 100 ms | How often a drain checks what is left. |
| `SCHEDR_HIDDEN__WORKER__DRAIN_ACK_WAIT` | 5 s | How long a drained worker waits for the master to acknowledge its last results. |
| `SCHEDR_HIDDEN__WORKER__STOP_POLL` | 100 ms | How often a stopped task's process group is checked until it is gone. |
| `SCHEDR_HIDDEN__WORKER__CONNECTION_JOB_CANCEL_GRACE` | 5 s | Time a connection test's process gets from SIGTERM to SIGKILL. |
| `SCHEDR_HIDDEN__WORKER__VAULT_MIN_RENEW_WAIT` | 1 s | Minimum wait before a Vault token renewal. |
| `SCHEDR_HIDDEN__WORKER__VAULT_RENEW_CHECK` | 1 min | How often a Vault session with nothing to renew is checked again. |
| `SCHEDR_HIDDEN__WORKER__VAULT_RENEW_RETRY` | 10 s | Wait before retrying a failed Vault renewal while the token lasts. |

### Standalone

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__STANDALONE__WORKER_STOP_AFTER_FAILURE` | 10 s | How long a standalone's worker gets to stop after its master fails. |

### TLS

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__TLS__HANDSHAKE_TIMEOUT` | 10 s | Time a peer has to complete a TLS handshake; slow or stalled connections depend on this limit. |
| `SCHEDR_HIDDEN__TLS__ACCEPT_RETRY` | 100 ms | Wait after a listener fails to accept a connection, such as when too many files are open, to avoid spinning. |

At startup, the query timeout must be at least 1 ms and shorter than the loop stall timeout.
The master lease must be at least three query timeouts and shorter than the loop stall timeout.
The drain termination period plus the report period must not exceed `[worker]
termination_grace_period`. The connection test timeout must be at least 1 ms and within the
API's maximum; an invalid value stops startup.

## Fixed limits

Four limits are set the same way, with the same warning: whole numbers above zero, in the
environment only.

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__MASTER__WORKER_REPORT_RECORDS` | 256 | The most wait step hosts, and the most warnings, the master keeps of one worker's report; the rest are dropped with a warning in its log. An honest worker sends a few. |
| `SCHEDR_HIDDEN__MASTER__WORKER_REPORT_TEXT` | 4096 | The most characters of each text in them (a host's code, a warning's message); longer ones are cut. |
| `SCHEDR_HIDDEN__MASTER__NOTIFICATION_BODY` | 1 MiB | The most bytes a notification's body may take once filled. |
| `SCHEDR_HIDDEN__MASTER__RETENTION_BATCH` | 100 | The most runs retention deletes in one transaction. |
