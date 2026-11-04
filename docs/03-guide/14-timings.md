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
| `[worker] cleanup_timeout` | 5 min | Limit for each cleanup try when the task doesn't set its own. Raise it for slow systems, or lower it to free slots sooner. The attempt holds its slot meanwhile. |
| `[worker] fork_server_import_retry` | 20 min | How long a failed preload import is retried with backoff. Change it for dependencies slow to become ready; `0s` disables retry. Threads after preloading and version mismatches are never retried. |
| `[tls] reload_interval` | 30 s | How often TLS files are read for changes. Shorten it for faster rotation, or lengthen it for fewer reads. |
| `[secret_storage.<name>] request_timeout` | 30 s | Limit for one Vault request. Raise it for a slow or distant Vault; a task waits this long at most for a hung request. |
| `[secret_storage.<name>] connect_timeout` | 10 s | Limit for connecting to Vault. Raise it on a slow network. |
| `[secret_storage.<name>] cache_ttl` | 60 s | Time a fetched secret stays in the agent's memory. Change it for freshness; `0s` fetches every time. |

A key that can't work at zero stops the start when set to `0s`: `[dags] poll_interval` and
`parse_file_timeout`, `[master] parse_job_timeout`, `[webserver] worker_request_timeout` and
`connection_test_timeout`, `[worker] log_retention`, `runner_start_timeout`,
`fork_server_preload_timeout` and `cleanup_timeout`, `[tls] reload_interval` (with TLS on), and
a storage's `request_timeout` and `connect_timeout`. Elsewhere zero means something: no wait, no
retry, no cache.

## Fixed timings

**Warning:** `SCHEDR_HIDDEN__*` is unsupported and used at your own risk; it is not
configuration. A wrong value can run a task twice or lose a healthy worker. It exists for
tests and experiments and can change or go without notice.

These variables work only in the environment. Setting one of these timings in a config file,
a flag or a `SCHEDR_CONFIG__*` variable fails as an unknown key. Each hidden variable set is
warned about at startup. An unknown name, a bad duration or zero stops the start.

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__MASTER__TICK_INTERVAL` | 1 s | Runs the master's scheduler, commands and dispatch loop; rounds and their work depend on it. |
| `SCHEDR_HIDDEN__MASTER__HEARTBEAT_INTERVAL` | 10 s | How often workers send heartbeats, set by the master and sent to workers; worker liveness depends on messages arriving. |
| `SCHEDR_HIDDEN__MASTER__LEASE_TIMEOUT` | 60 s | A worker silent this long is lost; its attempts are ended. |
| `SCHEDR_HIDDEN__MASTER__LEASE_PERIOD` | 30 s | Master's database lease; renewed every third of it. A standby takes over at most this long after a crash. It must fit the query and stall limits below. |
| `SCHEDR_HIDDEN__MASTER__READ_BACK_WINDOW` | 3 s | After a commit is cut off or the connection lost, the master checks whether it was stored for this long, holding its lock; rounds and worker messages wait. If not seen, it is taken as not stored. |
| `SCHEDR_HIDDEN__MASTER__READ_BACK_INTERVAL` | 500 ms | Interval between reads within the read-back window. |
| `SCHEDR_HIDDEN__DATABASE__QUERY_TIMEOUT` | 10 s | Limit for a query or waiting for a connection; a write transaction has a few of these. Lease renewal and stall detection depend on it. |
| `SCHEDR_HIDDEN__OPS__LOOP_STALL_TIMEOUT` | 120 s | Liveness fails if a role's main loop (the master's, a worker agent's) has not turned for this long; query and lease limits must be shorter. |
| `SCHEDR_HIDDEN__MASTER__SET_ASIDE` | 1 min | Time an assignment that failed on its own is left out of rounds before retry, so other tasks can proceed. |
| `SCHEDR_HIDDEN__MASTER__LISTEN_RETRY` | 30 min | How often the master retries starting command listening after failure; it polls meanwhile. |
| `SCHEDR_HIDDEN__MASTER__QUIET_DAG_AFTER` | 1 min | Time after the last active visit before a formerly busy DAG becomes quiet, unless its fire time is within the wake-ahead time. An otherwise idle DAG is quiet at once. |
| `SCHEDR_HIDDEN__MASTER__QUIET_DAG_WAKE_AHEAD` | 1 min | How far ahead of its next fire time a quiet DAG wakes and becomes busy. |
| `SCHEDR_HIDDEN__MASTER__QUIET_DAG_FULL_PASS_INTERVAL` | 1 min | Every quiet DAG is visited within this time, spread over rounds, and every DAG's ID and version are checked against memory. It bounds a missed wake's delay approximately. |
| `SCHEDR_HIDDEN__WEBSERVER__REFRESH_INTERVAL` | 15 s | Refreshes live UI pages when database changes cannot be followed. |
| `SCHEDR_HIDDEN__WEBSERVER__WORKER_CONNECT_TIMEOUT` | 5 s | Limits connection to a worker when fetching a task log; log access depends on it. |
| `SCHEDR_HIDDEN__WORKER__DRAIN_REPORT_PERIOD` | 2 s | Time from a drain's SIGKILL until the grace period ends, to report interruptions and deregister; results may otherwise end lost. It must fit with the termination period. |
| `SCHEDR_HIDDEN__WORKER__MEMORY_CHECK_INTERVAL` | 1 s | How often the watchdog samples each task's memory; budget enforcement depends on samples. |
| `SCHEDR_HIDDEN__TLS__HANDSHAKE_TIMEOUT` | 10 s | Time a peer has to complete a TLS handshake; slow or stalled connections depend on this limit. |

At startup, the query timeout must be at least 1 ms and shorter than the loop stall timeout.
The master lease must be at least three query timeouts and shorter than the loop stall timeout.
The drain termination period plus the report period must not exceed `[worker]
termination_grace_period`. The connection test timeout must be at least 1 ms and within the
API's maximum; an invalid value stops startup.
