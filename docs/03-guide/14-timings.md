# Timings

Some timings depend on the deployment: those are configuration keys that can be tuned. The
others have fixed defaults because they depend on each other or keep the system correct.
All are read once when a process starts; a change takes a restart.

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
| `[worker] cleanup_timeout` | 5 min, or what fits in `termination_grace_period` (25 s with its default) | Limit for each per-task cleanup try when the task doesn't set its own. Left unset, it fits in the grace period less the drain's terminate and report periods, so a cleanup isn't lost when the pod is killed at the grace period's end (B252). Raise it for slow systems, or lower it to free slots sooner. The attempt holds its slot meanwhile. |
| `[worker] resource_cleanup_timeout` | 30 min | Limit for each resource cleanup try without its own `timeout=`. Raise it for slow teardowns, or lower it to notice a hang sooner; retries each get the same limit. |
| `[worker] fork_server_import_retry` | 20 min | How long a failed preload import is retried with backoff. Change it for dependencies slow to become ready; `0s` disables retry. Threads after preloading and version mismatches are never retried. |
| `[watchers] block_limit` | 30 s | How long a wait step may hold its host's event loop without reaching an `await`. Past it, the host is killed. Raise it only when a step needs longer between awaits. |
| `[sensors] default_max_wait` | 24 h | How long a wait step without its own `max_wait` waits before it returns `TimedOut`. |
| `[notifications] timeout` | 10 s | Limit for one delivery. A timeout counts as a failed delivery and allows a retry. |
| `[notifications] give_up_after` | 24 h | How long to retry a notification before dropping it. |
| `[tls] reload_interval` | 30 s | How often TLS files are read for changes. Shorten it for faster rotation, or lengthen it for fewer reads. |
| `[secret_storage.<name>] request_timeout` | 30 s | Limit for one Vault request. Raise it for a slow or distant Vault; a task waits this long at most for a hung request. |
| `[secret_storage.<name>] connect_timeout` | 10 s | Limit for connecting to Vault. Raise it on a slow network. |
| `[secret_storage.<name>] cache_ttl` | 60 s | Time a fetched secret stays in the worker's shared cache. `0s` disables that cache; an attempt still keeps the values it already fetched. |

The `[retention]` ages also take durations, or `forever`; see the configuration reference.

A key that can't work at zero stops the start when set to `0s`: `[dags] poll_interval` and
`parse_file_timeout`, `[master] parse_job_timeout`, `[webserver] worker_request_timeout` and
`connection_test_timeout`, `[notifications] timeout`, `[sensors] default_max_wait`,
`[worker] log_retention`, `runner_start_timeout`,
`fork_server_preload_timeout`, `cleanup_timeout` and `resource_cleanup_timeout`, `[tls] reload_interval` (with TLS on), and
a storage's `request_timeout` and `connect_timeout`. Elsewhere zero means something: no wait, no
retry, no cache.

## Fixed timings

**Warning:** `SCHEDR_HIDDEN__*` is unsupported and used at your own risk; it is not
configuration. A wrong value can run a task twice or lose a healthy worker. It exists for
tests and experiments and can change or go without notice.

These variables work only in the environment. Setting one of these timings in a config file,
a flag or a `SCHEDR_CONFIG__*` variable fails as an unknown key. Each active override is
warned about at startup; retired names are accepted and ignored. An unknown name, a bad duration, zero or more than 3650 days stops
the start.

Some timings stay in the binary: those a task's own process uses (`schedr task-ctx`, `dag-ctx`
and the Python package), which reads no configuration; those two processes must agree on (a
task's API call, a connections check: one side's setting would break the other); the gauge
refresh limit, half the loop stall timeout; and the migration connection timing, which has no
query timeout to tie it to. [What stays in the binary](#what-stays-in-the-binary) lists them by
reason.

### Master

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__MASTER__TICK_INTERVAL` | 1 s | Paces scheduler, command and dispatch rounds. Triggers and events also have a separate loop woken by progress. |
| `SCHEDR_HIDDEN__MASTER__TRIGGER_RESTART_BACKOFF` | 5 s | First wait before restarting a failed event trigger; it doubles up to the maximum. |
| `SCHEDR_HIDDEN__MASTER__TRIGGER_RESTART_BACKOFF_MAX` | 5 min | Longest wait before restarting a failed event trigger. |
| `SCHEDR_HIDDEN__MASTER__HEARTBEAT_INTERVAL` | 10 s | How often workers send heartbeats, set by the master and sent to workers; worker liveness depends on messages arriving. |
| `SCHEDR_HIDDEN__MASTER__LEASE_TIMEOUT` | 60 s | A worker silent this long is lost; its attempts are ended (an unstarted assignment needs no timeout: it is taken back once the worker's messages show it never started, decision 460). A replaced incarnation's attempts are held this long; a worker also ends a stream silent this long. At least four times the heartbeat interval, and longer than the master's lead (`LEASE_PERIOD` less `QUERY_TIMEOUT`, 20 s by default), or schedr refuses to start: with the lease period at least three query timeouts, it must be longer than twice the query timeout. |
| `SCHEDR_HIDDEN__MASTER__EVENT_LOOKBACK` | 1 min | How far before the newest event already handled an `EventNamed` trigger looks for events stored late. |
| `SCHEDR_HIDDEN__MASTER__SCRIPT_ROOT_GRACE` | 5 min | Time without a report from any alive worker before a script root is omitted from the uses sent to workers, counted from master startup too. |
| `SCHEDR_HIDDEN__MASTER__LEASE_PERIOD` | 30 s | Master's database lease; renewed every third of it. A standby takes over at most this long after a crash. At least three times `QUERY_TIMEOUT` and shorter than the stall limit below; less `QUERY_TIMEOUT` (the master's lead), shorter than `LEASE_TIMEOUT` (decision 444). Lowering the worker lease can mean lowering this too. |
| `SCHEDR_HIDDEN__MASTER__READ_BACK_WINDOW` | 3 s | After a commit is cut off or the connection lost, the master checks whether it was stored for this long, holding its lock; rounds and worker messages wait. If not seen, it is taken as not stored. |
| `SCHEDR_HIDDEN__MASTER__READ_BACK_INTERVAL` | 500 ms | Interval between reads within the read-back window. |
| `SCHEDR_HIDDEN__OPS__LOOP_STALL_TIMEOUT` | 120 s | Liveness fails if a role's main loop (the master's, a worker agent's) has not turned for this long; query and lease limits must be shorter. |
| `SCHEDR_HIDDEN__MASTER__SET_ASIDE` | 1 min | Time an assignment that failed on its own is left out of rounds before retry, so other tasks can proceed. |
| `SCHEDR_HIDDEN__MASTER__HANDED_CALL_MIN` | 1 min | The least an `@sensor(wait_for=...)`'s call gets once it is handed a run that ended within its `max_wait`, though little or none of that is left. |
| `SCHEDR_HIDDEN__MASTER__CLEANUP_MOVE_WAIT` | 10 s | Time a lost worker's cleanup waits before it moves to another worker, on top of the cancel grace: a worker only cut off has stopped the task by then. |
| `SCHEDR_HIDDEN__MASTER__LISTEN_RETRY` | 30 min | How often the master retries starting command listening after failure; it polls meanwhile. |
| `SCHEDR_HIDDEN__MASTER__ROUND_GAP` | 50 ms | Minimum gap between the trigger loop's passes. |
| `SCHEDR_HIDDEN__MASTER__TASK_ROUND_GAP` | 10 ms | Minimum gap between a round's end and one a worker's results wake. |
| `SCHEDR_HIDDEN__MASTER__NOTIFICATION_RETRY` | 30 s | Wait before retrying failed notifications; polling continues meanwhile. |
| `SCHEDR_HIDDEN__MASTER__ALIVE_INTERVAL` | 5 s | How often the master's loop turns at least while waiting; it must stay shorter than the loop stall timeout, or a waiting master looks stuck. |
| `SCHEDR_HIDDEN__MASTER__GAUGE_REFRESH` | 15 s | How often the master reads gauges from the database. |
| `SCHEDR_HIDDEN__MASTER__OUTBOX_POLL` | 5 s | How often the sender looks for notifications to deliver. |
| `SCHEDR_HIDDEN__MASTER__OUTBOX_RETRY_FIRST` | 10 s | First wait before retrying a failed notification; it doubles up to the maximum. |
| `SCHEDR_HIDDEN__MASTER__OUTBOX_RETRY_MAX` | 1 h | Longest wait before retrying a failed notification. |
| `SCHEDR_HIDDEN__MASTER__RETENTION_INTERVAL` | 1 h | How often the leading master runs retention. |
| `SCHEDR_HIDDEN__MASTER__SAMPLE_INTERVAL` | 1 min | How often the leading master samples each process, and a webserver in a process of its own itself. |
| `SCHEDR_HIDDEN__MASTER__SAMPLES_KEPT` | 24 h | How long minute samples are kept. Must be longer than `MASTER__SAMPLE_ROLLUP_GRACE` and the 15-minute window. |
| `SCHEDR_HIDDEN__MASTER__ROLLUPS_KEPT` | 30 d | How long the 15-minute rollups are kept. |
| `SCHEDR_HIDDEN__MASTER__PARSE_RETRY_FIRST` | 30 s | First wait before retrying a failed parse job; it doubles up to the maximum. |
| `SCHEDR_HIDDEN__MASTER__PARSE_RETRY_MAX` | 10 min | Longest wait before retrying a failed parse job. |
| `SCHEDR_HIDDEN__MASTER__NODE_HEARTBEAT` | 10 s | How often the master writes its row of the deployment's nodes. A master heard from within three of them is on the Deployments page and sampled; a longer interval keeps it there longer after it is gone. |
| `SCHEDR_HIDDEN__MASTER__NODE_ROW_KEPT` | 1 h | How long a gone master's row stays before it is removed; the Deployments page shows only those heard from lately either way. |
| `SCHEDR_HIDDEN__MASTER__SAMPLE_ROLLUP_GRACE` | 2 min | How long after its end a 15-minute window waits to be rolled up, so a sample stored late is in it. A sample stored later than this is missing from its rollup, and so from the 7- and 30-day charts. Must be shorter than `WEBSERVER__METRICS_FINAL_AFTER`, and with the 15-minute window shorter than `MASTER__SAMPLES_KEPT` (or a window's first minutes would be deleted before its rollup), or schedr refuses to start. |
| `SCHEDR_HIDDEN__MASTER__CODE_RETRY_DELAY` | 35 s | When a task whose code wasn't on its worker yet is tried again: about a git-sync interval and the settle time. Set it near your code delivery's interval; shorter only retries sooner, `[master] code_wait` still bounds the whole wait. |
| `SCHEDR_HIDDEN__MASTER__SLOW_EQUALS_FLOOR` | 1 s | The least the cluster's slow-equals is: a worker's waiting queue whose oldest task waited longer gives back a task each second (decision 509). Below about four give-backs' cost (a round, an assignment and a start each) a give-back can't pay. |
| `SCHEDR_HIDDEN__MASTER__FOR_FUTURE_TOLERANCE` | 1 min | How far ahead of the master's clock a run's `--for` may be, for clocks apart; later is refused. Longer lets a run be planned for a moment that hasn't come. |

Three master timings are retired: `QUIET_DAG_AFTER`, `QUIET_DAG_WAKE_AHEAD` and
`QUIET_DAG_FULL_PASS_INTERVAL` (the `MASTER` group): which DAGs a round visits comes from the
database now ([Busy and quiet DAGs](04-master.md#busy-and-quiet-dags)). They are still
accepted, so an old deployment starts, and have no effect.

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
| `SCHEDR_HIDDEN__WEBSERVER__METRICS_FINAL_AFTER` | 15 min | When a Metrics bucket is final: kept by the webserver between requests and never read again (decision 492). A sample stored later, such as from a process whose clock is behind by more, shows only after the webserver restarts. Must be longer than `MASTER__SAMPLE_ROLLUP_GRACE`. Longer reads more on each refresh. |
| `SCHEDR_HIDDEN__WEBSERVER__MEMORY_PEAKS_WINDOW` | 30 days | How far back the DAG page's memory p95s reach. Longer reads more attempts on each load of the page. |
| `SCHEDR_HIDDEN__EVENTS__FUTURE_TOLERANCE` | 1 min | How far ahead of the clock a posted event's `at` may be, posted to a webserver or by a task through its master; later is refused. Set it on both roles. |

### UI

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__UI__KEEP_ALIVE` | 15 s | How often a page stream sends a ping and the log tail sends a comment. |
| `SCHEDR_HIDDEN__UI__COALESCE` | 1 s | Minimum interval between change messages on a page stream, per deployment. |
| `SCHEDR_HIDDEN__UI__TAIL_INTERVAL` | 1 s | How often the log tail reads the log. |
| `SCHEDR_HIDDEN__UI__TAIL_GIVE_UP` | 30 s | How long the log tail retries an unreachable log before reporting it and stopping. |
| `SCHEDR_HIDDEN__UI__SLOW_READ` | 50 ms | A Runs page whose deployments took longer than this to answer is slow. |
| `SCHEDR_HIDDEN__UI__SLOW_READ_WAIT` | 5 s | How often a slow Runs page reads again at most when runs change (otherwise once a second). |
| `SCHEDR_HIDDEN__UI__RELAY_RETRY_FIRST` | 1 s | First wait before a relay reopens its stream; it doubles up to the maximum. |
| `SCHEDR_HIDDEN__UI__RELAY_RETRY_MAX` | 30 s | Longest wait between a relay's retries. |

### Worker

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__WORKER__DRAIN_REPORT_PERIOD` | 2 s | Time from a drain's SIGKILL until the grace period ends, to report interruptions and deregister; results may otherwise end lost. It must fit with the termination period. |
| `SCHEDR_HIDDEN__WORKER__MASTER_SAMPLE_WINDOW` | 10 min | How far back a worker looks for its tightest reading of master time (decision 447); a longer window keeps an older sample, whose drift allowance grows with its age. |
| `SCHEDR_HIDDEN__WORKER__CLOCK_SKEW_WARNING` | 2 s | How far the master's wall clock may differ from a worker's, beyond the round trip, before the worker warns, once a session (decision 449). Only for people: no deadline reads a wall clock. |
| `SCHEDR_HIDDEN__WORKER__MEMORY_CHECK_INTERVAL` | 1 s | How often the watchdog samples each task's memory; budget enforcement depends on samples. |
| `SCHEDR_HIDDEN__WORKER__OUTPUT_DRAIN_TIMEOUT` | 5 s | How long a stopped or ended task's output may take to reach its log when another process holds its pipes open; then what is left of the task is killed (B274). |
| `SCHEDR_HIDDEN__WORKER__FORK_ANSWER_TIMEOUT` | 30 s | How long a fork server may take to answer; then it is restarted and the task spawned. |
| `SCHEDR_HIDDEN__WORKER__FORK_BACKOFF_FIRST` | 1 s | First wait before restarting a fork server; it doubles up to the maximum. |
| `SCHEDR_HIDDEN__WORKER__FORK_BACKOFF_MAX` | 5 min | Longest wait before restarting a fork server. |
| `SCHEDR_HIDDEN__WORKER__FORK_BACKOFF_RESET_AFTER` | 10 min | Time a fork server must stay up to reset its restart waits. |
| `SCHEDR_HIDDEN__WORKER__LOG_SWEEP_INTERVAL` | 1 h | How often old task logs are swept. |
| `SCHEDR_HIDDEN__WORKER__ORPHAN_POLL` | 50 ms | How often an orphan found at startup is checked while it stops. |
| `SCHEDR_HIDDEN__WORKER__ORPHAN_KILL_WAIT` | 5 s | How long an orphan may take to go after SIGKILL. |
| `SCHEDR_HIDDEN__WORKER__ORPHAN_GRACE` | 10 s | Time an orphan found at startup gets from SIGTERM to SIGKILL. |
| `SCHEDR_HIDDEN__WORKER__MASTER_CONNECT_TIMEOUT` | 5 s | How long a worker waits for the master's address to answer before it tries again; a black-holed address (a master pod just killed) fails after it. |
| `SCHEDR_HIDDEN__WORKER__REGISTER_TIMEOUT` | 10 s | How long a worker waits for a stream to open and the master to register it before it tries again; an address that accepts and says nothing fails after it. Doubled after each such try, up to 8 times it. |
| `SCHEDR_HIDDEN__WORKER__UNANSWERED_TIMEOUT` | 25 s | How long a worker's registered stream may go without a word from the master (its heartbeats answered as they arrive) before the worker opens another, which replaces it; at most half the lease. It ends the connection only, never a task: the lease and the cutoff count as before. |
| `SCHEDR_HIDDEN__MASTER__UNOFFERED_WAIT` | 10 min | How long an extension task waits, its reason shown, when no worker of its queue offers its extension at its version, before it is `unschedulable`; a worker's registration queues it again. |
| `SCHEDR_HIDDEN__WORKER__RECONNECT_BACKOFF_FIRST` | 200 ms | First wait after the master connection drops; it doubles with jitter up to the maximum. |
| `SCHEDR_HIDDEN__WORKER__RECONNECT_BACKOFF_MAX` | 30 s | Longest wait between tries to reach the master; once the worker knows its lease, each wait is also capped at half that lease. |
| `SCHEDR_HIDDEN__WORKER__ALIVE_INTERVAL` | 5 s | How often the agent's loop turns at least, even when quiet; it must stay shorter than the loop stall timeout, or a quiet agent looks stuck. |
| `SCHEDR_HIDDEN__WORKER__DEREGISTER_WAIT` | 1 h | How long a stopping worker waits, at most, for the master to take its deregistration. Its drain's end bounds it too (a quarter of the report period before the grace period ends), and by default that end alone: a master stalled for a few seconds still takes it, and shows the worker offline. Shorter, a worker may leave before a stalled master reads it, and shows as disconnected, then lost. |
| `SCHEDR_HIDDEN__WORKER__DRAIN_TICK` | 100 ms | How often a drain checks what is left. |
| `SCHEDR_HIDDEN__WORKER__EXTENSION_FIGURES_INTERVAL` | 1 min | How often a worker sends its plugins' metric figures to the master, which keeps a sample a minute (decision 513). Shorter, nothing more is kept: figures within a minute already written wait for the next. |
| `SCHEDR_HIDDEN__WORKER__QUEUE_CHECK_INTERVAL` | 1 s | How often a worker compares its waiting queue's oldest task with the cluster's slow-equals; each time it is older, one task goes back from the queue's tail (decision 509). Longer, a slow queue hands its work to idle workers more slowly. |
| `SCHEDR_HIDDEN__WORKER__DRAIN_ACK_WAIT` | 5 s | How long a drained worker waits for the master to acknowledge its last results; never past half the report period before the grace period ends, left to deregister and exit. |
| `SCHEDR_HIDDEN__WORKER__STOP_POLL` | 100 ms | How often a stopped task's process group is checked until it is gone. |
| `SCHEDR_HIDDEN__WORKER__CONNECTION_JOB_CANCEL_GRACE` | 5 s | Time a connection test's process gets from SIGTERM to SIGKILL. |
| `SCHEDR_HIDDEN__WORKER__VAULT_MIN_RENEW_WAIT` | 1 s | Minimum wait before a Vault token renewal. |
| `SCHEDR_HIDDEN__WORKER__VAULT_RENEW_CHECK` | 1 min | How often a Vault session with nothing to renew is checked again. |
| `SCHEDR_HIDDEN__WORKER__VAULT_RENEW_RETRY` | 10 s | Wait before retrying a failed Vault renewal while the token lasts. |

### Client

The CLI's and `schedr ui`'s requests to a webserver.

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__CLIENT__CONNECT_TIMEOUT` | 5 s | How long connecting to a webserver may take; a black-holed address fails after it. |
| `SCHEDR_HIDDEN__CLIENT__REQUEST_TIMEOUT` | 30 s | How long one request may take, from connecting to the end of its answer. Raise it for a slow link or a large list; a hung webserver holds a command, or a UI page, this long. A stream (a page's live part, a log tail) has its own limit. |
| `SCHEDR_HIDDEN__CLIENT__STREAM_IDLE` | 60 s | How long `schedr ui` waits for the next bytes of a webserver's live stream before it opens it again; keep it well over `WEBSERVER__EVENTS_KEEP_ALIVE`. |
| `SCHEDR_HIDDEN__CLIENT__COMMAND_WAIT` | 30 s | How long the CLI waits for a command's decision before reporting it as still pending. The master can decide later. UI actions poll until a decision; this does not limit that polling. |
| `SCHEDR_HIDDEN__DAGS__TEST_RUN_TIMEOUT` | 1 h | How long `schedr dags test` waits for its run; a task hanging past its own timeout is stopped before. |

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

These limits are set the same way, with the same warning: whole numbers from 1 to 16,777,216,
in the environment only; anything else stops the start. The two events backlogs take their
memory up front, a slot each. Each is a cap a deployment of an odd
size might need to move: more does more at once and holds others up longer, less spreads a
burst over more rounds or reads. The defaults are measured (B18's stress runs); change one only
after measuring yours, and one at a time.

### Master

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__MASTER__WORKER_REPORT_RECORDS` | 256 | The most wait step hosts, and the most warnings, the master keeps of one worker's report; the rest are dropped with a warning in its log. An honest worker sends a few. |
| `SCHEDR_HIDDEN__MASTER__WORKER_REPORT_TEXT` | 4096 | The most characters of each text in them (a host's code, a warning's message); longer ones are cut. |
| `SCHEDR_HIDDEN__MASTER__NOTIFICATION_BODY` | 1 MiB | The most bytes a notification's body may take once filled. |
| `SCHEDR_HIDDEN__MASTER__RETENTION_BATCH` | 100 | The most runs retention deletes in one transaction. |
| `SCHEDR_HIDDEN__MASTER__EXTENSION_EVENTS_KEPT` | 200 | The events the master keeps per extension, the newest: what its page's events panel can show. |
| `SCHEDR_HIDDEN__MASTER__TRIGGER_RUNS_KEPT` | 20 | The runs of a DAG's trigger the master keeps, the newest: the ones whose logs its DAG page and `schedr triggers logs` reach. |
| `SCHEDR_HIDDEN__MASTER__MESSAGES_PER_TURN` | 80 | The most messages from one worker taken in a turn. Its `Started`s and results are queued for the next round, which stores every worker's in one transaction (decision 505). |
| `SCHEDR_HIDDEN__MASTER__ASSIGNMENTS_PER_ROUND` | 50 | The most assignments a round makes in a batch transaction. The rest wait for later rounds, the longest ready first. More starts a burst sooner and holds the core lock longer for that transaction. |
| `SCHEDR_HIDDEN__MASTER__EXPIRIES_PER_ROUND` | 50 | The most assignments a round takes back as never started. |
| `SCHEDR_HIDDEN__MASTER__WAITING_REASONS_PER_ROUND` | 100 | The most waiting reasons a round writes, a statement each; they are only shown, so the rest wait a round. |
| `SCHEDR_HIDDEN__MASTER__PENDING_REASONS_PER_ROUND` | 500 | The most pending reasons a round writes, in one statement, for the tasks past its assignments (decision 481). |
| `SCHEDR_HIDDEN__MASTER__COMMANDS_PER_ROUND` | 50 | The most commands a round applies, oldest first. Each command has its own transaction and holds the core lock for that transaction. |
| `SCHEDR_HIDDEN__MASTER__SCHEDULED_RUNS_PER_ROUND` | 10 | The most scheduled runs a round creates for one DAG; one catching up after a long pause goes over a few rounds, the earliest first. |
| `SCHEDR_HIDDEN__MASTER__STARTED_INSTANCES_PER_ROUND` | 2000 | The most task instances a round creates for runs that just started; a run is never split, so a run wider than this still starts whole, alone. |
| `SCHEDR_HIDDEN__MASTER__RETRIES_PER_ROUND` | 100 | The most failed tasks a round queues again once their retry delay passed. |
| `SCHEDR_HIDDEN__MASTER__RUNS_PER_PLANNING_PASS` | 100 | The most runs the scheduler creates for one DAG in one pass (a `catchup` backlog with no `max_active_runs` goes on in the next). |
| `SCHEDR_HIDDEN__MASTER__RUN_OF_RUNS_PER_PASS` | 50 | The most runs one pass starts for a DAG started by `RunOf`; `[triggers] max_queued_events` caps it too. |
| `SCHEDR_HIDDEN__MASTER__FAILED_GROUP_MESSAGES_PER_TURN` | 10 | The most messages of a failed batch of results handled one by one in a round (M4B.3), so a failing batch doesn't hold the lock for a long run of single transactions; the rest wait for the next round. |
| `SCHEDR_HIDDEN__MASTER__WORKER_STREAM_BUFFER` | 1024 | The messages that wait for a worker before it is taken for one that stopped reading and disconnected. Less disconnects a slow worker sooner; more holds more memory per worker. |
| `SCHEDR_HIDDEN__MASTER__NOTIFICATIONS_PER_PASS` | 50 | The most notifications one pass of the sender delivers; the rest go in the next pass. |
| `SCHEDR_HIDDEN__MASTER__SLOW_EQUALS_FACTOR` | 3 | The cluster's slow-equals is the median of every worker's queue head age (an empty queue 0) times this, at least the floor (decision 509): a queue that much slower than the cluster's usual gives work back. |

### Webserver and UI

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__WEBSERVER__METRICS_CACHE_KEYS` | 64 | The Metrics cache's keys (each range, for all processes or one), the least recently used dropped first (one a request holds never). More keeps more process panels warm, in memory. |
| `SCHEDR_HIDDEN__WEBSERVER__METRICS_CACHE_BUCKETS` | 100,000 | The buckets kept for one key; a key with more (thousands of processes over 30 days) is read whole each time instead of held. |
| `SCHEDR_HIDDEN__WEBSERVER__MEMORY_PEAKS_RUNS` | 1000 | The runs a DAG's memory p95s are taken from, its last to end and those still going; and a task's attempts read for its chart. Bounds the query: more reads more on each load of the DAG page. |
| `SCHEDR_HIDDEN__WEBSERVER__EVENTS_BACKLOG` | 1024 | The changes a slow event stream may fall behind by before it gets `refresh` (the page reloads its live part whole). |
| `SCHEDR_HIDDEN__WEBSERVER__AUTH_REMEMBERED_MAX` | 1000 | The password checks remembered at once, the oldest dropped first; one dropped costs a bcrypt check again. |
| `SCHEDR_HIDDEN__UI__EVENTS_BACKLOG` | 1024 | The changes a slow browser may fall behind by before it gets `refresh`. |

### Database

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__DATABASE__CHANGES_POLL_ROWS` | 1000 | The most rows of SQLite's `changes` one poll reads; a burst larger goes over the next polls. |
| `SCHEDR_HIDDEN__DATABASE__DAG_VERSIONS_CACHED` | 4096 | The DAG versions a process keeps parsed; past them the cache is cleared and the current ones are parsed again. More holds more memory in a deployment of many DAGs. |

### Worker

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__WORKER__RESULTS_PER_JOURNAL_GROUP` | 32 | The most results a worker writes to its journal as one group (M4B.5), each a thread writing its file meanwhile. |
| `SCHEDR_HIDDEN__WORKER__STARTS_PER_JOURNAL_GROUP` | 32 | The most assignments whose journal records are written as one group (M5.1b). |
| `SCHEDR_HIDDEN__WORKER__QUEUE_FACTOR` | 0.5 | A worker's waiting queue's depth per slot: depth = ceil(slots x factor), a number from 0 to 4 with at most two decimals; 0, no queue (decision 509). `[worker] queue = false` turns the queue off too. |
| `SCHEDR_HIDDEN__WORKER__RESULT_RESENDS` | 8 | How many times a result is sent again over one session before the worker reconnects, so that its registration's journal settles it. |
| `SCHEDR_HIDDEN__WORKER__RESULT_RESEND_GAP_MAX` | 32 | The most heartbeats between two sends of an unacknowledged result; the gap doubles from one. |
| `SCHEDR_HIDDEN__WORKER__UNACKED_MESSAGES_KEPT` | 100,000 | The messages a worker keeps waiting to be heard of, to read master time from (B240), the oldest dropped first. |
| `SCHEDR_HIDDEN__WORKER__UNSENT_CONNECTION_ANSWERS` | 16 | The connections jobs' answers a worker keeps while no master listens; older ones are dropped and their jobs end as lost. |
| `SCHEDR_HIDDEN__WORKER__TASK_CONNECTIONS_OPEN` | 64 | The most context connections a command task may have open to its agent at once; one more is refused with a message. |
| `SCHEDR_HIDDEN__WORKER__WATCHER_LOG_BACKLOG` | 16 | A wait step's messages waiting for its attempt before its host waits too: a step printing faster than its log is written slows down instead of filling memory. |
| `SCHEDR_HIDDEN__WORKER__EXTENSION_REPORTS_PER_SECOND` | 10 | The reports (statuses, links) an extension attempt sends a second: past them, its newest status waits for the attempt's end and a link is dropped, with one warning. Metric points aren't counted: only their figures travel, once a minute. |
| `SCHEDR_HIDDEN__WORKER__EXTENSION_REPORTS_AT_ONCE` | 50 | The reports an extension attempt sends at once after a quiet spell. |
| `SCHEDR_HIDDEN__WORKER__EXTENSION_EVENTS_PENDING` | 100 | The extension events (a plugin discovered, skipped, refused, a point or figures dropped) a worker holds for its next heartbeat; past them the oldest go. The master takes at most `MASTER__WORKER_REPORT_RECORDS` of them a heartbeat. |

### TLS

| Variable | Default | What it does and what depends on it |
|---|---|---|
| `SCHEDR_HIDDEN__TLS__HANDSHAKES_AT_ONCE` | 256 | How many TLS handshakes a listener runs at once; beyond them accepting waits, so connections that never finish hold this many sockets, not all. |

## What stays in the binary

Every other number schedr uses is fixed for a reason (decision 493):

- **Data and protocol:** what is stored or sent, such as the samples' minute and 15-minute
  steps, the message sizes and chunks, the protocol's version and field numbers.
- **The databases' parameter limits:** the rows or IDs one statement takes.
- **The system:** the descriptors one datagram carries, exit codes and signals, clock drift
  bounds, the daylight-saving margin.
- **Contracts users see:** ID, key and parameter sizes, the API's list sizes, the DSL's defaults,
  the smallest memory a Python process starts in.
- **Derived from another value:** the workers' "heard within" windows (from the usage interval),
  the write limits (from the query timeout), the gauge refresh limit (from the loop stall).
- **Shared by two processes:** a task's API call limit, a connections check's limit.
- **Display:** the UI's layout, page sizes and log tails, the eight series a chart draws.
- **A task's own process:** `schedr task-ctx`, `dag-ctx` and the Python package read no
  configuration.
