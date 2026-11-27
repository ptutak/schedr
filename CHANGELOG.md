# Changelog

Each release's section is its release notes on GitHub (`scripts/release.py notes`). From 1.0.0
on, the database schema changes only through new migrations and the protocol between schedr's
processes only additively within a major version (decision 456); 0.1.0 was a preview, its
schema and protocol changed in place (decision 156).

## Unreleased

- **A worker's new connection replaces one that died without a word** (B301, decision 525): each
  `Register` numbers its connection; a newer one of the same process replaces the old stream at
  once, where the master refused it as "already live" until the lease ran out and its tasks were
  lost. An equal or older one is refused, so a registration held on its way no longer takes back
  attempts the worker started since (B302, which could run a task twice). Protocol 2.3: **upgrade
  the workers with the master**; a worker older than 2.3 is refused after its first lost connection.
- **A worker leaves a connection the master stopped answering** (decision 526) after 25 s, at most
  half the lease (`SCHEDR_HIDDEN__WORKER__UNANSWERED_TIMEOUT`), and connects again within its lease.
- **Extension tasks go only to workers offering their extension at its version** (decision 527): no
  queue of their own needed, and a rolling upgrade of a plugin routes each task to the workers still
  serving its version. One nobody offers waits, its reason shown, and is `unschedulable` after
  10 minutes (decision 529).
- **A `cleanup_anywhere` cleanup runs at least once** (decision 528): let through, then its worker
  lost, it is still moved; the guarantees page says so.
- **A moved cleanup runs, and runs once** (B304, decisions 533 and 535): a worker cut off while its
  `cleanup_anywhere` task ran, its cleanup waiting to move, could come back, be told to stop, and
  report the refused cleanup as not needed; the try then closed with no worker running its cleanup.
  A worker back while its own cleanup still waits now gets it back and runs it. Each hold of an
  attempt has its own ID: a moved cleanup runs under `<attempt>_<n>` on its worker (its log there
  too), and only the attempt's current hold's messages count. Migration 0004
  (`attempts.cleanup_holder`).
- **`schedr-ctx`, a small program for scripts** (M5.3c, decision 532): `schedr task-ctx`'s commands,
  arguments, output and exit codes, starting in about 3.6 ms against 10.5 ms. The worker puts its
  own program's folder first on every task's `PATH`, so a task finds the `schedr-ctx` beside its
  `schedr`; `schedr task-ctx` stays.
- A worker's connection to the master is kept in one place the master's hot paths read without its
  lock (M5.3i, decision 531): deliveries, the stream readers' checks and heartbeat answers no longer
  wait behind a round.
- Scenarios 1 and 6 of the stress suite: the round's p99 limit 7 s.

## 1.2.1

Connections with a limit: a worker, the master and the web UI no longer wait without end for an
address that answers nothing (a pod just killed), and a worker no longer loses its tasks to it. The
Helm chart can run hot standby masters. No migration and no protocol change: the master first, as always.
One case stays open (B301): a worker whose connection ended without the master hearing of it is
refused until its lease runs out.

- **A worker's connection to the master has a time limit** (B296, 5 s): after a master pod was killed,
  a worker whose connection attempt got no answer waited about two minutes for the kernel to give
  up, was counted lost, and lost its tasks. It now tries again.
  Each of the master's addresses gets a share of the limit, and a limit never takes more than a
  quarter of the worker lease. A `https://` master URL with no TLS configured is refused.
- **A worker leaves a master that accepts it and says nothing** (B297): 10 s to be registered,
  doubling up to 80 s at each silent try; it waited without a limit before its first registration,
  and for a lease after it. A master doesn't register a worker that has left meanwhile.
- **A draining worker finishes the connection it is making when it has results to send** (B300).
- **Listening for Postgres's notifications has the query limit** (B299), at first and again after a
  loss: an address that dropped packets held the master's loop, and stopped live pages for good.
- **`schedr ui` leaves a silent webserver's live stream** (B298) after a minute, where it stayed up
  to a day.
- **Hot standby masters in the chart** (decision 523): `master.replicas` above 1 in mode distributed
  keeps masters waiting to lead when the leader dies. An upgrade still stops them all. The chart only.

## 1.2.0

A faster master, and extensions: task kinds run by a plugin, in any language, beside the
built-in ones. The master's round costs about a third less, a worker queues the ready tasks
past its slots, and a trigger's events are stored in batches: on CI's runner the stress suite's
throughput scenario runs 107-112 tasks a second (the first CI numbers recorded, on the guide's
Stress tests page). An extension's
plugin runs each task of its kind, reports its status, links, metrics and events, may keep a
long-running service on each worker, and gets an Extensions page; a Kubernetes plugin runs each
task as a pod.

### Upgrading from 1.1.0

- **Upgrade the master first, then migrate.** The protocol goes from 2.1 to 2.2: a 1.2.0 master
  takes 1.1.0 workers, a 1.1.0 master refuses 1.2.0 ones. One migration adds whether a task is a
  cleanup or a wait step, an attempt's statuses and links, the extensions' declarations, samples
  and events, whether a run's start and a posted event were announced (runs and events before
  the upgrade count as announced: nothing already going is announced again), the services' part
  of a worker's memory, and each trigger's runs: run `schedr db migrate` (or start the master
  with `--migrate`) before the webservers. A database migrated by 1.2.0rc1 isn't taken by
  1.2.0rc2 or later (that migration changed after rc1): start from a copy of the 1.1.0 database
  instead.
- **A worker's certificate must name the worker** (decision 520): with TLS, the master takes a
  worker's client certificate only if it names `[tls] worker_name` (the chart's do), so another
  role's certificate can't act as a worker. Certificates made by hand need that name. With
  `[tls] worker_identity = "certificate"` the worker's ID must be named too, as
  `<id>.<worker_name>`.
- **A worker without a default secret storage refuses DAGs reading secrets** (decision 511):
  validation refuses a DAG reading `secrets[...]` with no storage named that the worker can't
  serve, instead of the task failing at run time. A worker may use the `db` storage only by
  opting in (`[secret_storage.db] type = "db"`), and is warned at start; use Vault in
  production.
- **A trigger stops in 5 s** (decision 522): a trigger's time between SIGTERM and SIGKILL is
  `[triggers] cancel_grace`, 5 s by default, no longer `[master] cancel_grace` (30 s, still the
  tasks'). A trigger needing longer in its `finally`: set it. A lost worker's trigger now starts
  elsewhere only after that grace, so it never runs twice at once.
- **Logs are structured** (decision 514): a Python task's `logging` records are written as JSON
  lines (`schedr_log`, `level`, `message`, `fields`, `time`), shown with their level and
  filterable in the Logs tab; a task configuring logging itself keeps its own. `[worker]
  log_format = "plain"` stores them as readable lines instead.
- **The Runs page pages differently** (decisions 517, DN39): its Older runs button is gone. One
  deployment's runs are in numbered pages; every deployment's together move by Newest,
  Previous, Next and Oldest. `GET /api/v1/runs`, `GET /api/v1/dags/{dag}/runs` and `GET
  /api/v1/dags` take `page` and `limit` and say what follows (`more`, `pages_ahead`,
  `more_past`); `schedr runs list` and `schedr dags list` take `--page` and `--limit`.

### New

- **Extensions** (decisions 510, 512-519): a task kind run by a plugin executable the worker
  supervises, one process a task, never sharing a loop with another task. A plugin is written
  in Python (`schedr_extension`, which imports nothing of schedr), in Rust (the
  `schedr-extension` crate), or as a shell script (`schedr extension shell`, `schedr task-ctx
  extension`); `[extensions]` discovers and configures them. The protocol between the worker and
  a plugin is specified on its own page, with a schema of its messages
  (`schema/extension.schema.json`).
- **An extension's service** (decision 518): one long-running process per worker beside its
  plugins, for what tasks of the kind share (a client, a watch): started before the first task,
  restarted with backoff, its memory, calls and readiness reported; a task of the kind goes only
  to a worker whose service is ready.
- **Plugins observed and controlled as tasks are** (decision 513): an attempt's statuses and
  links, the plugin's declared metrics stored as worker samples, stop reasons; the
  **Extensions page**: a tab per extension across deployments, one version at a time (the
  highest by default, the others in a dropdown), then a tab per deployment running it and per
  worker, with its panels (decision 516: a chart's point, combine, group_by and limit) and its
  events (DN40).
- **A Kubernetes plugin** (M5.2i2, M5.2i11): each task a pod, through an async client with
  watches, with panels of every kind; `make demo-kind` shows it on kind with the Kubernetes
  Dashboard.
- **Notifications of extensions and more events** (decision 515): an extension's own declared
  events, and `RUN_STARTED`, `TRIGGER_FIRED` and `EVENT_POSTED`.
- **A trigger's runs and logs** (B272): each run of a DAG's trigger is kept (the newest 20), its
  log read from its worker while it runs and after: on the DAG page, through `GET
  /api/v1/dags/{dag}/trigger/runs` and `.../runs/{instance}/log`, and with `schedr triggers runs`
  and `schedr triggers logs`.
- **Numbered pages** (decision 517): the Runs and DAGs pages show `< Previous 1 2 [3] 4 5 ...
  Next >`, with no total counted; every deployment's runs move by cursor (DN39).
- **A worker's identity in its certificate** (decision 520): `[tls] worker_identity =
  "certificate"` binds a worker's ID to its certificate (the chart issues one per pod through
  cert-manager's CSI driver). Standalone's gRPC port and log endpoint use mTLS made in memory at
  each start.
- **A worker's waiting queue** (decision 509): ready tasks past a worker's slots wait on it, so
  a slot freed starts the next at once; given back when the cluster is faster (`[worker]
  queue`).
- **`[database] prepared_statements`** (decision 503): on by default; off for a pooler in
  transaction mode that doesn't carry prepared statements.

### Performance

- **The master's round a third cheaper** (decisions 501, 502, 505, 506): the core lock released
  during database calls, one probe a round, only the round moving runs forward with each
  worker's starts and results queued for it, and its slow parts on due flags.
- **The worker's start path** (decision 507): its journal written beside its loop, a start in
  flight held unreported.
- **A trigger's events batched** (M5.2h-M5.2h3): several in flight per trigger, a turn's events
  stored in one transaction, about seven statements a trigger instead of seven an event.
- **Poke reports in the heartbeat** (M5.2f), and many DAG triggers in a stress scenario.
- **Extensions never hold the master up** (decision 521): their reports are written off the
  round, in batches, and dropped first under pressure, counted and warned of.
- **The CLI reads a command's decision after 50 ms** (M5.2h5), not a second.
- **Deep pages and cursors read a range** (M5.2j): a run list's page 1,000 of 250,000 runs in
  about 30 ms; Next from near the oldest run in 9 ms (1.1.0's Older runs: about 0.5 s).

### Fixed

- **A worker reporting a trigger the master doesn't know held every `wait_for` DAG waiting on
  it** (B279) until it stopped it; held now for at most the grace and a lease.
- **A cut-off worker kept its triggers running** (B290) when it ran no task whose cleanup may
  move; it stops them at its deadline now, and the master starts them elsewhere only after
  their grace.
- **A transaction Postgres ended for its idle time or length lost a worker's message** (B289):
  it counts as the database's trouble, and the message is handled again after the reconnect.
- **A task unschedulable for missing code, or for its extension's service not ready, stayed so**
  (B284, B285, B287) after the code or the service came; a worker able again gives it another
  chance.
- **Prepared statements leaked** with the cache off (B256): each query was prepared under a new
  name until its connection retired.
- **Through PgBouncer in transaction mode the webserver could panic at start** (B264).
- **The gauge refresh counted every run in history every 15 s inside the master's loop** (B270);
  it runs on its own, off the loop.
- **A drained worker's interrupted task sometimes never ended** after the worker was killed and
  came back (B282).
- **A cancelled or failed task's refusals stayed in the master's memory** (B286).
- **A worker's RUNNING count included its wait steps** (B254): tasks and wait steps are counted
  apart, against their own slots.
- **A worker's memory split left out its extensions' services** (B281).
- **`make distributed`'s workers on the latest release were given what they refuse** (B273).

## 1.1.0

Faster scheduling and metrics in the web UI. On the same machine as 1.0.0, the stress suite
runs 34-67 % more tasks a second (26.7 instead of 17.0 on Postgres), a run takes about a third
less time, and 500 waiting sensors all start in about a second instead of 20 (the numbers are
on the guide's Stress tests page, "After M5.1"). The web UI gains a Metrics page (memory, CPU,
throughput and start latency per deployment), a Status block on the Deployments page, and task
memory charts.

### Upgrading from 1.0.0

- **Upgrade the master first, then migrate.** The protocol goes from 2.0 to 2.1: a 1.1.0 master
  takes 1.0.0 workers, a 1.0.0 master refuses 1.1.0 ones. One migration adds stored scheduling
  moments (each trigger's next fire time, each wait step's next look), the samples table and
  the processes' usage, with their indexes, and an index of each DAG's runs by trigger time: run
  `schedr db migrate` (or start the master with `--migrate`) before the webservers. A database
  migrated by 1.1.0rc1 isn't taken by 1.1.0rc2 or later (that migration changed after rc1):
  start from a copy of the 1.0.0 database instead.
- **An attempt's `started_at` is the worker's start** (M5.1m), turned into master time, not the
  moment the master handled the worker's message. Start-latency numbers read from the database
  are lower than 1.0.0's for the same work.
- **Cleanups get 25 s by default outside the chart, not 5 minutes** (B252). Left unset,
  `[worker] cleanup_timeout` is now the shorter of 5 minutes and what a drain leaves of
  `termination_grace_period` for a cleanup: 25 s with the default 30 s grace, so a cleanup still
  running when the pod is killed at the grace period's end isn't lost. This limits every
  cleanup's try, also when nothing is shutting down. The Helm chart's 600 s grace keeps 5
  minutes. Cleanups that take longer: set `cleanup_timeout` (and a grace period it fits in).

### New

- **The Metrics page** (decisions 465-467, 494): per deployment, the memory and CPU of each
  process (the master, webservers, workers: the agent, fork servers, watcher hosts and tasks),
  against their limits, over 1 hour, 24 hours, 7 or 30 days; the tasks ended per bucket
  (succeeded, failed, stopped) and the p50 and p95 of their start latency. Charts are drawn by
  the server; the samples are taken each minute and rolled up after 15 minutes. Processes that
  ended in the range (a restart's old ones) are one line together, not one each (decision 499).
- **Filters on the DAGs page** (decision 500), beside the column titles: by name as you type,
  by the trigger's switch (All, Active, Paused, remembered), and by trigger kind and tag in
  dropdowns; in the address, so a filtered list can be linked. The Runs page's filters moved
  beside its column titles in the same style, its DAG field matching a part of the name as you
  type (`dag_contains` on `GET /api/v1/runs`). The DAGs page's DAG title sorts the list A to Z or
  Z to A. In the run lists, times show seconds and a chained run's "By" is a short link to the run
  that started it.
- **A Status block on the Deployments page** (decision 462): per deployment, its masters by role
  (red when none leads), workers by state, slots in use, errors and warnings, mixed versions, and
  every process's memory with a 24 h sparkline. Each node's panel has its own chart, and the
  Workers page a Memory column.
- **Task memory charts** (decision 467): a task's panel shows its last 30 attempts' peak memory,
  and the DAG page each task's p95 peak against its limit.
- **The DAGs page by deployment** (M5.1l1): one list of every DAG, or a tab per deployment.
- **`schedr standalone --dag-path` and `--script-path`** (decision 488): the folders on the
  command line, each repeatable and taking `.`, over `SCHEDR_DAG_PATH` and
  `SCHEDR_SCRIPT_PATH`. Getting started shows the project folder itself as the DAG folder
  (with a `.schedrignore`) and a plain script job.
- **A poke may be a plain `def`** (decision 489): `@sensor.poke(every=...)` and
  `@trigger.poke(every=...)` take a synchronous function too, run on the host's threads within
  the same call window.
- **Tunable constants as hidden settings** (decision 493): 11 timings and 30 limits a deployment
  might need to change (per-round caps, the worker's journal groups, event backlogs, the Metrics
  cache, the client's timeouts) are `SCHEDR_HIDDEN__*` variables, their defaults unchanged; the
  configuration reference lists them.

### Performance

- **The master's lock per transaction** (M5.1c1, M5.1c3): a round's parts, a worker's turn and
  each command take the core lock for one transaction each instead of a whole turn; a round's
  assignments and a command are written set-based, one transaction each.
- **Start records in groups** (M5.1b): a burst of starts is journaled and recorded as one group.
- **A worker's turn takes up to 80 messages** (decision 484, from stress runs of 10 to 160).
- **Fewer statements behind PgBouncer** (M5.1d): claims, tries and sensor starts set-based.
- **An idle master reads one statement a tick** (decisions 485, 486): a probe answers whether
  each read of the round would find anything, with each DAG's and trigger's next moment stored;
  triggers and named events run in a loop of their own, woken when something happens. An idle
  standalone went from 22.5 statements a second to 1.9, and from 2.0 % of a core to 0.3 %.

### Changed

- **A worker with no DAG directories warns at start** (decision 476): "no DAG directories",
  naming `SCHEDR_DAG_PATH` and `[dags] path`. Before, a standalone started without them showed
  only an empty UI.
- **A role waits for a database it can't reach yet** (decision 496). The master, the webserver
  and standalone started before their database (refused, timed out, Postgres starting up or out
  of connections) used to exit; they now try again every 5 s, warning with the database's host
  and port (never its user or password) at once and every minute. A wrong password, database
  name or TLS setting still exits at once.
- **PyPI shows a page of its own** (decision 477): installing and a first run, with links to the
  new **Getting started** section of the documentation, instead of the repository's README.

### Fixed

- **A worker could start a task after its cutoff** (B246, critical, found by Grok's review of
  M5.1b). When a worker loses contact with the master, its cutoff is the moment it must stop
  starting tasks, since the master may hand them to another worker from then on. An
  assignment already in hand could still be started after the cutoff (in 1.0.0 one such
  assignment, after the alive tick had noticed the cutoff), so the same task could run on two
  workers. The worker now starts nothing once it is cut off.
- **A stopped standalone kept the master's lease** (B248). Every restart, a Ctrl-C's included,
  waited 20-30 s as a standby before scheduling anything. The master now gives the lease up
  when it stops cleanly, so the next start leads at once; after a crash it still waits the
  lease out.
- **A stopping worker could show as lost** (B242). It left 5 s after sending its `Deregister`,
  so a master stalled for longer (a slow database) never read it, and the worker showed as
  disconnected, then lost. It now waits for the master until its grace period's end.
- **Every start warned about the default `cleanup_timeout`** (B252): the default didn't fit in
  the default grace period. It does now (see Upgrading).
- **A warning for every wait step host that ended** (B253): when a host's last wait step ended,
  the worker stopped it and then warned that its channel had ended, once per host (every few
  seconds on a busy worker). A host stopped that way is now logged at debug; one ending unasked
  is still a warning.

## 1.0.0

The first stable release (`Development Status :: 5 - Production/Stable`). From 1.0.0 on, the
database schema changes only through new migrations, which `schedr db migrate` applies; the
protocol between schedr's processes (now 2.0) changes only additively within 2.x; DAG JSON, the
REST API and the configuration keep accepting what they accept now.

### Upgrading from 0.1.0

- **A fresh database, every node at once.** 0.1.0 was a preview: its schema and protocol were
  changed in place since. A 1.0.0 master refuses a 0.1 worker, and a worker of the 1.0.0rc1
  rehearsal (protocol 1.0; another protocol major, decision 460), and the
  schema starts afresh: `schedr db status` reports a 0.1 database as created by an earlier
  schedr, and the master, the webserver and `schedr db migrate` refuse it.
- **The images' plain tag is now glibc** (decision 454): `ghcr.io/ptutak/schedr`,
  `schedr-slim` and `schedr-server` are built on Debian (`python:3.14.8-slim`) from the
  manylinux wheel; the Alpine ones, the plain tag in 0.1.0, are tagged `-alpine`
  (`schedr:1.0.0-alpine`). An image of your own built `FROM` the base takes `apt` (or `apk` on
  `-alpine`); `examples/kubernetes/Dockerfile` shows both. The glibc base has PostgreSQL 18's
  client from PostgreSQL's own apt source.
- **A slim image** (decision 383): `ghcr.io/ptutak/schedr-slim`, Python and schedr without the
  task libraries and connection clients; the full image stays the default.
- **Timings a deployment rarely tunes left the configuration** (M4.8t, decisions 231-235): 18 of
  them are set only by `SCHEDR_HIDDEN__<SECTION>__<NAME>` (environment only, warned about,
  unsupported); the others are keys with today's values as defaults.
- **A shortened worker lease may be refused at start** (decision 444). A master
  keeps acting for its lead after its last lease renewal: `SCHEDR_HIDDEN__MASTER__LEASE_PERIOD`
  less `SCHEDR_HIDDEN__DATABASE__QUERY_TIMEOUT` (30 s less 10 s, 20 s by default). The worker
  lease (`SCHEDR_HIDDEN__MASTER__LEASE_TIMEOUT`, 60 s) must now be longer than that lead, or
  schedr refuses to start. The lease period must already be at least three query timeouts, so
  the worker lease must be longer than twice the query timeout (20 s by default). Anyone who
  lowered the worker lease to 20 s or less, keeping the default lease period and query timeout,
  must also lower those before upgrading: for example a 15 s worker lease with a 3 s heartbeat
  interval (the lease must stay at least four heartbeats), a 6 s lease period and a 2 s query
  timeout (a 4 s lead).

### New since 0.1.0

- **Cleanups and resources** (M4.8, decisions 214-230, 252-279): per-task `cleanup=` with
  registered handles (masked, journaled), `cleanup_timeout` and retries, a failed cleanup
  failing the run; `cleanup_anywhere`, a lost worker's cleanup moving to another, a worker cut
  off stopping those tasks at its lease; `@resource` and `uses=`, `CommandResource` and
  `ScriptResource`, `task-ctx cleanup` and `resource`; `schedr tasks cancel` stopping a hung
  resource cleanup.
- **Script roots and `schedr.toml`** (M4.9, decisions 292-305): `SCHEDR_SCRIPT_PATH`, standalone
  jobs declared in TOML, the `script` task kind and `ScriptOperator`, libraries shipped in roots;
  only `.py` files mentioning `schedr` are parsed.
- **Async tasks** (M4.10, decision 316): `async def` tasks, cleanups and resources, on `uvloop`
  when installed (`schedr[uvloop]`).
- **Wait steps, triggers and events** (M4.11, decisions 320-356): `@sensor` and `@sensor.poke`
  on a watcher host, `TimedOut`; `@trigger` and `@trigger.poke` (with `batch=`) starting event
  runs; `first_of` and `at_least`; `RunOf` to wait for or start on another DAG's run; named
  events posted through the API; the `delay` and `until` sensors; `schedr.api`, the REST API
  from Python, inside a task through its agent.
- **Runs between and over time** (M4.12, decisions 357-407): `TriggerDagOperator` (a target
  needs `trigger=Chained()`), with `.partial()`/`.expand()`; backfills (`schedr runs backfill`,
  the API and the DAG page's form).
- **Notifications** (M4.13, decisions 408-429): eight events, webhooks and email through an
  outbox, `Dag(notify=...)`, `channel.send` from tasks.
- **Retention** (M4.14, decisions 430-435): `[retention]` for runs, outputs, events and
  commands, never the latest run nor an unfinished one.
- **Per-task memory** (M4.15a, decisions 50, 436-438): `memory=` budgets and
  `hard_memory_limit=`.
- **Master time** (M4B.0, decisions 443-453): liveness decided on the master's monotonic clock,
  carried on every message; a worker's cutoff on master time; a replaced
  incarnation's attempts held for its lease; a new leader waiting out a deposed one (lease
  epochs); metrics for the clock and the fencing.
- **A DAG's trigger has the switch** (decision 459): pausing switches a trigger off, so a DAG
  without one has nothing to pause (`dags pause` and `unpause` are refused; manual runs start
  either way). A trigger is stored paused when it appears or changes kind; another of the same
  kind keeps the switch. The API's `paused` is now `trigger_state` (`paused`, `active` or
  null); `dags list` has STATE (`paused`, `active`, `manual`); the UI shows the switch in the
  Trigger column. Tables: `dag_triggers`, and `triggers` and `trigger_passed` renamed
  `trigger_progress` and `trigger_progress_passed`.
- **A DAG page's actions in one row** (decision 461): an event trigger's Restart and Skip run sit
  beside Trigger, Backfill and Pause; Restart only for a trigger stopped, failing or stuck (a
  poke trigger with no call ended for over three times its `every` plus a heartbeat: the API's
  `stuck`, `(stuck)` in `schedr triggers list`).
- **Numbered messages with an echo** (decision 460, B240): every message between master and
  worker is numbered and names the latest the other side had, so the master takes an
  assignment back only once the worker's messages show it never started it. No start
  deadline: `AssignTask.start_by_ms`, the `TOO_LATE` rejection and the metrics
  `worker_start_slack_seconds` and `worker_too_late_rejections` are gone. Protocol 2.0.
- **Throughput** (M4B, decisions 396, 397, 452): a worker's turn handled in groups, its results
  written together, the agent's journal in groups: about half again as many tasks a second on
  Postgres (the guide, Stress tests).
- **A simulator** (decisions 446, 455): the real master and workers on one virtual clock,
  through cuts, lags, restarts and takeovers, checking that no task instance runs twice;
  600 fresh scenarios nightly.
- **Connections in the web UI** (M4.7e, decisions 207-211): a **Connections** page (every
  deployment's, by DAG root) and a section on each DAG's page (those its tasks declare), with
  **Check all** per root and **Test** per connection (after a confirmation), the results
  filled in as the workers answer. The API: `GET /api/v1/connections`, each root's
  connections with their type, file and the DAGs and tasks declaring them.
- **Every connection type works in the base image** (decision 212): psycopg and PyMySQL join
  its libraries, and the command-line clients commands use their connections with: psql,
  mysql and mariadb (with their dump tools), Mongo's database tools, ssh and scp, curl (the
  image grows to about 670 MB). `pip install schedr[connections]` installs every type's
  client elsewhere.
- **A connections demo** (M4.7d, decisions 202-205): `make demo` runs every connection type's
  server, two deployments (`dev`, `prod`) over one DAG folder with secrets of their own, and
  one UI on http://127.0.0.1:8082, in containers; `make demo-check` runs it unattended. See
  `examples/demo/README.md`.
- **Mongo's login database documented** (decision 206): a user logs in on the URI's
  `authSource`, else its path's database, else `admin`; `database=` is only the database
  the task gets.
- **`schedr connections check` and `test`** (M4.7c, decisions 174, 193, 198-200): `check
  [ROOT]` renders every connection on a worker having the root's current code, saying of
  each secret whether it is there (never its value); `test NAME` opens one there and runs its
  check, beside the tasks (no slot, one at a time per worker), stopped after 60 s
  (`--timeout`). `--worker` picks the worker. Exit 1 when one fails. The API:
  `POST /api/v1/connections/check` and `POST /api/v1/connections/{name}/test`.
- **Commands get connections as variables and files** (M4.7b2, decisions 184, 190-197):
  `CommandOperator(connections={"DB": warehouse})` gives `DB_*` variables and writes the
  files each type's tools read in a folder of the attempt's own (0700, in memory under
  `/dev/shm` where the machine has it, else the data directory, unless `[worker]
  connection_dir` says otherwise; removed when it ends): curl's `-K` config, an SSH config (`ssh -F "$DB_SSH_CONFIG" target`) with its key
  and `known_hosts`, Postgres's `.pgpass` named in `DB_URL` (`psql "$DB_URL"`), one option
  file for MySQL's and MariaDB's clients (`mysql --defaults-file="$DB_MYSQL_CNF"`),
  Mongo's database tools' file (`mongodump --config "$DB_TOOLS_CONFIG"`), and `DB_JSON` with
  every value. A URL variable never holds a password; the TLS defaults are Python's.
  `schedr task-ctx connection NAME --env PREFIX` now prints these same variables, the files
  written. **MariaDB** is supported (decision 196): `SQL` takes `mariadb://`.
- **Built-in connection types** (M4.7b1, decisions 174, 183): `HTTP`, `SSH`, `Mongo`, `SQL`
  (Postgres, MySQL) and `Generic` in `schedr.connections`, named by their first argument,
  giving `httpx.Client`, a paramiko `SSHClient`, a pymongo `Database`, a DB-API connection or
  a dict; each library an extra (`schedr[http]`, `[ssh]`, `[mongo]`, `[postgres]`,
  `[mysql]`), imported on use. Each has a check (`run_check()`); `@name.check` adds one to a
  `@connection`. SSH trusts only the host keys in `known_hosts` unless told
  `insecure_skip_host_check=True`; SQL verifies TLS (`sslmode=verify-full`, libpq's
  `sslmode` for MySQL too, decision 187) unless the URL says `?sslmode=...`, and Mongo
  unless the URI says `tls=false` (decision 189). `SQL` and `Mongo` take `user` and
  `password` apart from the URL; a value a t-string fills into their URL is percent-encoded
  (decision 188).
- **Connections** (M4.7a, decisions 174, 179, 180): `@connection(...)` on a generator function
  defines one by name for the DAG folder, its values secrets and plain values; a task
  declares what it opens (`@task(connections=[...])`, `CommandOperator(connections=)`), and
  the worker renders those before it starts, masking their secrets. `with name() as client:`
  in a Python task; `schedr task-ctx connection NAME --env PREFIX` (export lines for `eval`)
  or `--json` in a command. The error kinds `connection_not_found` (retried) and
  `connection_denied`; a DAG's page warns of a declared connection its folder lacks, and
  `schedr dags check DIR` fails on it. A connection's values take at most 1 MiB. Doubles: `task_context(connections=)`,
  `run_dag(connections=)`, `schedr dags test --connection name=JSON`.
- **Live pages share one stream per browser** (decision 175, B31 and B32): a SharedWorker
  holds the UI's `/events` stream for all of a browser's schedr pages, so six live tabs no
  longer leave clicks and page loads waiting for one of the browser's six connections; a
  running try's log is polled instead of streamed. `/events` now carries every change as JSON
  (each page decides which concern it). htmx's SSE extension is no longer shipped.
- **Secrets** (M4.6): `secrets['path']['key']` (or `secrets.value('path#key')`),
  `secrets.<storage>[...]` and a whole secret `secrets['path']`, typed as the values they
  become (`str`, `dict[str, str]`), as task arguments, in `env=` and in t-strings
  (`text(t"...")` types one as `str`); `.get(key, fallback)` and `value(..., default=)` for a
  missing key, a fallback written in the DAG and never masked; `secrets=` declarations with
  `SecretPrefix` for reads on demand (`task_ctx.secret(path, key)`, `task_ctx.secret(path)`,
  `task_ctx.secret_value('path#key')` in a Python task, `schedr task-ctx secret get` in a command); every fetched value masked in the task's
  log and stored output; the error kinds `secret_unavailable` (retried), `secret_denied` and
  `secret_not_found`; workers report their storages and the master refuses a DAG naming
  another. Test values with `schedr dags test --secret`, `run_dag(secrets=)` and
  `task_context(secrets=)`. **The `db` storage** (standalone): values encrypted in the
  database under `SCHEDR_SECRET_KEY` (XChaCha20-Poly1305, each bound to its `path#key`),
  always there and the default; `schedr secrets set/list/get/delete`, `schedr secrets
  export --to DIR` (one file per path, for `vault kv put`) and `schedr secrets new-key`;
  `[secret_storage]` in the configuration (`default`, `min_mask_length`, a storage's
  `cache_ttl` and `unmasked_prefixes`; several storages need `default`). **Vault** (KV v2) with Kubernetes auth (the chart's
  `secretStorage.vault`, a projected service-account token) or a token, renewed by the agent;
  the chart's `secretStorage.db` for standalone's key. A task can log in to Vault as its
  worker (decision 173): teams that must be kept apart run separate deployments, still in one
  federated UI.

### Fixed

Every bug known at the release, but the DAG page layout's (B38, B39: a redesign, in M6).
Among them: a worker lost just as an assignment reached it no longer has it
queued again at once when its task has `cleanup_anywhere` (B234, decision 455).
A worker connecting again gets back the parse jobs it held, instead of the DAG waiting
`parse_job_timeout` (B235); a database of 0.1 is refused rather than read as current (B236).
A draining worker whose master acknowledges its last results late still deregisters and
exits within its grace period: the wait ends half the report period before it (B237).
A worker whose messages were cut off while an assignment still reached it no longer has that
assignment run again beside it (B240).

## 0.1.0

The first release, a preview (`Development Status :: 3 - Alpha`). **No upgrade path:** 0.2
will need a fresh database; the schema and the protocol change in place until 1.0.0.

Install with `pip install schedr` (Python 3.14; wheels for Linux x86_64 with glibc or musl,
Linux aarch64 and macOS arm64). The images, x86_64: `ghcr.io/ptutak/schedr` (with Python:
standalone and workers, the base for your own image) and `ghcr.io/ptutak/schedr-server`
(master, webserver and UI, no Python).

What it has (milestones M1 to M4.5 in PLAN.md):

- **DAGs in Python:** `Dag`, `@task`, `CommandOperator`, `EmptyOperator`; data flow between
  tasks, `>>` and `chain`; `Cron`, `Every`, `Once` and presets; `StepDefaults` (retries,
  timeouts, queues); typed outputs (dataclasses, Pydantic models with `schedr[pydantic]`),
  outcomes and routing on them (`.on(...)`, `@branch`), task groups, mapping
  (`.partial(...).expand(...)`), concurrency limits.
- **Run parameters:** `Dag(params=...)` from a dataclass or a model, checked by the master
  before a run exists, given with `runs trigger -p` / `--params`, the API or the UI's
  Trigger form, stored with the run and masked where they hold secrets; `dag_ctx.params` in
  a task, `schedr dag-ctx` in a command, `schedr.script` in a plain script.
- **Testing DAGs:** `schedr dags check`, `schedr dags test`, and `schedr.testing`
  (`run_dag`, `task_context`) for pytest.
- **Running:** `schedr standalone` (master, webserver, UI and a worker in one process, SQLite
  or Postgres), or `schedr master`, `schedr worker`, `schedr webserver` and `schedr ui` as
  separate processes on Postgres with mTLS; a standby master taking over through a lease;
  workers that keep running tasks while the master restarts, and drain on SIGTERM; a fork
  server for fast task starts.
- **Web UI:** DAGs, runs with their graph, task logs and outputs live, actions (trigger,
  cancel, pause, retry, drain) with confirmations, import errors, workers, the deployments
  graph; several deployments in one UI; sign-in by proxy header or basic auth.
- **Operations:** the REST API (`/api/v1`, OpenAPI) and the CLI over it; Prometheus metrics,
  `/healthz` and `/readyz`; a memory watchdog per task; secrets masked in logs, outputs and
  configuration views; a Helm chart in the repository (`charts/schedr`, not published).
