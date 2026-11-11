# Glossary

The names used across the documentation, the API and the logs.

**DAG**
: A set of tasks and the edges between them, defined in a Python file. Identified by its DAG
  ID. A DAG has **versions**: each parse that changes it stores a new one.

**DAG root**
: One directory of DAG files (an entry of `SCHEDR_DAG_PATH`), named after its last path
  component unless renamed. A DAG file is referenced as root and relative path, e.g.
  `etl:team_a/sync.py`, so references are the same on every worker.

**Cleanup**
: Work run after a task attempt on the same worker, before its slot is released and before
  downstream tasks or a retry proceed. See [Cleaning up after a task](../02-dags/15-cleanup.md).

**Connection**
: A named set of values for a task to connect to a database, API or host. A task declares
  which it uses. See [Connections](../02-dags/14-connections.md).

**Connection type**
: A built-in connection for a kind of server, with a fixed set of values and a Python
  client, or `Generic` for values of one's own. See [Built-in types](../02-dags/14-connections.md#built-in-types).

**Content hash**
: A hash of every file in a root (paths and contents), combined for DAG roots with the
  hashes of other DAG roots and any used script roots. Workers with the same code report the
  same hash, so the master knows which workers have which code.

**Trigger** (event trigger)
: A watcher belonging to a DAG that finds events and starts runs for new keys. See
  [Event triggers](../02-dags/19-triggers.md). A DAG can also use
  [`RunOf`](../02-dags/21-wait-for.md#starting-a-dag-after-another) to start runs after another DAG succeeds, or
  [`EventNamed`](../02-dags/22-events.md) for events posted by outside systems. A
  `@trigger(wait_for=...)` inspects each matching run or named event in a call before
  deciding whether to start a run.

**Event**
: A trigger's finding, or a successful run found by `RunOf`, identified by a key within its DAG,
  or an event posted by an outside system, with data and a time.
  A new key starts an event run.

**Bookmark**
: The last event key a trigger acted on, passed back as `after` when it starts again.

**Parse job**
: The master's request to one worker to import every DAG file of a root at one content hash
  and send back the DAGs as data (DAG JSON). The master has no Python, so it never reads DAG
  files itself. See [Parsing and code versions](06-parsing.md).

**Run** (DAG run)
: One execution of a DAG, for one moment (`planned_for`). Its kind says who created it
  (`scheduled`, `manual`, …). States: `queued` (waiting for `max_active_runs`), `running`,
  then `success`, `failed` or `cancelled`. A [yellow success](03-life-of-a-run.md#7-the-next-task-and-the-end) still has state `success` and asks for attention after a late cleanup failure. A run holds its place until its [resource cleanups](../02-dags/16-resources.md) end; a failed cleanup fails the run. Cancelling a run keeps those cleanups, while cancelling one as a task stops it.

**Task instance**
: One task of one run. States: `scheduled`, `queued`, `unschedulable`, `running`,
  `cleaning_up` (its process ended and its cleanup runs; it still holds the slot), `up_for_retry`, `success`, `failed`, `cancelled`, `skipped`,
  `upstream_failed`. An `unschedulable` resource user has not ended and holds its [resource cleanup](../02-dags/16-resources.md) until it ends. On success it has an **outcome**.

**Wait step**
: A step that waits for a value or times out. A [sensor](../02-dags/18-sensors.md) runs
  on a watcher; [`wait_for(RunOf(...))`](../02-dags/21-wait-for.md) waits for another
  DAG's run or task, and [`wait_for(EventNamed(...))`](../02-dags/22-events.md#waiting-in-a-run)
  waits for a posted event. Both are ended by the master. An `@sensor(wait_for=...)`
  waits there for a matching run or named event, then receives it in a watcher call.

**Outcome**
: Which kind of success a finished task had. A name (`SUCCESS`, `Rows`, `CHANGED`).
  Stored with the attempt's output. Shown on the run graph when it is something other
  than `SUCCESS`. [Routing by outcome](../02-dags/08-outcomes.md).

**Selection edge**
: An edge from a `@branch` task to one of its direct downstream tasks. Taken when the
  branch chose that target; otherwise it counts as skipped, like any edge not taken.

**first_of** and **at_least**
: Nodes the master ends when one, or a given number, of their options have succeeded.
  They pass the chosen values on and can also give their task IDs. See
  [Acting on the first of several](../02-dags/20-first-of.md).

**run_if** (trigger rule)
: When a task runs, from the states of its incoming edges. `RunIf.NONE_FAILED` (the
  default): runs when every edge that wasn't skipped succeeded and at least one did;
  skipped when every edge was skipped; `upstream_failed` as soon as one failed.
  `RunIf.ALL_SUCCEEDED`: runs only when every edge succeeded; any skip skips it.
  [The master](04-master.md#when-a-task-is-ready).

**Attempt** (try)
: One try of a task instance on one worker, with its own ID. Its try number counts every
  attempt; the retry budget counts only the attempts that used a retry. How it ended is its
  status: `success`, `failed`, `timed_out`, `oom_killed`, `cancelled`, `interrupted`,
  `lost` or `assignment_expired`. `start_method` (`spawn` or `fork`) is set when the worker's
  `Started` is stored, and the attempts API shows it. A forked Python task is `fork`: the
  worker sends a second `Started`, and the start time stays the first one's. A spawned
  process is `spawn`. Once the result is stored, the attempts API also shows `usage` and
  `core_dumped` (see [Workers](05-workers.md#the-fork-server)).

**Context channel**
: The Unix datagram socket one command attempt inherits (`SCHEDR_CTX_FD`). The command's
  processes each open a connection of their own and read the run's parameters and run
  information over it, until the attempt ends. See
  [Workers](05-workers.md#a-commands-context-channel) and
  [Reading the run from a command](../02-dags/06-context.md#reading-the-run-from-a-command).

**Fork server**
: A runner started as `python -m schedr.runner fork_server`. It imports libraries once and
  then starts a process per request. That process runs `task` or `parse` as a spawned runner
  does, with its own channel, log and process group. On Linux, a worker whose DAG interpreter
  has `fork_server = true` starts one or two when the worker starts, and forks Python tasks
  and parse jobs from them. A Python task can ask for the bare server only, or for a new
  process every time. A command stays a new process. See [Workers](05-workers.md#the-fork-server).

**Shim**
: The process a fork server puts between itself and one forked process. It reports the
  process ID once that process's group exists, then the exit and what the process used, and
  it holds the exited process until the status socket closes, so the ID is not reused. It is
  not in the process's group. See [Workers](05-workers.md#the-fork-server).

**Command**
: A request to change something (trigger a run, cancel a run or task, retry a task,
  pause or unpause a DAG, drain or resume a worker), stored in the database by the
  webserver. The master applies it, then marks it `applied` (with what it produced) or
  `rejected` (with the reason). The CLI waits for that decision.

**Round** (tick)
: One pass of the master's scheduling loop: commands, the scheduler, dispatching, parse-job
  timeouts, lost workers. The scheduler visits each busy DAG and the quiet DAGs woken that
  round. See [The master](04-master.md).

**Busy DAG**
: A DAG the leading master visits every round. It has a run running or queued, a fire time
  within the [wake-ahead time](14-timings.md#fixed-timings) (60 s), or its last active visit was less than
  the [quiet wait](14-timings.md#fixed-timings) (60 s) ago. See [The master](04-master.md#busy-and-quiet-dags).

**Quiet DAG**
: A DAG rounds skip until it wakes: nothing in progress, and no fire time that near. Its
  timer, a mark (a queued run, a run ending, a command, a new version), the sweep or the ID
  check wakes it. The list is the leading master's memory; the first round of a term visits
  every DAG. See [The master](04-master.md#busy-and-quiet-dags).

**Worker ID** and **incarnation**
: The worker ID is stable across restarts (by default the host name, which is the pod name in
  Kubernetes). The incarnation ID is new each time the agent process starts. It tells the
  master whether a worker reconnected (tasks still running) or restarted (its tasks are gone:
  those its journal doesn't report end `lost` once the old incarnation's lease is over).

**Lease**
: How long a worker may stay silent (the [worker lease](14-timings.md#fixed-timings), 60 s, counted from when its
  latest message arrived, or after a restart from the master's start plus the time a master
  it replaced may still answer, 20 s by default) before the master
  declares it `lost` and ends its attempts. Heartbeats renew it.

**Journal**
: A file per attempt on the worker's disk, holding attempts that are running and results the
  master hasn't acknowledged yet. It lets a restarted agent report results it already had,
  and stop the processes its previous incarnation left behind.

**Slot**
: Room for one task on a worker (`[worker] slots`, 2). The master never assigns a worker more
  tasks than it has slots. Parsing has its own slot.

**Queue**
: A name that routes tasks to a group of workers. A task names one (`default` unless set), and
  a worker serves the queues in `[worker] queues` (`default` when none are set).

**Drain**
: A worker taking no new work. On SIGTERM it lets running tasks finish within the grace
  period, then stops; `schedr workers drain` keeps it running and idle until resumed.

**Current code**
: For each root, the content hash the newest code has. Only workers at that hash (or newer)
  get the root's tasks. See [Parsing and code versions](06-parsing.md).

**Deployment**
: One schedr the web UI shows: a master, workers, a webserver and a database. `schedr ui`
  is given a list of them (`[[ui.deployments]]`: a name and the webserver's URL). Standalone
  is one, named `default`. See [The web UI](12-ui.md).
