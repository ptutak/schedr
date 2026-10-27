# Glossary

The names used across the documentation, the API and the logs.

**DAG**
: A set of tasks and the edges between them, defined in a Python file. Identified by its DAG
  ID. A DAG has **versions**: each parse that changes it stores a new one.

**DAG root**
: One directory of DAG files (an entry of `SCHEDR_DAG_PATH`), named after its last path
  component unless renamed. A DAG file is referenced as root and relative path, e.g.
  `etl:team_a/sync.py`, so references are the same on every worker.

**Content hash**
: A hash of every file in a root (paths and contents). Workers with the same code report the
  same hash, so the master knows which workers have which code.

**Parse job**
: The master's request to one worker to import every DAG file of a root at one content hash
  and send back the DAGs as data (DAG JSON). The master has no Python, so it never reads DAG
  files itself. See [Parsing and code versions](06-parsing.md).

**Run** (DAG run)
: One execution of a DAG, for one moment (`planned_for`). Its kind says who created it
  (`scheduled`, `manual`, …). States: `queued` (waiting for `max_active_runs`), `running`,
  then `success`, `failed` or `cancelled`.

**Task instance**
: One task of one run. States: `scheduled`, `queued`, `unschedulable`, `running`,
  `cleaning_up` (its process ended and its own cleanup runs; it still holds the slot; comes
  with cleanups), `up_for_retry`, `success`, `failed`, `cancelled`, `skipped`,
  `upstream_failed`.

**Attempt** (try)
: One try of a task instance on one worker, with its own ID. Its try number counts every
  attempt; the retry budget counts only the attempts that used a retry. How it ended is its
  status: `success`, `failed`, `timed_out`, `oom_killed`, `cancelled`, `interrupted`,
  `lost` or `assignment_expired`. `start_method` (`spawn` or `fork`) is set when the worker's
  `Started` is stored, and the attempts API shows it. A forked Python task is `fork`: the
  worker sends a second `Started`, and the start time stays the first one's. A spawned
  process is `spawn`.

**Fork server**
: A runner started as `python -m schedr.runner fork_server`. It imports libraries once and
  then starts a process per request. That process runs `task` or `parse` as a spawned runner
  does, with its own channel, log and process group. On Linux, a worker whose DAG interpreter
  has `fork_server = true` starts one or two when the worker starts, and forks Python tasks
  from them. Parse jobs stay spawned. See [Workers](05-workers.md#the-fork-server).

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
  within `[master] quiet_dag_wake_ahead`, or its last active visit was less than
  `[master] quiet_dag_after` ago. See [The master](04-master.md#busy-and-quiet-dags).

**Quiet DAG**
: A DAG rounds skip until it wakes: nothing in progress, and no fire time that near. Its
  timer, a mark (a queued run, a run ending, a command, a new version), the sweep or the ID
  check wakes it. The list is the leading master's memory; the first round of a term visits
  every DAG. See [The master](04-master.md#busy-and-quiet-dags).

**Worker ID** and **incarnation**
: The worker ID is stable across restarts (by default the host name, which is the pod name in
  Kubernetes). The incarnation ID is new each time the agent process starts. It tells the
  master whether a worker reconnected (tasks still running) or restarted (its tasks are gone).

**Lease**
: How long a worker may stay silent (`[master] lease_timeout`, 60 s, counted from when its
  latest message arrived, or from the master's start after a restart) before the master
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
