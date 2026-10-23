# State and failures

## Where state lives

| State | Where | Survives |
|---|---|---|
| DAG versions, runs, task instances, attempts, results, commands, the worker registry, each root's current hash | the database | everything but losing the database |
| Connected workers, their slots and code, parse jobs | the master's memory | nothing; rebuilt as workers reconnect |
| Attempts running on a worker, results not yet acknowledged | the worker's journal (its disk) | agent restarts |
| Task logs | the worker's disk | agent restarts, until retention deletes them |

The database is the source of truth. The master can restart at any time and read everything
back, and workers fill in the rest when they reconnect.

## When something stops

| What happens | What schedr does |
|---|---|
| **The master restarts** | Workers keep running their tasks and reconnect with backoff. Each `Register` reports the worker's journal, and the master answers per attempt: *continue*, *kill* (it was ended meanwhile) or *acknowledge* (its result is stored). Every worker's lease starts again from zero, so the master's downtime never makes a worker lost. Schedules missed while it was down are created when it comes back (only the latest one when `catchup` is off). Commands sent meanwhile are applied in the first round. |
| **The webserver restarts** | Nothing else notices; the CLI's request fails, and a command it had stored is still applied. |
| **A worker's connection drops** | Its tasks keep running. The master marks it `disconnected`, and if it reconnects within `[master] lease_timeout` (60 s, same incarnation) nothing is lost. Otherwise a round marks it `lost` and ends its attempts `lost`, each using a retry. If it comes back after that, it is told to kill them. |
| **A worker's agent restarts** (crash, out of memory, new pod) | The new incarnation stops the process groups the old one left (they can't report any more) and reports the results it had. The master ends the attempts that were running `lost`, using a retry, with "worker restarted while the attempt ran (possibly out of memory)". |
| **A task uses too much memory** | Only that task is killed (`oom_killed`), before the kernel would kill the whole pod (see [Workers](05-workers.md)). |
| **A worker is drained** (SIGTERM) | Running tasks get the grace period; what still runs then is `interrupted` (SIGTERM, then SIGKILL `[worker] drain_terminate_period` later, still before the pod's deadline) and runs again elsewhere without using a retry. |
| **An assignment gets no `Started`** | After 10 s the master takes it back (`assignment_expired`) and queues the task again, without using a retry. |
| **The database returns errors or is down** | Each round fails at its first error and is logged; the next one tries again. A command is never rejected for an outage; it stays pending, and one cut off partway is applied again safely (see [The master](04-master.md#a-round-is-never-cut-short)). Messages the failed round had built are still sent. A database out for longer than the master's lease lets it renew pauses the master (no rounds; worker messages wait) until a renewal succeeds. The ops port still answers, and `/healthz` stays ok: an outage elsewhere mustn't restart the master in a loop. |
| **A second master starts on the same database** (a rollout overlap, `replicas: 2`) | It waits as a standby (not ready) while the first holds the lease, and takes over when the first stops, at once on a clean stop, or within `[master] lease_period` (30 s) after a crash. The database refuses every write of a master whose lease was taken over, however late it arrives, so two never both change it (see [The master](04-master.md#one-master-per-database-the-lease)). |
| **A master was taken over while still running** (paused too long behind a slow database, then another took the lease) | Its next write is refused; its next renewal finds the lease gone, and it exits (in standalone, the whole process: the API and the worker too, the worker's tasks interrupted). Restarted, it waits as a standby. |
| **The database goes silent mid-query** (a network partition) | schedr cuts the query off at `[database] query_timeout` (10 s; a write's transaction, a few of these), so the round fails rather than waits; on Postgres a write's statements are cut off in Postgres too, so nothing of it runs on holding locks. Renewals of the lease are limited the same way: the master pauses if it can't renew, and resumes when the database answers. |
| **`schedr db migrate` while a master runs** (by hand, or two migrations at once) | Refused: migrating takes the master's lease, and a master or another migration holds it. `--wait` waits for it instead (the chart's init container does, so it waits out the lease of a master that died). |

## Retries

The master decides retries. What uses a retry, and what doesn't:

| The attempt ended | Uses a retry? |
|---|---|
| `failed`, `timed_out`, `oom_killed`, `lost` | yes, while the task's `retries` last |
| `interrupted` (drain), `assignment_expired`, rejected | no: the task runs again |
| `cancelled` | never retried |
| failed with an error certain to repeat (e.g. inputs too large, a template that can't render) | never retried |

## Delivery

Every message between master and worker may arrive more than once. The attempt ID makes
repeats harmless: a second `Finished` is acknowledged and ignored, and an assignment the agent
already has is ignored. The master stores each assignment before sending it, and the agent
journals before it reports. So after any crash the side that restarts can find out what the
other already knows.
