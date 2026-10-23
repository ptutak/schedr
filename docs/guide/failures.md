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
| **A task uses too much memory** | Only that task is killed (`oom_killed`), before the kernel would kill the whole pod (see [Workers](workers.md)). |
| **A worker is drained** (SIGTERM) | Running tasks get the grace period; what still runs then is `interrupted` and runs again elsewhere without using a retry. A task slow to exit on SIGTERM can outlive the pod and end `lost` instead (B8). |
| **An assignment gets no `Started`** | After 10 s the master takes it back (`assignment_expired`) and queues the task again, without using a retry. |
| **The database returns errors or is down** | Each round fails at its first error and is logged; the next one tries again. A command is never rejected for an outage; it stays pending. Messages the failed round had built are dropped: assignments expire and are made again, but a cancel waits for the worker to reconnect (B7). The ops port still answers, and `/healthz` stays ok: an outage elsewhere mustn't restart the master in a loop. |
| **The database goes silent mid-query** (a network partition) | The round waits, the loop stops beating, and after `[ops] loop_stall_timeout` (2 min) `/healthz` fails and Kubernetes restarts the master. This is the clean recovery: a restart reads everything back, as above. |

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

```{note}
Known gaps, in `BUGS.md`: the master applies a command and records its decision in two
separate writes, so a crash or a database error between them makes the next round apply the
command again, e.g. a second manual run, and an undelivered cancel waits for the worker to
reconnect (B7). A drained task that ignores SIGTERM is killed only after `[master]
cancel_grace`, possibly after the pod is gone (B8). Both fixes are planned.
```
