# Resources

A resource is one thing with two parts: a setup and a cleanup. Both always exist. Use it
for something created once and shared by tasks, such as a cluster for a run.

```{literalinclude} ../../examples/docs/resources.py
:language: python
:caption: examples/docs/resources.py
```

## Setup and cleanup

`@resource` marks a setup function, called like a task. It can also take options, as in
`@resource(queue="compute")`; its options are those of `@task`. The return value is the
resource's value. `@cluster.cleanup` marks its cleanup, which takes that value as its one
parameter. If the setup returns `None`, the cleanup takes no parameter. The decorator can
also take options, as in `@cluster.cleanup(retries=5)`.

The cleanup inherits the setup's secrets and connections and can declare more of its own.
Its queue defaults to the setup's queue and can be overridden. It also accepts `timeout`,
`retries`, `retry_delay`, `blocking` and `unschedulable_timeout`.

Without a declared cleanup, the resource still has an empty cleanup. The master
finishes it without an attempt or log, with the reason "no cleanup declared: nothing to clean".
A resource has one cleanup wherever it is imported; declaring another from a different place is an
error. A different cleanup needs a different resource.

For a resource without setup code, create `tmp = Resource("tmp-prefix")` inside the DAG.
Its setup is empty and has no value, so its cleanup takes no parameter and its users name it
with `uses=[tmp]`. Attach the cleanup with `@tmp.cleanup`, bare or with options.

A setup can be called only once per DAG. It cannot use `.expand()` or return an outcome
union; these are parse errors.

## A command setup

Inside `with Dag(...)`, `CommandResource(name, command, value=T, ...)` adds a resource
whose setup is a command. As with `CommandOperator`, the command is a shell string or an
argument list, and can take `env=`, `queue=`, `retries=`, secrets and connections.
This example reports a fixed cluster value and prints its cleanup, so it needs no cluster
service:

```{literalinclude} ../../examples/docs/command_resources.py
:language: python
:caption: examples/docs/command_resources.py
```

The setup reports its value with `schedr task-ctx resource set`. Give an object's fields
as `key=value` for text or `key:=json` for JSON: `size=8` is text, while `size:=8` is a
number. Alternatively, `--json` takes the whole value, including a scalar such as
`schedr task-ctx resource set --json 7` for `value=int`. Use fields or `--json`, not both.

The agent checks the value against `value=` immediately, without coercing text to numbers
or other types. A dataclass must give **every field**, including fields with defaults:
no Python constructor fills them in before a command reads the value. Pydantic model
validation does not run in the agent; use a dataclass for these checks at report time.
Report once per setup attempt; a second report is refused. `resource set` works only in
a `CommandResource` setup.

With `value=`, a successful setup must both report its value and exit 0. Exiting 0 without
reporting fails with "resource value not reported" (`resource_not_reported`). The setup's
output is the reported value, rather than a `ProcessResult`. Its users take `cluster.value`,
as `load` does above, or declare `uses=[cluster]` when they need no value. Without `value=`,
the resource has no value and its setup does not report one.

For a union value, give the type twice for mypy:
`CommandResource[int | None]("number", command, value=int | None)`. To report the `None`
case, use `schedr task-ctx resource set --json null`; it still needs a report.

## A command cleanup

Any resource can use `<resource>.cleanup_command([...])` instead of a Python cleanup:
an `@resource` function, a `CommandResource`, or a `Resource` without setup code. The
method takes a shell string or argument list, `env=`, and the same cleanup options as
`@cluster.cleanup(...)`, including `blocking=`. A resource still has only one cleanup.

While that cleanup runs, `schedr task-ctx resource get` reads the setup's value as JSON;
`schedr task-ctx resource get --field cluster_id` reads one top-level field. A string
field prints as text, other fields as JSON, each followed by a newline. Dates and times
are ISO 8601 text, durations are ISO 8601 durations, and tuples are lists. `resource get`
works only in a resource's command cleanup; a resource without a value returns `null`.

The command line and `env` can also take t-strings, filled before the cleanup starts.
For a `CommandResource`, `t"{cluster.cluster_id}"` and `t"{cluster.value.cluster_id}"`
refer to the same field. For an `@resource` setup, use `t"{cluster.value.cluster_id}"`:
the reference is resolved when that resource is set up in the DAG. The example uses
both forms, one in an argument and the other in `env`.

## Users

Passing the setup's value to a task makes that task a user. When a task needs the resource
but not its value, declare `uses=[cluster]` or `uses=[made]`, where `made` is the setup's
output. `uses=` is also available on operators and groups; a group's tasks inherit its
`uses` unless they name their own. A resource must be set up in the DAG that uses it.

Users depend on the setup. A requirement such as `uses=` can stop a user when setup fails,
is skipped or is cancelled; it does not by itself make a user run. A resource with no users
gets a parse hint.

Once every user has ended, the cleanup can start and receives the setup's value. For a mapped
user, every instance must have ended. An `unschedulable` user has not ended, so the cleanup
waits for it. If setup failed, was skipped or was cancelled, its cleanup does not run.

To bound the wait for an `unschedulable` user, for example:

```python
from datetime import timedelta

@cluster.cleanup(unschedulable_timeout=timedelta(hours=1))
def remove_cluster(cluster_id: str) -> None:
    ...
```

After the limit, the user fails with a reason such as "no worker within 1h: it uses a
resource whose `unschedulable_timeout` ran out". Its downstream tasks can become
`upstream_failed`, and cleanup can proceed once all users have ended. The default is no
limit; zero fails the user at once, and the shortest limit applies when it uses several
resources.

## Groups and cleanup

For a resource set up in a group, `g >> after` also makes `after` wait for its cleanup. The exception is when
`after`, or a task downstream of it, still uses that resource: waiting for cleanup would form
a cycle. If that cleanup fails, `after` becomes `upstream_failed`. Set
`@cluster.cleanup(blocking=False)` to let what follows the group proceed without waiting for
that cleanup; if it fails, the run still fails, but `after` isn't stopped. A resource outside
any group holds nothing back this way.

A resource cleanup is shared by all its users. A [per-task cleanup](15-cleanup.md) instead
cleans up after each started task attempt; a resource setup can have one of those too, for
work left behind if setup fails partway through.

A connection is a generator because it opens and closes in one process, inside one task.
A resource has separate setup and cleanup tasks, in separate processes, maybe hours apart:
only the setup's value reaches the cleanup.

## Scheduling and cancellation

Ready resource cleanups go before other queued tasks, and `max_active_tasks`, `max_active`
and `max_active_per_run` do not hold them back.
The run keeps its `max_active_runs` place until its cleanups end, even with
`blocking=False`.

Cancelling a run still runs the cleanups of resources that were set up, including their
retries. The run ends `cancelled`, unless a cleanup or another task fails, in which case it
ends `failed`. `schedr tasks cancel` refuses to cancel a resource cleanup. A cleanup that
fails after its retries fails the run.

## Seeing and testing resources

The run's Resources table shows each resource's state, setup, cleanup and users. In the
[graph](../03-guide/12-ui.md#the-graph), setup and cleanup are caps joined by a rail,
with lines to their users; the task panel's Cleanup tab links the two parts. Use
[`run_dag`](07-testing.md#run_dag) to assert a resource's state.
