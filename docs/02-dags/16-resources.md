# Resources

A resource is one thing with two parts: a setup and a cleanup. Both always exist. Use it
for something created once and shared by tasks, such as a cluster for a run.

```{note}
This release parses resources, but the master does not run them as resources yet: it does
not hold the cleanup until the users end. That behavior lands in the next step, M4.8c2.
```

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
`retries`, `retry_delay` and `blocking`.

Without a `@cluster.cleanup` function, the resource still has an empty cleanup. A resource
has one cleanup wherever it is imported; declaring another from a different place is an
error. A different cleanup needs a different resource.

For a resource without setup code, create `tmp = Resource("tmp-prefix")` inside the DAG.
Its setup is empty and has no value, so its cleanup takes no parameter and its users name it
with `uses=[tmp]`. Attach the cleanup with `@tmp.cleanup`, bare or with options.

A setup can be called only once per DAG. It cannot use `.expand()` or return an outcome
union; these are parse errors.

## Users

Passing the setup's value to a task makes that task a user. When a task needs the resource
but not its value, declare `uses=[cluster]` or `uses=[made]`, where `made` is the setup's
output. `uses=` is also available on operators and groups; a group's tasks inherit its
`uses` unless they name their own. A resource must be set up in the DAG that uses it.

Users depend on the setup. The `uses=` dependency can stop a user when setup fails or is
skipped; it does not by itself make a user run. A resource with no users gets a parse hint.

## Groups and cleanup

The intended resource lifecycle runs cleanup once all users are done. For a resource set up
in a group, `g >> after` also makes `after` wait for its cleanup. The exception is when
`after`, or a task downstream of it, still uses that resource: waiting for cleanup would form
a cycle. If that cleanup fails, `after` becomes `upstream_failed`. Set
`@cluster.cleanup(blocking=False)` to let what follows the group proceed without waiting for
that cleanup; if it fails, the run still fails, but `after` isn't stopped. A resource outside
any group holds nothing back this way.

A resource cleanup is shared by all its users. A [per-task cleanup](15-cleanup.md) instead
cleans up after each started task attempt; a resource setup can have one of those too, for
work left behind if setup fails partway through.

A connection is a generator because it opens and closes in one process, inside one task. A
resource is two functions because its setup and cleanup are separate tasks, in separate
processes, maybe hours apart: only the setup's value reaches the cleanup.
