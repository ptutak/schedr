# Schedules

`trigger` decides which moments get a scheduled run. This file defines the same task on a
cron, a preset, an interval, a single fire time and no trigger. `bounds` is defined once
and called inside each block.

```{literalinclude} ../../examples/docs/schedules.py
:language: python
:caption: examples/docs/schedules.py
```

| `trigger` | When a run is for |
|---|---|
| `Cron("0 3 * * *", tz="Europe/Warsaw")` | 03:00 in that time zone. `tz` defaults to UTC. |
| `DAILY`, `HOURLY`, `WEEKLY`, `MONTHLY`, `YEARLY` | The presets below, in UTC. `.override(tz="Europe/Warsaw")` returns a new trigger; `DAILY` is unchanged. |
| `Every(timedelta(hours=1))` | A fixed interval of real time, UTC. Daylight-saving changes do not move it. |
| `Once("2026-12-24T23:00", tz="Europe/Warsaw")` | That one moment. |
| `Chained()` | Runs started by other DAGs' operators; see [Starting another DAG](23-chained-runs.md). |
| `None` | Manual runs only. |

Event triggers are [Sensors and triggers](../_generated/08-history/project/07-python-api-structure.md#sensors-and-triggers).

## Cron

Five fields: minute, hour, day of month, month, day of week. Six fields put seconds first.
A year field is rejected. Names and ranges and steps are the expression's, for example
`MON-FRI` and `*/15`.

When both the day of month and the day of week are restricted, a time matches if either
one matches (`0 0 1 * MON` is the 1st, and also every Monday). A `+` before the day of
week requires both (`0 0 1 * +MON` is a 1st that is a Monday).

| Preset | Fire time | Constant |
|---|---|---|
| `@hourly` | every hour, on the hour | `HOURLY` |
| `@daily`, `@midnight` | every day at midnight | `DAILY` is `@daily` |
| `@weekly` | Sunday at midnight | `WEEKLY` |
| `@monthly` | the 1st at midnight | `MONTHLY` |
| `@yearly`, `@annually` | 1 January at midnight | `YEARLY` |

The presets in a constant are UTC. `Cron("@daily", tz="Europe/Warsaw")` is midnight in
Warsaw.

`Every` measures from `start`, or from the Unix epoch when `start` is omitted, so
`Every(timedelta(hours=1))` fires on the hour. The interval is a positive `timedelta` of
whole milliseconds.

## `start`, `end` and `catchup`

`start` and `end` are a `datetime`, a `date` (midnight) or an ISO 8601 string. A naive
value is read in the trigger's time zone: the `tz` of a `Cron` or a `Once`, and UTC for
`Every` and for a DAG with no trigger. A value that already has an offset keeps it.
`start` after `end` is a `ValueError`. `Once` has neither, and no `catchup`.

Without `start`, scheduled runs begin at the moment the master first stored the DAG. The
last run is for the last fire time at or before `end`. `end` need not be a fire time.

| `catchup` | What the scheduler creates |
|---|---|
| `False` (the default) | Only the most recent fire time after the last scheduled run, from `start` up to now and not after `end`. A fire time that finds the DAG already at `max_active_runs` is missed. When a place frees, the fire time that is the most recent one then is created. |
| `True` | Every such fire time, earliest first. One round creates at most 10 for a DAG; the rest wait for later rounds. `max_active_runs` still caps how many run at once, and the ones that do not fit wait. |

The scheduler skips a fire time that already has a successful, queued or running run of
any kind. With `catchup`, it continues to later fire times. If a covering run fails later,
rerun that period by hand. [Backfill](#backfill) fills a past range on demand; `catchup`
is the scheduler creating its own missed runs.

`schedr dags test` is a manual run, so `start`, `end` and a `Once` in the future do not
stop it. The rules for the run's window are below.

## Run window

A run covers `(since, planned_for]`. `planned_for` is the fire time for a scheduled run,
and `--for` or now for a manual run (`schedr runs trigger`, `schedr dags test`).

| The run | `since` |
|---|---|
| On a fire time, and that fire time is the first at or after `start` (or the moment the DAG was first stored), and `initial_since` is set | `initial_since` |
| On a fire time otherwise | The previous fire time. If there is none, `initial_since`. If that is unset too, `planned_for`: the window is empty. |
| Manual, and not a fire time | The nearest earlier run of any kind, or else `initial_since`, or else the previous fire time, or else `planned_for`. |

`dags test` uses a fresh database, so there is no earlier run to measure from.

Passing `dag_ctx.run.since` into a task or a command is checked at import for a `Once` and
for a DAG with no trigger. Without `initial_since` the import fails:
`` DAG `manual_only` has no trigger, so its first run has no previous fire time, but task `bounds` uses dag_ctx.run.since: set initial_since= ``.
A `Cron` or an `Every` is not refused. Reading `dag_ctx.run.since` inside the function
body is not seen at import; at run time the window can still be empty. `initial_since` is
read the same way as `start`.

`--for` more than a minute ahead of the master's clock is rejected. Without an offset it
is UTC, and the command warns `warning: no offset given: interpreted as ... (UTC)`. A date
is midnight UTC. `2026-10-15T03:00+02:00` keeps the offset.

## How many runs at once

`max_active_runs` defaults to 1. `None` means no limit. `0` is a `ValueError`.

A manual run at the limit is stored as `queued` and starts when a place frees. A scheduled
run that is due takes a freed place before a queued one. Runs already going are left to
finish.

## Pausing

A new DAG is stored paused, and so is a DAG that was removed and comes back.
`schedr dags unpause <id>` lets the scheduler create runs. `schedr dags pause <id>` stops
only that: fire times pass with no scheduled run, runs already going finish, and a manual
run still starts. A paused `Chained` DAG refuses operators until unpaused. For
`trigger=None`, the UI shows neither Pause nor Unpause: it has only manual runs.

After unpause, `catchup` applies to the fire times that passed. With it off, only the
latest one is created. With it on, the missed ones are created in order.

## Backfill

A backfill creates a `backfill` run for each fire time in a past range, each covering one
schedule period. `--from` and `--to` are inclusive; `--to` defaults to now, and a later
value ends at now. `start` and `end` do not limit the range. The trigger must have fire
times: `Cron` (including its presets), `Every` or `Once`. `None`, `Chained()`, event
triggers, `RunOf` and `EventNamed` have none, so a backfill is refused.

By default, a fire time with a successful, queued or running run of any kind is skipped.
`--all` creates a run for every fire time. Runs start oldest first, after the other kinds
of runs, within `max_active_runs`. They start even when the DAG is paused; a held DAG
refuses the backfill.

```text
$ schedr runs backfill report --from 2026-10-01T00:00:00Z --to 2026-10-03T00:00:00Z
backfill 01900000-0000-7000-8000-000000000001 of report: 3 fire time(s), 1 skipped, 2 run(s) created
```

This example assumes a daily midnight schedule and one covered fire time. Use `--dry-run`
to count without creating anything. Supply `-p key=value` (or `-p key:=json`) or
`--params` with a JSON object for every run; the parameters are checked once before the
runs are created. `[backfill] max_runs` limits the number of fire times in the range,
including skipped ones, to 1000 by default. Split a bigger range.

`schedr runs cancel --backfill <id>` cancels its queued and running runs; finished runs
stay as they are.

## Daylight saving

A cron is matched against the wall clock in its time zone, then turned into a real moment.

| The wall-clock time | The fire time |
|---|---|
| It falls in a gap (the clock springs forward and the time does not exist) | The first moment after the gap. Several matches in the same gap become that one moment. |
| It falls in an overlap and the hour is a fixed number (`30 2 * * *`) | Once, at the first occurrence. |
| It falls in an overlap and the hour is a wildcard, a step, a range or a list (`*/15 * * * *`) | Once at each occurrence. |
| `Every` | Unchanged. The interval is real time. |

`description` and `tags` are stored on the DAG and returned with it. `tags` is a list of
strings; a bare string is a `TypeError`.
