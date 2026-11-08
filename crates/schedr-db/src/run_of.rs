//! What a `wait_for(RunOf(...))` step reads of another DAG's runs (decision 353).

use jiff::Timestamp;

use crate::database::{Database, DbError};
use crate::query::{db_time, in_tx, on_pool, parse_time, sql};

/// A run of the DAG waited for, as far as the step needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtherRun {
    /// The run's ID.
    pub run_id: String,
    /// The run's state, or its task's when the step waits for a task (`queued` for a run whose
    /// tasks aren't made yet, `missing` for a run that ended without that task).
    pub state: String,
    /// When it ended, if it did.
    pub ended_at: Option<Timestamp>,
}

/// Which runs of the other DAG count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Matching {
    /// The runs planned for this moment (`lag` given).
    PlannedFor(Timestamp),
    /// The first to end at or after this moment (no `lag`).
    EndedAfter(Timestamp),
    /// The first to end strictly after this run, which ended at this moment (an `@sensor`'s
    /// next after the one it returned `NOT_YET` for).
    EndedAfterRun(Timestamp, String),
}

/// Where an `@sensor(wait_for=RunOf(...))` stands (decision 353).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SensorWait {
    /// Since when it waits: its `max_wait` runs from here.
    pub since: Timestamp,
    /// The run last handed to its function, and when it (or its task) ended.
    pub handed: Option<(String, Timestamp)>,
    /// The last run it returned `NOT_YET` for, and when that run (or its task) ended.
    pub waited: Option<(String, Timestamp)>,
}

impl Database {
    /// The run of `dag` that `matching` picks (the newest planned for the moment, or the first to
    /// end after it), with its state, or `task`'s state in it; none yet: empty.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails.
    pub async fn runs_of(
        &self,
        dag: &str,
        task: Option<&str>,
        matching: Matching,
    ) -> Result<Vec<OtherRun>, DbError> {
        // Of the runs planned for the moment only the newest counts (the user's, option C): a
        // run started again for it replaces the older ones, as Airflow's clear would.
        const RUNS_FOR: &str = "SELECT id{text}, state, {ts<}ended_at{>ts} FROM runs \
            WHERE dag_id = $1 AND planned_for = $2{ts} ORDER BY triggered_at DESC, id DESC LIMIT 1";
        // A run whose tasks aren't made yet (queued) still counts: a rerun on its way.
        const TASKS_FOR: &str = "SELECT r.id{text}, COALESCE(t.state, CASE WHEN r.state IN \
            ('queued', 'running') THEN 'queued' ELSE 'missing' END), {ts<}t.ended_at{>ts} \
            FROM runs r LEFT JOIN task_instances t \
            ON t.run_id = r.id AND t.task_id = $3 AND t.map_index = -1 \
            WHERE r.dag_id = $1 AND r.planned_for = $2{ts} \
            ORDER BY r.triggered_at DESC, r.id DESC LIMIT 1";
        const RUN_ENDED: &str = "SELECT id{text}, state, {ts<}ended_at{>ts} FROM runs \
            WHERE dag_id = $1 AND ended_at >= $2{ts} ORDER BY ended_at, id LIMIT 1";
        const TASK_ENDED: &str = "SELECT r.id{text}, t.state, {ts<}t.ended_at{>ts} \
            FROM task_instances t JOIN runs r ON r.id = t.run_id \
            WHERE t.task_id = $3 AND t.map_index = -1 AND t.ended_at >= $2{ts} AND r.dag_id = $1 \
            ORDER BY t.ended_at, r.id LIMIT 1";
        const RUN_AFTER: &str = "SELECT id{text}, state, {ts<}ended_at{>ts} FROM runs \
            WHERE dag_id = $1 AND ended_at >= $2{ts} AND (ended_at > $2{ts} OR id > $4{uuid}) \
            ORDER BY ended_at, id LIMIT 1";
        const TASK_AFTER: &str = "SELECT r.id{text}, t.state, {ts<}t.ended_at{>ts} \
            FROM task_instances t JOIN runs r ON r.id = t.run_id \
            WHERE t.task_id = $3 AND t.map_index = -1 AND t.ended_at >= $2{ts} AND r.dag_id = $1 \
            AND (t.ended_at > $2{ts} OR r.id > $4{uuid}) ORDER BY t.ended_at, r.id LIMIT 1";
        let (query, at, after) = match (matching, task.is_some()) {
            (Matching::PlannedFor(at), false) => (RUNS_FOR, at, None),
            (Matching::PlannedFor(at), true) => (TASKS_FOR, at, None),
            (Matching::EndedAfter(at), false) => (RUN_ENDED, at, None),
            (Matching::EndedAfter(at), true) => (TASK_ENDED, at, None),
            (Matching::EndedAfterRun(at, run), false) => (RUN_AFTER, at, Some(run)),
            (Matching::EndedAfterRun(at, run), true) => (TASK_AFTER, at, Some(run)),
        };
        let rows: Vec<(String, String, Option<String>)> = on_pool!(self, |pool, backend| {
            let mut statement = sqlx::query_as(sql(backend, query))
                .bind(dag)
                .bind(db_time(at))
                .bind(task.unwrap_or_default());
            if let Some(after) = &after {
                statement = statement.bind(after);
            }
            statement.fetch_all(pool).await?
        });
        rows.into_iter()
            .map(|(run_id, state, ended_at)| {
                Ok(OtherRun {
                    run_id,
                    state,
                    ended_at: ended_at.map(|at| parse_time("ended_at", &at)).transpose()?,
                })
            })
            .collect()
    }

    /// Since when a queued task instance is ready, if it is.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the stored time doesn't read.
    pub async fn ready_since(&self, instance_id: &str) -> Result<Option<Timestamp>, DbError> {
        const SINCE: &str = "SELECT {ts<}ready_at{>ts} FROM task_instances WHERE id = $1{uuid} AND ready_at IS NOT NULL";
        let row: Option<(String,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, SINCE))
                .bind(instance_id)
                .fetch_optional(pool)
                .await?
        });
        row.map(|(at,)| parse_time("task_instances.ready_at", &at))
            .transpose()
    }

    /// The runs of `dag` (or of its `task`) that succeeded after `after` (or, without it, since
    /// `since`), oldest first, to `limit`, that haven't started a run of `started` yet
    /// (decision 353): with each its `planned_for` and when it (or its task) ended. `after` is
    /// strict, by end and run ID, so a run is never read twice, events kept or not.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored time doesn't read.
    pub async fn succeeded_since(
        &self,
        of: (&str, Option<&str>),
        started: &str,
        from: RunOfFrom<'_>,
        limit: u32,
    ) -> Result<Vec<(String, Timestamp, Timestamp)>, DbError> {
        self.runs_since(of, started, from, limit, false).await
    }

    /// As [`Self::succeeded_since`], every run that ended, whatever its state: what an
    /// `@trigger(wait_for=...)`'s function is called for.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored time doesn't read.
    pub async fn ended_since(
        &self,
        of: (&str, Option<&str>),
        from: RunOfFrom<'_>,
        limit: u32,
    ) -> Result<Vec<(String, Timestamp, Timestamp)>, DbError> {
        self.runs_since(of, "", from, limit, true).await
    }

    async fn runs_since(
        &self,
        (dag, task): (&str, Option<&str>),
        started: &str,
        from: RunOfFrom<'_>,
        limit: u32,
        any_end: bool,
    ) -> Result<Vec<(String, Timestamp, Timestamp)>, DbError> {
        const RUNS_ENDED: &str = "SELECT r.id{text}, {ts<}r.planned_for{>ts}, {ts<}r.ended_at{>ts} FROM runs r \
            WHERE r.dag_id = $1 AND r.state IN ('success', 'failed', 'cancelled') AND r.ended_at >= $2{ts} \
            AND (r.ended_at > $2{ts} OR $4 IS NULL OR r.id > $4{uuid}) \
            ORDER BY r.ended_at, r.id LIMIT $3";
        const TASKS_ENDED: &str = "SELECT r.id{text}, {ts<}r.planned_for{>ts}, {ts<}t.ended_at{>ts} \
            FROM task_instances t JOIN runs r ON r.id = t.run_id \
            WHERE t.task_id = $5 AND t.map_index = -1 \
            AND t.state IN ('success', 'failed', 'cancelled', 'skipped', 'upstream_failed') \
            AND t.ended_at >= $2{ts} AND r.dag_id = $1 \
            AND (t.ended_at > $2{ts} OR $4 IS NULL OR r.id > $4{uuid}) \
            ORDER BY t.ended_at, r.id LIMIT $3";
        const RUNS: &str = "SELECT r.id{text}, {ts<}r.planned_for{>ts}, {ts<}r.ended_at{>ts} FROM runs r \
            WHERE r.dag_id = $1 AND r.state = 'success' AND r.ended_at >= $2{ts} \
            AND (r.ended_at > $2{ts} OR $5 IS NULL OR r.id > $5{uuid}) \
            AND NOT EXISTS (SELECT 1 FROM events e WHERE e.dag_id = $3 AND e.key = r.id{text}) \
            ORDER BY r.ended_at, r.id LIMIT $4";
        const TASKS: &str = "SELECT r.id{text}, {ts<}r.planned_for{>ts}, {ts<}t.ended_at{>ts} \
            FROM task_instances t JOIN runs r ON r.id = t.run_id \
            WHERE t.task_id = $6 AND t.map_index = -1 AND t.state = 'success' \
            AND t.ended_at >= $2{ts} AND r.dag_id = $1 \
            AND (t.ended_at > $2{ts} OR $5 IS NULL OR r.id > $5{uuid}) \
            AND NOT EXISTS (SELECT 1 FROM events e WHERE e.dag_id = $3 AND e.key = r.id{text}) \
            ORDER BY t.ended_at, r.id LIMIT $4";
        let (since, after_run) = match from {
            RunOfFrom::Since(at) => (at, None),
            RunOfFrom::After(seen) => (seen.at, Some(seen.run.as_str())),
        };
        let rows: Vec<(String, String, String)> = on_pool!(self, |pool, backend| {
            // What ended for a function is never deduplicated by its events: it may start none,
            // or key them its own way; its cursor alone says what it has had.
            let query = match (task.is_some(), any_end) {
                (true, false) => TASKS,
                (false, false) => RUNS,
                (true, true) => TASKS_ENDED,
                (false, true) => RUNS_ENDED,
            };
            let mut statement = sqlx::query_as(sql(backend, query))
                .bind(dag)
                .bind(db_time(since));
            if !any_end {
                statement = statement.bind(started);
            }
            statement = statement.bind(i64::from(limit)).bind(after_run);
            if let Some(task) = task {
                statement = statement.bind(task);
            }
            statement.fetch_all(pool).await?
        });
        rows.into_iter()
            .map(|(id, at, ended)| {
                Ok::<_, DbError>((
                    id,
                    parse_time("runs.planned_for", &at)?,
                    parse_time("ended_at", &ended)?,
                ))
            })
            .collect()
    }

    /// Since when a DAG has been live: unpaused and defined without a break (decision 353).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the stored time doesn't read.
    pub async fn live_since(&self, dag: &str) -> Result<Option<Timestamp>, DbError> {
        const SINCE: &str =
            "SELECT {ts<}live_since{>ts} FROM dags WHERE dag_id = $1 AND live_since IS NOT NULL";
        let row: Option<(String,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, SINCE))
                .bind(dag)
                .fetch_optional(pool)
                .await?
        });
        row.map(|(at,)| parse_time("dags.live_since", &at))
            .transpose()
    }

    /// How far a `trigger=RunOf(...)` reading `source` has looked: the last run it started one
    /// for. A cursor of another source (the trigger changed) counts as none.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the stored time doesn't read.
    pub async fn run_of_seen(&self, dag: &str, source: &str) -> Result<Option<RunOfSeen>, DbError> {
        const SEEN: &str = "SELECT {ts<}run_of_seen_at{>ts}, run_of_seen_run FROM triggers \
            WHERE dag_id = $1 AND run_of_source = $2 AND run_of_seen_at IS NOT NULL \
            AND run_of_seen_run IS NOT NULL";
        let row: Option<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, SEEN))
                .bind(dag)
                .bind(source)
                .fetch_optional(pool)
                .await?
        });
        row.map(|(at, run)| {
            Ok(RunOfSeen {
                at: parse_time("triggers.run_of_seen_at", &at)?,
                run,
            })
        })
        .transpose()
    }

    /// Records the last run a `trigger=RunOf(...)` reading `source` started one for, on its
    /// `triggers` row.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn set_run_of_seen(
        &self,
        dag: &str,
        source: &str,
        seen: &RunOfSeen,
    ) -> Result<(), DbError> {
        const SET: &str = "INSERT INTO triggers \
            (dag_id, failures, run_of_source, run_of_seen_at, run_of_seen_run) \
            VALUES ($1, 0, $2, $3{ts}, $4) ON CONFLICT (dag_id) DO UPDATE SET \
            run_of_source = excluded.run_of_source, run_of_seen_at = excluded.run_of_seen_at, \
            run_of_seen_run = excluded.run_of_seen_run";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, SET))
                .bind(dag)
                .bind(source)
                .bind(db_time(seen.at))
                .bind(&seen.run)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }
}

/// The last run a `trigger=RunOf(...)` started one for: when it (or its task) ended, and its ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOfSeen {
    /// When it (or its task) ended.
    pub at: Timestamp,
    /// Its run ID.
    pub run: String,
}

/// Where a pass of `trigger=RunOf(...)` reads from.
#[derive(Debug, Clone, Copy)]
pub enum RunOfFrom<'a> {
    /// From this moment on, inclusive: when the DAG became live.
    Since(Timestamp),
    /// Strictly after this run.
    After(&'a RunOfSeen),
}

impl Database {
    /// Where an `@sensor(wait_for=RunOf(...))` instance stands, its waiting starting `now` if
    /// it hadn't (decision 353).
    ///
    /// # Errors
    ///
    /// [`DbError`] if a statement fails or a stored time doesn't read.
    pub async fn sensor_wait(
        &self,
        instance_id: &str,
        now: Timestamp,
    ) -> Result<Option<SensorWait>, DbError> {
        // From when it was queued, not when a round first looked (a round's write may have
        // failed); written once, then only read (Grok's review of M4.11d2).
        const START: &str = "UPDATE task_instances \
            SET waiting_since = COALESCE(ready_at, $2{ts}) \
            WHERE id = $1{uuid} AND waiting_since IS NULL";
        const READ: &str = "SELECT {ts<}waiting_since{>ts}, handed_run, {ts<}handed_at{>ts}, \
            waited_run, {ts<}waited_at{>ts} FROM task_instances \
            WHERE id = $1{uuid} AND waiting_since IS NOT NULL";
        type Row = (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let read: Option<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, READ))
                .bind(instance_id)
                .fetch_optional(pool)
                .await?
        });
        let row = if let Some(row) = read {
            Some(row)
        } else {
            in_tx!(self, |tx, backend| {
                sqlx::query(sql(backend, START))
                    .bind(instance_id)
                    .bind(db_time(now))
                    .execute(&mut *tx)
                    .await?;
                sqlx::query_as(sql(backend, READ))
                    .bind(instance_id)
                    .fetch_optional(&mut *tx)
                    .await?
            })
        };
        let pair = |run: Option<String>, at: Option<String>, what| match (run, at) {
            (Some(run), Some(at)) => parse_time(what, &at).map(|at| Some((run, at))),
            _ => Ok(None),
        };
        row.map(|(since, handed, handed_at, waited, waited_at)| {
            let handed = pair(handed, handed_at, "task_instances.handed_at")?;
            let waited = pair(waited, waited_at, "task_instances.waited_at")?;
            Ok(SensorWait {
                since: parse_time("task_instances.waiting_since", &since)?,
                handed,
                waited,
            })
        })
        .transpose()
    }

    /// Records the run handed to an `@sensor(wait_for=...)`'s function as it is assigned, and
    /// when it (or its task) ended.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn hand_run(
        &self,
        instance_id: &str,
        run_id: &str,
        ended_at: Timestamp,
    ) -> Result<(), DbError> {
        const HAND: &str = "UPDATE task_instances SET handed_run = $2, handed_at = $3{ts} \
            WHERE id = $1{uuid}";
        // Fenced: a master that lost its lease doesn't overwrite what the leader handed.
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, HAND))
                .bind(instance_id)
                .bind(run_id)
                .bind(db_time(ended_at))
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Records that the function returned `NOT_YET` for the run it was handed: the next call is
    /// for a later one.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn not_yet(&self, instance_id: &str) -> Result<(), DbError> {
        const SEEN: &str = "UPDATE task_instances SET waited_run = handed_run, waited_at = handed_at \
             WHERE id = $1{uuid}";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, SEEN))
                .bind(instance_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// What an `@sensor(wait_for=...)`'s function is given of `run_id` (its `RunResult`):
    /// the run's ID, DAG, `planned_for`, state and end, and with `task` that task's state,
    /// outcome and output (a mapped task's, the list of its elements'). `None` for a run gone.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a query fails or what is stored doesn't read.
    pub async fn run_result(
        &self,
        run_id: &str,
        task: Option<&schedr_model::TaskId>,
    ) -> Result<Option<serde_json::Value>, DbError> {
        const RUN: &str = "SELECT dag_id, {ts<}planned_for{>ts}, state, {ts<}ended_at{>ts} \
            FROM runs WHERE id = $1{uuid}";
        const TASK: &str = "SELECT state, outcome, {ts<}ended_at{>ts} FROM task_instances \
            WHERE run_id = $1{uuid} AND task_id = $2 AND map_index = -1";
        let run: Option<(String, String, String, Option<String>)> =
            on_pool!(self, |pool, backend| {
                sqlx::query_as(sql(backend, RUN))
                    .bind(run_id)
                    .fetch_optional(pool)
                    .await?
            });
        let Some((dag_id, planned_for, state, ended_at)) = run else {
            return Ok(None);
        };
        // Tagged as the Python API stores a `datetime` (`{"$datetime": ...}`), to decode as one.
        let time = |what, at: &str| {
            parse_time(what, at).map(|t| serde_json::json!({"$datetime": t.to_string()}))
        };
        let mut result = serde_json::json!({
            "run_id": run_id,
            "dag_id": dag_id,
            "planned_for": time("runs.planned_for", &planned_for)?,
            "state": state,
            "ended_at": ended_at.as_deref().map(|at| time("runs.ended_at", at)).transpose()?,
        });
        let Some(task) = task else {
            return Ok(Some(result));
        };
        let found: Option<(String, Option<String>, Option<String>)> =
            on_pool!(self, |pool, backend| {
                sqlx::query_as(sql(backend, TASK))
                    .bind(run_id)
                    .bind(task.as_str())
                    .fetch_optional(pool)
                    .await?
            });
        // A mapped task's values are its elements', in order: the list a downstream reads.
        let mut output = self.outputs_of(run_id, &[task]).await?.remove(task);
        if output.is_none() {
            let elements = self.instance_results(run_id, task).await?;
            output = (!elements.is_empty()).then(|| {
                serde_json::Value::Array(
                    elements
                        .into_iter()
                        .map(|e| e.output.unwrap_or(serde_json::Value::Null))
                        .collect(),
                )
            });
        }
        result["task_id"] = serde_json::json!(task.as_str());
        result["task_state"] = serde_json::json!(found.as_ref().map(|(state, ..)| state));
        result["outcome"] =
            serde_json::json!(found.as_ref().and_then(|(_, outcome, _)| outcome.as_ref()));
        result["output"] = output.unwrap_or(serde_json::Value::Null);
        Ok(Some(result))
    }
}

impl Database {
    /// Records the run an `@trigger(wait_for=...)` of `dag` was given, as it is placed: what its
    /// clean end moves it past, or a skip passes (decision 353).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn hand_trigger_run(&self, dag: &str, seen: &RunOfSeen) -> Result<(), DbError> {
        // Another run than the one it failed on starts with no failures (Codex's review): those
        // were the dropped run's.
        const HAND: &str = "INSERT INTO triggers (dag_id, failures, run_of_handed_run, \
            run_of_handed_at) VALUES ($1, 0, $2, $3{ts}) ON CONFLICT (dag_id) DO UPDATE SET \
            failures = CASE WHEN COALESCE(triggers.run_of_handed_run, '') = excluded.run_of_handed_run \
                THEN triggers.failures ELSE 0 END, \
            not_before = CASE WHEN COALESCE(triggers.run_of_handed_run, '') = excluded.run_of_handed_run \
                THEN triggers.not_before ELSE NULL END, \
            run_of_handed_run = excluded.run_of_handed_run, \
            run_of_handed_at = excluded.run_of_handed_at";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, HAND))
                .bind(dag)
                .bind(&seen.run)
                .bind(db_time(seen.at))
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// The run an `@trigger(wait_for=...)` of `dag` was last given and hasn't passed yet.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the stored time doesn't read.
    pub async fn trigger_run_handed(&self, dag: &str) -> Result<Option<RunOfSeen>, DbError> {
        const READ: &str = "SELECT run_of_handed_run, {ts<}run_of_handed_at{>ts} FROM triggers \
            WHERE dag_id = $1 AND run_of_handed_run IS NOT NULL AND run_of_handed_at IS NOT NULL";
        let row: Option<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, READ))
                .bind(dag)
                .fetch_optional(pool)
                .await?
        });
        row.map(|(run, at)| {
            Ok(RunOfSeen {
                at: parse_time("triggers.run_of_handed_at", &at)?,
                run,
            })
        })
        .transpose()
    }

    /// Moves an `@trigger(wait_for=...)` of `dag` reading `source` past the run it was given:
    /// done with it, or passing it (`skipped`: why, kept as its last error). Its failures in a
    /// row start again. With `call`, ends that call in the same statement, so the call can't
    /// end without the run passed (Codex's review); without, only while no call runs. Gives
    /// whether it was passed.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn pass_trigger_run(
        &self,
        dag: &str,
        source: &str,
        skipped: Option<&str>,
        call: Option<&str>,
    ) -> Result<bool, DbError> {
        const PASS: &str = "UPDATE triggers SET run_of_source = $2, \
            run_of_seen_at = run_of_handed_at, run_of_seen_run = run_of_handed_run, \
            run_of_handed_run = NULL, run_of_handed_at = NULL, failures = 0, not_before = NULL, \
            last_error = COALESCE($3, last_error) \
            WHERE dag_id = $1 AND run_of_handed_run IS NOT NULL AND instance_id IS NULL";
        const END_AND_PASS: &str = "UPDATE triggers SET run_of_source = $2, \
            run_of_seen_at = run_of_handed_at, run_of_seen_run = run_of_handed_run, \
            run_of_handed_run = NULL, run_of_handed_at = NULL, failures = 0, not_before = NULL, \
            last_error = COALESCE($3, last_error), instance_id = NULL, worker_id = NULL, \
            dag_version_id = NULL, code_hash = NULL \
            WHERE dag_id = $1 AND run_of_handed_run IS NOT NULL AND instance_id = $4";
        // Kept as passed in the same transaction, under the same conditions (decision 354).
        const KEEP: &str = "INSERT INTO trigger_passed (dag_id, item) \
            SELECT dag_id, run_of_handed_run FROM triggers WHERE dag_id = $1 \
            AND run_of_handed_run IS NOT NULL AND ($2 IS NULL AND instance_id IS NULL \
            OR instance_id = $2) ON CONFLICT (dag_id, item) DO NOTHING";
        let passed = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, KEEP))
                .bind(dag)
                .bind(call)
                .execute(&mut *tx)
                .await?;
            let statement = match call {
                Some(call) => sqlx::query(sql(backend, END_AND_PASS))
                    .bind(dag)
                    .bind(source)
                    .bind(skipped)
                    .bind(call),
                None => sqlx::query(sql(backend, PASS))
                    .bind(dag)
                    .bind(source)
                    .bind(skipped),
            };
            statement.execute(&mut *tx).await?.rows_affected()
        });
        Ok(passed == 1)
    }
}

impl Database {
    /// An `@trigger(wait_for=...)` of `dag` whose call `call` returned an event the master has
    /// stored: its item is passed, the call left running (its end passes nothing more). Gives
    /// whether it was passed.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn pass_trigger_run_stored(
        &self,
        dag: &str,
        source: &str,
        call: &str,
    ) -> Result<bool, DbError> {
        const PASS: &str = "UPDATE triggers SET run_of_source = $2, \
            run_of_seen_at = run_of_handed_at, run_of_seen_run = run_of_handed_run, \
            run_of_handed_run = NULL, run_of_handed_at = NULL, failures = 0, not_before = NULL \
            WHERE dag_id = $1 AND run_of_handed_run IS NOT NULL AND instance_id = $3";
        const KEEP: &str = "INSERT INTO trigger_passed (dag_id, item) \
            SELECT dag_id, run_of_handed_run FROM triggers WHERE dag_id = $1 \
            AND run_of_handed_run IS NOT NULL AND instance_id = $2 \
            ON CONFLICT (dag_id, item) DO NOTHING";
        let passed = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, KEEP))
                .bind(dag)
                .bind(call)
                .execute(&mut *tx)
                .await?;
            sqlx::query(sql(backend, PASS))
                .bind(dag)
                .bind(source)
                .bind(call)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(passed == 1)
    }

    /// An `@trigger(wait_for=...)` of `dag` whose call couldn't be made for its run (too big to
    /// send): a failure with `error`, not started again before `not_before`.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn trigger_call_refused(
        &self,
        dag: &str,
        error: &str,
        not_before: Option<Timestamp>,
    ) -> Result<(), DbError> {
        const REFUSED: &str = "UPDATE triggers SET last_error = $2, failures = failures + 1, \
            not_before = $3{ts} WHERE dag_id = $1 AND instance_id IS NULL";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, REFUSED))
                .bind(dag)
                .bind(error)
                .bind(not_before.map(db_time))
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }
}
