//! Retention (decisions 430-435): what the master's retention job deletes once `[retention]` no
//! longer keeps it, a batch per transaction (runs and outputs one DAG at a time). Never an
//! unfinished run, nor one something unfinished reads, nor what a DAG still needs (see
//! [`KEPT`]), unless the DAG was removed long enough ago; a removed DAG then goes with its last
//! run.

use jiff::Timestamp;

use crate::database::{Database, DbError};
use crate::query::{BATCH_LIMITS, db_time, in_tx, on_pool, sql};

#[cfg(test)]
mod tests;

/// Of the runs `r` of a DAG `d`, the ones it still needs (decision 430): its newest scheduled
/// run, its newest run, the last to end `success` or `failed`, and the successful runs of other
/// kinds planned after its newest scheduled one, or, without one, its successful backfill runs:
/// catchup counts their fire times as done (decision 440). A removed DAG keeps them until its
/// removal is as old as `$1`'s cut: a file missing for a while (a bad sync) costs no history.
/// Never NULL: a NULL would keep every run, `NOT NULL` being NULL.
const KEPT: &str = "(d.removed_at IS NULL OR d.removed_at >= $1{ts}) AND ( \
    r.id IN (SELECT s.id FROM runs s WHERE s.dag_id = r.dag_id AND s.kind = 'scheduled' \
        ORDER BY s.planned_for DESC, s.id DESC LIMIT 1) \
    OR r.id IN (SELECT s.id FROM runs s WHERE s.dag_id = r.dag_id \
        ORDER BY s.planned_for DESC, s.id DESC LIMIT 1) \
    OR r.id IN (SELECT s.id FROM runs s WHERE s.dag_id = r.dag_id \
        AND s.state IN ('success', 'failed') AND s.ended_at IS NOT NULL \
        ORDER BY s.ended_at DESC, s.id DESC LIMIT 1) \
    OR (r.kind <> 'scheduled' AND r.state = 'success' AND (r.planned_for > \
        COALESCE((SELECT MAX(s.planned_for) FROM runs s WHERE s.dag_id = r.dag_id \
         AND s.kind = 'scheduled'), r.planned_for) \
        OR (r.kind = 'backfill' AND NOT EXISTS (SELECT 1 FROM runs s \
         WHERE s.dag_id = r.dag_id AND s.kind = 'scheduled')))))";

/// Of the runs `r`, those something unfinished still reads, whatever their DAG: a
/// `TriggerDagOperator`'s open attempt waiting for it (gone, it would be started again), a
/// waiting step or trigger handed it (a retry gets the same run). Codex's review of M4.14.
const HELD: &str = "r.id IN (SELECT a.chained_run_id FROM attempts a \
        WHERE a.status IS NULL AND a.chained_run_id IS NOT NULL) \
    OR r.id{text} IN (SELECT t.handed_run FROM task_instances t WHERE t.handed_run IS NOT NULL \
        AND t.state NOT IN ('success', 'failed', 'cancelled', 'skipped', 'upstream_failed')) \
    OR r.id{text} IN (SELECT g.run_of_handed_run FROM triggers g \
        WHERE g.run_of_handed_run IS NOT NULL)";

/// The finished runs `r` of DAG `d` that ended before `$1` and nothing keeps.
const OLD: &str = "r.state IN ('success', 'failed', 'cancelled') AND r.ended_at < $1{ts}";

/// Taken before a DAG's runs or versions are deleted, so a definition of it (which takes the
/// same lock) waits, or is seen (Codex's review of M4.14). SQLite's writes are one at a time.
const LOCK_DAG: &str = "SELECT dag_id FROM dags WHERE dag_id = $1 FOR UPDATE";

impl Database {
    /// The DAG of the oldest run `select` (a query over `runs r JOIN dags d` with `$1`, the
    /// time) gives, if any: retention works through one DAG per transaction, its row locked.
    async fn oldest_dag(&self, select: &str, before: &str) -> Result<Option<String>, DbError> {
        let query = format!(
            "SELECT r.dag_id FROM runs r JOIN dags d ON d.dag_id = r.dag_id \
            WHERE {select} ORDER BY r.ended_at, r.id LIMIT 1"
        );
        let dag: Option<String> = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(crate::query::sql_text(backend, &query))
                .bind(before)
                .fetch_optional(pool)
                .await?
        });
        Ok(dag)
    }

    /// Deletes at most `batch` finished runs of one DAG that ended before `before` and aren't
    /// kept, with their tasks, attempts and outputs; a run they started (`TriggerDagOperator`)
    /// loses its link to them (decision 431). Then the backfills left without runs, made before
    /// `before`. Gives how many runs: 0 when none is left.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a statement fails (nothing of the batch is deleted then).
    pub async fn delete_old_runs(&self, before: Timestamp, batch: u32) -> Result<u64, DbError> {
        const UNLINK: &str = "UPDATE runs SET parent_run_id = NULL WHERE parent_run_id = $1{uuid}";
        const PASSED: &str = "DELETE FROM trigger_passed WHERE item = $1";
        const DELETE: &str = "DELETE FROM runs WHERE id = $1{uuid}";
        const BACKFILLS: &str = "DELETE FROM backfills WHERE created_at < $1{ts} \
            AND NOT EXISTS (SELECT 1 FROM runs WHERE runs.backfill_id = backfills.id)";
        let eligible = format!("{OLD} AND NOT ({KEPT}) AND NOT ({HELD})");
        let before = db_time(before);
        let Some(dag_id) = self.oldest_dag(&eligible, &before).await? else {
            return Ok(0);
        };
        // Chosen again under the lock: what the DAG is now.
        let old = format!(
            "SELECT r.id{{text}} FROM runs r JOIN dags d ON d.dag_id = r.dag_id \
             WHERE r.dag_id = $3 AND {eligible} ORDER BY r.ended_at, r.id LIMIT $2"
        );
        let deleted = in_tx!(self, BATCH_LIMITS, |tx, backend| {
            if backend == crate::Backend::Postgres {
                sqlx::query(sql(backend, LOCK_DAG))
                    .bind(&dag_id)
                    .execute(&mut *tx)
                    .await?;
            }
            let ids: Vec<String> = sqlx::query_scalar(crate::query::sql_text(backend, &old))
                .bind(&before)
                .bind(i64::from(batch))
                .bind(&dag_id)
                .fetch_all(&mut *tx)
                .await?;
            let mut deleted = 0;
            for id in &ids {
                sqlx::query(sql(backend, UNLINK))
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query(sql(backend, PASSED))
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                deleted += sqlx::query(sql(backend, DELETE))
                    .bind(id)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();
            }
            sqlx::query(sql(backend, BACKFILLS))
                .bind(&before)
                .execute(&mut *tx)
                .await?;
            deleted
        });
        Ok(deleted)
    }

    /// Deletes the outputs of at most `batch` finished runs of one DAG that ended before
    /// `before` and aren't kept, marking each run (`outputs_deleted_at = now`): its tasks and
    /// outcomes stay, and an output asked for answers "deleted by retention" (decision 435).
    /// Gives how many runs: 0 when none is left.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a statement fails.
    pub async fn delete_old_outputs(
        &self,
        before: Timestamp,
        batch: u32,
        now: Timestamp,
    ) -> Result<u64, DbError> {
        const OUTPUTS: &str = "DELETE FROM outputs WHERE task_instance_id IN \
            (SELECT id FROM task_instances WHERE run_id = $1{uuid})";
        const MARK: &str = "UPDATE runs SET outputs_deleted_at = $2{ts} WHERE id = $1{uuid}";
        let eligible =
            format!("{OLD} AND r.outputs_deleted_at IS NULL AND NOT ({KEPT}) AND NOT ({HELD})");
        let (before, now) = (db_time(before), db_time(now));
        let Some(dag_id) = self.oldest_dag(&eligible, &before).await? else {
            return Ok(0);
        };
        let old = format!(
            "SELECT r.id{{text}} FROM runs r JOIN dags d ON d.dag_id = r.dag_id \
             WHERE r.dag_id = $3 AND {eligible} ORDER BY r.ended_at, r.id LIMIT $2"
        );
        let marked = in_tx!(self, BATCH_LIMITS, |tx, backend| {
            if backend == crate::Backend::Postgres {
                sqlx::query(sql(backend, LOCK_DAG))
                    .bind(&dag_id)
                    .execute(&mut *tx)
                    .await?;
            }
            let ids: Vec<String> = sqlx::query_scalar(crate::query::sql_text(backend, &old))
                .bind(&before)
                .bind(i64::from(batch))
                .bind(&dag_id)
                .fetch_all(&mut *tx)
                .await?;
            for id in &ids {
                sqlx::query(sql(backend, OUTPUTS))
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query(sql(backend, MARK))
                    .bind(id)
                    .bind(&now)
                    .execute(&mut *tx)
                    .await?;
            }
            ids.len() as u64
        });
        Ok(marked)
    }

    /// Deletes the versions of one DAG that it doesn't run now and no run or trigger uses, its
    /// row locked: a definition of the same code again stores its version anew (archived
    /// Retention: DAG versions). Gives how many: 0 when no DAG has any left.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a statement fails.
    pub async fn delete_unused_versions(&self) -> Result<u64, DbError> {
        const UNUSED: &str = "NOT EXISTS (SELECT 1 FROM dags c WHERE c.current_version_id = v.id) \
            AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.dag_version_id = v.id) \
            AND NOT EXISTS (SELECT 1 FROM triggers t WHERE t.dag_version_id = v.id{text})";
        let any = format!("SELECT v.dag_id FROM dag_versions v WHERE {UNUSED} LIMIT 1");
        let delete = format!(
            "DELETE FROM dag_versions WHERE id IN \
            (SELECT v.id FROM dag_versions v WHERE v.dag_id = $1 AND {UNUSED})"
        );
        let dag: Option<String> = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(crate::query::sql_text(backend, &any))
                .fetch_optional(pool)
                .await?
        });
        let Some(dag_id) = dag else {
            return Ok(0);
        };
        let deleted = in_tx!(self, BATCH_LIMITS, |tx, backend| {
            if backend == crate::Backend::Postgres {
                sqlx::query(sql(backend, LOCK_DAG))
                    .bind(&dag_id)
                    .execute(&mut *tx)
                    .await?;
            }
            sqlx::query(crate::query::sql_text(backend, &delete))
                .bind(&dag_id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(deleted)
    }

    /// Deletes at most `batch` events triggers found and at most `batch` events posted through
    /// the API before `before` (decision 434), with what triggers passed of the posted ones.
    /// Gives how many in all.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a statement fails.
    pub async fn delete_old_events(&self, before: Timestamp, batch: u32) -> Result<u64, DbError> {
        const FOUND: &str = "DELETE FROM events WHERE (dag_id, key) IN \
            (SELECT dag_id, key FROM events WHERE received_at < $1{ts} \
             ORDER BY received_at LIMIT $2)";
        // Not one a waiting step was handed: its retry gets the same (Codex's review of M4.14).
        const POSTED: &str = "SELECT id{text} FROM named_events WHERE posted_at < $1{ts} \
            AND id{text} NOT IN (SELECT t.handed_run FROM task_instances t \
                WHERE t.handed_run IS NOT NULL AND t.state NOT IN \
                ('success', 'failed', 'cancelled', 'skipped', 'upstream_failed')) \
            ORDER BY posted_at, id LIMIT $2";
        const PASSED: &str = "DELETE FROM trigger_passed WHERE item = $1";
        const DELETE: &str = "DELETE FROM named_events WHERE id = $1{uuid}";
        let before = db_time(before);
        let deleted = in_tx!(self, BATCH_LIMITS, |tx, backend| {
            let mut deleted = sqlx::query(sql(backend, FOUND))
                .bind(&before)
                .bind(i64::from(batch))
                .execute(&mut *tx)
                .await?
                .rows_affected();
            let ids: Vec<String> = sqlx::query_scalar(sql(backend, POSTED))
                .bind(&before)
                .bind(i64::from(batch))
                .fetch_all(&mut *tx)
                .await?;
            for id in &ids {
                sqlx::query(sql(backend, PASSED))
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                deleted += sqlx::query(sql(backend, DELETE))
                    .bind(id)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();
            }
            deleted
        });
        Ok(deleted)
    }

    /// Deletes at most `batch` commands decided (applied or rejected) that were asked for
    /// before `before`: the audit trail, kept apart from the runs. Gives how many.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the statement fails.
    pub async fn delete_old_commands(&self, before: Timestamp, batch: u32) -> Result<u64, DbError> {
        const DELETE: &str = "DELETE FROM commands WHERE id IN (SELECT id FROM commands \
            WHERE status <> 'pending' AND requested_at < $1{ts} ORDER BY requested_at LIMIT $2)";
        let before = db_time(before);
        let deleted = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, DELETE))
                .bind(&before)
                .bind(i64::from(batch))
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(deleted)
    }

    /// Deletes the DAGs removed before `before` that have no run left, with everything kept
    /// about them (versions, backfills, events, their trigger, wait step strikes): one whose ID
    /// appears again starts with no history. Gives how many.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a statement fails.
    pub async fn delete_removed_dags(&self, before: Timestamp) -> Result<u64, DbError> {
        const GONE: &str = "SELECT dag_id FROM dags d WHERE removed_at < $1{ts} \
            AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.dag_id = d.dag_id) \
            ORDER BY removed_at LIMIT 100";
        // Taken first, so a DAG defined again meanwhile is left alone: its definition waits
        // for this transaction, or this one sees it isn't removed any more.
        const LOCK_POSTGRES: &str = "SELECT dag_id FROM dags WHERE dag_id = $1 \
            AND removed_at IS NOT NULL FOR UPDATE";
        const STILL_SQLITE: &str = "SELECT dag_id FROM dags WHERE dag_id = $1 \
            AND removed_at IS NOT NULL";
        const RUNS: &str = "SELECT 1 FROM runs WHERE dag_id = $1 LIMIT 1";
        const DEPENDENTS: [&str; 8] = [
            "UPDATE dags SET current_version_id = NULL WHERE dag_id = $1",
            "DELETE FROM backfills WHERE dag_id = $1",
            "DELETE FROM dag_versions WHERE dag_id = $1",
            "DELETE FROM events WHERE dag_id = $1",
            "DELETE FROM triggers WHERE dag_id = $1",
            "DELETE FROM trigger_passed WHERE dag_id = $1",
            "DELETE FROM wait_step_strikes WHERE dag_id = $1",
            "DELETE FROM dags WHERE dag_id = $1",
        ];
        let before = db_time(before);
        let gone: Vec<String> = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(sql(backend, GONE))
                .bind(&before)
                .fetch_all(pool)
                .await?
        });
        let mut deleted = 0;
        for dag_id in &gone {
            deleted += in_tx!(self, BATCH_LIMITS, |tx, backend| {
                let lock = match backend {
                    crate::Backend::Postgres => LOCK_POSTGRES,
                    crate::Backend::Sqlite => STILL_SQLITE,
                };
                let still: Option<String> = sqlx::query_scalar(sql(backend, lock))
                    .bind(dag_id)
                    .fetch_optional(&mut *tx)
                    .await?;
                let runs: Option<i32> = sqlx::query_scalar(sql(backend, RUNS))
                    .bind(dag_id)
                    .fetch_optional(&mut *tx)
                    .await?;
                if still.is_none() || runs.is_some() {
                    0
                } else {
                    for statement in DEPENDENTS {
                        sqlx::query(sql(backend, statement))
                            .bind(dag_id)
                            .execute(&mut *tx)
                            .await?;
                    }
                    1
                }
            });
        }
        Ok(deleted)
    }
}
