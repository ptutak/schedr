//! Wait steps that keep getting their host killed (decision 338): counted here, switched off
//! after so many in a row, on again with new code or by hand.

use jiff::Timestamp;

use crate::database::{Database, DbError};
use crate::query::{db_time, in_tx, on_pool, parse_time, sql};

#[cfg(test)]
mod tests;

/// A wait step with strikes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitStepStrikes {
    /// Its DAG.
    pub dag_id: String,
    /// The wait step.
    pub task_id: String,
    /// Its host kills in a row.
    pub strikes: u32,
    /// Switched on by new code: one strike switches it off again.
    pub one_chance: bool,
    /// When it was switched off, if it is.
    pub off_at: Option<Timestamp>,
    /// The code it was switched off on.
    pub off_code: Option<String>,
}

type Row = (String, String, i32, i32, Option<String>, Option<String>);

fn stored(row: Row) -> Result<WaitStepStrikes, DbError> {
    let (dag_id, task_id, strikes, one_chance, off_at, off_code) = row;
    Ok(WaitStepStrikes {
        dag_id,
        task_id,
        strikes: u32::try_from(strikes).unwrap_or(0),
        one_chance: one_chance != 0,
        off_at: off_at
            .map(|at| parse_time("wait_step_strikes.off_at", &at))
            .transpose()?,
        off_code,
    })
}

impl Database {
    /// A wait step's strikes, if it has any.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the statement fails.
    pub async fn wait_step_strikes(
        &self,
        dag_id: &str,
        task_id: &str,
    ) -> Result<Option<WaitStepStrikes>, DbError> {
        const READ: &str = "SELECT dag_id, task_id, strikes, one_chance, off_at{text}, off_code \
                            FROM wait_step_strikes WHERE dag_id = $1 AND task_id = $2";
        let row: Option<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, READ))
                .bind(dag_id)
                .bind(task_id)
                .fetch_optional(pool)
                .await?
        });
        row.map(stored).transpose()
    }

    /// The wait steps switched off now, by DAG and task.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the statement fails.
    pub async fn switched_off_wait_steps(&self) -> Result<Vec<WaitStepStrikes>, DbError> {
        const LIST: &str = "SELECT dag_id, task_id, strikes, one_chance, off_at{text}, off_code \
                            FROM wait_step_strikes WHERE off_at IS NOT NULL \
                            ORDER BY dag_id, task_id";
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, LIST)).fetch_all(pool).await?
        });
        rows.into_iter().map(stored).collect()
    }

    /// Counts a strike; at `limit` in a row (0: never), or on its one chance, it is switched off
    /// on code `code`. Returns whether it is off now.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a statement fails.
    pub async fn strike_wait_step(
        &self,
        (dag_id, task_id): (&str, &str),
        limit: u32,
        code: &str,
        now: Timestamp,
    ) -> Result<bool, DbError> {
        const COUNT: &str = "INSERT INTO wait_step_strikes (dag_id, task_id, strikes) \
                             VALUES ($1, $2, 1) ON CONFLICT (dag_id, task_id) \
                             DO UPDATE SET strikes = wait_step_strikes.strikes + 1 \
                             RETURNING strikes, one_chance";
        const OFF: &str = "UPDATE wait_step_strikes SET off_at = $1{ts}, off_code = $2, one_chance = 0 \
                           WHERE dag_id = $3 AND task_id = $4";
        let at = db_time(now);
        let off = in_tx!(self, |tx, backend| {
            let (strikes, one_chance): (i32, i32) = sqlx::query_as(sql(backend, COUNT))
                .bind(dag_id)
                .bind(task_id)
                .fetch_one(&mut *tx)
                .await?;
            let reached = limit > 0 && u32::try_from(strikes).unwrap_or(0) >= limit;
            let off = reached || (limit > 0 && one_chance != 0);
            if off {
                sqlx::query(sql(backend, OFF))
                    .bind(&at)
                    .bind(code)
                    .bind(dag_id)
                    .bind(task_id)
                    .execute(&mut *tx)
                    .await?;
            }
            off
        });
        Ok(off)
    }

    /// New code: on again, with one chance.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the statement fails.
    pub async fn wait_step_on_new_code(&self, dag_id: &str, task_id: &str) -> Result<(), DbError> {
        const ON: &str = "UPDATE wait_step_strikes \
                          SET off_at = NULL, off_code = NULL, one_chance = 1, strikes = 0 \
                          WHERE dag_id = $1 AND task_id = $2";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, ON))
                .bind(dag_id)
                .bind(task_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Forgets its strikes (a success, or switched on by hand). Returns whether it had any.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the statement fails.
    pub async fn clear_wait_step(&self, dag_id: &str, task_id: &str) -> Result<bool, DbError> {
        const CLEAR: &str = "DELETE FROM wait_step_strikes WHERE dag_id = $1 AND task_id = $2";
        let changed = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, CLEAR))
                .bind(dag_id)
                .bind(task_id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(changed == 1)
    }
}
