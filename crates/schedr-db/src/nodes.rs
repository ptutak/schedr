//! A deployment's nodes, for the Deployments page (decisions 121-123): each master (leader or
//! standby) heartbeating its row, and each worker's master, process and configuration.
//!
//! Configurations are masked by the node that writes them, before they leave its process
//! (`schedr_config::masked`): nothing here masks or unmasks.

use jiff::{SignedDuration, Timestamp};

use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, in_tx, on_pool, parse_time, sql, storable_json};

#[cfg(test)]
mod tests;

/// How long a master's row stays after its last heartbeat before it is removed for good (one
/// gone this long is long gone; the page shows only those heard from lately).
pub const MASTER_ROW_KEPT: SignedDuration = SignedDuration::from_hours(1);

/// A master as it heartbeats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterNode<'a> {
    /// Its ID: its lease holder's.
    pub master_id: &'a str,
    /// The process it runs in (shared with a worker in standalone).
    pub process_id: &'a str,
    /// Its host name.
    pub host: &'a str,
    /// Its schedr version.
    pub schedr_version: &'a str,
    /// Its effective configuration, masked.
    pub config: &'a serde_json::Value,
    /// When it started.
    pub started_at: Timestamp,
}

/// A master as stored.
#[derive(Debug, Clone, PartialEq)]
pub struct MasterRecord {
    /// Its ID.
    pub master_id: String,
    /// The process it runs in.
    pub process_id: String,
    /// Its host name.
    pub host: String,
    /// Its schedr version.
    pub schedr_version: String,
    /// Its effective configuration, masked.
    pub config: serde_json::Value,
    /// When it started.
    pub started_at: Timestamp,
    /// When it was last heard from.
    pub heard_at: Timestamp,
    /// Whether it holds the lease now (the leader).
    pub leading: bool,
}

/// A worker's node: which master it registered with, its process and configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerNode {
    /// Its ID.
    pub worker_id: String,
    /// The master it registered with last.
    pub master_id: Option<String>,
    /// The process it runs in.
    pub process_id: Option<String>,
    /// Its effective configuration, masked.
    pub config: Option<serde_json::Value>,
}

impl Database {
    /// Writes `master`'s heartbeat (its row, made or brought up to date), and removes the rows
    /// of masters not heard from for [`MASTER_ROW_KEPT`]. A standby heartbeats too: called on
    /// a database not [fenced](Database::fenced).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails.
    pub async fn heartbeat_master(
        &self,
        master: &MasterNode<'_>,
        now: Timestamp,
    ) -> Result<(), DbError> {
        const UPSERT: &str = "INSERT INTO masters (master_id, process_id, host, schedr_version, \
            config, started_at, heard_at) VALUES ($1, $2, $3, $4, $5{json}, $6{ts}, $7{ts}) \
            ON CONFLICT (master_id) DO UPDATE SET process_id = excluded.process_id, \
            host = excluded.host, schedr_version = excluded.schedr_version, \
            config = excluded.config, heard_at = excluded.heard_at";
        const PRUNE: &str = "DELETE FROM masters WHERE heard_at < $1{ts}";
        let config = storable_json(master.config.to_string());
        let gone = now
            .checked_sub(MASTER_ROW_KEPT)
            .unwrap_or(Timestamp::UNIX_EPOCH);
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, UPSERT))
                .bind(master.master_id)
                .bind(master.process_id)
                .bind(master.host)
                .bind(master.schedr_version)
                .bind(&config)
                .bind(db_time(master.started_at))
                .bind(db_time(now))
                .execute(&mut *tx)
                .await?;
            sqlx::query(sql(backend, PRUNE))
                .bind(db_time(gone))
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// The masters heard from since `since`, the leader (the lease's holder, its lease not
    /// expired, by the database's clock) first, then by ID.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn masters(&self, since: Timestamp) -> Result<Vec<MasterRecord>, DbError> {
        const MASTERS: &str = "SELECT m.master_id, m.process_id, m.host, m.schedr_version, \
            m.config{text}, {ts<}m.started_at{>ts}, {ts<}m.heard_at{>ts} \
            FROM masters m WHERE m.heard_at >= $1{ts} ORDER BY m.master_id";
        type Row = (String, String, String, String, String, String, String);
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, MASTERS))
                .bind(db_time(since))
                .fetch_all(pool)
                .await?
        });
        let holder = self.lease_holder().await?;
        let mut masters = rows
            .into_iter()
            .map(
                |(master_id, process_id, host, version, config, started, heard)| {
                    Ok(MasterRecord {
                        leading: holder.as_deref() == Some(master_id.as_str()),
                        master_id,
                        process_id,
                        host,
                        schedr_version: version,
                        config: from_json("masters.config", &config)?,
                        started_at: parse_time("masters.started_at", &started)?,
                        heard_at: parse_time("masters.heard_at", &heard)?,
                    })
                },
            )
            .collect::<Result<Vec<_>, DbError>>()?;
        masters.sort_by_key(|m| !m.leading);
        Ok(masters)
    }

    /// Records the node a worker runs as: the master it registered with, its process and its
    /// configuration (masked); each `None` when not known (an older agent sends neither).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn set_worker_node(
        &self,
        worker_id: &str,
        master_id: Option<&str>,
        process_id: Option<&str>,
        config: Option<&serde_json::Value>,
    ) -> Result<(), DbError> {
        const SET: &str = "UPDATE workers SET master_id = $1, process_id = $2, config = $3{json} \
            WHERE worker_id = $4";
        let config = config.map(|c| storable_json(c.to_string()));
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, SET))
                .bind(master_id)
                .bind(process_id)
                .bind(config)
                .bind(worker_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Every worker's node, by ID.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn worker_nodes(&self) -> Result<Vec<WorkerNode>, DbError> {
        const NODES: &str = "SELECT worker_id, master_id, process_id, config{text} FROM workers \
            ORDER BY worker_id";
        type Row = (String, Option<String>, Option<String>, Option<String>);
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, NODES)).fetch_all(pool).await?
        });
        rows.into_iter()
            .map(|(worker_id, master_id, process_id, config)| {
                Ok(WorkerNode {
                    worker_id,
                    master_id,
                    process_id,
                    config: config
                        .map(|c| from_json("workers.config", &c))
                        .transpose()?,
                })
            })
            .collect()
    }
}
