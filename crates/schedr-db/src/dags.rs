//! The DAG catalog's storage: parse results with their files, DAGs and DAG versions.
//!
//! A parse result is stored as the worker reported it (after the master's validation); the master
//! works out the DAG list from the current parse of each root and stores the changes
//! ([`DagChange`]). Each DAG version belongs to one parse (see archive/PROJECT.md, DAG code
//! delivery and DAG ID conflicts).

use std::collections::{BTreeMap, HashMap};

use jiff::Timestamp;
use schedr_model::connection::Registry;
use schedr_model::runner::{ErrorSummary, ParsedFile};
use schedr_model::{Dag, DagId, DagLocation};
use uuid::Uuid;

use crate::database::{Database, DbError};
use crate::query::{BATCH_LIMITS, db_time, from_json, in_tx, on_pool, parse_time, sql, to_json};

/// The rows of removed DAGs' unfinished runs with something not started: the read's `FROM ... WHERE` and its probe check's (decision 485).
macro_rules! removed_dags_runs_from {
    () => {
        "FROM runs r JOIN dags d ON d.dag_id = r.dag_id WHERE d.removed_at IS NOT NULL AND r.state IN ('queued', 'running') AND (r.state = 'queued' OR EXISTS (SELECT 1 FROM task_instances t WHERE t.run_id = r.id AND t.state IN ('scheduled', 'queued', 'up_for_retry', 'unschedulable')) OR EXISTS (SELECT 1 FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id WHERE t.run_id = r.id AND t.state = 'running' AND a.status IS NULL AND a.started_at IS NULL AND t.cancel_requested_at IS NULL))"
    };
}
pub(crate) use removed_dags_runs_from;

/// The rows of scheduled DAGs that can run whose next fire time has come at `$1` or isn't
/// known: the scheduler's read and its probe check's (decision 485).
macro_rules! scheduled_due_from {
    () => {
        "FROM dag_triggers t JOIN dags d ON d.dag_id = t.dag_id \
         WHERE t.kind = 'schedule' AND NOT t.paused \
         AND d.current_version_id IS NOT NULL AND d.removed_at IS NULL AND d.conflict IS NULL \
         AND (t.next_fire_at IS NULL OR t.next_fire_at <= $1{ts})"
    };
}
pub(crate) use scheduled_due_from;

/// A stored next fire time meaning "none ahead" (decision 485): a schedule past its `end`, a
/// `@once` done.
pub const NO_FIRE: Timestamp = Timestamp::constant(253_370_764_800, 0);

#[cfg(test)]
mod tests;

/// A parse result to store.
#[derive(Debug, Clone)]
pub struct NewParse<'a> {
    /// The root's name.
    pub root: &'a str,
    /// The content hash parsed.
    pub content_hash: &'a str,
    /// The schedr version of the runner that parsed it.
    pub schedr_version: &'a str,
    /// The worker that parsed it.
    pub worker_id: &'a str,
    /// When the master received it.
    pub parsed_at: Timestamp,
    /// Its files.
    pub files: &'a [ParsedFile],
}

/// A DAG's current version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentVersion {
    /// The version's ID.
    pub version_id: String,
    /// The parse it comes from.
    pub parse_id: String,
    /// The DAG JSON.
    pub dag: Dag,
}

/// A stored DAG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDag {
    /// The DAG's ID.
    pub dag_id: DagId,
    /// Its trigger's switch (decision 459): whether it is paused; `None` without a trigger
    /// (run only by hand), which has no switch.
    pub paused: Option<bool>,
    /// Whether it was removed (hidden, history kept).
    pub removed: bool,
    /// The places defining it, while it is in conflict.
    pub conflict: Option<Vec<DagLocation>>,
    /// Its current version (none for a DAG only ever seen in conflict).
    pub current: Option<CurrentVersion>,
}

/// A change to the DAG list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DagChange {
    /// The DAG is defined in one place by this parse: it becomes the current version (stored
    /// once per parse), and the DAG is no longer removed or in conflict. A trigger new to it,
    /// or of another kind, is stored paused, and losing it deletes its switch (decision 459);
    /// `pause` pauses an existing one too.
    Define {
        /// The definition.
        dag: Box<Dag>,
        /// The parse it comes from.
        parse_id: String,
        /// Pause its trigger (a removed DAG that came back).
        pause: bool,
    },
    /// The DAG ID is defined in more than one place: it can't run.
    Conflict {
        /// The DAG's ID.
        dag_id: DagId,
        /// Every place defining it.
        places: Vec<DagLocation>,
        /// Pause its trigger (a removed DAG that came back).
        pause: bool,
    },
    /// The DAG is no longer defined: hidden, its history kept.
    Remove {
        /// The DAG's ID.
        dag_id: DagId,
    },
}

/// The row of a parse file: path, DAGs JSON, error JSON.
type FileRow = (String, String, Option<String>, String, String);

/// The row of a stored DAG with its current version.
type DagRow = (
    String,
    Option<bool>,
    bool,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// A file of a root's current parse that failed to import (decision 66).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportErrorRecord {
    /// The DAG root.
    pub root: String,
    /// The root's content hash the parse is of.
    pub content_hash: String,
    /// The schedr version of the worker that parsed it.
    pub schedr_version: String,
    /// The worker that parsed it.
    pub worker_id: String,
    /// The master that stored it; `None` if not known.
    pub master_id: Option<String>,
    /// When the parse was stored.
    pub parsed_at: Timestamp,
    /// The file, relative to the root.
    pub path: String,
    /// Why it failed.
    pub error: ErrorSummary,
}

/// A row of [`Database::current_import_errors`]'s query.
type ImportErrorRow = (
    String,
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    String,
);

/// A stored parse of a (root, content hash).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredParse {
    /// Its ID.
    pub id: String,
    /// The schedr version of the workers that parsed it.
    pub schedr_version: String,
    /// When it was stored.
    pub parsed_at: Timestamp,
}

impl Database {
    /// Records that a worker reported `root` at `hash`: a **switch** (the worker had another
    /// hash before) makes it the most recent; a first report only records it if unknown.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn record_root_hash(
        &self,
        root: &str,
        content_hash: &str,
        switched: bool,
        now: Timestamp,
    ) -> Result<(), DbError> {
        const SWITCH: &str = "INSERT INTO root_hashes (root, content_hash, switched_at) \
            VALUES ($1, $2, $3{ts}) ON CONFLICT (root, content_hash) \
            DO UPDATE SET switched_at = excluded.switched_at";
        const SEEN: &str = "INSERT INTO root_hashes (root, content_hash, switched_at) \
            VALUES ($1, $2, $3{ts}) ON CONFLICT (root, content_hash) DO NOTHING";
        let query = if switched { SWITCH } else { SEEN };
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, query))
                .bind(root)
                .bind(content_hash)
                .bind(db_time(now))
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// When workers last switched to each recorded hash of `root`.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored time doesn't read.
    pub async fn root_hash_switches(
        &self,
        root: &str,
    ) -> Result<BTreeMap<String, Timestamp>, DbError> {
        const SWITCHES: &str = "SELECT content_hash, {ts<}switched_at{>ts} FROM root_hashes \
            WHERE root = $1";
        let rows: Vec<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, SWITCHES))
                .bind(root)
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|(hash, at)| Ok((hash, parse_time("root_hashes.switched_at", &at)?)))
            .collect()
    }

    /// Every stored parse of (root, hash), one per schedr version, oldest first.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored time doesn't read.
    pub async fn parses_of(
        &self,
        root: &str,
        content_hash: &str,
    ) -> Result<Vec<StoredParse>, DbError> {
        const PARSES: &str = "SELECT id{text}, schedr_version, {ts<}parsed_at{>ts} \
            FROM parse_results WHERE root = $1 AND content_hash = $2 ORDER BY parsed_at, id";
        let rows: Vec<(String, String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, PARSES))
                .bind(root)
                .bind(content_hash)
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|(id, schedr_version, parsed_at)| {
                Ok(StoredParse {
                    id,
                    schedr_version,
                    parsed_at: parse_time("parse_results.parsed_at", &parsed_at)?,
                })
            })
            .collect()
    }

    /// The ID of the parse of (root, hash, schedr version), if stored.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn find_parse(
        &self,
        root: &str,
        content_hash: &str,
        schedr_version: &str,
    ) -> Result<Option<String>, DbError> {
        const FIND: &str = "SELECT id{text} FROM parse_results \
            WHERE root = $1 AND content_hash = $2 AND schedr_version = $3";
        let id = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(sql(backend, FIND))
                .bind(root)
                .bind(content_hash)
                .bind(schedr_version)
                .fetch_optional(pool)
                .await?
        });
        Ok(id)
    }

    /// Stores a parse result and its files, and returns its ID. A parse of the same (root,
    /// hash, schedr version) stored meanwhile wins: its ID is returned and nothing is stored.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails (nothing is stored then).
    pub async fn insert_parse(&self, parse: &NewParse<'_>) -> Result<String, DbError> {
        const INSERT: &str = "INSERT INTO parse_results \
            (id, root, content_hash, schedr_version, worker_id, parsed_at) \
            VALUES ($1{uuid}, $2, $3, $4, $5, $6{ts}) \
            ON CONFLICT (root, content_hash, schedr_version) DO NOTHING";
        const FILE: &str = "INSERT INTO parse_files \
            (parse_result_id, path, dags, error, worker_id, master_id, parsed_at, connections, \
            script_roots) \
            VALUES ($1{uuid}, $2, $3{json}, $4{json}, $5, $6, $7{ts}, $8{json}, $9{json})";
        let id = Uuid::now_v7().to_string();
        let inserted = in_tx!(self, BATCH_LIMITS, |tx, backend| {
            let done = sqlx::query(sql(backend, INSERT))
                .bind(&id)
                .bind(parse.root)
                .bind(parse.content_hash)
                .bind(parse.schedr_version)
                .bind(parse.worker_id)
                .bind(db_time(parse.parsed_at))
                .execute(&mut *tx)
                .await?;
            let inserted = done.rows_affected() == 1;
            if inserted {
                for file in parse.files {
                    sqlx::query(sql(backend, FILE))
                        .bind(&id)
                        .bind(&file.path)
                        .bind(to_json(&file.dags))
                        .bind(file.error.as_ref().map(to_json))
                        .bind(parse.worker_id)
                        .bind(self.fenced_as())
                        .bind(db_time(parse.parsed_at))
                        .bind(to_json(&file.connections))
                        .bind(to_json(&file.script_roots))
                        .execute(&mut *tx)
                        .await?;
                }
            }
            inserted
        });
        if inserted {
            return Ok(id);
        }
        let existing = self
            .find_parse(parse.root, parse.content_hash, parse.schedr_version)
            .await?;
        Ok(existing.unwrap_or(id))
    }

    /// Replaces files of a stored parse (a re-parse of files that failed transiently,
    /// decision 72.2): each of `files` takes the place of the stored file at its path, parsed
    /// by `worker_id` at `parsed_at` (decision 93).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails.
    pub async fn replace_parse_files(
        &self,
        parse_id: &str,
        files: &[ParsedFile],
        worker_id: &str,
        parsed_at: Timestamp,
    ) -> Result<(), DbError> {
        const DELETE: &str =
            "DELETE FROM parse_files WHERE parse_result_id = $1{uuid} AND path = $2";
        const FILE: &str = "INSERT INTO parse_files \
            (parse_result_id, path, dags, error, worker_id, master_id, parsed_at, connections, \
            script_roots) \
            VALUES ($1{uuid}, $2, $3{json}, $4{json}, $5, $6, $7{ts}, $8{json}, $9{json})";
        in_tx!(self, BATCH_LIMITS, |tx, backend| {
            for file in files {
                sqlx::query(sql(backend, DELETE))
                    .bind(parse_id)
                    .bind(&file.path)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query(sql(backend, FILE))
                    .bind(parse_id)
                    .bind(&file.path)
                    .bind(to_json(&file.dags))
                    .bind(file.error.as_ref().map(to_json))
                    .bind(worker_id)
                    .bind(self.fenced_as())
                    .bind(db_time(parsed_at))
                    .bind(to_json(&file.connections))
                    .bind(to_json(&file.script_roots))
                    .execute(&mut *tx)
                    .await?;
            }
        });
        Ok(())
    }

    /// The worker that parsed each file of a parse, by path (decision 93): the parse's own,
    /// or the one that parsed the file again.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn parse_file_workers(
        &self,
        parse_id: &str,
    ) -> Result<BTreeMap<String, String>, DbError> {
        const WORKERS: &str = "SELECT path, worker_id FROM parse_files \
            WHERE parse_result_id = $1{uuid}";
        let rows: Vec<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, WORKERS))
                .bind(parse_id)
                .fetch_all(pool)
                .await?
        });
        Ok(rows.into_iter().collect())
    }

    /// The files of a stored parse, in path order.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails, [`DbError::InvalidData`] if a stored file doesn't
    /// read back.
    pub async fn parse_files(&self, parse_id: &str) -> Result<Vec<ParsedFile>, DbError> {
        const FILES: &str = "SELECT path, dags{text}, error{text}, connections{text}, \
            script_roots{text} FROM parse_files WHERE parse_result_id = $1{uuid} ORDER BY path";
        let rows: Vec<FileRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, FILES))
                .bind(parse_id)
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|(path, dags, error, connections, script_roots)| {
                Ok(ParsedFile {
                    dags: from_json("parse_files.dags", &dags)?,
                    connections: from_json("parse_files.connections", &connections)?,
                    script_roots: from_json("parse_files.script_roots", &script_roots)?,
                    error: error
                        .map(|e| from_json("parse_files.error", &e))
                        .transpose()?,
                    path,
                    warnings: Vec::new(),
                })
            })
            .collect()
    }

    /// The connections of `root`'s current parse (decision 174), as the master builds its
    /// registry: empty when it has none.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails, [`DbError::InvalidData`] if a stored connection
    /// doesn't read back.
    pub async fn current_connections(&self, root: &str) -> Result<Registry, DbError> {
        const CONNECTIONS: &str = "SELECT f.path, f.connections{text} FROM current_parses c \
            JOIN parse_files f ON f.parse_result_id = c.parse_result_id \
            WHERE c.root = $1 ORDER BY f.path";
        let rows: Vec<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, CONNECTIONS))
                .bind(root)
                .fetch_all(pool)
                .await?
        });
        let files = rows
            .into_iter()
            .map(|(path, connections)| {
                Ok(ParsedFile {
                    script_roots: Vec::new(),
                    connections: from_json("parse_files.connections", &connections)?,
                    path,
                    dags: Vec::new(),
                    error: None,
                    warnings: Vec::new(),
                })
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        Ok(Registry::of(&files))
    }

    /// Records `parse_id` as `root`'s current parse (the master's decision), at `now`.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn set_current_parse(
        &self,
        root: &str,
        parse_id: &str,
        now: Timestamp,
    ) -> Result<(), DbError> {
        const SET: &str = "INSERT INTO current_parses (root, parse_result_id, decided_at) \
            VALUES ($1, $2{uuid}, $3{ts}) ON CONFLICT (root) DO UPDATE SET \
            parse_result_id = excluded.parse_result_id, decided_at = excluded.decided_at";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, SET))
                .bind(root)
                .bind(parse_id)
                .bind(db_time(now))
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Each root's current parse, as last recorded by [`Self::set_current_parse`]: root to
    /// parse ID.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn current_parses(&self) -> Result<BTreeMap<String, String>, DbError> {
        const CURRENT: &str = "SELECT root, parse_result_id{text} FROM current_parses";
        let rows: Vec<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, CURRENT))
                .fetch_all(pool)
                .await?
        });
        Ok(rows.into_iter().collect())
    }

    /// The files of each root's current parse that failed to import, by root and path
    /// (decision 66).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails, [`DbError::InvalidData`] if a stored error or time
    /// doesn't read back.
    pub async fn current_import_errors(&self) -> Result<Vec<ImportErrorRecord>, DbError> {
        const ERRORS: &str = "SELECT c.root, r.content_hash, r.schedr_version, f.worker_id, \
            f.master_id, {ts<}f.parsed_at{>ts}, f.path, f.error{text} \
            FROM current_parses c \
            JOIN parse_results r ON r.id = c.parse_result_id \
            JOIN parse_files f ON f.parse_result_id = c.parse_result_id \
            WHERE f.error IS NOT NULL ORDER BY c.root, f.path";
        let rows: Vec<ImportErrorRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, ERRORS)).fetch_all(pool).await?
        });
        rows.into_iter()
            .map(
                |(
                    root,
                    content_hash,
                    schedr_version,
                    worker_id,
                    master_id,
                    parsed_at,
                    path,
                    error,
                )| {
                    Ok(ImportErrorRecord {
                        root,
                        content_hash,
                        schedr_version,
                        worker_id,
                        master_id,
                        parsed_at: parse_time("parse_files.parsed_at", &parsed_at)?,
                        path,
                        error: from_json("parse_files.error", &error)?,
                    })
                },
            )
            .collect()
    }

    /// Every stored DAG (removed ones too), with its current version, in DAG ID order.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails, [`DbError::InvalidData`] if stored JSON doesn't
    /// read back.
    pub async fn stored_dags(&self) -> Result<Vec<StoredDag>, DbError> {
        const DAGS: &str = "SELECT d.dag_id, t.paused, d.removed_at IS NOT NULL, \
            d.conflict{text}, v.id{text}, v.parse_result_id{text}, v.dag_json{text} \
            FROM dags d LEFT JOIN dag_triggers t ON t.dag_id = d.dag_id \
            LEFT JOIN dag_versions v ON v.id = d.current_version_id \
            ORDER BY d.dag_id";
        let rows: Vec<DagRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, DAGS)).fetch_all(pool).await?
        });
        rows.into_iter().map(stored_dag).collect()
    }

    /// The DAGs whose queued tasks mustn't be assigned, each `true` when removed (their tasks
    /// not started are cancelled, B192) and `false` when in an ID conflict (they wait until it
    /// is resolved, B193; decision 107).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn held_dags(&self) -> Result<BTreeMap<String, bool>, DbError> {
        const HELD: &str = "SELECT dag_id, removed_at IS NOT NULL FROM dags \
            WHERE removed_at IS NOT NULL OR conflict IS NOT NULL";
        let rows: Vec<(String, bool)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, HELD)).fetch_all(pool).await?
        });
        Ok(rows.into_iter().collect())
    }

    /// The unfinished runs of removed DAGs with something not started yet (B192): queued runs,
    /// and running ones with a task scheduled, queued, up for retry or unschedulable, or with
    /// an assignment not started whose cancel isn't asked yet. The master cancels what they
    /// haven't started; their running attempts finish.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn removed_dags_runs(&self) -> Result<Vec<String>, DbError> {
        const RUNS: &str = concat!(
            "SELECT r.id{text} ",
            removed_dags_runs_from!(),
            " ORDER BY r.triggered_at, r.id"
        );
        if self.skipped(crate::probe::Check::RemovedDagsRuns).await? {
            return Ok(Vec::new());
        }
        Ok(on_pool!(self, |pool, backend| {
            sqlx::query_scalar(sql(backend, RUNS))
                .fetch_all(pool)
                .await?
        }))
    }

    /// The scheduled DAGs that can run whose next fire time has come at `now` or isn't known
    /// (decision 485), with what is stored (`None`: not known).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored time doesn't read.
    pub async fn scheduled_due(
        &self,
        now: Timestamp,
    ) -> Result<Vec<(DagId, Option<Timestamp>)>, DbError> {
        const DUE: &str = concat!(
            "SELECT t.dag_id, {ts<}t.next_fire_at{>ts} ",
            scheduled_due_from!(),
            " ORDER BY t.dag_id"
        );
        if self.skipped_at(crate::probe::Check::Scheduled, now).await? {
            return Ok(Vec::new());
        }
        let rows: Vec<(String, Option<String>)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, DUE))
                .bind(db_time(now))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|(dag, at)| {
                let at = at
                    .map(|at| parse_time("dag_triggers.next_fire_at", &at))
                    .transpose()?;
                Ok((crate::runs::dag_id_of(dag)?, at))
            })
            .collect()
    }

    /// Every scheduled DAG's stored next fire time (`None`: not known), for a round visiting
    /// every DAG (decision 485).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored time doesn't read.
    pub async fn next_fires(&self) -> Result<HashMap<DagId, Option<Timestamp>>, DbError> {
        const ALL: &str = "SELECT dag_id, {ts<}next_fire_at{>ts} FROM dag_triggers \
            WHERE kind = 'schedule'";
        let rows: Vec<(String, Option<String>)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, ALL)).fetch_all(pool).await?
        });
        rows.into_iter()
            .map(|(dag, at)| {
                let at = at
                    .map(|at| parse_time("dag_triggers.next_fire_at", &at))
                    .transpose()?;
                Ok((crate::runs::dag_id_of(dag)?, at))
            })
            .collect()
    }

    /// Stores scheduled DAG `dag_id`'s next fire time (decision 485): when the master visits it
    /// next ([`NO_FIRE`]: none ahead). A DAG without a schedule's switch changes nothing.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn set_next_fire(&self, dag_id: &DagId, at: Timestamp) -> Result<(), DbError> {
        const SET: &str = "UPDATE dag_triggers SET next_fire_at = $1{ts} \
            WHERE dag_id = $2 AND kind = 'schedule'";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, SET))
                .bind(db_time(at))
                .bind(dag_id.as_str())
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// DAG `dag_id` as [`Self::stored_dags`] lists it, read alone (B136: one DAG's reads
    /// read every DAG); `None` if it was never stored.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or its stored DAG doesn't read.
    pub async fn stored_dag(&self, dag_id: &DagId) -> Result<Option<StoredDag>, DbError> {
        const DAG: &str = "SELECT d.dag_id, t.paused, d.removed_at IS NOT NULL, \
            d.conflict{text}, v.id{text}, v.parse_result_id{text}, v.dag_json{text} \
            FROM dags d LEFT JOIN dag_triggers t ON t.dag_id = d.dag_id \
            LEFT JOIN dag_versions v ON v.id = d.current_version_id \
            WHERE d.dag_id = $1";
        let row: Option<DagRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, DAG))
                .bind(dag_id.as_str())
                .fetch_optional(pool)
                .await?
        });
        row.map(stored_dag).transpose()
    }

    /// Pauses or unpauses a DAG's trigger. Returns whether it has one (decision 459: a DAG run
    /// only by hand has no switch).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn set_paused(&self, dag_id: &DagId, paused: bool) -> Result<bool, DbError> {
        self.set_paused_at(dag_id, paused, Timestamp::now()).await
    }

    /// As [`Self::set_paused`], unpausing at `now`: its `live_since` (decision 353) changes in the
    /// same statement, cleared by a pause, set by an unpause unless it was live already.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn set_paused_at(
        &self,
        dag_id: &DagId,
        paused: bool,
        now: Timestamp,
    ) -> Result<bool, DbError> {
        // A switch changed: its next fire time isn't known any more (decision 485).
        const PAUSE: &str = "UPDATE dag_triggers SET paused = $1, next_fire_at = NULL, \
            live_since = CASE WHEN $1 THEN NULL WHEN EXISTS (SELECT 1 FROM dags d \
                WHERE d.dag_id = dag_triggers.dag_id AND d.removed_at IS NULL \
                AND d.conflict IS NULL) THEN COALESCE(live_since, $3{ts}) END \
            WHERE dag_id = $2";
        let changed = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, PAUSE))
                .bind(paused)
                .bind(dag_id.as_str())
                .bind(db_time(now))
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(changed == 1)
    }

    /// Stores changes to the DAG list, all or none.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails (nothing is stored then).
    #[expect(
        clippy::too_many_lines,
        reason = "one transaction over each kind of change, its statements read together"
    )]
    pub async fn apply_dag_changes(
        &self,
        changes: &[DagChange],
        now: Timestamp,
    ) -> Result<(), DbError> {
        const ENSURE: &str = "INSERT INTO dags (dag_id, first_seen_at) \
            VALUES ($1, $2{ts}) ON CONFLICT (dag_id) DO NOTHING";
        const VERSION: &str = "INSERT INTO dag_versions \
            (id, dag_id, parse_result_id, dag_json, created_at) \
            VALUES ($1{uuid}, $2, $3{uuid}, $4{json}, $5{ts}) \
            ON CONFLICT (dag_id, parse_result_id) DO NOTHING";
        const OLD_TRIGGER: &str = "SELECT v.dag_json{text} FROM dags d \
            JOIN dag_versions v ON v.id = d.current_version_id WHERE d.dag_id = $1";
        // Its trigger's switch (decision 459), before the DAG's row changes: new, or of another
        // kind ($2), paused; else kept, paused by $3. `live_since` (decision 353) with it: kept
        // while it stays live with the same trigger, set when it comes back unpaused or its
        // trigger changes ($4), cleared while it is paused, in conflict or removed.
        const SWITCH: &str = "INSERT INTO dag_triggers (dag_id, kind, paused, live_since) \
            VALUES ($1, $2, TRUE, NULL) ON CONFLICT (dag_id) DO UPDATE SET \
            paused = CASE WHEN dag_triggers.kind <> excluded.kind THEN TRUE \
                ELSE (dag_triggers.paused OR $3) END, \
            live_since = CASE WHEN dag_triggers.kind <> excluded.kind \
                OR dag_triggers.paused OR $3 THEN NULL \
                WHEN EXISTS (SELECT 1 FROM dags d WHERE d.dag_id = $1 \
                    AND d.removed_at IS NULL AND d.conflict IS NULL) AND NOT $4 \
                THEN COALESCE(dag_triggers.live_since, $5{ts}) ELSE $5{ts} END, \
            kind = excluded.kind, next_fire_at = NULL";
        const NO_SWITCH: &str = "DELETE FROM dag_triggers WHERE dag_id = $1";
        const CURRENT: &str = "UPDATE dags SET current_version_id = (SELECT id FROM dag_versions \
            WHERE dag_id = $1 AND parse_result_id = $2{uuid}), \
            removed_at = NULL, conflict = NULL WHERE dag_id = $1";
        const CONFLICT: &str = "UPDATE dags SET conflict = $1{json}, removed_at = NULL \
            WHERE dag_id = $2";
        const CONFLICT_SWITCH: &str = "UPDATE dag_triggers SET live_since = NULL, \
            paused = (paused OR $1), next_fire_at = NULL WHERE dag_id = $2";
        const REMOVE: &str = "UPDATE dags SET removed_at = $1{ts}, conflict = NULL \
            WHERE dag_id = $2";
        const REMOVE_SWITCH: &str =
            "UPDATE dag_triggers SET live_since = NULL, next_fire_at = NULL WHERE dag_id = $1";
        const FORGET_PASSED: &str = "DELETE FROM trigger_progress_passed WHERE dag_id = $1";
        // As retention's: not one a new run's foreign key waits for.
        const LOCK: &str = "SELECT dag_id FROM dags WHERE dag_id = $1 FOR NO KEY UPDATE";
        let now = db_time(now);
        in_tx!(self, BATCH_LIMITS, |tx, backend| {
            for change in changes {
                match change {
                    DagChange::Define {
                        dag,
                        parse_id,
                        pause,
                    } => {
                        sqlx::query(sql(backend, ENSURE))
                            .bind(dag.id.as_str())
                            .bind(&now)
                            .execute(&mut *tx)
                            .await?;
                        // As retention does before deleting its versions: one waits for the
                        // other, so the version made current here isn't deleted meanwhile
                        // (Codex's review of M4.14). SQLite's writes are one at a time.
                        if backend == crate::Backend::Postgres {
                            sqlx::query(sql(backend, LOCK))
                                .bind(dag.id.as_str())
                                .execute(&mut *tx)
                                .await?;
                        }
                        sqlx::query(sql(backend, VERSION))
                            .bind(Uuid::now_v7().to_string())
                            .bind(dag.id.as_str())
                            .bind(parse_id)
                            .bind(to_json(dag))
                            .bind(&now)
                            .execute(&mut *tx)
                            .await?;
                        let old: Option<(String,)> = sqlx::query_as(sql(backend, OLD_TRIGGER))
                            .bind(dag.id.as_str())
                            .fetch_optional(&mut *tx)
                            .await?;
                        let new_trigger = old.is_some_and(|(json,)| {
                            serde_json::from_str::<serde_json::Value>(&json)
                                .ok()
                                .and_then(|mut old| {
                                    old.get_mut("trigger").map(serde_json::Value::take)
                                })
                                != serde_json::to_value(&dag.trigger).ok()
                        });
                        if let Some(kind) = dag.trigger.kind() {
                            sqlx::query(sql(backend, SWITCH))
                                .bind(dag.id.as_str())
                                .bind(kind)
                                .bind(*pause)
                                .bind(new_trigger)
                                .bind(&now)
                                .execute(&mut *tx)
                                .await?;
                        } else {
                            sqlx::query(sql(backend, NO_SWITCH))
                                .bind(dag.id.as_str())
                                .execute(&mut *tx)
                                .await?;
                        }
                        sqlx::query(sql(backend, CURRENT))
                            .bind(dag.id.as_str())
                            .bind(parse_id)
                            .execute(&mut *tx)
                            .await?;
                    }
                    DagChange::Conflict {
                        dag_id,
                        places,
                        pause,
                    } => {
                        sqlx::query(sql(backend, ENSURE))
                            .bind(dag_id.as_str())
                            .bind(&now)
                            .execute(&mut *tx)
                            .await?;
                        sqlx::query(sql(backend, CONFLICT))
                            .bind(to_json(places))
                            .bind(dag_id.as_str())
                            .execute(&mut *tx)
                            .await?;
                        sqlx::query(sql(backend, CONFLICT_SWITCH))
                            .bind(*pause)
                            .bind(dag_id.as_str())
                            .execute(&mut *tx)
                            .await?;
                    }
                    DagChange::Remove { dag_id } => {
                        sqlx::query(sql(backend, REMOVE))
                            .bind(&now)
                            .bind(dag_id.as_str())
                            .execute(&mut *tx)
                            .await?;
                        sqlx::query(sql(backend, REMOVE_SWITCH))
                            .bind(dag_id.as_str())
                            .execute(&mut *tx)
                            .await?;
                        // What its trigger passed is read only while it is live (B186).
                        sqlx::query(sql(backend, FORGET_PASSED))
                            .bind(dag_id.as_str())
                            .execute(&mut *tx)
                            .await?;
                    }
                }
            }
        });
        Ok(())
    }
}

fn stored_dag(row: DagRow) -> Result<StoredDag, DbError> {
    let (dag_id, paused, removed, conflict, version_id, parse_id, dag_json) = row;
    let current = match (version_id, parse_id, dag_json) {
        (Some(version_id), Some(parse_id), Some(json)) => Some(CurrentVersion {
            version_id,
            parse_id,
            dag: from_json("dag_versions.dag_json", &json)?,
        }),
        _ => None,
    };
    Ok(StoredDag {
        dag_id: DagId::new(dag_id).map_err(|e| DbError::InvalidData {
            what: "dags.dag_id",
            reason: e.to_string(),
        })?,
        paused,
        removed,
        conflict: conflict
            .map(|c| from_json("dags.conflict", &c))
            .transpose()?,
        current,
    })
}
