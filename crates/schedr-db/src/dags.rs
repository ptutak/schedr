//! The DAG catalog's storage: parse results with their files, DAGs and DAG versions.
//!
//! A parse result is stored as the worker reported it (after the master's validation); the
//! master works out the DAG list from the current parse of each root and stores the changes
//! ([`DagChange`]). Each DAG version belongs to one parse (see PROJECT.md, DAG code delivery
//! and DAG ID conflicts).

use jiff::Timestamp;
use schedr_model::runner::ParsedFile;
use schedr_model::{Dag, DagId, DagLocation};
use sqlx::AssertSqlSafe;
use uuid::Uuid;

use crate::backend::Backend;
use crate::database::{Database, DbError};

#[cfg(test)]
mod tests;

/// Runs `$body` with `$pool` bound to the pool and `$backend` to its [`Backend`], once per
/// backend (the queries are the same text, typed per driver).
macro_rules! on_pool {
    ($db:expr, |$pool:ident, $backend:ident| $body:expr) => {
        match $db {
            Database::Sqlite($pool) => {
                let $backend = Backend::Sqlite;
                $body
            }
            Database::Postgres($pool) => {
                let $backend = Backend::Postgres;
                $body
            }
        }
    };
}

/// A query in this module's dialect for one backend: `{uuid}`, `{ts}` and `{json}` after a
/// parameter cast it for Postgres (whose columns are typed) and vanish for SQLite (text
/// columns); `{text}` after a column reads it as text.
fn sql(backend: Backend, query: &'static str) -> AssertSqlSafe<String> {
    let casts: [(&str, &str); 4] = match backend {
        Backend::Sqlite => [("{uuid}", ""), ("{ts}", ""), ("{json}", ""), ("{text}", "")],
        Backend::Postgres => [
            ("{uuid}", "::uuid"),
            ("{ts}", "::timestamptz"),
            ("{json}", "::jsonb"),
            ("{text}", "::text"),
        ],
    };
    let text = casts.iter().fold(query.to_owned(), |text, (marker, cast)| {
        text.replace(marker, cast)
    });
    // Safe: built only from the static queries of this module and fixed casts.
    AssertSqlSafe(text)
}

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
    /// Whether it is paused.
    pub paused: bool,
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
    /// once per parse), and the DAG is no longer removed or in conflict. A new DAG is stored
    /// paused; `pause` pauses an existing one too.
    Define {
        /// The definition.
        dag: Box<Dag>,
        /// The parse it comes from.
        parse_id: String,
        /// Pause the DAG (a removed DAG that came back).
        pause: bool,
    },
    /// The DAG ID is defined in more than one place: it can't run. A new DAG is stored paused.
    Conflict {
        /// The DAG's ID.
        dag_id: DagId,
        /// Every place defining it.
        places: Vec<DagLocation>,
        /// Pause the DAG (a removed DAG that came back).
        pause: bool,
    },
    /// The DAG is no longer defined: hidden, its history kept.
    Remove {
        /// The DAG's ID.
        dag_id: DagId,
    },
}

/// The row of a parse file: path, DAGs JSON, error JSON.
type FileRow = (String, String, Option<String>);

/// The row of a stored DAG with its current version.
type DagRow = (
    String,
    bool,
    bool,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

impl Database {
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
        const FILE: &str = "INSERT INTO parse_files (parse_result_id, path, dags, error) \
            VALUES ($1{uuid}, $2, $3{json}, $4{json})";
        let id = Uuid::now_v7().to_string();
        let inserted = on_pool!(self, |pool, backend| {
            let mut tx = pool.begin().await?;
            let done = sqlx::query(sql(backend, INSERT))
                .bind(&id)
                .bind(parse.root)
                .bind(parse.content_hash)
                .bind(parse.schedr_version)
                .bind(parse.worker_id)
                .bind(parse.parsed_at.to_string())
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
                        .execute(&mut *tx)
                        .await?;
                }
            }
            tx.commit().await?;
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

    /// The files of a stored parse, in path order.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails, [`DbError::InvalidData`] if a stored file doesn't
    /// read back.
    pub async fn parse_files(&self, parse_id: &str) -> Result<Vec<ParsedFile>, DbError> {
        const FILES: &str = "SELECT path, dags{text}, error{text} FROM parse_files \
            WHERE parse_result_id = $1{uuid} ORDER BY path";
        let rows: Vec<FileRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, FILES))
                .bind(parse_id)
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|(path, dags, error)| {
                Ok(ParsedFile {
                    dags: from_json("parse_files.dags", &dags)?,
                    error: error
                        .map(|e| from_json("parse_files.error", &e))
                        .transpose()?,
                    path,
                })
            })
            .collect()
    }

    /// Every stored DAG (removed ones too), with its current version, in DAG ID order.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails, [`DbError::InvalidData`] if stored JSON doesn't
    /// read back.
    pub async fn stored_dags(&self) -> Result<Vec<StoredDag>, DbError> {
        const DAGS: &str = "SELECT d.dag_id, d.paused, d.removed_at IS NOT NULL, \
            d.conflict{text}, v.id{text}, v.parse_result_id{text}, v.dag_json{text} \
            FROM dags d LEFT JOIN dag_versions v ON v.id = d.current_version_id \
            ORDER BY d.dag_id";
        let rows: Vec<DagRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, DAGS)).fetch_all(pool).await?
        });
        rows.into_iter().map(stored_dag).collect()
    }

    /// Stores changes to the DAG list, all or none.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails (nothing is stored then).
    pub async fn apply_dag_changes(
        &self,
        changes: &[DagChange],
        now: Timestamp,
    ) -> Result<(), DbError> {
        const ENSURE: &str = "INSERT INTO dags (dag_id, paused, first_seen_at) \
            VALUES ($1, TRUE, $2{ts}) ON CONFLICT (dag_id) DO NOTHING";
        const VERSION: &str = "INSERT INTO dag_versions \
            (id, dag_id, parse_result_id, dag_json, created_at) \
            VALUES ($1{uuid}, $2, $3{uuid}, $4{json}, $5{ts}) \
            ON CONFLICT (dag_id, parse_result_id) DO NOTHING";
        const CURRENT: &str = "UPDATE dags SET current_version_id = (SELECT id FROM dag_versions \
            WHERE dag_id = $1 AND parse_result_id = $2{uuid}), \
            removed_at = NULL, conflict = NULL, paused = (paused OR $3) WHERE dag_id = $1";
        const CONFLICT: &str = "UPDATE dags SET conflict = $1{json}, removed_at = NULL, \
            paused = (paused OR $2) WHERE dag_id = $3";
        const REMOVE: &str =
            "UPDATE dags SET removed_at = $1{ts}, conflict = NULL WHERE dag_id = $2";
        let now = now.to_string();
        on_pool!(self, |pool, backend| {
            let mut tx = pool.begin().await?;
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
                        sqlx::query(sql(backend, VERSION))
                            .bind(Uuid::now_v7().to_string())
                            .bind(dag.id.as_str())
                            .bind(parse_id)
                            .bind(to_json(dag))
                            .bind(&now)
                            .execute(&mut *tx)
                            .await?;
                        sqlx::query(sql(backend, CURRENT))
                            .bind(dag.id.as_str())
                            .bind(parse_id)
                            .bind(*pause)
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
                    }
                }
            }
            tx.commit().await?;
        });
        Ok(())
    }
}

fn to_json(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).expect("model types always serialize")
}

fn from_json<T: serde::de::DeserializeOwned>(what: &'static str, text: &str) -> Result<T, DbError> {
    serde_json::from_str(text).map_err(|e| DbError::InvalidData {
        what,
        reason: e.to_string(),
    })
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
