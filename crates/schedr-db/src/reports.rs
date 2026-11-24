//! What a running attempt reported (decision 513): its statuses, each with its time, and its
//! links by label. Written as the master hears them; read with the attempt.

use jiff::Timestamp;

use crate::database::{Database, DbError};
use crate::query::{db_time, in_tx, on_pool, parse_time, sql};

/// The most statuses an attempt keeps: past it, the oldest after the first goes.
pub const MAX_STATUSES: i64 = 100;
/// The most links an attempt keeps.
pub const MAX_LINKS: i64 = 10;

/// One status an attempt reported.
#[derive(Debug, Clone, PartialEq)]
pub struct AttemptStatus {
    /// When the worker got it.
    pub at: Timestamp,
    /// Its phase.
    pub phase: String,
    /// More about it.
    pub message: Option<String>,
    /// How far along, 0 to 1.
    pub progress: Option<f64>,
}

/// One link an attempt reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptLink {
    /// What it links to.
    pub label: String,
    /// Its URL.
    pub url: String,
}

/// A status a worker reported for one of its attempts, to store (decision 521).
#[derive(Debug, Clone, PartialEq)]
pub struct ReportedStatus {
    /// The attempt.
    pub attempt_id: String,
    /// The worker that reported it: kept only when the attempt is its.
    pub worker_id: String,
    /// The extension whose plugin reported it.
    pub extension: String,
    /// Its version (DN40, B292).
    pub version: String,
    /// The status.
    pub status: AttemptStatus,
}

/// A link a worker reported for one of its attempts, to store (decision 521).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportedLink {
    /// The attempt.
    pub attempt_id: String,
    /// The worker that reported it: kept only when the attempt is its.
    pub worker_id: String,
    /// The link.
    pub link: AttemptLink,
}

impl Database {
    /// Appends a status to `attempt_id`'s, keeping at most [`MAX_STATUSES`]: past it, the
    /// oldest after the first goes, so the first phase and the latest stay.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails.
    pub async fn add_attempt_status(
        &self,
        attempt_id: &str,
        (extension, version): (&str, &str),
        status: &AttemptStatus,
    ) -> Result<(), DbError> {
        const NEXT: &str = "SELECT CAST(COALESCE(MAX(seq) + 1, 0) AS BIGINT), COUNT(*) FROM attempt_statuses \
            WHERE attempt_id = $1{uuid}";
        const ADD: &str = "INSERT INTO attempt_statuses \
            (attempt_id, seq, at, phase, message, progress, extension, extension_version) \
            VALUES ($1{uuid}, $2, $3{ts}, $4, $5, $6, $7, $8)";
        const DROP_SECOND: &str = "DELETE FROM attempt_statuses WHERE attempt_id = $1{uuid} \
            AND seq = (SELECT seq FROM attempt_statuses WHERE attempt_id = $1{uuid} \
            ORDER BY seq LIMIT 1 OFFSET 1)";
        let at = db_time(status.at);
        in_tx!(self, |tx, backend| {
            let (next, count): (i64, i64) = crate::query::query_as(sql(backend, NEXT))
                .bind(attempt_id)
                .fetch_one(&mut *tx)
                .await?;
            if count >= MAX_STATUSES {
                crate::query::query(sql(backend, DROP_SECOND))
                    .bind(attempt_id)
                    .execute(&mut *tx)
                    .await?;
            }
            crate::query::query(sql(backend, ADD))
                .bind(attempt_id)
                .bind(i32::try_from(next).unwrap_or(i32::MAX))
                .bind(&at)
                .bind(&status.phase)
                .bind(status.message.as_deref())
                .bind(status.progress)
                .bind(extension)
                .bind(version)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Sets `attempt_id`'s link labelled `label`, replacing its URL; a new label past
    /// [`MAX_LINKS`] is not kept. Whether it was.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails.
    pub async fn set_attempt_link(
        &self,
        attempt_id: &str,
        link: &AttemptLink,
    ) -> Result<bool, DbError> {
        const REPLACE: &str = "UPDATE attempt_links SET url = $3 \
            WHERE attempt_id = $1{uuid} AND label = $2";
        const COUNT: &str = "SELECT COUNT(*) FROM attempt_links WHERE attempt_id = $1{uuid}";
        const ADD: &str = "INSERT INTO attempt_links (attempt_id, label, url, position) \
            VALUES ($1{uuid}, $2, $3, $4)";
        let kept = in_tx!(self, |tx, backend| {
            let replaced = crate::query::query(sql(backend, REPLACE))
                .bind(attempt_id)
                .bind(&link.label)
                .bind(&link.url)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            if replaced > 0 {
                true
            } else {
                let count: i64 = crate::query::query_scalar(sql(backend, COUNT))
                    .bind(attempt_id)
                    .fetch_one(&mut *tx)
                    .await?;
                if count >= MAX_LINKS {
                    false
                } else {
                    crate::query::query(sql(backend, ADD))
                        .bind(attempt_id)
                        .bind(&link.label)
                        .bind(&link.url)
                        .bind(i32::try_from(count).unwrap_or(i32::MAX))
                        .execute(&mut *tx)
                        .await?;
                    true
                }
            }
        });
        Ok(kept)
    }

    /// Stores a flush of reports at once (decision 521): one transaction, each kept only when
    /// its attempt is the reporting worker's (checked in the statements, no read first), each
    /// status appended as [`Self::add_attempt_status`] does and each link set as
    /// [`Self::set_attempt_link`] does. How many of each were kept. An attempt ended meanwhile
    /// still takes them: written off the round, its last reports, sent before its end, come
    /// after it.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails.
    pub async fn add_attempt_reports(
        &self,
        statuses: &[ReportedStatus],
        links: &[ReportedLink],
    ) -> Result<(usize, usize), DbError> {
        // The worker's attempt.
        const OWNED: &str = "EXISTS (SELECT 1 FROM attempts WHERE id = $1{uuid} \
            AND worker_id = $2)";
        let add = format!(
            "INSERT INTO attempt_statuses (attempt_id, seq, at, phase, message, progress, \
             extension, extension_version) SELECT $1{{uuid}}, COALESCE((SELECT MAX(seq) + 1 \
             FROM attempt_statuses WHERE attempt_id = $1{{uuid}}), 0), $3{{ts}}, $4, $5, $6, $7, $8 \
             WHERE {OWNED}"
        );
        // Past MAX_STATUSES, the oldest after the first goes.
        let drop_second = format!(
            "DELETE FROM attempt_statuses WHERE attempt_id = $1{{uuid}} \
             AND (SELECT COUNT(*) FROM attempt_statuses WHERE attempt_id = $1{{uuid}}) > {MAX_STATUSES} \
             AND seq = (SELECT seq FROM attempt_statuses WHERE attempt_id = $1{{uuid}} \
             ORDER BY seq LIMIT 1 OFFSET 1)"
        );
        let replace = format!(
            "UPDATE attempt_links SET url = $4 WHERE attempt_id = $1{{uuid}} AND label = $3 \
             AND {OWNED}"
        );
        let add_link = format!(
            "INSERT INTO attempt_links (attempt_id, label, url, position) SELECT $1{{uuid}}, $3, \
             $4, CAST((SELECT COUNT(*) FROM attempt_links WHERE attempt_id = $1{{uuid}}) AS INTEGER) \
             WHERE {OWNED} AND (SELECT COUNT(*) FROM attempt_links WHERE attempt_id = $1{{uuid}}) \
             < {MAX_LINKS}"
        );
        if statuses.is_empty() && links.is_empty() {
            return Ok((0, 0));
        }
        let kept = in_tx!(self, |tx, backend| {
            let mut kept = (0, 0);
            for reported in statuses {
                let status = &reported.status;
                let added = crate::query::query(crate::query::sql_text(backend, &add))
                    .bind(&reported.attempt_id)
                    .bind(&reported.worker_id)
                    .bind(db_time(status.at))
                    .bind(&status.phase)
                    .bind(status.message.as_deref())
                    .bind(status.progress)
                    .bind(&reported.extension)
                    .bind(&reported.version)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();
                if added > 0 {
                    kept.0 += 1;
                    crate::query::query(crate::query::sql_text(backend, &drop_second))
                        .bind(&reported.attempt_id)
                        .execute(&mut *tx)
                        .await?;
                }
            }
            for reported in links {
                let link = &reported.link;
                let mut set = crate::query::query(crate::query::sql_text(backend, &replace))
                    .bind(&reported.attempt_id)
                    .bind(&reported.worker_id)
                    .bind(&link.label)
                    .bind(&link.url)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();
                if set == 0 {
                    set = crate::query::query(crate::query::sql_text(backend, &add_link))
                        .bind(&reported.attempt_id)
                        .bind(&reported.worker_id)
                        .bind(&link.label)
                        .bind(&link.url)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected();
                }
                if set > 0 {
                    kept.1 += 1;
                }
            }
            kept
        });
        Ok(kept)
    }

    /// `attempt_id`'s statuses, oldest first, and its links, in the order first reported.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails, or a stored time doesn't read.
    pub async fn attempt_reports(
        &self,
        attempt_id: &str,
    ) -> Result<(Vec<AttemptStatus>, Vec<AttemptLink>), DbError> {
        const STATUSES: &str = "SELECT {ts<}at{>ts}, phase, message, progress FROM attempt_statuses \
            WHERE attempt_id = $1{uuid} ORDER BY seq";
        const LINKS: &str = "SELECT label, url FROM attempt_links WHERE attempt_id = $1{uuid} \
            ORDER BY position";
        type StatusRow = (String, String, Option<String>, Option<f64>);
        let statuses: Vec<StatusRow> = on_pool!(self, |pool, backend| {
            crate::query::query_as(sql(backend, STATUSES))
                .bind(attempt_id)
                .fetch_all(pool)
                .await?
        });
        let links: Vec<(String, String)> = on_pool!(self, |pool, backend| {
            crate::query::query_as(sql(backend, LINKS))
                .bind(attempt_id)
                .fetch_all(pool)
                .await?
        });
        let statuses = statuses
            .into_iter()
            .map(|(at, phase, message, progress)| {
                Ok(AttemptStatus {
                    at: parse_time("attempt_statuses.at", &at)?,
                    phase,
                    message,
                    progress,
                })
            })
            .collect::<Result<_, DbError>>()?;
        let links = links
            .into_iter()
            .map(|(label, url)| AttemptLink { label, url })
            .collect();
        Ok((statuses, links))
    }

    /// `extension`'s attempts with a status, newest first, at most `limit`: each one's latest
    /// (DN19).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails, or a stored time doesn't read.
    pub async fn extension_attempts(
        &self,
        extension: &str,
        (worker, version): (Option<&str>, Option<&str>),
        limit: u32,
    ) -> Result<Vec<ExtensionAttempt>, DbError> {
        // `worker`'s only when given (DN21; agy's review of M5.2i5: a worker's tab listed the
        // attempts other workers ran), and `version`'s (DN40, B292).
        type Row = (
            String,
            String,
            String,
            String,
            i64,
            i64,
            Option<String>,
            String,
            String,
            Option<String>,
            Option<f64>,
        );
        // A direct `extension_version = $4` when a version is given, so the index
        // (extension, extension_version, at) serves it (agy's review of B292: an `IS NULL OR`
        // kept it from the index, every other version's rows read).
        const SELECT: &str = "SELECT a.id{text}, r.dag_id, r.id{text}, ti.task_id, \
            CAST(ti.map_index AS BIGINT), CAST(a.try_number AS BIGINT), a.status, \
            {ts<}s.at{>ts}, s.phase, s.message, s.progress \
            FROM attempt_statuses s \
            JOIN attempts a ON a.id = s.attempt_id \
            JOIN task_instances ti ON ti.id = a.task_instance_id \
            JOIN runs r ON r.id = ti.run_id \
            WHERE s.extension = $1 AND s.seq = \
            (SELECT MAX(l.seq) FROM attempt_statuses l WHERE l.attempt_id = s.attempt_id) \
            AND ($3{text} IS NULL OR a.worker_id = $3)";
        let latest = if version.is_some() {
            format!("{SELECT} AND s.extension_version = $4 ORDER BY s.at DESC, a.id DESC LIMIT $2")
        } else {
            format!("{SELECT} ORDER BY s.at DESC, a.id DESC LIMIT $2")
        };
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            let query = crate::query::query_as(crate::query::sql_text(backend, &latest))
                .bind(extension)
                .bind(i64::from(limit))
                .bind(worker);
            match version {
                Some(version) => query.bind(version),
                None => query,
            }
            .fetch_all(pool)
            .await?
        });
        rows.into_iter()
            .map(
                |(
                    attempt_id,
                    dag_id,
                    run_id,
                    task_id,
                    map_index,
                    try_number,
                    ended,
                    at,
                    phase,
                    message,
                    progress,
                )| {
                    Ok(ExtensionAttempt {
                        attempt_id,
                        dag_id,
                        run_id,
                        task_id,
                        map_index,
                        try_number,
                        ended,
                        status: AttemptStatus {
                            at: parse_time("attempt_statuses.at", &at)?,
                            phase,
                            message,
                            progress,
                        },
                    })
                },
            )
            .collect()
    }
}

/// An extension's attempt and the latest status it reported (DN19).
#[derive(Debug, Clone, PartialEq)]
pub struct ExtensionAttempt {
    /// The attempt.
    pub attempt_id: String,
    /// Its DAG.
    pub dag_id: String,
    /// Its run.
    pub run_id: String,
    /// Its task.
    pub task_id: String,
    /// Its map index, -1 for a task that isn't mapped.
    pub map_index: i64,
    /// Its try.
    pub try_number: i64,
    /// How it ended; none while it runs.
    pub ended: Option<String>,
    /// Its latest status.
    pub status: AttemptStatus,
}
