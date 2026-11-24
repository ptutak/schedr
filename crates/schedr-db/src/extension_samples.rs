//! The extensions workers ran and their metrics' samples (decision 513): what each version
//! declares, by worker, and a row a minute per worker and series, rolled up per 15 minutes
//! across workers as the processes' samples are (see [`crate::samples`]).

use std::collections::BTreeMap;

use jiff::Timestamp;
use schedr_model::extension::Declarations;
use serde::{Deserialize, Serialize};

use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, in_tx, on_pool, parse_time, sql, sql_text, to_json};

/// The rows one insert writes at most: 8 parameters each.
const ROWS_PER_INSERT: usize = 500;

/// An extension version a worker was heard running, to record (decision 521).
#[derive(Debug, Clone, PartialEq)]
pub struct SeenVersion {
    /// The extension.
    pub name: String,
    /// Its version.
    pub version: String,
    /// The worker.
    pub worker_id: String,
    /// Its declarations and series cap, when sent.
    pub declared: Option<(Declarations, u32)>,
}

/// One extension version on one worker, as it last declared itself.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtensionVersion {
    /// The extension.
    pub name: String,
    /// Its version.
    pub version: String,
    /// The worker that ran it.
    pub worker_id: String,
    /// Its metrics and page.
    pub declarations: Declarations,
    /// The most series each metric keeps.
    pub max_series: u32,
    /// When the worker last sent its figures.
    pub seen_at: Timestamp,
}

/// What one series did in a sample's step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SampleData {
    /// A counter: how much it grew.
    Counter {
        /// Its growth (over workers, their sum).
        growth: f64,
        /// A rollup's growth combined over workers otherwise (decision 516); none in a
        /// minute's sample, one worker's.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        across: Option<CounterAcross>,
    },
    /// A gauge: its average and highest value over the minutes it covers.
    Gauge {
        /// The average (over workers, of their sum).
        avg: f64,
        /// The highest.
        peak: f64,
        /// The minutes it covers.
        minutes: u32,
        /// A rollup's points of its minutes, each combined over workers (decision 516); none
        /// in a minute's sample, one worker's reading.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        across: Option<Box<GaugeAcross>>,
    },
    /// A histogram: its new observations per bucket and their sum.
    Histogram {
        /// Per bucket, not cumulative, the last past its last bound.
        counts: Vec<u64>,
        /// Their sum.
        sum: f64,
    },
}

/// A rollup's counter growth combined over workers each minute, summed over its minutes.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CounterAcross {
    /// The highest worker's.
    pub max: f64,
    /// The lowest worker's.
    pub min: f64,
    /// The workers' average.
    pub mean: f64,
}

/// A gauge's points over a rollup's minutes: its last, average, highest and lowest.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct GaugePoints {
    /// The last minute's.
    pub last: f64,
    /// The minutes' average.
    pub mean: f64,
    /// The highest minute's.
    pub max: f64,
    /// The lowest minute's.
    pub min: f64,
}

/// A rollup's gauge: each minute's readings combined over workers four ways, then its points
/// over the window's minutes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct GaugeAcross {
    /// Each minute's sum.
    pub sum: GaugePoints,
    /// Each minute's highest.
    pub max: GaugePoints,
    /// Each minute's lowest.
    pub min: GaugePoints,
    /// Each minute's average.
    pub mean: GaugePoints,
}

impl SampleData {
    /// A minute's counter sample: one worker's growth.
    #[must_use]
    pub const fn counter(growth: f64) -> Self {
        Self::Counter {
            growth,
            across: None,
        }
    }

    /// A minute's gauge sample: one worker's reading.
    #[must_use]
    pub const fn gauge(value: f64) -> Self {
        Self::Gauge {
            avg: value,
            peak: value,
            minutes: 1,
            across: None,
        }
    }

    /// `self` and `other`, two series folded into one in the same minute, as one: everything
    /// added, a gauge's value and peak too (Codex's second review of M5.2i5: two folded gauges
    /// kept the later's value, not their sum). Of different kinds (or bucket counts), `other`.
    #[must_use]
    pub fn added(self, other: Self) -> Self {
        match (self, other) {
            (
                Self::Gauge {
                    avg: a, peak: p, ..
                },
                Self::Gauge {
                    avg, peak, minutes, ..
                },
            ) => Self::Gauge {
                avg: a + avg,
                peak: p + peak,
                minutes,
                across: None,
            },
            (earlier, other) => earlier.merged(other),
        }
    }

    /// `self` and `later`, of the same series and step, as one: a counter's growth and a
    /// histogram's observations added, a gauge's latest value with the highest peak. Of
    /// different kinds (or bucket counts), `later`.
    #[must_use]
    pub fn merged(self, later: Self) -> Self {
        match (self, later) {
            (Self::Counter { growth: a, .. }, Self::Counter { growth: b, .. }) => {
                Self::counter(a + b)
            }
            (
                Self::Gauge { peak: a, .. },
                Self::Gauge {
                    avg, peak, minutes, ..
                },
            ) => Self::Gauge {
                avg,
                peak: peak.max(a),
                minutes,
                across: None,
            },
            (Self::Histogram { counts: a, sum: x }, Self::Histogram { counts: b, sum: y })
                if a.len() == b.len() =>
            {
                Self::Histogram {
                    counts: a
                        .iter()
                        .zip(&b)
                        .map(|(a, b)| a.saturating_add(*b))
                        .collect(),
                    sum: x + y,
                }
            }
            (_, later) => later,
        }
    }
}

/// One extension metric sample.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtensionSample {
    /// The extension.
    pub name: String,
    /// Its version.
    pub version: String,
    /// The worker; empty in a rollup.
    pub worker_id: String,
    /// The metric.
    pub metric: String,
    /// Its labels' values.
    pub labels: BTreeMap<String, String>,
    /// 60 or 900.
    pub step_s: u32,
    /// Its minute, or its window's start.
    pub at: Timestamp,
    /// What it did.
    pub data: SampleData,
}

type SampleRow = (String, String, String, String, String, i64, String, String);

fn sample_of(row: SampleRow) -> Result<ExtensionSample, DbError> {
    let (name, version, worker_id, metric, labels, step_s, at, data) = row;
    Ok(ExtensionSample {
        name,
        version,
        worker_id,
        metric,
        labels: from_json("extension_samples.labels", &labels)?,
        step_s: u32::try_from(step_s).unwrap_or(0),
        at: parse_time("extension_samples.at", &at)?,
        data: from_json("extension_samples.data", &data)?,
    })
}

impl Database {
    /// Keeps what `worker_id` runs of extension `name` `version`: its declarations when given,
    /// and when it was heard of.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails.
    pub async fn extension_seen(
        &self,
        name: &str,
        version: &str,
        worker_id: &str,
        declared: Option<(&Declarations, u32)>,
        now: Timestamp,
    ) -> Result<(), DbError> {
        const DECLARED: &str = "INSERT INTO extension_versions \
            (name, version, worker_id, declarations, max_series, seen_at) \
            VALUES ($1, $2, $3, $4, $5, $6{ts}) ON CONFLICT (name, version, worker_id) \
            DO UPDATE SET declarations = $4, max_series = $5, seen_at = $6{ts}";
        const SEEN: &str = "UPDATE extension_versions SET seen_at = $4{ts} \
            WHERE name = $1 AND version = $2 AND worker_id = $3";
        let now = db_time(now);
        in_tx!(self, |tx, backend| {
            if let Some((declarations, max_series)) = declared {
                crate::query::query(sql(backend, DECLARED))
                    .bind(name)
                    .bind(version)
                    .bind(worker_id)
                    .bind(to_json(declarations))
                    .bind(i64::from(max_series))
                    .bind(&now)
                    .execute(&mut *tx)
                    .await?;
            } else {
                crate::query::query(sql(backend, SEEN))
                    .bind(name)
                    .bind(version)
                    .bind(worker_id)
                    .bind(&now)
                    .execute(&mut *tx)
                    .await?;
            }
        });
        Ok(())
    }

    /// [`Self::extension_seen`] for each of `seen` (decision 521: a flush's in one transaction).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails.
    pub async fn extensions_seen(
        &self,
        seen: &[SeenVersion],
        now: Timestamp,
    ) -> Result<(), DbError> {
        const DECLARED: &str = "INSERT INTO extension_versions \
            (name, version, worker_id, declarations, max_series, seen_at) \
            VALUES ($1, $2, $3, $4, $5, $6{ts}) ON CONFLICT (name, version, worker_id) \
            DO UPDATE SET declarations = $4, max_series = $5, seen_at = $6{ts}";
        const SEEN: &str = "UPDATE extension_versions SET seen_at = $4{ts} \
            WHERE name = $1 AND version = $2 AND worker_id = $3";
        if seen.is_empty() {
            return Ok(());
        }
        let now = db_time(now);
        in_tx!(self, |tx, backend| {
            for version in seen {
                if let Some((declarations, max_series)) = &version.declared {
                    crate::query::query(sql(backend, DECLARED))
                        .bind(&version.name)
                        .bind(&version.version)
                        .bind(&version.worker_id)
                        .bind(to_json(declarations))
                        .bind(i64::from(*max_series))
                        .bind(&now)
                        .execute(&mut *tx)
                        .await?;
                } else {
                    crate::query::query(sql(backend, SEEN))
                        .bind(&version.name)
                        .bind(&version.version)
                        .bind(&version.worker_id)
                        .bind(&now)
                        .execute(&mut *tx)
                        .await?;
                }
            }
        });
        Ok(())
    }

    /// Every extension version workers ran, by name, version, then worker.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails or a row doesn't read.
    pub async fn extension_versions(&self) -> Result<Vec<ExtensionVersion>, DbError> {
        const ALL: &str = "SELECT name, version, worker_id, declarations, CAST(max_series AS BIGINT), \
            {ts<}seen_at{>ts} FROM extension_versions ORDER BY name, version, worker_id";
        let rows: Vec<(String, String, String, String, i64, String)> =
            on_pool!(self, |pool, backend| {
                crate::query::query_as(sql(backend, ALL))
                    .fetch_all(pool)
                    .await?
            });
        rows.into_iter()
            .map(
                |(name, version, worker_id, declarations, max_series, seen_at)| {
                    Ok(ExtensionVersion {
                        name,
                        version,
                        worker_id,
                        declarations: from_json("extension_versions.declarations", &declarations)?,
                        max_series: u32::try_from(max_series).unwrap_or(1),
                        seen_at: parse_time("extension_versions.seen_at", &seen_at)?,
                    })
                },
            )
            .collect()
    }

    /// Adds `samples` (minute samples, each a series' growth) to those stored for their
    /// minute: merged with what is there ([`SampleData::merged`]), so figures sent twice in a
    /// minute both count, and with each other when two fold into one series.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails, or a stored sample doesn't read.
    pub async fn add_extension_minutes(&self, samples: &[ExtensionSample]) -> Result<(), DbError> {
        // An extension's minute, all its workers' samples, in one read (decision 521): by the
        // primary key's first columns (agy's review: without `name`, a whole table's scan).
        const STORED: &str = "SELECT version, worker_id, metric, labels, data \
            FROM extension_samples WHERE name = $1 AND step_s = $2 AND at = $3{ts}";
        type Key = (String, String, String, i64, String, String);
        let mut merged: BTreeMap<Key, ExtensionSample> = BTreeMap::new();
        for sample in samples {
            let key = (
                sample.name.clone(),
                sample.version.clone(),
                sample.worker_id.clone(),
                sample.at.as_second(),
                sample.metric.clone(),
                to_json(&sample.labels),
            );
            match merged.remove(&key) {
                // In one batch: series folded together, added.
                Some(mut earlier) => {
                    earlier.data = earlier.data.added(sample.data.clone());
                    merged.insert(key, earlier);
                }
                None => {
                    merged.insert(key, sample.clone());
                }
            }
        }
        if merged.is_empty() {
            return Ok(());
        }
        let minutes: std::collections::BTreeSet<(String, i64)> = merged
            .keys()
            .map(|(name, _, _, at, _, _)| (name.clone(), *at))
            .collect();
        in_tx!(self, |tx, backend| {
            let mut rows: Vec<ExtensionSample> = Vec::new();
            for (name, second) in &minutes {
                let at = Timestamp::from_second(*second).unwrap_or(Timestamp::UNIX_EPOCH);
                let stored: Vec<(String, String, String, String, String)> =
                    crate::query::query_as(sql(backend, STORED))
                        .bind(name)
                        .bind(i64::from(crate::samples::MINUTE))
                        .bind(db_time(at))
                        .fetch_all(&mut *tx)
                        .await?;
                let stored: BTreeMap<Key, String> = stored
                    .into_iter()
                    .map(|(version, worker_id, metric, labels, data)| {
                        (
                            (name.clone(), version, worker_id, *second, metric, labels),
                            data,
                        )
                    })
                    .collect();
                for (key, sample) in &merged {
                    if (&key.0, key.3) != (name, *second) {
                        continue;
                    }
                    let mut sample = sample.clone();
                    if let Some(data) = stored.get(key) {
                        let earlier: SampleData = from_json("extension_samples.data", data)?;
                        sample.data = earlier.merged(sample.data);
                    }
                    rows.push(sample);
                }
            }
            for chunk in rows.chunks(ROWS_PER_INSERT) {
                let query = insert_query(chunk.len(), true);
                let mut insert = crate::query::query(sql_text(backend, &query));
                for sample in chunk {
                    insert = insert
                        .bind(sample.name.clone())
                        .bind(sample.version.clone())
                        .bind(sample.worker_id.clone())
                        .bind(sample.metric.clone())
                        .bind(to_json(&sample.labels))
                        .bind(i64::from(sample.step_s))
                        .bind(db_time(sample.at))
                        .bind(to_json(&sample.data));
                }
                insert.execute(&mut *tx).await?;
            }
        });
        Ok(())
    }

    /// Stores `samples`; one already stored for its key is kept.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a statement fails.
    pub async fn insert_extension_samples(
        &self,
        samples: &[ExtensionSample],
    ) -> Result<(), DbError> {
        if samples.is_empty() {
            return Ok(());
        }
        let queries: Vec<(String, &[ExtensionSample])> = samples
            .chunks(ROWS_PER_INSERT)
            .map(|chunk| (insert_query(chunk.len(), false), chunk))
            .collect();
        in_tx!(self, |tx, backend| {
            for (query, chunk) in &queries {
                let mut insert = crate::query::query(sql_text(backend, query));
                for sample in *chunk {
                    insert = insert
                        .bind(sample.name.clone())
                        .bind(sample.version.clone())
                        .bind(sample.worker_id.clone())
                        .bind(sample.metric.clone())
                        .bind(to_json(&sample.labels))
                        .bind(i64::from(sample.step_s))
                        .bind(db_time(sample.at))
                        .bind(to_json(&sample.data));
                }
                insert.execute(&mut *tx).await?;
            }
        });
        Ok(())
    }

    /// The samples of step `step_s` in `[from, to)`, of extension `name` (or all), by time.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a row doesn't read.
    pub async fn extension_samples(
        &self,
        name: Option<&str>,
        step_s: u32,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<ExtensionSample>, DbError> {
        const SAMPLES: &str = "SELECT name, version, worker_id, metric, labels, CAST(step_s AS BIGINT), \
            {ts<}at{>ts}, data FROM extension_samples \
            WHERE ($1 IS NULL OR name = $1) AND step_s = $2 AND at >= $3{ts} AND at < $4{ts} \
            ORDER BY at, name, version, worker_id, metric, labels";
        let rows: Vec<SampleRow> = on_pool!(self, |pool, backend| {
            crate::query::query_as(sql(backend, SAMPLES))
                .bind(name)
                .bind(i64::from(step_s))
                .bind(db_time(from))
                .bind(db_time(to))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(sample_of).collect()
    }

    /// Deletes the extension samples of step `step_s` older than `before`; how many.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the statement fails.
    pub async fn delete_old_extension_samples(
        &self,
        step_s: u32,
        before: Timestamp,
    ) -> Result<u64, DbError> {
        const DELETE: &str = "DELETE FROM extension_samples WHERE step_s = $1 AND at < $2{ts}";
        let deleted = in_tx!(self, |tx, backend| {
            crate::query::query(sql(backend, DELETE))
                .bind(i64::from(step_s))
                .bind(db_time(before))
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(deleted)
    }

    /// The series of `extension`'s `metric` with minute samples since `since`: their labels
    /// (for the master's series cap across workers after a takeover).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails, or stored labels don't read.
    pub async fn extension_series_since(
        &self,
        extension: &str,
        metric: &str,
        since: Timestamp,
    ) -> Result<Vec<BTreeMap<String, String>>, DbError> {
        const READ: &str = "SELECT DISTINCT labels FROM extension_samples \
            WHERE name = $1 AND metric = $2 AND step_s = $3 AND at >= $4{ts}";
        let rows: Vec<String> = on_pool!(self, |pool, backend| {
            crate::query::query_scalar(sql(backend, READ))
                .bind(extension)
                .bind(metric)
                .bind(i64::from(crate::samples::MINUTE))
                .bind(db_time(since))
                .fetch_all(pool)
                .await?
        });
        rows.iter()
            .map(|labels| from_json("extension_samples.labels", labels))
            .collect()
    }

    /// Keeps `events` a worker sent (DN19), each of an extension it ran; none past `kept` per
    /// extension (the master's `EXTENSION_EVENTS_KEPT`) are kept: the oldest go.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails.
    pub async fn insert_extension_events(
        &self,
        worker_id: &str,
        events: &[schedr_model::extension::ExtensionEvent],
        kept: usize,
    ) -> Result<(), DbError> {
        let events: Vec<(String, schedr_model::extension::ExtensionEvent)> = events
            .iter()
            .map(|e| (worker_id.to_owned(), e.clone()))
            .collect();
        self.insert_workers_extension_events(&events, kept).await
    }

    /// [`Self::insert_extension_events`] for events of several workers, each with its worker
    /// (decision 521: a flush's in multi-row inserts, then one trim per extension).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails.
    pub async fn insert_workers_extension_events(
        &self,
        events: &[(String, schedr_model::extension::ExtensionEvent)],
        kept: usize,
    ) -> Result<(), DbError> {
        // Past the newest kept: what is older than the KEPT-th newest goes.
        const TRIM: &str = "DELETE FROM extension_events WHERE name = $1 AND id IN \
            (SELECT id FROM extension_events WHERE name = $1 \
            ORDER BY at DESC, id DESC LIMIT -1 OFFSET $2)";
        const TRIM_PG: &str = "DELETE FROM extension_events WHERE name = $1 AND id IN \
            (SELECT id FROM extension_events WHERE name = $1 \
            ORDER BY at DESC, id DESC OFFSET $2)";
        if events.is_empty() {
            return Ok(());
        }
        let names: std::collections::BTreeSet<&str> =
            events.iter().map(|(_, e)| e.extension.as_str()).collect();
        let queries: Vec<(String, &[(String, schedr_model::extension::ExtensionEvent)])> = events
            .chunks(ROWS_PER_INSERT)
            .map(|chunk| (events_insert(chunk.len()), chunk))
            .collect();
        in_tx!(self, |tx, backend| {
            for (query, chunk) in &queries {
                let mut insert = crate::query::query(sql_text(backend, query));
                for (worker_id, event) in *chunk {
                    let at = Timestamp::from_millisecond(event.at_ms)
                        .unwrap_or_else(|_| Timestamp::now());
                    insert = insert
                        .bind(event.extension.clone())
                        .bind(worker_id.clone())
                        .bind(event.version.clone().unwrap_or_default())
                        .bind(event.kind.name())
                        .bind(db_time(at))
                        .bind(event.message.clone());
                }
                insert.execute(&mut *tx).await?;
            }
            let trim = if backend == crate::Backend::Postgres {
                TRIM_PG
            } else {
                TRIM
            };
            for name in &names {
                crate::query::query(sql(backend, trim))
                    .bind(*name)
                    .bind(i64::try_from(kept).unwrap_or(i64::MAX))
                    .execute(&mut *tx)
                    .await?;
            }
        });
        Ok(())
    }

    /// The extensions with events, each with its workers and when it was last heard of: for
    /// those no worker has run (a plugin skipped or refused), known only by their events.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails, or a stored time doesn't read.
    pub async fn extensions_with_events(
        &self,
    ) -> Result<Vec<(String, Vec<String>, Timestamp)>, DbError> {
        const READ: &str = "SELECT name, worker_id, {ts<}MAX(at){>ts} FROM extension_events \
            GROUP BY name, worker_id ORDER BY name, worker_id";
        let rows: Vec<(String, String, String)> = on_pool!(self, |pool, backend| {
            crate::query::query_as(sql(backend, READ))
                .fetch_all(pool)
                .await?
        });
        let mut by_name: BTreeMap<String, (Vec<String>, Timestamp)> = BTreeMap::new();
        for (name, worker_id, at) in rows {
            let at = parse_time("extension_events.at", &at)?;
            let entry = by_name.entry(name).or_insert((Vec::new(), at));
            entry.0.push(worker_id);
            entry.1 = entry.1.max(at);
        }
        Ok(by_name
            .into_iter()
            .map(|(name, (workers, at))| (name, workers, at))
            .collect())
    }

    /// `extension`'s events, `worker`'s only when given (DN21; agy's review of M5.2i5: a worker's
    /// tab cut the newest of all workers', a noisy one hiding the rest), newest first, at most
    /// `limit`.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails, or a stored row doesn't read.
    pub async fn extension_events(
        &self,
        extension: &str,
        (worker, version): (Option<&str>, Option<&str>),
        limit: u32,
    ) -> Result<Vec<StoredEvent>, DbError> {
        // Of `version` and those of none (discovered, a service's: every version's), before
        // the limit (agy's review of DN40: filtered after it, an older version's were cut).
        const READ: &str = "SELECT worker_id, version, kind, {ts<}at{>ts}, message \
            FROM extension_events WHERE name = $1 AND ($3{text} IS NULL OR worker_id = $3) \
            AND ($4{text} IS NULL OR version = '' OR version = $4) \
            ORDER BY at DESC, id DESC LIMIT $2";
        let rows: Vec<(String, String, String, String, String)> =
            on_pool!(self, |pool, backend| {
                crate::query::query_as(sql(backend, READ))
                    .bind(extension)
                    .bind(i64::from(limit))
                    .bind(worker)
                    .bind(version)
                    .fetch_all(pool)
                    .await?
            });
        rows.into_iter()
            .map(|(worker_id, version, kind, at, message)| {
                Ok(StoredEvent {
                    worker_id,
                    version,
                    kind: schedr_model::extension::EventKind::parse(&kind).ok_or_else(|| {
                        DbError::InvalidData {
                            what: "extension_events.kind",
                            reason: format!("`{kind}`"),
                        }
                    })?,
                    at: parse_time("extension_events.at", &at)?,
                    message,
                })
            })
            .collect()
    }

    /// Deletes extension events older than `before`, with the samples (DN19). How many.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails.
    pub async fn delete_old_extension_events(&self, before: Timestamp) -> Result<u64, DbError> {
        const DELETE: &str = "DELETE FROM extension_events WHERE at < $1{ts}";
        let deleted = in_tx!(self, |tx, backend| {
            crate::query::query(sql(backend, DELETE))
                .bind(db_time(before))
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(deleted)
    }
}

/// An extension's event as kept (DN19).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    /// The worker it happened on.
    pub worker_id: String,
    /// The extension's version; empty when not known.
    pub version: String,
    /// What happened.
    pub kind: schedr_model::extension::EventKind,
    /// When.
    pub at: Timestamp,
    /// What about it.
    pub message: String,
}

/// The insert of `rows` samples: one already stored kept (`replace` false: a rollup run again),
/// or replaced by the new one (a minute's merged sample).
/// A multi-row insert of `rows` extension events.
fn events_insert(rows: usize) -> String {
    use std::fmt::Write as _;
    let mut query = String::from(
        "INSERT INTO extension_events (name, worker_id, version, kind, at, message) VALUES ",
    );
    for row in 0..rows {
        let n = row * 6;
        if row > 0 {
            query.push_str(", ");
        }
        let _ = write!(
            query,
            "(${}, ${}, ${}, ${}, ${}{{ts}}, ${})",
            n + 1,
            n + 2,
            n + 3,
            n + 4,
            n + 5,
            n + 6
        );
    }
    query
}

fn insert_query(rows: usize, replace: bool) -> String {
    let mut query = String::from(
        "INSERT INTO extension_samples \
         (name, version, worker_id, metric, labels, step_s, at, data) VALUES ",
    );
    for row in 0..rows {
        let n = row * 8;
        if row > 0 {
            query.push_str(", ");
        }
        let cell = |k: usize| {
            if k == 7 {
                format!("${}{{ts}}", n + k)
            } else {
                format!("${}", n + k)
            }
        };
        query.push('(');
        query.push_str(&(1..=8).map(cell).collect::<Vec<_>>().join(", "));
        query.push(')');
    }
    query.push_str(" ON CONFLICT (name, step_s, at, version, worker_id, metric, labels)");
    query.push_str(if replace {
        " DO UPDATE SET data = excluded.data"
    } else {
        " DO NOTHING"
    });
    query
}

#[cfg(test)]
mod tests;
