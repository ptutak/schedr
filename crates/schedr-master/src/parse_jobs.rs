//! Parse jobs: each (root, content hash, schedr version) is parsed once, by one worker that
//! reported it.
//!
//! Jobs live in the master's memory: after a restart, workers report their hashes again and a
//! hash with no stored parse gets a new job. A job whose worker is lost, or that isn't
//! finished within the job timeout, goes to another worker that reported the same hash (see
//! PROJECT.md, DAG ID conflicts: no duplicate parsing).

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use jiff::Timestamp;
use uuid::Uuid;

use crate::metrics::MasterMetrics;

#[cfg(test)]
mod tests;

/// How long a worker has for a parse job before it goes to another worker (or the same one
/// again, if no other reported the hash).
pub const PARSE_JOB_TIMEOUT: Duration = Duration::from_mins(10);

/// What a parse job parses.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ParseKey {
    /// The root's name.
    pub root: String,
    /// The content hash.
    pub content_hash: String,
    /// The schedr version of the workers that reported it.
    pub schedr_version: String,
}

/// A parse job given to a worker: what `AssignParse` sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseAssignment {
    /// The job's ID.
    pub job_id: String,
    /// The worker that parses it.
    pub worker_id: String,
    /// What it parses.
    pub key: ParseKey,
}

#[derive(Debug, Clone)]
struct Job {
    id: String,
    worker: Option<(String, Timestamp)>,
    candidates: BTreeSet<String>,
    /// When it was created (for its duration).
    created: Instant,
}

/// The parse jobs not finished yet.
#[derive(Debug, Clone)]
pub struct ParseJobs {
    timeout: Duration,
    jobs: BTreeMap<ParseKey, Job>,
    metrics: MasterMetrics,
}

impl ParseJobs {
    /// No jobs, with the given job timeout.
    #[must_use]
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            jobs: BTreeMap::new(),
            metrics: MasterMetrics::default(),
        }
    }

    /// Counts jobs and their durations in `metrics`.
    pub fn set_metrics(&mut self, metrics: MasterMetrics) {
        self.metrics = metrics;
    }

    /// A worker reported a hash with no stored parse: the job is created and given to it,
    /// unless a job for the hash exists (the worker can then take it over later).
    pub fn request(
        &mut self,
        key: ParseKey,
        worker_id: &str,
        now: Timestamp,
    ) -> Option<ParseAssignment> {
        let metrics = &self.metrics;
        let job = self
            .jobs
            .entry(key.clone())
            .or_insert_with(|| new_job(metrics));
        job.candidates.insert(worker_id.to_owned());
        if job.worker.is_some() {
            return None;
        }
        job.worker = Some((worker_id.to_owned(), now));
        Some(ParseAssignment {
            job_id: job.id.clone(),
            worker_id: worker_id.to_owned(),
            key,
        })
    }

    /// A worker that takes no parse jobs (draining) reported a hash with no stored parse: the
    /// job exists, for the next worker that reports the hash to take.
    pub fn wait(&mut self, key: ParseKey) {
        let metrics = &self.metrics;
        self.jobs.entry(key).or_insert_with(|| new_job(metrics));
    }

    /// The parse of `key` was stored (by any worker, under any job): its job is done.
    /// Returns the finished job's ID, if there was one.
    pub fn finish(&mut self, key: &ParseKey) -> Option<String> {
        let job = self.jobs.remove(key)?;
        self.metrics.parse_jobs_finished.inc();
        self.metrics
            .parse_duration
            .observe(job.created.elapsed().as_secs_f64());
        Some(job.id)
    }

    /// A worker is gone: it is no longer a candidate, and its jobs go to other candidates
    /// (or wait for one).
    pub fn worker_lost(&mut self, worker_id: &str, now: Timestamp) -> Vec<ParseAssignment> {
        let mut assignments = Vec::new();
        for (key, job) in &mut self.jobs {
            job.candidates.remove(worker_id);
            if job.worker.as_ref().is_some_and(|(w, _)| w == worker_id) {
                job.worker = None;
                if let Some(next) = job.candidates.iter().next().cloned() {
                    assignments.push(assign(key, job, next, now));
                }
            }
        }
        self.jobs.retain(|_, job| !job.candidates.is_empty());
        assignments
    }

    /// Jobs not finished within the timeout go to the next candidate after their worker (the
    /// same worker if it is the only one).
    pub fn expire(&mut self, now: Timestamp) -> Vec<ParseAssignment> {
        let mut assignments = Vec::new();
        for (key, job) in &mut self.jobs {
            let Some((worker, since)) = &job.worker else {
                continue;
            };
            if now.duration_since(*since).unsigned_abs() < self.timeout {
                continue;
            }
            let next = job
                .candidates
                .range::<String, _>((
                    std::ops::Bound::Excluded(worker),
                    std::ops::Bound::Unbounded,
                ))
                .chain(job.candidates.iter())
                .next()
                .cloned()
                .unwrap_or_else(|| worker.clone());
            assignments.push(assign(key, job, next, now));
        }
        assignments
    }

    /// The number of jobs not finished.
    #[must_use]
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    /// Whether every job is finished.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
}

fn assign(key: &ParseKey, job: &mut Job, worker: String, now: Timestamp) -> ParseAssignment {
    job.worker = Some((worker.clone(), now));
    ParseAssignment {
        job_id: job.id.clone(),
        worker_id: worker,
        key: key.clone(),
    }
}

/// A new job, counted in `metrics`.
fn new_job(metrics: &MasterMetrics) -> Job {
    metrics.parse_jobs_started.inc();
    Job {
        id: Uuid::now_v7().to_string(),
        worker: None,
        candidates: BTreeSet::new(),
        created: Instant::now(),
    }
}
