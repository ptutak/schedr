//! Parse jobs: each (root, content hash, schedr version) is parsed once, by one worker that
//! reported it. A parse whose files failed **transiently** (a timeout, a runner that died) is
//! parsed again (decision 72.2), after a delay that doubles with each retry up to
//! [`MAX_RETRY_DELAY`], preferring another worker: without a limit, since the code may be fine.
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

/// The delay before the first retry of a parse that failed transiently.
pub const FIRST_RETRY_DELAY: Duration = Duration::from_secs(30);

/// The longest delay between two retries of a parse that keeps failing transiently.
pub const MAX_RETRY_DELAY: Duration = Duration::from_mins(10);

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
    /// A retry: not handed out before this time.
    not_before: Option<Timestamp>,
    /// A retry: the worker whose parse failed, the last one to get it if others can.
    failed_on: Option<String>,
    /// How many times the parse was retried (for the delay).
    retries: u32,
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
        if job.worker.is_some() || job.not_before.is_some_and(|at| at > now) {
            return None;
        }
        // A retry goes to another candidate than the worker that failed, if there is one: the
        // round's expiry hands it out.
        if job.failed_on.as_deref() == Some(worker_id) && job.candidates.len() > 1 {
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

    /// The stored parse of `key` has files that failed transiently (on `failed_on`, if known):
    /// a retry is planned, for the `candidates` (the workers reporting the key), handed out by
    /// [`Self::expire`] once its delay is over. A job of `key` is finished first.
    pub fn retry(
        &mut self,
        key: ParseKey,
        candidates: BTreeSet<String>,
        failed_on: Option<&str>,
        now: Timestamp,
    ) {
        // The first retry when the job failing wasn't one itself.
        let retries = self
            .jobs
            .get(&key)
            .filter(|job| job.not_before.is_some())
            .map_or(0, |job| job.retries + 1);
        self.finish(&key);
        let delay = FIRST_RETRY_DELAY
            .saturating_mul(2_u32.saturating_pow(retries.min(16)))
            .min(MAX_RETRY_DELAY);
        let delay = jiff::SignedDuration::try_from(delay).unwrap_or(jiff::SignedDuration::MAX);
        let mut job = new_job(&self.metrics);
        job.candidates = candidates;
        job.not_before = Some(now.saturating_add(delay).unwrap_or(Timestamp::MAX));
        job.failed_on = failed_on.map(str::to_owned);
        job.retries = retries;
        self.jobs.insert(key, job);
    }

    /// A worker reports other code than `key` now: it can't parse `key` any more.
    /// If the job was its, it is handed out again at the next round ([`Self::expire`]); the
    /// agent drops a job for code it no longer has, without an answer.
    pub fn moved_away(&mut self, key: &ParseKey, worker_id: &str, now: Timestamp) {
        let Some(job) = self.jobs.get_mut(key) else {
            return;
        };
        job.candidates.remove(worker_id);
        // A first parse nobody reports any more isn't needed; a retry waits for a reporter.
        if job.candidates.is_empty() && job.not_before.is_none() {
            self.jobs.remove(key);
            return;
        }
        if job.worker.as_ref().is_some_and(|(w, _)| w == worker_id) {
            job.worker = None;
            job.not_before = Some(job.not_before.map_or(now, |at| at.min(now)));
        }
    }

    /// Whether `key` has a job (a retry included).
    #[must_use]
    pub fn has(&self, key: &ParseKey) -> bool {
        self.jobs.contains_key(key)
    }

    /// A worker is gone: it is no longer a candidate, and its jobs go to other candidates
    /// (or wait for one).
    pub fn worker_lost(&mut self, worker_id: &str, now: Timestamp) -> Vec<ParseAssignment> {
        let mut assignments = Vec::new();
        for (key, job) in &mut self.jobs {
            job.candidates.remove(worker_id);
            if job.worker.as_ref().is_some_and(|(w, _)| w == worker_id) {
                job.worker = None;
                if let Some(next) = next_candidate(job) {
                    assignments.push(assign(key, job, next, now));
                }
            }
        }
        // A retry waits for a worker reporting its hash (one resumed, or registered again).
        self.jobs
            .retain(|_, job| !job.candidates.is_empty() || job.not_before.is_some());
        assignments
    }

    /// Jobs not finished within the timeout go to the next candidate after their worker (the
    /// same worker if it is the only one); retries whose delay is over go to a candidate other
    /// than the worker that failed, if there is one.
    pub fn expire(&mut self, now: Timestamp) -> Vec<ParseAssignment> {
        let mut assignments = Vec::new();
        for (key, job) in &mut self.jobs {
            let Some((worker, since)) = &job.worker else {
                if job.not_before.is_some_and(|at| at <= now)
                    && let Some(next) = next_candidate(job)
                {
                    assignments.push(assign(key, job, next, now));
                }
                continue;
            };
            if now.duration_since(*since).unsigned_abs() < self.timeout {
                continue;
            }
            // The candidates after its worker in turn; a retry passes over the worker whose
            // parse failed while another can take it.
            let in_turn: Vec<&String> = job
                .candidates
                .range::<String, _>((
                    std::ops::Bound::Excluded(worker),
                    std::ops::Bound::Unbounded,
                ))
                .chain(job.candidates.iter())
                .collect();
            let next = in_turn
                .iter()
                .find(|c| job.failed_on.as_ref() != Some(**c))
                .or_else(|| in_turn.first())
                .map_or_else(|| worker.clone(), |c| (*c).clone());
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

/// The candidate a job goes to: one other than the worker whose parse failed, if there is one.
fn next_candidate(job: &Job) -> Option<String> {
    job.candidates
        .iter()
        .find(|c| job.failed_on.as_ref() != Some(*c))
        .or_else(|| job.candidates.iter().next())
        .cloned()
}

/// A new job, counted in `metrics`.
fn new_job(metrics: &MasterMetrics) -> Job {
    metrics.parse_jobs_started.inc();
    Job {
        id: Uuid::now_v7().to_string(),
        worker: None,
        candidates: BTreeSet::new(),
        created: Instant::now(),
        not_before: None,
        failed_on: None,
        retries: 0,
    }
}
