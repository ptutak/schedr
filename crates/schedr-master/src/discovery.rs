//! DAG discovery on the master: reported hashes, parse jobs, uploaded parses and the DAG list.
//!
//! A worker reports each root's content hash (at registration and on every change); a hash
//! with no stored parse for the worker's schedr version gets a parse job. Workers can report
//! different hashes for a root (git-sync not everywhere yet): its **current** parse is that of
//! the **newest code** an alive worker reports, newest meaning the hash workers **switched to
//! most recently** (recorded in the database, so a master restart decides the same). A switch
//! is a worker moving **away from the current hash**: a revert to earlier content is one, so
//! it is newest, but a lagging worker catching up through older revisions (or reporting older
//! code again) isn't, and changes nothing. A hash never seen before is newest when first
//! reported. Workers on a hash newer than the current one (its parse on its way) may run the
//! root's tasks too: they have newer code still. Of that hash's parses, the one of the newest schedr version an alive worker
//! reporting it has is used (so after an upgrade the new parse takes over once stored). A
//! root no alive worker reports keeps its current parse.
//!
//! Each time a root's current parse changes, the DAG list is worked out again from every
//! root's current parse ([`catalog::plan`]) and the changes are stored. Only the newest code
//! runs (tasks go to workers on the current hash), so a worker on older code can't put a DAG
//! in conflict (see PROJECT.md, DAG ID conflicts).

use std::collections::{BTreeMap, BTreeSet};

use jiff::Timestamp;
use schedr_db::dags::{DagChange, NewParse, StoredParse};
use schedr_db::{Database, DbError};
use schedr_model::SchedrVersion;
use schedr_model::runner::{ErrorSummary, ParsedFile};

use crate::catalog::{self, Source};
use crate::metrics::MasterMetrics;
use crate::parse_jobs::{ParseAssignment, ParseJobs, ParseKey};
use crate::quiet::{WakeReason, Wakes};
use crate::validate::validate_files;

#[cfg(test)]
mod tests;

/// A parse result uploaded by a worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseUpload {
    /// The worker that parsed it.
    pub worker_id: String,
    /// What was parsed.
    pub key: ParseKey,
    /// Its files.
    pub files: Vec<ParsedFile>,
}

/// The master's view of DAG discovery.
#[derive(Debug)]
pub struct Discovery {
    db: Database,
    /// Marks waking the DAGs it defines anew.
    wakes: Wakes,
    master: SchedrVersion,
    /// The master's message limit: a task whose output can't fit it is refused (B33).
    max_message_size: usize,
    jobs: ParseJobs,
    /// What each alive worker last reported for each root.
    reports: BTreeMap<String, BTreeMap<String, ParseKey>>,
    /// Each root's current parse.
    current: BTreeMap<String, Current>,
    /// The hashes reported for each root when it was last decided (for the mixed-code
    /// warning).
    hashes: BTreeMap<String, BTreeSet<String>>,
    /// When workers last switched to each hash of a root (read once per root, then kept).
    switches: BTreeMap<String, BTreeMap<String, Timestamp>>,
    /// The stored parses of each (root, hash) (read once, dropped when one is stored or no
    /// worker reports the hash any more).
    parses: BTreeMap<(String, String), Vec<StoredParse>>,
    /// Each root's hashes whose workers may run its tasks: the current one, and newer ones
    /// whose parse is on its way.
    accepted: BTreeMap<String, BTreeSet<String>>,
    /// Workers that take no parse jobs (draining or drained).
    no_parse: BTreeSet<String>,
}

/// A root's current parse.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Current {
    parse_id: String,
    content_hash: String,
    schedr_version: Option<SchedrVersion>,
}

impl Discovery {
    /// Counts parse jobs in `metrics`.
    pub fn set_metrics(&mut self, metrics: MasterMetrics) {
        self.jobs.set_metrics(metrics);
    }

    /// Wakes the DAGs it defines anew (a new version, or back) through `wakes`.
    pub fn set_wakes(&mut self, wakes: Wakes) {
        self.wakes = wakes;
    }

    /// Discovery refusing tasks whose output can't fit `bytes`, the master's message limit.
    #[must_use]
    pub const fn with_max_message_size(mut self, bytes: usize) -> Self {
        self.max_message_size = bytes;
        self
    }

    /// Discovery over a database, checking DAG JSON for a master of version `master`.
    #[must_use]
    pub fn new(db: Database, master: SchedrVersion, jobs: ParseJobs) -> Self {
        Self {
            db,
            master,
            max_message_size: crate::service::DEFAULT_MAX_MESSAGE_SIZE,
            jobs,
            reports: BTreeMap::new(),
            current: BTreeMap::new(),
            hashes: BTreeMap::new(),
            switches: BTreeMap::new(),
            parses: BTreeMap::new(),
            accepted: BTreeMap::new(),
            no_parse: BTreeSet::new(),
            wakes: Wakes::default(),
        }
    }

    /// A worker drains (or is drained): it gets no parse jobs, and its unfinished ones go to
    /// other workers that reported the same hash. Returns those assignments.
    pub fn stop_parsing(&mut self, worker_id: &str, now: Timestamp) -> Vec<ParseAssignment> {
        self.no_parse.insert(worker_id.to_owned());
        self.jobs.worker_lost(worker_id, now)
    }

    /// A worker takes parse jobs again (resumed, or registered anew): the hashes it reports
    /// that have no parse yet are requested again. Returns the jobs it gets.
    ///
    /// # Errors
    ///
    /// [`DbError`] if reading fails.
    pub async fn allow_parsing(
        &mut self,
        worker_id: &str,
        now: Timestamp,
    ) -> Result<Vec<ParseAssignment>, DbError> {
        if !self.no_parse.remove(worker_id) {
            return Ok(Vec::new());
        }
        let keys: Vec<ParseKey> = self
            .reports
            .get(worker_id)
            .map(|roots| roots.values().cloned().collect())
            .unwrap_or_default();
        let mut jobs = Vec::new();
        for key in keys {
            let found = self
                .db
                .find_parse(&key.root, &key.content_hash, &key.schedr_version)
                .await?;
            match found {
                // No parse yet, or a retry planned (decision 72.2).
                None => jobs.extend(self.jobs.request(key, worker_id, now)),
                Some(_) if self.jobs.has(&key) => {
                    jobs.extend(self.jobs.request(key, worker_id, now));
                }
                // A parse that failed transiently, its retry unknown to a master restarted
                // while the worker was drained.
                Some(parse_id) => self.plan_stored_retry(key, &parse_id, now).await?,
            }
        }
        Ok(jobs)
    }

    /// Each root's current content hash, the hashes whose workers may run its tasks (the
    /// current one and newer ones whose parse is on its way), and the schedr version of the
    /// current parse (workers older than that don't get its tasks).
    pub fn current_code(
        &self,
    ) -> impl Iterator<Item = (&str, &str, &BTreeSet<String>, Option<SchedrVersion>)> {
        self.current.iter().filter_map(|(root, c)| {
            let accepted = self.accepted.get(root)?;
            Some((
                root.as_str(),
                c.content_hash.as_str(),
                accepted,
                c.schedr_version,
            ))
        })
    }

    /// The parse jobs not finished.
    #[must_use]
    pub const fn jobs(&self) -> &ParseJobs {
        &self.jobs
    }

    /// The parse jobs, to hand out again when a worker is lost or a job times out.
    pub const fn jobs_mut(&mut self) -> &mut ParseJobs {
        &mut self.jobs
    }

    /// A worker registered and reported its roots: they replace what it reported before (a
    /// root it no longer has stops counting), a change against its previous report can be a
    /// switch. Returns the parse jobs to send it.
    ///
    /// # Errors
    ///
    /// [`DbError`] if reading or storing fails.
    pub async fn registered(
        &mut self,
        worker_id: &str,
        keys: Vec<ParseKey>,
        now: Timestamp,
    ) -> Result<Vec<ParseAssignment>, DbError> {
        let previous = self.reports.remove(worker_id).unwrap_or_default();
        let mut roots: BTreeSet<String> = previous.keys().cloned().collect();
        let mut jobs = Vec::new();
        for key in keys {
            roots.insert(key.root.clone());
            jobs.extend(
                self.report(worker_id, previous.get(&key.root), key, now)
                    .await?,
            );
        }
        for root in &roots {
            self.decide(root, now).await?;
        }
        Ok(jobs)
    }

    /// A worker reported a root's content hash. Returns the parse job to send it, if the hash
    /// needs one.
    ///
    /// # Errors
    ///
    /// [`DbError`] if reading or storing fails.
    pub async fn root_reported(
        &mut self,
        worker_id: &str,
        key: ParseKey,
        now: Timestamp,
    ) -> Result<Option<ParseAssignment>, DbError> {
        let previous = self
            .reports
            .get(worker_id)
            .and_then(|roots| roots.get(&key.root))
            .cloned();
        let root = key.root.clone();
        let job = self.report(worker_id, previous.as_ref(), key, now).await?;
        self.decide(&root, now).await?;
        Ok(job)
    }

    /// Records a worker's report of a root (`previous`: what it reported before); returns the
    /// parse job it needs, if any.
    async fn report(
        &mut self,
        worker_id: &str,
        previous: Option<&ParseKey>,
        key: ParseKey,
        now: Timestamp,
    ) -> Result<Option<ParseAssignment>, DbError> {
        // A switch: moving away from the root's current hash (a revert, or new code).
        let switched = previous.is_some_and(|p| {
            p.content_hash != key.content_hash
                && self
                    .current
                    .get(&key.root)
                    .is_some_and(|c| c.content_hash == p.content_hash)
        });
        self.record_switch(&key.root, &key.content_hash, switched, now)
            .await?;
        // Its earlier code's job (a retry) can't be parsed by it any more.
        if let Some(previous) = previous.filter(|p| **p != key) {
            self.jobs.moved_away(previous, worker_id, now);
        }
        self.reports
            .entry(worker_id.to_owned())
            .or_default()
            .insert(key.root.clone(), key.clone());
        let found = self
            .db
            .find_parse(&key.root, &key.content_hash, &key.schedr_version)
            .await?;
        Ok(match found {
            Some(_) if self.no_parse.contains(worker_id) => None,
            // A retry planned (decision 72.2): one more worker that can take it.
            Some(_) if self.jobs.has(&key) => self.jobs.request(key, worker_id, now),
            Some(parse_id) => {
                // After a master restart, a parse that failed transiently isn't planned again
                // yet.
                self.plan_stored_retry(key, &parse_id, now).await?;
                None
            }
            None if self.no_parse.contains(worker_id) => {
                self.jobs.wait(key);
                None
            }
            None => self.jobs.request(key, worker_id, now),
        })
    }

    /// A worker is gone (deregistered or lost): its reports no longer count.
    ///
    /// # Errors
    ///
    /// [`DbError`] if reading or storing fails.
    pub async fn worker_gone(&mut self, worker_id: &str, now: Timestamp) -> Result<(), DbError> {
        let Some(roots) = self.reports.remove(worker_id) else {
            return Ok(());
        };
        for root in roots.keys() {
            self.decide(root, now).await?;
        }
        Ok(())
    }

    /// The workers that report `key` and take parse jobs.
    /// Plans the retry of a stored parse with files that failed transiently, unknown to this
    /// master (it restarted): it avoids the worker that parsed such a file, as stored with it
    /// (decision 93), while another can take it.
    async fn plan_stored_retry(
        &mut self,
        key: ParseKey,
        parse_id: &str,
        now: Timestamp,
    ) -> Result<(), DbError> {
        let files = self.db.parse_files(parse_id).await?;
        let Some(failed) = files
            .iter()
            .find(|f| f.error.as_ref().is_some_and(ErrorSummary::is_transient))
        else {
            return Ok(());
        };
        let workers = self.db.parse_file_workers(parse_id).await?;
        let candidates = self.candidates(&key);
        let failed_on = workers.get(&failed.path).map(String::as_str);
        self.jobs.retry(key, candidates, failed_on, now);
        Ok(())
    }

    fn candidates(&self, key: &ParseKey) -> BTreeSet<String> {
        self.reports
            .iter()
            .filter(|(worker, roots)| {
                !self.no_parse.contains(*worker) && roots.get(&key.root) == Some(key)
            })
            .map(|(worker, _)| worker.clone())
            .collect()
    }

    /// Stores an uploaded parse (after validation) and finishes its job. If it is the parse of
    /// the root's reported hash, it becomes current and the DAG list is updated. Returns the
    /// stored parse's ID.
    ///
    /// When `key` has a stored parse, the upload is a **retry** (decision 72.2): it replaces
    /// only the stored files that failed transiently, so a file that loaded once keeps its
    /// result. Files that (still) failed transiently get another retry, after a delay.
    ///
    /// # Errors
    ///
    /// [`DbError`] if storing fails.
    pub async fn parse_uploaded(
        &mut self,
        upload: ParseUpload,
        now: Timestamp,
    ) -> Result<String, DbError> {
        let files = validate_files(
            &upload.key.root,
            upload.files,
            self.master,
            self.max_message_size,
        );
        let key = &upload.key;
        let stored = self
            .db
            .find_parse(&key.root, &key.content_hash, &key.schedr_version)
            .await?;
        let (parse_id, files, retried) = if let Some(parse_id) = stored {
            let before = self.db.parse_files(&parse_id).await?;
            let replaced = retried_files(&before, files);
            self.db
                .replace_parse_files(&parse_id, &replaced, &upload.worker_id, now)
                .await?;
            let files = self.db.parse_files(&parse_id).await?;
            (parse_id, files, !replaced.is_empty())
        } else {
            let parse_id = self
                .db
                .insert_parse(&NewParse {
                    root: &key.root,
                    content_hash: &key.content_hash,
                    schedr_version: &key.schedr_version,
                    worker_id: &upload.worker_id,
                    parsed_at: now,
                    files: &files,
                })
                .await?;
            (parse_id, files, false)
        };
        let failed: Vec<&str> = files
            .iter()
            .filter(|f| f.error.as_ref().is_some_and(ErrorSummary::is_transient))
            .map(|f| f.path.as_str())
            .collect();
        if failed.is_empty() {
            self.jobs.finish(key);
        } else {
            let candidates = self.candidates(key);
            self.jobs
                .retry(key.clone(), candidates, Some(&upload.worker_id), now);
            tracing::warn!(
                root = key.root.as_str(),
                content_hash = key.content_hash.as_str(),
                worker_id = upload.worker_id.as_str(),
                files = failed.join(", "),
                "files failed to import transiently (a timeout or a runner that died): parsed \
                 again later; their DAGs keep their last good version meanwhile, so a file \
                 that keeps failing doesn't take effect (see the import errors)"
            );
        }
        self.parses
            .remove(&(key.root.clone(), key.content_hash.clone()));
        self.decide(&key.root, now).await?;
        // A retry changes the files of a parse that may already be current: `decide` sees no
        // new parse then.
        if retried
            && self
                .current
                .get(&key.root)
                .is_some_and(|c| c.parse_id == parse_id)
        {
            self.refresh_or_forget(&key.root, now).await?;
        }
        Ok(parse_id)
    }

    /// Works out the DAG list from every root's current parse and stores the changes.
    /// Returns them.
    ///
    /// # Errors
    ///
    /// [`DbError`] if reading or storing fails.
    pub async fn refresh(&self, now: Timestamp) -> Result<Vec<DagChange>, DbError> {
        let mut sources = Vec::with_capacity(self.current.len());
        for (root, current) in &self.current {
            sources.push(Source {
                root: root.clone(),
                parse_id: current.parse_id.clone(),
                files: self.db.parse_files(&current.parse_id).await?,
            });
        }
        let changes = catalog::plan(&sources, &self.db.stored_dags().await?);
        if !changes.is_empty() {
            self.db.apply_dag_changes(&changes, now).await?;
            for change in &changes {
                if let DagChange::Define { dag, .. } = change {
                    self.wakes.wake(&dag.id, WakeReason::NewVersion);
                }
            }
        }
        Ok(changes)
    }

    /// Records a report of `root` at `hash` (a switch, or a first report).
    async fn record_switch(
        &mut self,
        root: &str,
        hash: &str,
        switched: bool,
        now: Timestamp,
    ) -> Result<(), DbError> {
        let known = self.switches_of(root).await?.contains_key(hash);
        if switched || !known {
            self.db.record_root_hash(root, hash, switched, now).await?;
            self.switches
                .entry(root.to_owned())
                .or_default()
                .insert(hash.to_owned(), now);
        }
        Ok(())
    }

    /// When workers last switched to each hash of `root`.
    async fn switches_of(&mut self, root: &str) -> Result<&BTreeMap<String, Timestamp>, DbError> {
        if !self.switches.contains_key(root) {
            let stored = self.db.root_hash_switches(root).await?;
            self.switches.insert(root.to_owned(), stored);
        }
        Ok(&self.switches[root])
    }

    /// The stored parses of (root, hash).
    async fn parses_of(&mut self, root: &str, hash: &str) -> Result<&Vec<StoredParse>, DbError> {
        let key = (root.to_owned(), hash.to_owned());
        if !self.parses.contains_key(&key) {
            let stored = self.db.parses_of(root, hash).await?;
            self.parses.insert(key.clone(), stored);
        }
        Ok(&self.parses[&key])
    }

    /// Decides a root's current parse from what alive workers report (see the module's
    /// documentation) and refreshes the DAG list if it changed.
    async fn decide(&mut self, root: &str, now: Timestamp) -> Result<(), DbError> {
        // Each reported hash, with the schedr versions of the workers reporting it.
        let mut reported: BTreeMap<&str, BTreeSet<Option<SchedrVersion>>> = BTreeMap::new();
        let mut by_hash: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (worker, roots) in &self.reports {
            if let Some(key) = roots.get(root) {
                reported
                    .entry(&key.content_hash)
                    .or_default()
                    .insert(key.schedr_version.parse().ok());
                by_hash.entry(&key.content_hash).or_default().push(worker);
            }
        }
        if reported.is_empty() {
            return Ok(());
        }
        let hashes: BTreeSet<String> = reported.keys().map(|h| (*h).to_owned()).collect();
        if hashes.len() > 1 && self.hashes.get(root) != Some(&hashes) {
            let detail: Vec<String> = by_hash
                .iter()
                .map(|(hash, workers)| format!("{hash} on {}", workers.join(", ")))
                .collect();
            tracing::warn!(
                root,
                "workers report {} code versions of the root ({}): its tasks go to the workers \
                 with the newest",
                hashes.len(),
                detail.join("; ")
            );
        }
        self.hashes.insert(root.to_owned(), hashes);
        let reported: Vec<(String, BTreeSet<Option<SchedrVersion>>)> = reported
            .into_iter()
            .map(|(h, v)| (h.to_owned(), v))
            .collect();
        let switches = self.switches_of(root).await?.clone();
        let mut newest: Option<(Option<Timestamp>, String, &StoredParse)> = None;
        let mut parses = Vec::with_capacity(reported.len());
        for (hash, _) in &reported {
            parses.push(self.parses_of(root, hash).await?.clone());
        }
        for ((hash, versions), parses) in reported.iter().zip(&parses) {
            let Some(parse) = usable(parses, versions) else {
                continue;
            };
            let at = switches.get(hash).copied();
            if newest.as_ref().is_none_or(|(t, h, _)| (at, hash) > (*t, h)) {
                newest = Some((at, hash.clone(), parse));
            }
        }
        // Parses of hashes nobody reports any more aren't needed.
        self.parses
            .retain(|(r, h), _| r != root || reported.iter().any(|(reported, _)| reported == h));
        let Some((at, hash, parse)) = newest else {
            return Ok(());
        };
        let accepted: BTreeSet<String> = reported
            .iter()
            .map(|(h, _)| h)
            .filter(|h| **h == hash || switches.get(*h).copied() > at)
            .cloned()
            .collect();
        self.accepted.insert(root.to_owned(), accepted);
        let current = Current {
            parse_id: parse.id.clone(),
            content_hash: hash,
            schedr_version: parse.schedr_version.parse().ok(),
        };
        if self.current.get(root) != Some(&current) {
            // Written down for the API's import errors (decision 66).
            self.db
                .set_current_parse(root, &current.parse_id, now)
                .await?;
            self.current.insert(root.to_owned(), current);
            self.refresh_or_forget(root, now).await?;
        }
        Ok(())
    }

    /// Refreshes the catalog; if that fails, forgets `root`'s current parse, so the next
    /// report of its code decides (and refreshes) again instead of finding nothing new.
    async fn refresh_or_forget(&mut self, root: &str, now: Timestamp) -> Result<(), DbError> {
        let refreshed = self.refresh(now).await;
        if refreshed.is_err() {
            self.current.remove(root);
        }
        refreshed.map(drop)
    }
}

/// Of a retry's `files`, those that replace the stored ones (`before`): where the stored file
/// failed transiently (or is missing), and the retry's result differs.
fn retried_files(before: &[ParsedFile], files: Vec<ParsedFile>) -> Vec<ParsedFile> {
    let before: BTreeMap<&str, &ParsedFile> = before.iter().map(|f| (f.path.as_str(), f)).collect();
    files
        .into_iter()
        .filter(|file| match before.get(file.path.as_str()) {
            None => true,
            Some(old) => {
                old.error.as_ref().is_some_and(ErrorSummary::is_transient)
                    && (old.dags != file.dags || old.error != file.error)
            }
        })
        .collect()
}

/// The parse to use of a hash's `parses`: the one of the newest schedr version among
/// `versions` (those of the workers reporting the hash), else the newest one no newer than
/// all of them; `None` when every parse is newer (the reporters' own parse is on its way).
fn usable<'a>(
    parses: &'a [StoredParse],
    versions: &BTreeSet<Option<SchedrVersion>>,
) -> Option<&'a StoredParse> {
    let highest = versions.iter().flatten().max().copied();
    let version = |p: &StoredParse| p.schedr_version.parse::<SchedrVersion>().ok();
    let reported = parses
        .iter()
        .filter(|p| versions.contains(&version(p)))
        .max_by_key(|p| version(p));
    let older = || {
        parses
            .iter()
            .filter(|p| version(p).is_some_and(|v| highest.is_none_or(|h| v <= h)))
            .max_by_key(|p| version(p))
    };
    reported.or_else(older)
}
