//! DAG discovery on the master: reported hashes, parse jobs, uploaded parses and the DAG list.
//!
//! A worker reports each root's content hash (at registration and on every change). A hash
//! with a stored parse becomes the root's current parse at once; otherwise a parse job is
//! created and the root's previous parse stays current until the new one is stored. Each time
//! a root's current parse changes, the DAG list is worked out again from every root's current
//! parse ([`catalog::plan`]) and the changes are stored.
//!
//! M1 has one worker, so a root has one reported hash. With several workers (M2) each worker
//! has its own current parses and the conflict rules across workers apply (see PROJECT.md, DAG
//! ID conflicts).

use std::collections::BTreeMap;

use jiff::Timestamp;
use schedr_db::dags::{DagChange, NewParse};
use schedr_db::{Database, DbError};
use schedr_model::SchedrVersion;
use schedr_model::runner::ParsedFile;

use crate::catalog::{self, Source};
use crate::parse_jobs::{ParseAssignment, ParseJobs, ParseKey};
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
    master: SchedrVersion,
    jobs: ParseJobs,
    reported: BTreeMap<String, ParseKey>,
    current: BTreeMap<String, String>,
}

impl Discovery {
    /// Discovery over a database, checking DAG JSON for a master of version `master`.
    #[must_use]
    pub const fn new(db: Database, master: SchedrVersion, jobs: ParseJobs) -> Self {
        Self {
            db,
            master,
            jobs,
            reported: BTreeMap::new(),
            current: BTreeMap::new(),
        }
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
        self.reported.insert(key.root.clone(), key.clone());
        let found = self
            .db
            .find_parse(&key.root, &key.content_hash, &key.schedr_version)
            .await?;
        match found {
            Some(parse_id) => {
                self.make_current(&key.root, parse_id, now).await?;
                Ok(None)
            }
            None => Ok(self.jobs.request(key, worker_id, now)),
        }
    }

    /// Stores an uploaded parse (after validation) and finishes its job. If it is the parse of
    /// the root's reported hash, it becomes current and the DAG list is updated. Returns the
    /// stored parse's ID.
    ///
    /// # Errors
    ///
    /// [`DbError`] if storing fails.
    pub async fn parse_uploaded(
        &mut self,
        upload: ParseUpload,
        now: Timestamp,
    ) -> Result<String, DbError> {
        let files = validate_files(&upload.key.root, upload.files, self.master);
        let parse_id = self
            .db
            .insert_parse(&NewParse {
                root: &upload.key.root,
                content_hash: &upload.key.content_hash,
                schedr_version: &upload.key.schedr_version,
                worker_id: &upload.worker_id,
                parsed_at: now,
                files: &files,
            })
            .await?;
        self.jobs.finish(&upload.key);
        if self.reported.get(&upload.key.root) == Some(&upload.key) {
            self.make_current(&upload.key.root, parse_id.clone(), now)
                .await?;
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
        for (root, parse_id) in &self.current {
            sources.push(Source {
                root: root.clone(),
                parse_id: parse_id.clone(),
                files: self.db.parse_files(parse_id).await?,
            });
        }
        let changes = catalog::plan(&sources, &self.db.stored_dags().await?);
        if !changes.is_empty() {
            self.db.apply_dag_changes(&changes, now).await?;
        }
        Ok(changes)
    }

    async fn make_current(
        &mut self,
        root: &str,
        parse_id: String,
        now: Timestamp,
    ) -> Result<(), DbError> {
        if self.current.get(root) != Some(&parse_id) {
            self.current.insert(root.to_owned(), parse_id);
            self.refresh(now).await?;
        }
        Ok(())
    }
}
