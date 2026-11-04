//! `schedr connections check` and `test` on this worker (decisions 174, 193, 198-200): a
//! check renders each connection through the worker's secret storages, saying of every
//! secret it names whether it is there (never a value); a test opens one connection in a
//! runner and runs its check, as a one-off attempt beside the tasks (no slot), stopped after
//! its timeout. One job at a time per worker.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use schedr_model::commands::{ConnectionReport, SecretReport, SecretState};
use schedr_model::connection::Registry;
use schedr_model::runner::{ErrorSummary, RunInfo};
use serde_json::json;

use crate::attempt::AttemptSpec;
use crate::secrets::{AttemptSecrets, SecretError, SecretStores};

#[cfg(test)]
mod tests;

/// How long a test's process gets from SIGTERM to SIGKILL, unless `connection_job_cancel_grace`
/// says.
pub const CANCEL_GRACE: Duration = Duration::from_secs(5);

/// A connections job, as the master sent it.
#[derive(Debug, Clone)]
pub struct ConnectionJob {
    /// The root.
    pub root: String,
    /// The root's code, where a test imports the connection's file.
    pub code_dir: Option<PathBuf>,
    /// The connections to check (their files with them).
    pub registry: Arc<Registry>,
    /// A test: the connection, and how long it may take.
    pub test: Option<(String, Duration)>,
    /// How long a test's process gets from SIGTERM to SIGKILL.
    pub cancel_grace: Duration,
    /// A test's attempt ID (its folders, its journal record).
    pub attempt_id: String,
    /// When its time is up: an answer comes by then (a test's process stopped).
    pub deadline: tokio::time::Instant,
}

/// Renders each connection of `registry` (decision 174): its secrets present or missing,
/// and whether it renders, a refused one with why.
pub async fn check(stores: &Arc<SecretStores>, registry: &Arc<Registry>) -> Vec<ConnectionReport> {
    let mut reports = Vec::new();
    for (name, why) in &registry.refused {
        reports.push(failed(name, why.clone()));
    }
    for (name, def) in &registry.connections {
        let mut secrets = Vec::new();
        for secret in def.secrets() {
            let (state, message) = match stores.fetch(secret).await {
                Ok(_) => (SecretState::Present, None),
                // As an attempt reads it: the default stands in, in a storage the worker has.
                Err(SecretError::NotFound(_))
                    if secret.default.is_some() && stores.storage_of(secret).is_ok() =>
                {
                    (SecretState::Default, None)
                }
                Err(SecretError::NotFound(_)) => (SecretState::Missing, None),
                Err(SecretError::Denied(why)) => (SecretState::Denied, Some(why)),
                Err(SecretError::Unavailable(why)) => (SecretState::Unavailable, Some(why)),
            };
            secrets.push(SecretReport {
                secret: secret.to_string(),
                state,
                message,
            });
        }
        let attempt = AttemptSecrets::none(Arc::clone(stores))
            .with_connections(Arc::clone(registry), vec![name.clone()]);
        let error = attempt
            .connection(name)
            .await
            .err()
            .map(|e| attempt.mask.mask_text(&e.to_string()));
        // Read twice (states, then the rendering): ok only when both say so.
        let all_there = secrets
            .iter()
            .all(|s| matches!(s.state, SecretState::Present | SecretState::Default));
        let error = error.or_else(|| {
            (!all_there).then(|| format!("connection `{name}`: a secret it names isn't there"))
        });
        reports.push(ConnectionReport {
            name: name.clone(),
            ok: error.is_none(),
            secrets,
            error,
            duration_ms: None,
        });
    }
    reports
}

/// The one-off attempt a test of `name` runs (decision 198): a Python "task" of the
/// connection's file, its runner told to open and check the connection instead.
///
/// # Errors
///
/// Why it can't run: a refused connection, or one with no file recorded.
pub fn test_spec(
    job: &ConnectionJob,
    name: &str,
    timeout: Duration,
    attempt_id: String,
) -> Result<AttemptSpec, String> {
    if let Some(why) = job.registry.refused.get(name) {
        return Err(why.clone());
    }
    let Some(path) = job.registry.files.get(name) else {
        return Err(format!(
            "connection `{name}`: no file of root `{}` defines it",
            job.root
        ));
    };
    // Imported from the root's code only (the master refuses such a path too).
    let inside = std::path::Path::new(path)
        .components()
        .all(|c| matches!(c, std::path::Component::Normal(_)));
    if path.is_empty() || !inside {
        return Err(format!(
            "connection `{name}`: `{path}` isn't a path inside root `{}`",
            job.root
        ));
    }
    let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
    // Never forked from the preloaded server: a test is rare, and its imports its own.
    let task = serde_json::from_value(json!({
        "id": name,
        "kind": "python",
        "connections": [name],
        "settings": {"retries": 0, "retry_delay_ms": 0, "timeout_ms": timeout_ms,
                     "queue": "default", "max_output_size": 65_536, "preload": false,
                     "fresh_process": false},
        "output_type": {"type": "none"},
    }))
    .map_err(|e| format!("connection `{name}` can't be tested by this name: {e}"))?;
    let now = jiff::Timestamp::now();
    Ok(AttemptSpec {
        attempt_id,
        task,
        dag_root: job.root.clone(),
        dag_path: path.clone(),
        run: RunInfo {
            dag_id: String::new(),
            run_id: String::new(),
            kind: schedr_model::RunKind::Manual,
            planned_for: now,
            since: now,
            try_number: 1,
            map_index: None,
            params: json!({}),
        },
        inputs: std::collections::BTreeMap::new(),
        outcomes: std::collections::BTreeMap::new(),
        cancel_grace: job.cancel_grace,
        secrets: std::collections::BTreeMap::new(),
        mask: crate::mask::Mask::default(),
        code_dir: job.code_dir.clone(),
        connections: Arc::clone(&job.registry),
        connection_env: std::collections::BTreeMap::new(),
        connection_test: Some(name.to_owned()),
        cleanup_handles: None,
        journal: None,
        handle_sink: None,
    })
}

/// A test's report from its attempt's end (its error masked as any attempt's is).
#[must_use]
pub fn test_report(
    name: &str,
    finished: &schedr_proto::pb::Finished,
    took: Duration,
) -> ConnectionReport {
    use schedr_proto::pb::AttemptStatus;
    let status = AttemptStatus::try_from(finished.status).unwrap_or(AttemptStatus::Unspecified);
    let summary: Option<ErrorSummary> = serde_json::from_slice(&finished.error_summary_json).ok();
    let error = match status {
        AttemptStatus::Success => None,
        AttemptStatus::TimedOut => Some(format!(
            "stopped after its timeout ({:.1} s)",
            took.as_secs_f64()
        )),
        _ => Some(
            summary
                .map(|e| {
                    if e.exception.is_empty() {
                        e.message
                    } else {
                        format!("{}: {}", e.exception, e.message)
                    }
                })
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| format!("the test ended {}", status.as_str_name())),
        ),
    };
    ConnectionReport {
        name: name.to_owned(),
        ok: error.is_none(),
        secrets: Vec::new(),
        error,
        duration_ms: Some(u64::try_from(took.as_millis()).unwrap_or(u64::MAX)),
    }
}

/// Why a job ran out of its time while doing `what`.
#[must_use]
pub fn out_of_time(what: &str) -> String {
    format!("its time was up during its {what}")
}

/// A connection that couldn't be checked or tested, with why.
#[must_use]
pub fn failed(name: &str, why: String) -> ConnectionReport {
    ConnectionReport {
        name: name.to_owned(),
        ok: false,
        secrets: Vec::new(),
        error: Some(why),
        duration_ms: None,
    }
}
