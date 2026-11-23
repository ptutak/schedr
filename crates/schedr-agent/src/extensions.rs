//! The plugins that run a worker's extension tasks (decision 510): from `[extensions.<name>]`,
//! and with `[extensions] discover = true` from each `schedr-ext-*` executable on `PATH`, asked
//! with `--describe`. What a worker has here is what `Register.extensions` announces; a plugin
//! runs each task as its own process in a task slot (decision note 15), speaking the runner
//! protocol over `SCHEDR_AGENT_FD`.

use schedr_model::extension::{EventKind, ExtensionEvent};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use schedr_model::runner::EXTENSION_PROTOCOL;
use schedr_proto::pb;
use serde::Deserialize;

#[cfg(test)]
mod tests;

/// The prefix a discovered plugin's executable name starts with.
pub const DISCOVERED_PREFIX: &str = "schedr-ext-";

/// How long a discovered plugin's `--describe` may take; a slower one is skipped.
pub const DESCRIBE_TIMEOUT: Duration = Duration::from_secs(5);

/// The most `--describe` may print: one short JSON line.
const DESCRIBE_LIMIT: usize = 64 * 1024;

/// A plugin: how to start it, and the versions of its extension it serves.
#[derive(Debug, Clone, PartialEq)]
pub struct Plugin {
    /// The program, then its arguments.
    pub command: Vec<String>,
    /// The versions served, matched exactly.
    pub versions: BTreeSet<String>,
    /// Its metrics and page from its config (decision 513); none: its hello's.
    pub declarations: schedr_model::extension::Declarations,
    /// The most series each of its metrics keeps.
    pub max_series: u32,
    /// Its service (decision 518); none for a discovered plugin.
    pub service: Option<crate::service::ServiceConfig>,
}

/// A worker's plugins, by extension name.
pub type Plugins = BTreeMap<String, Plugin>;

/// What `Register.extensions` announces of `plugins`.
#[must_use]
pub fn offers(plugins: &Plugins) -> Vec<pb::ExtensionOffer> {
    plugins
        .iter()
        .map(|(name, plugin)| pb::ExtensionOffer {
            name: name.clone(),
            versions: plugin.versions.iter().cloned().collect(),
        })
        .collect()
}

/// What a plugin's `--describe` prints: one JSON line.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Description {
    /// The extension it runs.
    pub name: String,
    /// The versions it serves.
    pub versions: Vec<String>,
    /// The runner protocol version it speaks with the worker.
    pub protocol: u32,
}

impl Description {
    /// The description `out` (a plugin's standard output) holds: its first line, as JSON.
    ///
    /// # Errors
    ///
    /// Why it isn't one this worker can use.
    pub fn parse(out: &str) -> Result<Self, String> {
        let line = out.lines().next().unwrap_or_default();
        let description: Self = serde_json::from_str(line).map_err(|e| {
            format!("its --describe isn't one JSON line {{name, versions, protocol}}: {e}")
        })?;
        schedr_model::task::check_extension_name(&description.name)?;
        if description.versions.is_empty() {
            return Err("it describes no versions".to_owned());
        }
        for version in &description.versions {
            schedr_model::task::check_extension_version(version)?;
        }
        if description.protocol != EXTENSION_PROTOCOL {
            return Err(format!(
                "it speaks protocol {}, this worker {EXTENSION_PROTOCOL}",
                description.protocol
            ));
        }
        Ok(description)
    }
}

/// The `schedr-ext-*` executables on `path` (a `PATH` value), the first of each name, in
/// `PATH` order.
#[must_use]
pub fn discoverable(path: &str) -> Vec<PathBuf> {
    let mut seen = BTreeSet::new();
    let mut found = Vec::new();
    for dir in std::env::split_paths(path) {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        let mut names: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|file| {
                file.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                    n.starts_with(DISCOVERED_PREFIX) && n.len() > DISCOVERED_PREFIX.len()
                })
            })
            .filter(|file| is_executable(file))
            .collect();
        names.sort();
        for file in names {
            if seen.insert(file.file_name().map(ToOwned::to_owned)) {
                found.push(file);
            }
        }
    }
    found
}

fn is_executable(file: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(file).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

/// Asks `program` for its description: `--describe`, with only `PATH` in its environment, at
/// most [`DESCRIBE_TIMEOUT`], its standard output read up to 64 KiB. It runs in a
/// process group of its own, killed afterwards, so nothing it started outlives the question
/// (Grok's review of M5.2i).
///
/// # Errors
///
/// Why it gave none this worker can use.
pub async fn describe(program: &Path, path: &str) -> Result<Description, String> {
    use tokio::io::AsyncReadExt as _;

    let mut command = tokio::process::Command::new(program);
    command
        .arg("--describe")
        .env_clear()
        .env("PATH", path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .process_group(0)
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|e| format!("it can't be run: {e}"))?;
    let group = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(rustix::process::Pid::from_raw);
    let stdout = child.stdout.take();
    let asked = async {
        let mut out = Vec::new();
        if let Some(stdout) = stdout {
            // One byte past the limit tells an output over it from one at it.
            let limit = u64::try_from(DESCRIBE_LIMIT).unwrap_or(u64::MAX) + 1;
            stdout
                .take(limit)
                .read_to_end(&mut out)
                .await
                .map_err(|e| format!("its --describe can't be read: {e}"))?;
        }
        if out.len() > DESCRIBE_LIMIT {
            return Err("its --describe printed over 64 KiB".to_owned());
        }
        let status = child
            .wait()
            .await
            .map_err(|e| format!("its --describe can't be waited for: {e}"))?;
        if !status.success() {
            return Err(format!("its --describe ended with {status}"));
        }
        Ok(out)
    };
    let answered = tokio::time::timeout(DESCRIBE_TIMEOUT, asked).await;
    if let Some(group) = group {
        // Whatever the answer: what it started goes with it.
        let _ = rustix::process::kill_process_group(group, rustix::process::Signal::KILL);
    }
    let out = answered
        .map_err(|_| format!("its --describe took over {}s", DESCRIBE_TIMEOUT.as_secs()))??;
    Description::parse(&String::from_utf8_lossy(&out))
}

/// The plugins a worker runs: `configured`, then, when `discover`, those found on `path` whose
/// names aren't configured (configuration wins), and what happened to each found (DN19). Each
/// plugin skipped is logged as a warning; the worker starts without it.
pub async fn plugins(
    configured: Plugins,
    discover: bool,
    path: &str,
) -> (Plugins, Vec<ExtensionEvent>) {
    let mut plugins = configured;
    let mut events = Vec::new();
    if !discover {
        return (plugins, events);
    }
    for program in discoverable(path) {
        let shown = program.display().to_string();
        match describe(&program, path).await {
            Ok(description) if plugins.contains_key(&description.name) => {
                tracing::info!(
                    program = %program.display(),
                    "extension `{}`: configured, or found earlier on PATH, so this plugin is \
                     not used",
                    description.name
                );
                events.push(ExtensionEvent::now(
                    &description.name,
                    None,
                    EventKind::Skipped,
                    &format!("{shown}: configured, or found earlier on PATH, so not used"),
                ));
            }
            Ok(description) => {
                tracing::info!(
                    program = %program.display(),
                    "extension `{}` {:?}: discovered",
                    description.name,
                    description.versions
                );
                events.push(ExtensionEvent::now(
                    &description.name,
                    None,
                    EventKind::Discovered,
                    &format!("{shown}, versions {}", description.versions.join(", ")),
                ));
                plugins.insert(
                    description.name,
                    Plugin {
                        command: vec![program.to_string_lossy().into_owned()],
                        versions: description.versions.into_iter().collect(),
                        // Its own come in its hello (decision 513).
                        declarations: schedr_model::extension::Declarations::default(),
                        max_series: schedr_model::extension::DEFAULT_MAX_SERIES,
                        service: None,
                    },
                );
            }
            Err(why) => {
                tracing::warn!(
                    program = %program.display(),
                    "an extension plugin skipped: {why}"
                );
                // Named by its file: it said nothing of itself.
                let name = program
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.strip_prefix(DISCOVERED_PREFIX))
                    .unwrap_or_default()
                    .to_owned();
                events.push(ExtensionEvent::now(
                    &name,
                    None,
                    EventKind::Skipped,
                    &format!("{shown}: {why}"),
                ));
            }
        }
    }
    (plugins, events)
}
