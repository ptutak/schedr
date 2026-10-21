//! The environment of a task process: clean by default (see PROJECT.md, Worker environment).
//!
//! A task gets only a small allow-list of the agent's variables, those listed in
//! `[worker] pass_env`, the task's own `env`, and the `SCHEDR_*` run variables. The agent's
//! own configuration, certificate paths and credentials never reach a task by accident.

use std::collections::BTreeMap;

#[cfg(test)]
mod tests;

/// The agent's variables every task process gets (plus every `LC_*`).
pub const ALLOWED: [&str; 5] = ["PATH", "HOME", "LANG", "TZ", "TMPDIR"];

/// The prefix of the run variables, reserved for schedr.
pub const RESERVED_PREFIX: &str = "SCHEDR_";

/// Why a task's environment can't be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvError {
    /// The task's `env` sets a variable reserved for schedr.
    #[error("environment variable `{0}` is reserved for schedr (names starting with SCHEDR_)")]
    Reserved(String),
}

/// The run information every `command` and `script` task gets as `SCHEDR_*` variables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunVars {
    /// `SCHEDR_DAG_ID`.
    pub dag_id: String,
    /// `SCHEDR_RUN_ID`.
    pub run_id: String,
    /// `SCHEDR_RUN_KIND`.
    pub run_kind: String,
    /// `SCHEDR_TASK_ID`.
    pub task_id: String,
    /// `SCHEDR_MAP_INDEX`, for mapped tasks only.
    pub map_index: Option<u32>,
    /// `SCHEDR_TRY_NUMBER`.
    pub try_number: u32,
    /// `SCHEDR_PLANNED_FOR`, RFC 3339 in UTC.
    pub planned_for: String,
    /// `SCHEDR_SINCE`, RFC 3339 in UTC.
    pub since: String,
}

impl RunVars {
    /// The variables, by name.
    #[must_use]
    pub fn variables(&self) -> BTreeMap<String, String> {
        let mut vars = BTreeMap::from([
            ("SCHEDR_DAG_ID".to_owned(), self.dag_id.clone()),
            ("SCHEDR_RUN_ID".to_owned(), self.run_id.clone()),
            ("SCHEDR_RUN_KIND".to_owned(), self.run_kind.clone()),
            ("SCHEDR_TASK_ID".to_owned(), self.task_id.clone()),
            ("SCHEDR_TRY_NUMBER".to_owned(), self.try_number.to_string()),
            ("SCHEDR_PLANNED_FOR".to_owned(), self.planned_for.clone()),
            ("SCHEDR_SINCE".to_owned(), self.since.clone()),
        ]);
        if let Some(index) = self.map_index {
            vars.insert("SCHEDR_MAP_INDEX".to_owned(), index.to_string());
        }
        vars
    }
}

/// Whether an agent variable is on the allow-list.
#[must_use]
pub fn is_allowed(name: &str) -> bool {
    ALLOWED.contains(&name) || name.starts_with("LC_")
}

/// The complete environment of a task process.
///
/// Layers, later ones winning: the allow-listed agent variables, the agent variables named in
/// `pass_env`, the task's own `env`, the run variables.
///
/// # Errors
///
/// Returns [`EnvError::Reserved`] if the task's `env` sets a `SCHEDR_*` variable.
pub fn task_environment(
    agent_env: &BTreeMap<String, String>,
    pass_env: &[String],
    task_env: &BTreeMap<String, String>,
    run: &RunVars,
) -> Result<BTreeMap<String, String>, EnvError> {
    if let Some(name) = task_env.keys().find(|n| n.starts_with(RESERVED_PREFIX)) {
        return Err(EnvError::Reserved(name.clone()));
    }
    let mut env: BTreeMap<String, String> = agent_env
        .iter()
        .filter(|(name, _)| is_allowed(name) || pass_env.contains(name))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    env.extend(task_env.iter().map(|(k, v)| (k.clone(), v.clone())));
    env.extend(run.variables());
    Ok(env)
}
