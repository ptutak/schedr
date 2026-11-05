//! `schedr.toml`: the scripts a script root declares (PROJECT.md, Plain script delivery;
//! decisions 292-300). Read and checked here, in Rust, by the agent alone (decision 293): every
//! key known, unknown ones an error. A declaration with a schedule (`schedule`, `every` or
//! `once`) is a standalone job, a one-task DAG whose ID is its reference
//! (`etl:team_a/schedr.toml#report_daily`); one without is a script for DAGs only (296).

use std::collections::BTreeMap;

use jiff::Timestamp;
use jiff::civil::{Date, DateTime};
use jiff::tz::TimeZone;
use schedr_model::runner::{ErrorSummary, ParsedFile};
use schedr_model::secret::{SecretDecl, SecretName};
use schedr_model::task::{TaskKind, TaskSettings};
use schedr_model::value::{Reference, Template, TemplatePart, TemplateRef};
use schedr_model::{Dag, DagId, DagLocation, SchedrVersion, Task, TaskId, Trigger, TypeDesc};
use toml::Value;
use toml::value::Datetime;

/// A script configuration file's name.
pub const FILE_NAME: &str = "schedr.toml";

/// The exception of a `schedr.toml` that can't be read or checked, shown as its import error.
pub const INVALID_JOBS_EXCEPTION: &str = "InvalidScriptConfig";

/// The keys a declaration may have.
const JOB_KEYS: [&str; 27] = [
    "script",
    "interpreter",
    "args",
    "env",
    "secrets",
    "working_dir",
    "schedule",
    "every",
    "once",
    "timezone",
    "start",
    "end",
    "initial_since",
    "catchup",
    "max_active_runs",
    "notify",
    "queue",
    "retries",
    "retry_delay",
    "timeout",
    "max_output_size",
    "description",
    "tags",
    "cleanup_handles",
    "outcomes",
    "params",
    "cleanup",
];

/// The keys only a standalone job has: without a schedule they would do nothing (296).
const DAG_KEYS: [&str; 8] = [
    "timezone",
    "start",
    "end",
    "initial_since",
    "catchup",
    "max_active_runs",
    "notify",
    "params",
];

/// The keys of later steps (M4.9c), an error until then rather than ignored.
const NOT_YET: [&str; 4] = ["cleanup_handles", "outcomes", "params", "cleanup"];

/// Where a `schedr.toml` is.
pub struct Source<'a> {
    /// The script root's name.
    pub root: &'a str,
    /// The file's path in the root, with `/` separators.
    pub path: &'a str,
    /// Whether a file exists at a path in the root (`/` separators).
    pub exists: &'a dyn Fn(&str) -> bool,
}

impl Source<'_> {
    /// The file's directory in the root: `""` at the root itself.
    fn dir(&self) -> &str {
        self.path.rsplit_once('/').map_or("", |(dir, _)| dir)
    }

    /// A path written in the file, as a path in the root: relative to the file, no `..`, not
    /// absolute.
    fn in_root(&self, written: &str) -> Result<String, String> {
        if written.starts_with('/') || written.starts_with('\\') {
            return Err(format!(
                "`{written}` is absolute: a path is relative to `{FILE_NAME}`"
            ));
        }
        let mut parts: Vec<&str> = self.dir().split('/').filter(|p| !p.is_empty()).collect();
        for part in written.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    return Err(format!(
                        "`{written}` has `..`: a path stays in `{FILE_NAME}`'s directory or below"
                    ));
                }
                part => parts.push(part),
            }
        }
        Ok(parts.join("/"))
    }

    /// The reference of a declaration: `<root>:<path>#<name>`.
    fn reference(&self, name: &str) -> String {
        format!("{}:{}#{name}", self.root, self.path)
    }
}

/// A checked declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declaration {
    /// Its name in the file.
    pub name: String,
    /// Its reference, `<root>:<path>#<name>`.
    pub reference: String,
    /// The task it runs.
    pub task: Task,
    /// When it runs on its own: `None` for a script for DAGs only.
    pub schedule: Option<Schedule>,
    /// Its description.
    pub description: Option<String>,
    /// Its tags.
    pub tags: Vec<String>,
    /// The settings its file gives (the others are the defaults in `task`).
    pub given: Given,
}

/// The task settings a declaration gives itself: a `ScriptOperator` takes these, and the
/// DAG's defaults for the others.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Given {
    /// `retries`.
    pub retries: Option<u32>,
    /// `retry_delay`.
    pub retry_delay_ms: Option<u64>,
    /// `timeout`.
    pub timeout_ms: Option<u64>,
    /// `queue`.
    pub queue: Option<String>,
    /// `max_output_size`.
    pub max_output_size: Option<u64>,
}

impl Declaration {
    /// What a `ScriptOperator` referring to it gets (decision 293).
    #[must_use]
    pub fn for_operator(&self) -> schedr_model::runner::ScriptDeclaration {
        let TaskKind::Script {
            job,
            script,
            interpreter,
            args,
            env,
            working_dir,
        } = &self.task.kind
        else {
            unreachable!("a declaration's task is a script")
        };
        schedr_model::runner::ScriptDeclaration {
            job: job.clone(),
            script: script.clone(),
            interpreter: interpreter.clone(),
            args: args.clone(),
            env: env.clone(),
            secrets: self.task.secrets.clone(),
            working_dir: working_dir.clone(),
            retries: self.given.retries,
            retry_delay_ms: self.given.retry_delay_ms,
            timeout_ms: self.given.timeout_ms,
            queue: self.given.queue.clone(),
            max_output_size: self.given.max_output_size,
        }
    }
}

/// A standalone job's DAG-level settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    /// When it fires.
    pub trigger: Trigger,
    /// The `since` of its first run.
    pub initial_since: Option<Timestamp>,
    /// Its runs at once.
    pub max_active_runs: u32,
}

impl Declaration {
    /// A standalone job's one-task DAG at `location`; `None` for a script for DAGs only.
    #[must_use]
    pub fn dag(&self, location: &DagLocation) -> Option<Dag> {
        let schedule = self.schedule.as_ref()?;
        let id = DagId::new(self.reference.clone()).ok()?;
        Some(Dag {
            produced_by: SchedrVersion::current(),
            id,
            location: location.clone(),
            description: self.description.clone(),
            tags: self.tags.clone(),
            trigger: schedule.trigger.clone(),
            initial_since: schedule.initial_since,
            max_active_runs: Some(schedule.max_active_runs),
            max_active_tasks: None,
            tasks: vec![self.task.clone()],
            edges: Vec::new(),
            groups: Vec::new(),
            params: None,
            resources: Vec::new(),
        })
    }
}

/// Reads and checks a `schedr.toml`: its declarations in the order written, or every problem
/// found, each naming the job and key.
///
/// # Errors
///
/// The problems, when there is one.
pub fn read(text: &str, source: &Source<'_>) -> Result<Vec<Declaration>, Vec<String>> {
    let table: toml::Table = text
        .parse()
        .map_err(|e: toml::de::Error| vec![format!("not valid TOML: {}", e.message())])?;
    let mut problems = Vec::new();
    for key in table.keys() {
        if key != "jobs" {
            problems.push(format!(
                "unknown key `{key}`: a `{FILE_NAME}` has `[jobs.<name>]` tables"
            ));
        }
    }
    let jobs = match table.get("jobs") {
        None => {
            return if problems.is_empty() {
                Ok(Vec::new())
            } else {
                Err(problems)
            };
        }
        Some(Value::Table(jobs)) => jobs,
        Some(_) => {
            problems.push("`jobs` is a table of `[jobs.<name>]` tables".to_owned());
            return Err(problems);
        }
    };
    let mut declarations = Vec::new();
    for (name, job) in jobs {
        let mut job_problems = Vec::new();
        let declaration = job_of(name, job, source, &mut job_problems);
        problems.extend(
            job_problems
                .into_iter()
                .map(|p| format!("job `{name}`: {p}")),
        );
        declarations.extend(declaration);
    }
    if problems.is_empty() {
        Ok(declarations)
    } else {
        Err(problems)
    }
}

/// One declaration, or `None` with its problems added.
fn job_of(
    name: &str,
    job: &Value,
    source: &Source<'_>,
    problems: &mut Vec<String>,
) -> Option<Declaration> {
    if !valid_name(name) {
        problems.push(
            "a job's name is letters, digits, `_` and `-` (it names the job's task too)".to_owned(),
        );
    } else if let Err(error) = TaskId::new(name) {
        problems.push(format!("its name names the job's task: {error}"));
    }
    let Value::Table(keys) = job else {
        problems.push("a job is a table of keys".to_owned());
        return None;
    };
    let mut keys = Keys {
        table: keys,
        problems,
    };
    keys.check_known();
    let schedule = keys.schedule();
    if matches!(schedule, Scheduling::Unscheduled) {
        keys.none_of(
            &DAG_KEYS,
            "a script without `schedule`, `every` or `once` is for DAGs only: it would do nothing",
        );
    }
    let task = keys.task(name, source);
    let given = task
        .as_ref()
        .map(|(_, given)| given.clone())
        .unwrap_or_default();
    let task = task.map(|(task, _)| task);
    let description = keys.string("description");
    let tags = keys.strings("tags").unwrap_or_default();
    let reference = source.reference(name);
    if matches!(schedule, Scheduling::Scheduled(_)) && DagId::new(reference.clone()).is_err() {
        keys.problems.push(format!(
            "its ID `{reference}` isn't a valid DAG ID (at most 250 bytes of letters, digits and \
             `_.-:/#`): shorten the path or the name"
        ));
    }
    if !keys.problems.is_empty() {
        return None;
    }
    Some(Declaration {
        name: name.to_owned(),
        reference,
        task: task?,
        given,
        schedule: match schedule {
            Scheduling::Scheduled(schedule) => Some(schedule),
            Scheduling::Unscheduled | Scheduling::Bad => None,
        },
        description,
        tags,
    })
}

/// What a job's schedule keys say.
enum Scheduling {
    /// None of `schedule`, `every` and `once`: the script is for DAGs only.
    Unscheduled,
    Scheduled(Schedule),
    /// A problem, already reported.
    Bad,
}

/// A job name: letters, digits, `_` and `-` (decision 300).
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// A job's keys, read one by one, problems noted.
struct Keys<'a> {
    table: &'a toml::Table,
    problems: &'a mut Vec<String>,
}

impl Keys<'_> {
    fn check_known(&mut self) {
        for key in self.table.keys() {
            if !JOB_KEYS.contains(&key.as_str()) {
                self.problems.push(format!("unknown key `{key}`"));
            } else if NOT_YET.contains(&key.as_str()) {
                self.problems.push(format!("`{key}` isn't available yet"));
            } else if key == "notify" {
                // Decision 298: no channels yet.
                self.problems
                    .push("`notify` isn't available yet".to_owned());
            }
        }
    }

    fn none_of(&mut self, keys: &[&str], why: &str) {
        for key in keys {
            if self.table.contains_key(*key) && !NOT_YET.contains(key) && *key != "notify" {
                self.problems.push(format!("`{key}`: {why}"));
            }
        }
    }

    fn wrong(&mut self, key: &str, wanted: &str) {
        self.problems.push(format!("`{key}` is {wanted}"));
    }

    fn string(&mut self, key: &str) -> Option<String> {
        let value = self.table.get(key)?;
        if let Value::String(text) = value {
            return Some(text.clone());
        }
        self.wrong(key, "a string");
        None
    }

    fn required_string(&mut self, key: &str) -> Option<String> {
        if !self.table.contains_key(key) {
            self.problems.push(format!("`{key}` is required"));
            return None;
        }
        self.string(key)
    }

    fn strings(&mut self, key: &str) -> Option<Vec<String>> {
        let Value::Array(items) = self.table.get(key)? else {
            self.wrong(key, "a list of strings");
            return None;
        };
        let strings: Option<Vec<String>> = items
            .iter()
            .map(|item| item.as_str().map(str::to_owned))
            .collect();
        if strings.is_none() {
            self.wrong(key, "a list of strings");
        }
        strings
    }

    fn boolean(&mut self, key: &str) -> Option<bool> {
        let value = self.table.get(key)?;
        if let Value::Boolean(value) = value {
            return Some(*value);
        }
        self.wrong(key, "`true` or `false`");
        None
    }

    fn whole(&mut self, key: &str, min: i64) -> Option<u32> {
        match self.table.get(key)? {
            Value::Integer(n) if *n >= min => u32::try_from(*n).ok().or_else(|| {
                self.wrong(key, "a smaller number");
                None
            }),
            _ => {
                self.wrong(key, &format!("a whole number of at least {min}"));
                None
            }
        }
    }

    fn duration_ms(&mut self, key: &str) -> Option<u64> {
        let text = self.string(key)?;
        match schedr_config::units::parse_duration(&text) {
            Ok(duration) => Some(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)),
            Err(error) => {
                self.problems.push(format!("`{key}`: {error}"));
                None
            }
        }
    }

    fn size(&mut self, key: &str) -> Option<u64> {
        let text = self.string(key)?;
        match schedr_config::units::parse_size(&text) {
            Ok(size) => Some(size),
            Err(error) => {
                self.problems.push(format!("`{key}`: {error}"));
                None
            }
        }
    }

    /// The time zone dates and times without an offset are in, and its name (`UTC` by
    /// default).
    fn timezone(&mut self) -> (TimeZone, String) {
        let Some(name) = self.string("timezone") else {
            return (TimeZone::UTC, "UTC".to_owned());
        };
        let tz = TimeZone::get(&name).unwrap_or_else(|_| {
            self.problems
                .push(format!("`timezone`: `{name}` isn't a known time zone"));
            TimeZone::UTC
        });
        (tz, name)
    }

    /// A TOML date or date-time as a moment: with an offset as written, without one in `tz`
    /// (a date at its midnight).
    fn moment(&mut self, key: &str, tz: &TimeZone) -> Option<Timestamp> {
        let Value::Datetime(when) = self.table.get(key)? else {
            self.wrong(
                key,
                "a TOML date or date-time (`2026-01-01`, `2026-01-01T03:00:00`)",
            );
            return None;
        };
        match moment_of(when, tz) {
            Ok(moment) => Some(moment),
            Err(error) => {
                self.problems.push(format!("`{key}`: {error}"));
                None
            }
        }
    }

    /// The job's schedule, if it has one.
    fn schedule(&mut self) -> Scheduling {
        let given: Vec<&str> = ["schedule", "every", "once"]
            .into_iter()
            .filter(|key| self.table.contains_key(*key))
            .collect();
        if given.len() > 1 {
            self.problems.push(format!(
                "at most one of `schedule`, `every` and `once` (it has `{}`)",
                given.join("`, `")
            ));
            return Scheduling::Bad;
        }
        let Some(&kind) = given.first() else {
            return Scheduling::Unscheduled;
        };
        self.scheduled(kind)
            .map_or(Scheduling::Bad, Scheduling::Scheduled)
    }

    /// The schedule given by `kind` (`schedule`, `every` or `once`); `None` after a problem.
    fn scheduled(&mut self, kind: &str) -> Option<Schedule> {
        let (tz, tz_name) = self.timezone();
        let start = self.moment("start", &tz);
        let end = self.moment("end", &tz);
        let catchup = self.boolean("catchup").unwrap_or(false);
        let trigger = match kind {
            "schedule" => Trigger::Cron {
                expression: self.string("schedule")?,
                timezone: tz_name,
                catchup,
                start,
                end,
            },
            "every" => Trigger::Every {
                interval_ms: self.duration_ms("every")?,
                catchup,
                start,
                end,
            },
            _ => {
                self.none_of(
                    &["start", "end", "catchup"],
                    "`once` fires once: it would do nothing",
                );
                Trigger::Once {
                    at: self.moment("once", &tz)?,
                }
            }
        };
        if matches!(trigger, Trigger::Every { interval_ms: 0, .. }) {
            self.wrong("every", "longer than nothing");
        }
        Some(Schedule {
            trigger,
            initial_since: self.moment("initial_since", &tz),
            max_active_runs: self.whole("max_active_runs", 1).unwrap_or(1),
        })
    }

    /// The job's task, named after it, and the settings the file gives.
    fn task(&mut self, name: &str, source: &Source<'_>) -> Option<(Task, Given)> {
        let script = self.path("script", source, true);
        // Checked by the worker that runs it (rejection 3): workers may have different ones.
        let interpreter = self.required_string("interpreter");
        let working_dir = match self.table.get("working_dir") {
            None => Some(source.dir().to_owned()),
            Some(_) => self.path("working_dir", source, false),
        };
        let args = self
            .strings("args")
            .unwrap_or_default()
            .into_iter()
            .map(Template::text)
            .collect();
        let env = self.env();
        let secrets = self.secrets();
        let given = Given {
            retries: self.whole("retries", 0),
            retry_delay_ms: self.duration_ms("retry_delay"),
            timeout_ms: self.duration_ms("timeout"),
            queue: self.string("queue"),
            max_output_size: self.size("max_output_size"),
        };
        let defaults = TaskSettings::default();
        let settings = TaskSettings {
            retries: given.retries.unwrap_or(defaults.retries),
            retry_delay_ms: given.retry_delay_ms.unwrap_or(defaults.retry_delay_ms),
            timeout_ms: given.timeout_ms,
            queue: given.queue.clone().unwrap_or(defaults.queue.clone()),
            max_output_size: given.max_output_size.unwrap_or(defaults.max_output_size),
            ..defaults
        };
        let id = TaskId::new(name).ok()?;
        let task = Task {
            id,
            kind: TaskKind::Script {
                job: source.reference(name),
                script: script?,
                interpreter: interpreter?,
                args,
                env,
                working_dir: working_dir?,
            },
            settings,
            output_type: TypeDesc::ProcessResult,
            outcomes: schedr_model::task::default_outcomes(),
            group: None,
            expand: None,
            secrets,
            connections: Vec::new(),
            cleanup: None,
        };
        Some((task, given))
    }

    /// A path key as a path in the root; a `script` must name a file there.
    fn path(&mut self, key: &str, source: &Source<'_>, file: bool) -> Option<String> {
        let written = if file {
            self.required_string(key)?
        } else {
            self.string(key)?
        };
        match source.in_root(&written) {
            Ok(path) if file && !(source.exists)(&path) => {
                self.problems.push(format!(
                    "`{key}`: `{written}` isn't a file in the script root"
                ));
                None
            }
            Ok(path) => Some(path),
            Err(error) => {
                self.problems.push(format!("`{key}`: {error}"));
                None
            }
        }
    }

    /// `env`: a variable's value as it is, or `{ secret = "path#key", storage = "..." }`.
    fn env(&mut self) -> BTreeMap<String, Template> {
        let mut env = BTreeMap::new();
        let Some(given) = self.table.get("env") else {
            return env;
        };
        let Value::Table(given) = given else {
            self.wrong("env", "a table of variables");
            return env;
        };
        for (variable, value) in given {
            if !valid_variable(variable) {
                self.problems.push(format!(
                    "`env`: `{variable}` isn't a variable name (letters, digits and `_`, not a \
                     digit first)"
                ));
                continue;
            }
            let template = match value {
                Value::String(text) => Some(Template::text(text.clone())),
                Value::Table(secret) => secret_of(secret, false)
                    .map(|name| {
                        Template(vec![TemplatePart::Ref(TemplateRef {
                            reference: Reference::Secret(name),
                            format: None,
                            encode: None,
                        })])
                    })
                    .map_err(|e| self.problems.push(format!("`env`: `{variable}`: {e}")))
                    .ok(),
                _ => {
                    self.problems.push(format!(
                        "`env`: `{variable}` is a string or `{{ secret = \"path#key\" }}`"
                    ));
                    None
                }
            };
            env.extend(template.map(|t| (variable.clone(), t)));
        }
        env
    }

    /// `secrets`: `"path#key"`, `{ secret = "path#key", storage = "..." }` or
    /// `{ prefix = "dir/", storage = "..." }`.
    fn secrets(&mut self) -> Vec<SecretDecl> {
        let Some(given) = self.table.get("secrets") else {
            return Vec::new();
        };
        let Value::Array(items) = given else {
            self.wrong("secrets", "a list of secrets");
            return Vec::new();
        };
        let mut secrets = Vec::new();
        for item in items {
            let declared = match item {
                Value::String(spec) => Ok(SecretDecl::Secret(name_of(spec, None))),
                Value::Table(table) if table.contains_key("prefix") => prefix_of(table),
                Value::Table(table) => secret_of(table, true).map(SecretDecl::Secret),
                _ => Err("an entry is `\"path#key\"` or a table".to_owned()),
            };
            match declared.and_then(|d| d.check().map(|()| d)) {
                Ok(declared) => secrets.push(declared),
                Err(error) => self.problems.push(format!("`secrets`: {error}")),
            }
        }
        secrets
    }
}

/// An environment variable's name.
fn valid_variable(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `path#key`, or `path` for a whole secret, of `storage` (none: the default one).
fn name_of(spec: &str, storage: Option<String>) -> SecretName {
    let (path, key) = match spec.split_once('#') {
        Some((path, key)) => (path, Some(key)),
        None => (spec, None),
    };
    SecretName {
        storage,
        path: path.to_owned(),
        key: key.map(str::to_owned),
        default: None,
    }
}

/// `{ secret = "path#key", storage = "..." }`; in `env`, a key's (a variable is text).
fn secret_of(table: &toml::Table, whole: bool) -> Result<SecretName, String> {
    let mut storage = None;
    let mut spec = None;
    for (key, value) in table {
        match (key.as_str(), value) {
            ("secret", Value::String(text)) => spec = Some(text.as_str()),
            ("storage", Value::String(text)) => storage = Some(text.clone()),
            ("secret" | "storage", _) => return Err(format!("`{key}` is a string")),
            _ => {
                return Err(format!(
                    "unknown key `{key}` (a secret has `secret` and `storage`)"
                ));
            }
        }
    }
    let spec = spec.ok_or("`secret = \"path#key\"` is required")?;
    let name = name_of(spec, storage);
    if !whole && name.key.is_none() {
        return Err(format!(
            "`{spec}` is a whole secret: a variable takes one key (`path#key`)"
        ));
    }
    name.check()?;
    Ok(name)
}

/// `{ prefix = "dir/", storage = "..." }`.
fn prefix_of(table: &toml::Table) -> Result<SecretDecl, String> {
    let mut storage = None;
    let mut prefix = None;
    for (key, value) in table {
        match (key.as_str(), value) {
            ("prefix", Value::String(text)) => prefix = Some(text.clone()),
            ("storage", Value::String(text)) => storage = Some(text.clone()),
            ("prefix" | "storage", _) => return Err(format!("`{key}` is a string")),
            _ => {
                return Err(format!(
                    "unknown key `{key}` (a prefix has `prefix` and `storage`)"
                ));
            }
        }
    }
    let prefix = prefix.ok_or("`prefix` is required")?;
    Ok(SecretDecl::Prefix { storage, prefix })
}

/// A TOML date or date-time as a moment.
fn moment_of(when: &Datetime, tz: &TimeZone) -> Result<Timestamp, String> {
    let text = when.to_string();
    if when.date.is_none() {
        return Err(format!("`{text}` has no date"));
    }
    if when.offset.is_some() {
        return text
            .parse::<Timestamp>()
            .map_err(|e| format!("`{text}`: {e}"));
    }
    let civil = if when.time.is_some() {
        text.parse::<DateTime>()
            .map_err(|e| format!("`{text}`: {e}"))?
    } else {
        text.parse::<Date>()
            .map_err(|e| format!("`{text}`: {e}"))?
            .to_datetime(jiff::civil::Time::midnight())
    };
    tz.to_ambiguous_zoned(civil)
        .compatible()
        .map(|zoned| zoned.timestamp())
        .map_err(|e| format!("`{text}`: {e}"))
}

/// A script root as a parse sees it: where it is configured, and the copy of its code the
/// worker reports (decision 102).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptRoot {
    /// The configured directory (an absolute reference is resolved against it).
    pub configured: std::path::PathBuf,
    /// The copy its files are read from.
    pub copy: std::path::PathBuf,
}

/// The declaration a `ScriptOperator` refers to (decision 293): `reference` is
/// `<root>:<path>#<name>` or `/abs/path/schedr.toml#<name>`, read from the copies of this
/// worker's script roots `scripts`. Returns the script root it names, if it names one (the
/// DAG root then uses it, decision 304, found or not), and the declaration or why it can't
/// be used.
pub fn lookup(
    reference: &str,
    scripts: &BTreeMap<String, ScriptRoot>,
) -> (
    Option<String>,
    Result<schedr_model::runner::ScriptDeclaration, String>,
) {
    let Some((location, name)) = reference.rsplit_once('#') else {
        return (
            None,
            Err(format!(
                "`{reference}`: a reference is `<root>:<path>/{FILE_NAME}#<job>` (or an \
                 absolute path to the file, then `#<job>`)"
            )),
        );
    };
    let resolved = if location.starts_with('/') {
        let file = std::path::Path::new(location);
        scripts.iter().find_map(|(root, at)| {
            let configured = at
                .configured
                .canonicalize()
                .unwrap_or_else(|_| at.configured.clone());
            let relative = file
                .strip_prefix(&configured)
                .or_else(|_| file.strip_prefix(&at.configured))
                .ok()?;
            Some((root.clone(), relative.to_string_lossy().replace('\\', "/")))
        })
    } else {
        location
            .split_once(':')
            .map(|(root, path)| (root.to_owned(), path.to_owned()))
    };
    let Some((root, path)) = resolved else {
        let why = if location.starts_with('/') {
            format!("`{location}` isn't in a script root of this worker (`SCHEDR_SCRIPT_PATH`)")
        } else {
            format!(
                "`{reference}`: a reference is `<root>:<path>/{FILE_NAME}#<job>` (or an \
                 absolute path to the file, then `#<job>`)"
            )
        };
        return (None, Err(why));
    };
    let found = (|| {
        let at = scripts.get(&root).ok_or_else(|| {
            format!("script root `{root}` isn't on this worker (`SCHEDR_SCRIPT_PATH`)")
        })?;
        if !is_config(&path) {
            return Err(format!("`{root}:{path}` isn't a `{FILE_NAME}`"));
        }
        if !stays_inside(&path) {
            return Err(format!(
                "`{root}:{path}`: a reference goes down into its script root (no `..`, not \
                 absolute)"
            ));
        }
        let exists = |file: &str| at.copy.join(file).is_file();
        let source = Source {
            root: &root,
            path: &path,
            exists: &exists,
        };
        let text = std::fs::read_to_string(at.copy.join(&path))
            .map_err(|_| format!("`{root}:{path}` isn't in script root `{root}`"))?;
        let declared = read(&text, &source)
            .map_err(|problems| format!("`{root}:{path}`: {}", problems.join("; ")))?;
        declared
            .iter()
            .find(|declaration| declaration.name == name)
            .map(Declaration::for_operator)
            .ok_or_else(|| format!("`{root}:{path}` declares no job `{name}`"))
    })();
    (Some(root), found)
}

/// Whether a path relative to a root stays in it: only names going down (`.` aside).
#[must_use]
pub fn stays_inside(path: &str) -> bool {
    use std::path::Component;
    !path.is_empty()
        && std::path::Path::new(path)
            .components()
            .all(|part| matches!(part, Component::Normal(_) | Component::CurDir))
}

/// A warning for each declaration whose interpreter isn't one of `known`.
fn unknown_interpreters(declared: &[Declaration], known: &[String]) -> Vec<String> {
    declared
        .iter()
        .filter_map(|declaration| match &declaration.task.kind {
            TaskKind::Script { interpreter, .. } if !known.contains(interpreter) => Some(format!(
                "job `{}`: `interpreter`: `{interpreter}` isn't an `[interpreters]` entry here \
                 (a worker without it rejects the job's tasks)",
                declaration.name
            )),
            _ => None,
        })
        .collect()
}

/// Whether a path in a root is a `schedr.toml`.
#[must_use]
pub fn is_config(path: &str) -> bool {
    path.rsplit('/').next() == Some(FILE_NAME)
}

/// The `schedr.toml` files `wanted` of script root `root`, whose files (`files`, paths with
/// `/`) are in `dir`: a parsed file each, its standalone jobs as DAGs, or its problems as its
/// import error. With `interpreters` (a local check's configuration), a job whose interpreter
/// isn't one of them gets a warning: a worker without it rejects the job's tasks.
#[must_use]
pub fn read_root(
    root: &str,
    dir: &std::path::Path,
    (files, wanted): (&[String], &[String]),
    interpreters: Option<&[String]>,
) -> Vec<ParsedFile> {
    let exists = |path: &str| files.iter().any(|file| file == path);
    wanted
        .iter()
        .map(|path| {
            let source = Source {
                root,
                path,
                exists: &exists,
            };
            let read = std::fs::read_to_string(dir.join(path))
                .map_err(|e| vec![format!("can't read it: {e}")])
                .and_then(|text| self::read(&text, &source));
            let location = DagLocation {
                root: root.to_owned(),
                path: path.clone(),
            };
            match read {
                Ok(declared) => ParsedFile {
                    script_roots: Vec::new(),
                    path: path.clone(),
                    dags: declared.iter().filter_map(|d| d.dag(&location)).collect(),
                    connections: Vec::new(),
                    error: None,
                    warnings: interpreters
                        .map_or_else(Vec::new, |known| unknown_interpreters(&declared, known)),
                },
                Err(problems) => ParsedFile {
                    script_roots: Vec::new(),
                    path: path.clone(),
                    dags: Vec::new(),
                    connections: Vec::new(),
                    error: Some(ErrorSummary {
                        exception: INVALID_JOBS_EXCEPTION.to_owned(),
                        message: message(&problems),
                        traceback: String::new(),
                    }),
                    warnings: Vec::new(),
                },
            }
        })
        .collect()
}

/// The problems of a `schedr.toml` as one import error's message, a line each.
#[must_use]
pub fn message(problems: &[String]) -> String {
    problems.join("\n")
}

#[cfg(test)]
mod tests;
