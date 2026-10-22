//! Running a parsed command: loading the configuration and dispatching to the command.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::Path;

use schedr_config::{Config, ConfigError, FileSource, config_file, load};
use schedr_db::{Database, DbError, SchemaStatus, redact_password};

use crate::cli::{ClientArgs, Command, DagsCommand, DbCommand};
use crate::client::{Auth, Client, ClientError, HttpTransport, POLL_INTERVAL, WAIT_TIMEOUT};
use crate::interpreter::{Host, apply_local};
use crate::times::local_zone;
use crate::user::{Output, run_dags, run_runs};

#[cfg(test)]
mod tests;

/// Why a command failed; the binary prints it and exits with [`AppError::exit_code`].
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// The configuration file named by `--config` or `SCHEDR_CONFIG_FILE` can't be read.
    #[error("can't read the configuration file `{path}`: {reason}")]
    ReadConfig {
        /// The file.
        path: String,
        /// The I/O error.
        reason: String,
    },
    /// The configuration is invalid.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// A database operation failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// The command needs a database and none is configured.
    #[error("no database configured: set `SCHEDR_CONFIG__DATABASE__URL` (or [database] url)")]
    NoDatabase,
    /// The command exists but isn't implemented in this version yet.
    #[error("`schedr {command}` isn't implemented yet (milestone {milestone})")]
    NotImplemented {
        /// The command's name.
        command: &'static str,
        /// The milestone that brings it (PROJECT.md, Roadmap).
        milestone: &'static str,
    },
    /// Talking to the schedr server failed, or the master rejected the command.
    #[error(transparent)]
    Client(#[from] ClientError),
    /// An argument doesn't read (e.g. a `--for` time).
    #[error("{0}")]
    BadArgument(String),
    /// The command did its job and the answer is "no": checks found errors, a run didn't
    /// succeed.
    #[error("{0}")]
    Failed(String),
    /// Writing the command's output failed.
    #[error("can't write output: {0}")]
    Output(#[from] io::Error),
}

impl AppError {
    /// The process exit code: always 1 (2 is reserved for wrong usage, reported by the
    /// argument parser).
    #[must_use]
    pub const fn exit_code(&self) -> u8 {
        1
    }
}

/// Loads the configuration: chooses the file, reads it with `read`, and layers the
/// environment on top. A missing default file is fine; a missing explicit one is an error.
///
/// # Errors
///
/// Returns [`AppError::ReadConfig`] if an explicitly named file can't be read, or
/// [`AppError::Config`] if the configuration is invalid.
pub fn load_config(
    flag: Option<&Path>,
    env: &BTreeMap<String, String>,
    read: impl Fn(&Path) -> io::Result<String>,
) -> Result<Config, AppError> {
    let file = config_file(flag, env);
    let contents = match read(file.path()) {
        Ok(contents) => Some(contents),
        Err(e) if e.kind() == io::ErrorKind::NotFound && !file.is_required() => None,
        Err(e) => {
            return Err(AppError::ReadConfig {
                path: file.path().display().to_string(),
                reason: e.to_string(),
            });
        }
    };
    let source = contents.as_deref().map(|contents| FileSource {
        path: file.path(),
        contents,
    });
    Ok(load(source, env, &[])?)
}

/// A human-readable description of a schema status.
#[must_use]
pub fn describe_status(status: &SchemaStatus) -> String {
    match status {
        SchemaStatus::Uninitialized => "uninitialized: run `schedr db migrate`".to_owned(),
        SchemaStatus::UpToDate => "up to date".to_owned(),
        SchemaStatus::Outdated { missing } => {
            format!(
                "outdated: {} migration(s) missing; run `schedr db migrate`",
                missing.len()
            )
        }
        SchemaStatus::Newer { unknown } => format!(
            "newer than this schedr ({} unknown migration(s)); usable within the same major version",
            unknown.len()
        ),
    }
}

/// Runs a `schedr db` command against the configured database.
///
/// # Errors
///
/// Returns [`AppError::NoDatabase`] without a database URL, or [`AppError::Db`] if
/// connecting or migrating fails.
pub async fn run_db(
    command: DbCommand,
    config: &Config,
    out: &mut impl Write,
) -> Result<(), AppError> {
    let url = config.database.url.as_deref().ok_or(AppError::NoDatabase)?;
    let db = Database::connect(url).await?;
    match command {
        DbCommand::Migrate => {
            db.migrate().await?;
            writeln!(out, "migrated {}: schema up to date", redact_password(url))?;
        }
        DbCommand::Status => {
            let status = db.schema_status().await?;
            writeln!(
                out,
                "{}: {}",
                redact_password(url),
                describe_status(&status)
            )?;
        }
    }
    Ok(())
}

/// Runs a parsed command.
///
/// # Errors
///
/// Returns the command's [`AppError`]; commands of later milestones return
/// [`AppError::NotImplemented`].
pub async fn execute(
    command: &Command,
    config: &Config,
    env: &BTreeMap<String, String>,
    out: &mut impl Write,
    err: &mut impl Write,
) -> Result<(), AppError> {
    match (command, command.milestone()) {
        (Command::Db { command }, _) => run_db(*command, config, out).await,
        (Command::Standalone, None) => {
            crate::standalone::command(&local_config(config, env)?, env).await
        }
        (
            Command::Dags {
                client,
                command: command @ (DagsCommand::Check { .. } | DagsCommand::Test { .. }),
            },
            None,
        ) => {
            let config = local_config(config, env)?;
            crate::local::run_local(command, &config, env, client.json, out).await
        }
        (Command::Dags { client, command }, None) => {
            let (api, output) = connect(client, env);
            run_dags(command, &api, &output, out).await
        }
        (Command::Runs { client, command }, None) => {
            let (api, output) = connect(client, env);
            run_runs(command, &api, &output, out, err).await
        }
        (other, milestone) => Err(AppError::NotImplemented {
            command: other.name(),
            milestone: milestone.unwrap_or("unknown"),
        }),
    }
}

/// The configuration of a local command (`standalone`, `dags check`, `dags test`): the built-in
/// `python` interpreter looked for from here (see [`crate::interpreter`]).
///
/// # Errors
///
/// [`AppError::BadArgument`] if a pyenv shim can't be resolved.
pub fn local_config(config: &Config, env: &BTreeMap<String, String>) -> Result<Config, AppError> {
    let mut config = config.clone();
    let cwd = std::env::current_dir().ok();
    let exe = std::env::current_exe().ok();
    let host = Host {
        env,
        cwd: cwd.as_deref(),
        exe: exe.as_deref(),
    };
    apply_local(&mut config, &host).map_err(|e| AppError::BadArgument(e.to_string()))?;
    Ok(config)
}

/// The client and output settings of a user CLI command: the server, credentials (the password
/// from `SCHEDR_PASSWORD`), `--json` and the local zone (`TZ`, else the system's).
#[must_use]
pub fn connect(
    args: &ClientArgs,
    env: &BTreeMap<String, String>,
) -> (Client<HttpTransport>, Output) {
    let auth = match (&args.token, &args.user) {
        (Some(token), _) => Auth::Bearer(token.clone()),
        (None, Some(user)) => Auth::Basic {
            user: user.clone(),
            password: env.get("SCHEDR_PASSWORD").cloned(),
        },
        (None, None) => Auth::None,
    };
    let client = Client::new(
        HttpTransport::new(&args.server, auth),
        POLL_INTERVAL,
        WAIT_TIMEOUT,
    );
    let output = Output {
        json: args.json,
        local: local_zone(env.get("TZ").map(String::as_str)),
    };
    (client, output)
}
