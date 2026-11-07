//! The schedr binary's library part: everything `main.rs` runs, kept here so it can be tested.
//!
//! [`run`] takes the arguments, the environment, a file reader and the output streams as
//! inputs, so the whole command line can be exercised in unit tests.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::{self, Write};
use std::path::Path;

use clap::Parser;
use clap::error::ErrorKind;

pub mod app;
pub mod cli;
pub mod ctx;
pub use schedr_client as client;
pub mod interpreter;
pub mod local;
pub mod logging;
pub mod node;
pub mod ops;
pub mod roles;
pub mod secret_storage;
pub mod secrets;
pub mod standalone;
pub use schedr_client::times;
pub mod user;

#[cfg(test)]
mod tests;

/// The schedr version, shared by the binary and the Python package.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Exit code for wrong usage (unknown command, bad arguments).
pub const EXIT_USAGE: u8 = 2;

/// Runs `schedr` with the given arguments (including the program name) and returns the
/// process exit code: 0 on success, 1 on an error, 2 on wrong usage.
pub fn run<I, T>(
    args: I,
    env: &BTreeMap<String, String>,
    read_file: impl Fn(&Path) -> io::Result<String>,
    stdout_is_terminal: bool,
    out: &mut impl Write,
    err: &mut impl Write,
) -> u8
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let cli = match cli::Cli::try_parse_from(&args) {
        Ok(cli) => cli,
        Err(e) if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) => {
            return report(out, &e.to_string(), 0);
        }
        // clap quotes what it refused, and a `secrets` argument may be a value (`get etl/db
        // password=...`): only the kind of mistake (Grok's finding in M4.6c's review).
        Err(e) if args.iter().skip(1).any(|a| a == "secrets") => {
            let message = format!(
                "error: {}\n\nRun `schedr secrets --help` (the arguments aren't shown: one may \
                 be a secret's value)\n",
                e.kind()
            );
            return report(err, &message, EXIT_USAGE);
        }
        Err(e) => return report(err, &e.to_string(), EXIT_USAGE),
    };
    // Inside a task, before the configuration: a task may have none, and the call is one
    // request on the task's channel (decision 147: the descriptor is adopted before any
    // thread starts).
    if let cli::Command::DagCtx { command } = &cli.command {
        return match ctx::run_dag_ctx(command, env, VERSION, out) {
            Ok(()) => 0,
            Err(message) => report(err, &format!("error: {message}\n"), 1),
        };
    }
    if let cli::Command::TaskCtx {
        command: cli::TaskCtxCommand::Secret { command },
    } = &cli.command
    {
        return match ctx::run_task_ctx_secret(command, env, VERSION, out) {
            Ok(()) => 0,
            Err(message) => report(err, &format!("error: {message}\n"), 1),
        };
    }
    if let cli::Command::TaskCtx {
        command: cli::TaskCtxCommand::Connection(command),
    } = &cli.command
    {
        return match ctx::run_task_ctx_connection(command, env, VERSION, out) {
            Ok(()) => 0,
            Err(message) => report(err, &format!("error: {message}\n"), 1),
        };
    }
    if let cli::Command::TaskCtx {
        command: cli::TaskCtxCommand::Cleanup { command },
    } = &cli.command
    {
        return match ctx::run_task_ctx_cleanup(command, env, VERSION, out) {
            Ok(()) => 0,
            Err(message) => report(err, &format!("error: {message}\n"), 1),
        };
    }
    if let cli::Command::TaskCtx {
        command: cli::TaskCtxCommand::Resource { command },
    } = &cli.command
    {
        return match ctx::run_task_ctx_resource(command, env, VERSION, out) {
            Ok(()) => 0,
            Err(message) => report(err, &format!("error: {message}\n"), 1),
        };
    }
    let config = match app::load_config(cli.config.as_deref(), env, read_file) {
        Ok(config) => config,
        Err(e) => return report(err, &format!("error: {e}\n"), e.exit_code()),
    };
    if let Err(message) = logging::init(&config.log, stdout_is_terminal) {
        return report(err, &format!("error: {message}\n"), 1);
    }
    for name in &config.timings.overridden {
        tracing::warn!("{name} is set: an unsupported timing, at your own risk");
    }
    for name in &config.limits.overridden {
        tracing::warn!("{name} is set: an unsupported limit, at your own risk");
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            return report(
                err,
                &format!("error: can't start the async runtime: {e}\n"),
                1,
            );
        }
    };
    match runtime.block_on(app::execute(&cli.command, &config, env, out, err)) {
        Ok(()) => 0,
        Err(e) => report(err, &format!("error: {e}\n"), e.exit_code()),
    }
}

/// Writes `message` to `stream` and returns `code` (a failed write can't be reported anyway).
fn report(stream: &mut impl Write, message: &str, code: u8) -> u8 {
    let _ = stream.write_all(message.as_bytes());
    code
}
