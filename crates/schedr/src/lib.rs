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
pub mod client;
pub mod local;
pub mod logging;
pub mod standalone;
pub mod times;
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
    let cli = match cli::Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(e) if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) => {
            return report(out, &e.to_string(), 0);
        }
        Err(e) => return report(err, &e.to_string(), EXIT_USAGE),
    };
    let config = match app::load_config(cli.config.as_deref(), env, read_file) {
        Ok(config) => config,
        Err(e) => return report(err, &format!("error: {e}\n"), e.exit_code()),
    };
    if let Err(message) = logging::init(&config.log, stdout_is_terminal) {
        return report(err, &format!("error: {message}\n"), 1);
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
