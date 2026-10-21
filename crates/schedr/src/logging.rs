//! schedr's own logs (see PROJECT.md, Observability): stdout, JSON in pods and readable text
//! in a terminal, with `RUST_LOG`-style level filters.

use schedr_config::{LogFormat, LogSection};
use tracing_subscriber::EnvFilter;

#[cfg(test)]
mod tests;

/// The format actually used once `auto` is resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedFormat {
    /// One JSON object per line.
    Json,
    /// Human-readable lines.
    Text,
}

/// Resolves `[log] format`: `auto` is text in a terminal and JSON otherwise (pods).
#[must_use]
pub const fn resolve_format(format: LogFormat, stdout_is_terminal: bool) -> ResolvedFormat {
    match (format, stdout_is_terminal) {
        (LogFormat::Json, _) | (LogFormat::Auto, false) => ResolvedFormat::Json,
        (LogFormat::Text, _) | (LogFormat::Auto, true) => ResolvedFormat::Text,
    }
}

/// The level filter for `[log] level`, e.g. `info` or `info,schedr_master=debug`.
///
/// # Errors
///
/// Returns the filter's own message if the level isn't a valid filter directive.
pub fn level_filter(level: &str) -> Result<EnvFilter, String> {
    EnvFilter::try_new(level).map_err(|e| format!("[log] level `{level}`: {e}"))
}

/// Installs the global logger. Returns `false` if one was already installed (only the first
/// call in a process takes effect).
///
/// # Errors
///
/// Returns a message if `[log] level` is invalid.
pub fn init(section: &LogSection, stdout_is_terminal: bool) -> Result<bool, String> {
    let filter = level_filter(&section.level)?;
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    let installed = match resolve_format(section.format, stdout_is_terminal) {
        ResolvedFormat::Json => builder.json().try_init().is_ok(),
        ResolvedFormat::Text => builder.try_init().is_ok(),
    };
    Ok(installed)
}
