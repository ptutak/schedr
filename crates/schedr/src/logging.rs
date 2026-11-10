//! schedr's own logs (see archive/PROJECT.md, Observability): stdout, JSON in pods and readable
//! text in a terminal, with `RUST_LOG`-style level filters.

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

/// The target of sqlx's connect-options warnings, which quote a database URL's unknown
/// parameter with its value, a password among them (`PassWord=`, `sslpassword=`: B203's
/// review); schedr warns of such a parameter by name instead.
const SQLX_OPTIONS: &str = "sqlx_postgres::options";

/// A filter dropping sqlx's connect-options events (`sqlx_postgres::options`) whatever
/// `[log] level` says: a layer of its own, so no directive of the level
/// (`sqlx_postgres::options::parse=debug`) brings them back.
#[must_use]
pub fn quiet_sqlx_options()
-> tracing_subscriber::filter::FilterFn<impl Fn(&tracing::Metadata<'_>) -> bool> {
    tracing_subscriber::filter::filter_fn(|metadata| !metadata.target().starts_with(SQLX_OPTIONS))
}

/// Installs the global logger. Returns `false` if one was already installed (only the first
/// call in a process takes effect).
///
/// # Errors
///
/// Returns a message if `[log] level` is invalid.
pub fn init(section: &LogSection, stdout_is_terminal: bool) -> Result<bool, String> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    let filter = level_filter(&section.level)?;
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    let installed = match resolve_format(section.format, stdout_is_terminal) {
        ResolvedFormat::Json => builder
            .json()
            .finish()
            .with(quiet_sqlx_options())
            .try_init(),
        ResolvedFormat::Text => builder.finish().with(quiet_sqlx_options()).try_init(),
    }
    .is_ok();
    Ok(installed)
}
