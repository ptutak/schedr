//! The two database backends and their migrations.
//!
//! Production uses Postgres, local development SQLite (see archive/PROJECT.md, State storage). Each
//! backend has its own migration files because column types differ (`TIMESTAMPTZ` / `UUID` /
//! `JSONB` in Postgres, text in SQLite); both sets always have the same versions.

use sqlx::migrate::Migrator;

#[cfg(test)]
mod tests;

/// The migrations for SQLite, built into the binary.
static SQLITE_MIGRATIONS: Migrator = sqlx::migrate!("migrations/sqlite");

/// The migrations for Postgres, built into the binary.
static POSTGRES_MIGRATIONS: Migrator = sqlx::migrate!("migrations/postgres");

/// A database backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Backend {
    /// SQLite: local development and standalone mode.
    Sqlite,
    /// Postgres: production.
    Postgres,
}

/// The URL doesn't name a supported database.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unsupported database URL `{0}` (use `sqlite:...` or `postgres://...`)")]
pub struct UnsupportedUrl(pub String);

impl Backend {
    /// The backend a connection URL names: `sqlite:` or `postgres://` / `postgresql://`.
    ///
    /// # Errors
    ///
    /// Returns [`UnsupportedUrl`] for any other scheme. The URL in the error has any password
    /// removed.
    pub fn from_url(url: &str) -> Result<Self, UnsupportedUrl> {
        if url.starts_with("sqlite:") {
            Ok(Self::Sqlite)
        } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Ok(Self::Postgres)
        } else {
            Err(UnsupportedUrl(redact_password(url)))
        }
    }

    /// The migrations for this backend.
    #[must_use]
    pub fn migrator(self) -> &'static Migrator {
        match self {
            Self::Sqlite => &SQLITE_MIGRATIONS,
            Self::Postgres => &POSTGRES_MIGRATIONS,
        }
    }

    /// The versions of this backend's migrations, in order.
    #[must_use]
    pub fn migration_versions(self) -> Vec<i64> {
        self.migrator().iter().map(|m| m.version).collect()
    }
}

/// `url` with its passwords replaced by `***`, for messages and logs: the user-info part's
/// (`user:***@host`) and a query parameter's naming one (`?password=***`, `sslpassword`;
/// B203: sqlx takes both).
#[must_use]
pub fn redact_password(url: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (url, None),
    };
    let mut out = redact_user_info(base);
    if let Some(query) = query {
        let (query, fragment) = match query.split_once('#') {
            Some((query, fragment)) => (query, Some(fragment)),
            None => (query, None),
        };
        out.push('?');
        let pairs: Vec<String> = query
            .split('&')
            .map(|pair| match pair.split_once('=') {
                Some((name, _)) if names_a_password(name) => format!("{name}=***"),
                _ => pair.to_owned(),
            })
            .collect();
        out.push_str(&pairs.join("&"));
        if let Some(fragment) = fragment {
            out.push('#');
            out.push_str(fragment);
        }
    }
    out
}

/// The query parameters sqlx 0.9 reads from a Postgres URL (`sqlx-postgres`'s
/// `options/parse.rs`); it ignores any other, warning with its value.
const POSTGRES_PARAMETERS: [&str; 18] = [
    "sslmode",
    "ssl-mode",
    "sslrootcert",
    "ssl-root-cert",
    "ssl-ca",
    "sslcert",
    "ssl-cert",
    "sslkey",
    "ssl-key",
    "statement-cache-capacity",
    "host",
    "hostaddr",
    "port",
    "dbname",
    "user",
    "password",
    "application_name",
    "options",
];

/// The names of a Postgres URL's query parameters sqlx would ignore (`sslmod=require`, a
/// typo), percent-decoded: for schedr to warn of them by name, since sqlx's own warning,
/// kept out of the log, shows their values (B203's review).
#[must_use]
pub fn unknown_postgres_parameters(url: &str) -> Vec<String> {
    let Some((_, query)) = url.split_once('?') else {
        return Vec::new();
    };
    let query = query.split('#').next().unwrap_or_default();
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| percent_decoded(pair.split_once('=').map_or(pair, |(name, _)| name)))
        .filter(|name| {
            let indexed = name.starts_with("options[") && name.ends_with(']');
            !(indexed || POSTGRES_PARAMETERS.contains(&name.as_str()))
        })
        .collect()
}

/// Whether a query parameter's name, percent-decoded and in any case, names a password.
#[must_use]
pub fn names_a_password(name: &str) -> bool {
    percent_decoded(name)
        .to_ascii_lowercase()
        .ends_with("password")
}

/// `%70assword` → `password`; tabs and line breaks dropped, a malformed
/// escape kept as it is.
fn percent_decoded(text: &str) -> String {
    // Tabs and line breaks dropped first, as a URL parser drops them (sqlx's does: B203's
    // review, `pass<TAB>word=` is its `password`).
    let text: String = text
        .chars()
        .filter(|c| !matches!(c, '\t' | '\n' | '\r'))
        .collect();
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok());
        match (bytes[i], hex.and_then(|h| u8::from_str_radix(h, 16).ok())) {
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (byte, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn redact_user_info(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, path) = rest.split_at(authority_end);
    match authority.rsplit_once('@') {
        Some((user_info, host)) => match user_info.split_once(':') {
            Some((user, _password)) => format!("{scheme}://{user}:***@{host}{path}"),
            None => url.to_owned(),
        },
        None => url.to_owned(),
    }
}
