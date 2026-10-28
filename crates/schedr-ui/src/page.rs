//! What every page needs from its request: the path prefix links start with, and the
//! browser's time zone (from the cookie the page's script sets).

use axum::http::HeaderMap;
use jiff::Timestamp;
use jiff::tz::TimeZone;
use schedr_client::times::show;

#[cfg(test)]
mod tests;

/// The header a reverse proxy serving the UI under a path sets (e.g. `/schedr`).
pub const PREFIX_HEADER: &str = "x-forwarded-prefix";

/// The cookie holding the browser's IANA time zone, set by the page's inline script.
pub const ZONE_COOKIE: &str = "schedr_tz";

/// A page's context.
#[derive(Debug, Clone)]
pub struct Page {
    /// What links start with: the proxy's prefix, without a trailing slash (`""` at the root).
    pub base: String,
    /// The browser's zone, once its cookie is set; `None`: times in UTC only.
    pub zone: Option<TimeZone>,
    /// Whether the documentation is served (a link in the header).
    pub docs: bool,
    /// The page's stream of changes (`/events?...`), with the prefix; empty: the page isn't
    /// live.
    pub events: String,
    /// Where the page's live parts are read again from when something changed, with the
    /// prefix.
    pub refresh: String,
}

impl Page {
    /// The context of a request with `headers`.
    #[must_use]
    pub fn new(headers: &HeaderMap, docs: bool) -> Self {
        Self {
            base: prefix(headers),
            zone: zone(headers),
            docs,
            events: String::new(),
            refresh: String::new(),
        }
    }

    /// This page live: it follows `/events?<follow>` and reads its live parts again from
    /// `refresh` (both without the prefix, which is added).
    #[must_use]
    pub fn live(self, follow: &str, refresh: &str) -> Self {
        Self {
            events: format!("{}/events?{follow}", self.base),
            refresh: format!("{}{refresh}", self.base),
            ..self
        }
    }

    /// Whether the page is live.
    #[must_use]
    pub const fn is_live(&self) -> bool {
        !self.events.is_empty()
    }

    /// Whether the browser's zone is known (the script then doesn't reload the page).
    #[must_use]
    pub const fn has_zone(&self) -> bool {
        self.zone.is_some()
    }

    /// A moment as people read it: `2026-10-15 03:00 UTC (05:00 CEST)`, UTC only until the
    /// browser's zone is known.
    #[must_use]
    pub fn time(&self, at: &Timestamp) -> String {
        show(*at, self.zone.as_ref().unwrap_or(&TimeZone::UTC))
    }

    /// As [`Self::time`], for a narrow column, in lines: the date, the UTC time, the local
    /// time in brackets (shown by a cell with `white-space: pre-line`; non-breaking spaces
    /// keep each line whole).
    #[must_use]
    pub fn narrow_time(&self, at: &Timestamp) -> String {
        let text = self.time(at);
        let (utc, local) = text.split_once(" (").unwrap_or((&text, ""));
        let (date, clock) = utc.split_once(' ').unwrap_or((utc, ""));
        let keep = |part: &str| part.replace(' ', "\u{a0}");
        let mut lines = vec![date.to_owned(), keep(clock)];
        if !local.is_empty() {
            lines.push(format!("({}", keep(local)));
        }
        lines.join("\n")
    }
}

/// `X-Forwarded-Prefix` when it is a plain path (`/` and letters, digits, `-`, `_`, `.`,
/// `~`; no `..`), without its trailing slash; else `""`. Anything else is ignored rather than
/// put into links.
#[must_use]
pub fn prefix(headers: &HeaderMap) -> String {
    let Some(value) = headers.get(PREFIX_HEADER).and_then(|v| v.to_str().ok()) else {
        return String::new();
    };
    let plain = value.starts_with('/')
        && !value.contains("..")
        && !value.contains("//")
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/-_.~".contains(&b));
    if plain {
        value.trim_end_matches('/').to_owned()
    } else {
        String::new()
    }
}

/// The time zone in the `schedr_tz` cookie, when it names one jiff knows.
#[must_use]
pub fn zone(headers: &HeaderMap) -> Option<TimeZone> {
    headers
        .get_all(axum::http::header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|cookies| cookies.split(';'))
        .filter_map(|cookie| cookie.trim().strip_prefix(ZONE_COOKIE)?.strip_prefix('='))
        .find_map(|name| TimeZone::get(&percent_decoded(name)?).ok())
}

/// `text` with its `%XX` escapes decoded (as `encodeURIComponent` writes them); `None` for a
/// malformed escape or bytes that aren't UTF-8.
fn percent_decoded(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}
