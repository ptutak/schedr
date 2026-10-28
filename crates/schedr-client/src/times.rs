//! Times in the CLI: reading `--for` and showing a moment in UTC and local time (see
//! PROJECT.md, Web UI and CLI: times and time zones).
//!
//! `--for` without an offset is **UTC**, with a warning naming the moment taken; an explicit
//! offset is used as given. Every time shown to a person appears twice, UTC and the machine's
//! zone (`2026-10-15 03:00 UTC (05:00 CEST)`); JSON output has UTC only.

use jiff::civil::{Date, DateTime};
use jiff::tz::TimeZone;
use jiff::{Timestamp, Zoned};

#[cfg(test)]
mod tests;

/// A moment read from `--for`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForTime {
    /// The moment.
    pub at: Timestamp,
    /// A warning when it had no offset and was read as UTC.
    pub warning: Option<String>,
}

/// Reads a `--for` value: an RFC 3339 / ISO 8601 date-time with an offset, or one without
/// (UTC), or a date (midnight UTC).
///
/// # Errors
///
/// A message naming the accepted forms.
pub fn parse_for(text: &str) -> Result<ForTime, String> {
    if let Ok(at) = text.parse::<Timestamp>() {
        return Ok(ForTime { at, warning: None });
    }
    let civil = text
        .parse::<DateTime>()
        .or_else(|_| {
            text.parse::<Date>()
                .map(|d| d.to_datetime(jiff::civil::Time::midnight()))
        })
        .map_err(|_| {
            format!(
                "`--for {text}` isn't a date or a date-time \
                 (e.g. 2026-10-15T03:00, 2026-10-15T03:00+02:00, 2026-10-15)"
            )
        })?;
    let at = civil
        .to_zoned(TimeZone::UTC)
        .map_err(|e| e.to_string())?
        .timestamp();
    Ok(ForTime {
        at,
        warning: Some(format!(
            "warning: no offset given: interpreted as {} (UTC)",
            short_utc(at)
        )),
    })
}

/// A moment in UTC and in `local`, e.g. `2026-10-15 03:00 UTC (05:00 CEST)`; the local date is
/// shown only when it differs, and the local part is left out when `local` is UTC.
#[must_use]
pub fn show(at: Timestamp, local: &TimeZone) -> String {
    let utc = at.to_zoned(TimeZone::UTC);
    let text = format!("{} {} UTC", utc.strftime("%Y-%m-%d"), clock(&utc));
    let here = at.to_zoned(local.clone());
    if here.offset() == utc.offset() && local.iana_name() == Some("UTC") {
        return text;
    }
    let date = if here.date() == utc.date() {
        String::new()
    } else {
        format!("{} ", here.strftime("%Y-%m-%d"))
    };
    format!("{text} ({date}{} {})", clock(&here), here.strftime("%Z"))
}

/// A moment in short form, where space matters: UTC, then the local zone's offset from UTC
/// at that moment, e.g. `2026-01-01 00:00Z (local:+1h)`, `2026-10-15 01:00Z
/// (local:-4h)`, `2026-10-15 03:00Z (local:+5h30m)`; no offset part when it is zero.
#[must_use]
pub fn show_short(at: Timestamp, local: &TimeZone) -> String {
    let utc = at.to_zoned(TimeZone::UTC);
    let text = format!("{} {}Z", utc.strftime("%Y-%m-%d"), clock(&utc));
    let seconds = local.to_offset(at).seconds();
    if seconds == 0 {
        return text;
    }
    let sign = if seconds < 0 { '-' } else { '+' };
    let (hours, minutes) = (seconds.abs() / 3600, seconds.abs() % 3600 / 60);
    let offset = if minutes == 0 {
        format!("{hours}h")
    } else {
        format!("{hours}h{minutes:02}m")
    };
    format!("{text} (local:{sign}{offset})")
}

/// `HH:MM`, with seconds when they aren't zero.
fn clock(zoned: &Zoned) -> String {
    if zoned.second() == 0 && zoned.subsec_nanosecond() == 0 {
        zoned.strftime("%H:%M").to_string()
    } else {
        zoned.strftime("%H:%M:%S").to_string()
    }
}

/// `2026-10-15T03:00Z`, as the `--for` warning shows it.
fn short_utc(at: Timestamp) -> String {
    let utc = at.to_zoned(TimeZone::UTC);
    format!("{}T{}Z", utc.strftime("%Y-%m-%d"), clock(&utc))
}

/// The machine's time zone: `TZ` when set and known, else the system's, else UTC.
#[must_use]
pub fn local_zone(tz: Option<&str>) -> TimeZone {
    tz.and_then(|name| TimeZone::get(name.trim_start_matches(':')).ok())
        .or_else(|| TimeZone::try_system().ok())
        .unwrap_or(TimeZone::UTC)
}

/// A trigger as people write it: `0 3 * * * (Europe/Warsaw)`, `every 1h, catchup`,
/// `@daily, 2025-12-31 23:00Z (local:+1h)..` (its window, either end open), `once at …`,
/// `-`; times in UTC with `local`'s offset. `trigger` is a DAG's trigger in DAG JSON.
#[must_use]
pub fn trigger_text(trigger: &serde_json::Value, local: &TimeZone) -> String {
    let time = |key: &str| {
        trigger[key]
            .as_str()
            .and_then(|text| text.parse::<Timestamp>().ok())
            .map(|at| show_short(at, local))
    };
    let text = match trigger["kind"].as_str() {
        Some("cron") => {
            let expression = trigger["expression"].as_str().unwrap_or_default();
            match trigger["timezone"].as_str() {
                Some("UTC") | None => expression.to_owned(),
                Some(zone) => format!("{expression} ({zone})"),
            }
        }
        Some("every") => {
            let ms = trigger["interval_ms"].as_u64().unwrap_or(0);
            format!(
                "every {:#}",
                jiff::SignedDuration::from_millis(i64::try_from(ms).unwrap_or(i64::MAX))
            )
        }
        Some("once") => format!("once at {}", time("at").unwrap_or_default()),
        _ => "-".to_owned(),
    };
    let mut parts = vec![text];
    if trigger["catchup"].as_bool() == Some(true) {
        parts.push("catchup".to_owned());
    }
    match (time("start"), time("end")) {
        (None, None) => {}
        (start, end) => parts.push(format!(
            "{}..{}",
            start.unwrap_or_default(),
            end.unwrap_or_default()
        )),
    }
    parts.join(", ")
}
