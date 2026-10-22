//! Durations (`"30s"`, `"5m"`, `"1h30m"`) and sizes (`"4MiB"`) in configuration values.

use std::fmt;
use std::time::Duration;

use serde::de::{self, Deserializer, Visitor};

#[cfg(test)]
mod tests;

/// Why a string isn't a valid duration or size.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnitError {
    /// Not a duration such as `30s`, `5m` or `1h30m`.
    #[error("`{0}` is not a duration (use e.g. `500ms`, `30s`, `5m`, `1h30m`, `2d`)")]
    Duration(String),
    /// Not a size such as `512B`, `64KiB` or `4MiB`.
    #[error("`{0}` is not a size (use e.g. `512B`, `64KiB`, `4MiB`, `1GiB`)")]
    Size(String),
}

/// Parses a duration: one or more `<number><unit>` parts with units `ms`, `s`, `m`, `h`, `d`,
/// each unit at most once and from largest to smallest (`1h30m`, not `30m1h`).
///
/// # Errors
///
/// Returns [`UnitError::Duration`] for anything else, including an empty string and a
/// number without a unit.
pub fn parse_duration(s: &str) -> Result<Duration, UnitError> {
    const UNITS: [(&str, u64); 5] = [
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("m", 60_000),
        ("s", 1_000),
        ("ms", 1),
    ];
    let error = || UnitError::Duration(s.to_owned());
    let mut rest = s;
    let mut total_ms: u64 = 0;
    let mut last_unit = None;
    while !rest.is_empty() {
        let digits = rest.find(|c: char| !c.is_ascii_digit()).ok_or_else(error)?;
        if digits == 0 {
            return Err(error());
        }
        let number: u64 = rest[..digits].parse().map_err(|_| error())?;
        rest = &rest[digits..];
        let unit_len = rest
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(rest.len());
        let unit = &rest[..unit_len];
        let index = UNITS
            .iter()
            .position(|(name, _)| *name == unit)
            .ok_or_else(error)?;
        if last_unit.is_some_and(|last| index <= last) {
            return Err(error());
        }
        last_unit = Some(index);
        let part = number.checked_mul(UNITS[index].1).ok_or_else(error)?;
        total_ms = total_ms.checked_add(part).ok_or_else(error)?;
        rest = &rest[unit_len..];
    }
    if last_unit.is_none() {
        return Err(error());
    }
    Ok(Duration::from_millis(total_ms))
}

/// Parses a size: a whole number followed by `B`, `KiB`, `MiB` or `GiB`.
///
/// # Errors
///
/// Returns [`UnitError::Size`] for anything else (decimal units such as `MB` aren't
/// accepted, to avoid confusing them with binary ones).
pub fn parse_size(s: &str) -> Result<u64, UnitError> {
    const UNITS: [(&str, u64); 4] = [
        ("B", 1),
        ("KiB", 1 << 10),
        ("MiB", 1 << 20),
        ("GiB", 1 << 30),
    ];
    let error = || UnitError::Size(s.to_owned());
    let digits = s.find(|c: char| !c.is_ascii_digit()).ok_or_else(error)?;
    if digits == 0 {
        return Err(error());
    }
    let number: u64 = s[..digits].parse().map_err(|_| error())?;
    let factor = UNITS
        .iter()
        .find(|(name, _)| *name == &s[digits..])
        .map(|(_, f)| *f)
        .ok_or_else(error)?;
    number.checked_mul(factor).ok_or_else(error)
}

/// A serde `deserialize_with` helper for duration fields.
///
/// # Errors
///
/// Fails for a value that isn't a string, or a string [`parse_duration`] rejects.
pub fn deserialize_duration<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    struct DurationVisitor;

    impl Visitor<'_> for DurationVisitor {
        type Value = Duration;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a duration string such as `30s` or `1h30m`")
        }

        fn visit_str<E: de::Error>(self, s: &str) -> Result<Duration, E> {
            parse_duration(s).map_err(E::custom)
        }
    }

    d.deserialize_str(DurationVisitor)
}

/// A serde `deserialize_with` helper for optional size fields (`None` when left out).
///
/// # Errors
///
/// Fails for a value that isn't a string, or a string [`parse_size`] rejects.
pub fn deserialize_optional_size<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    struct SizeVisitor;

    impl Visitor<'_> for SizeVisitor {
        type Value = Option<u64>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a size string such as `4MiB` or `10GiB`")
        }

        fn visit_str<E: de::Error>(self, s: &str) -> Result<Option<u64>, E> {
            parse_size(s).map(Some).map_err(E::custom)
        }
    }

    d.deserialize_str(SizeVisitor)
}
