//! Durations (`"30s"`, `"5m"`, `"1h30m"`) and sizes (`"4MiB"`) in configuration values.

use std::borrow::Cow;
use std::fmt;
use std::fmt::Write as _;
use std::time::Duration;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::Serializer;
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
    /// Not a memory reserve: a percentage up to 90 % or a size.
    #[error("`{0}` is not a memory reserve (use e.g. `25%`, at most `90%`, or a size like `1GiB`)")]
    Reserve(String),
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

/// A serde `deserialize_with` helper for size fields.
///
/// # Errors
///
/// Fails for a value that isn't a string, or a string [`parse_size`] rejects.
pub fn deserialize_size<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    struct SizeVisitor;

    impl Visitor<'_> for SizeVisitor {
        type Value = u64;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a size string such as `4MiB` or `10GiB`")
        }

        fn visit_str<E: de::Error>(self, s: &str) -> Result<u64, E> {
            parse_size(s).map_err(E::custom)
        }
    }

    d.deserialize_str(SizeVisitor)
}

/// A serde `deserialize_with` helper for optional size fields (`None` when left out).
///
/// # Errors
///
/// Fails for a value that isn't a string, or a string [`parse_size`] rejects.
pub fn deserialize_optional_size<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    deserialize_size(d).map(Some)
}

/// Writes a duration as [`parse_duration`] reads it, in the largest units that are exact
/// (`2m`, not `120s`; `1h30m`; `0s` for zero).
#[must_use]
pub fn format_duration(duration: Duration) -> String {
    const UNITS: [(&str, u128); 5] = [
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("m", 60_000),
        ("s", 1_000),
        ("ms", 1),
    ];
    let mut rest = duration.as_millis();
    if rest == 0 {
        return "0s".to_owned();
    }
    let mut text = String::new();
    for (name, ms) in UNITS {
        if rest >= ms {
            let _ = write!(text, "{}{name}", rest / ms);
            rest %= ms;
        }
    }
    text
}

/// Writes a size as [`parse_size`] reads it, in the largest unit that is exact (`4MiB`, not
/// `4096KiB`).
#[must_use]
pub fn format_size(bytes: u64) -> String {
    const UNITS: [(&str, u64); 3] = [("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)];
    UNITS
        .iter()
        .find(|(_, factor)| bytes != 0 && bytes.is_multiple_of(*factor))
        .map_or_else(
            || format!("{bytes}B"),
            |(name, factor)| format!("{}{name}", bytes / factor),
        )
}

/// A serde `serialize_with` helper for duration fields (see [`format_duration`]).
///
/// # Errors
///
/// The serializer's.
pub fn serialize_duration<S: Serializer>(duration: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format_duration(*duration))
}

/// A serde `serialize_with` helper for optional size fields (see [`format_size`]).
///
/// # Errors
///
/// The serializer's.
pub fn serialize_optional_size<S: Serializer>(size: &Option<u64>, s: S) -> Result<S::Ok, S::Error> {
    match size {
        Some(bytes) => s.serialize_some(&format_size(*bytes)),
        None => s.serialize_none(),
    }
}

/// Writes a size as [`format_size`] does.
///
/// # Errors
///
/// The serializer's.
pub fn serialize_size<S: Serializer>(size: &u64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format_size(*size))
}

/// The JSON Schema of a duration field: a string with format `duration`.
#[derive(Debug)]
pub struct DurationText;

impl JsonSchema for DurationText {
    fn schema_name() -> Cow<'static, str> {
        "Duration".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "format": "duration",
            "description": "A duration such as `500ms`, `30s`, `5m`, `1h30m` or `2d`."
        })
    }
}

/// The JSON Schema of a size field: a string with format `size`.
#[derive(Debug)]
pub struct SizeText;

impl JsonSchema for SizeText {
    fn schema_name() -> Cow<'static, str> {
        "Size".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "format": "size",
            "description": "A size such as `512B`, `64KiB`, `4MiB` or `1GiB`."
        })
    }
}

/// `[worker] memory_reserve`: the part of the pod's memory limit kept out of task budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryReserve {
    /// A percentage of the limit (`25%`).
    Percent(u8),
    /// A size (`1GiB`).
    Size(u64),
}

/// Parses a memory reserve: `<n>%` with `n` at most 90, or a size (see [`parse_size`]).
///
/// # Errors
///
/// Returns [`UnitError::Reserve`] for anything else.
pub fn parse_reserve(s: &str) -> Result<MemoryReserve, UnitError> {
    let error = || UnitError::Reserve(s.to_owned());
    match s.strip_suffix('%') {
        Some(number) => match number.parse::<u8>() {
            Ok(percent) if percent <= 90 => Ok(MemoryReserve::Percent(percent)),
            _ => Err(error()),
        },
        None => parse_size(s).map(MemoryReserve::Size).map_err(|_| error()),
    }
}

/// Writes a memory reserve as [`parse_reserve`] reads it.
#[must_use]
pub fn format_reserve(reserve: MemoryReserve) -> String {
    match reserve {
        MemoryReserve::Percent(percent) => format!("{percent}%"),
        MemoryReserve::Size(bytes) => format_size(bytes),
    }
}

impl<'de> serde::Deserialize<'de> for MemoryReserve {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        parse_reserve(&text).map_err(de::Error::custom)
    }
}

impl serde::Serialize for MemoryReserve {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format_reserve(*self))
    }
}

impl JsonSchema for MemoryReserve {
    fn schema_name() -> Cow<'static, str> {
        "MemoryReserve".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "format": "reserve",
            "description": "A percentage of the pod's memory limit (`25%`, at most `90%`) or a size (`1GiB`)."
        })
    }
}
