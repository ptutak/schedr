//! A configuration as it may be shown or sent (decision 122, the user's requirement of
//! 2026-10-31): masked by the node itself before it leaves the process, so no secret is stored
//! or sent in clear.
//!
//! Secrets come only from environment variables, never the configuration file; the masking is
//! the second line, for what a URL or a setting carries anyway. It works on the serialized
//! configuration, every string of it, so a field added later is covered without being named:
//!
//! - **A value under a key naming a secret** is `****` whole, whatever it is (text, a number,
//!   a list, an object). A name is compared without case and separators (`api_key`, `apiKey`
//!   and `API-KEY` alike); a path to a secret file (`token_file`, `[tls] key`) is shown: it is
//!   not the secret.
//! - **In any text, a password before `@`** is `****`, the user, host, port and path kept
//!   (`postgres://schedr:****@db:5432/schedr`): with a scheme or `//` or none (`u:p@db`),
//!   several in one text, a password holding `@` or `/`.
//! - **In any text, a value after a name naming a secret** and `=` or `:` is `****`: a URL's
//!   query (`?password=…`, the name percent-decoded: `%70assword`), a DSN
//!   (`host=db password=…`), a message (`token: …`).
//!
//! Masking errs towards hiding: a port before `@` may be hidden with a password, an email's
//! name after `mailto:` too. Never the other way.

use serde_json::Value;

#[cfg(test)]
mod tests;

/// What a masked secret is shown as.
pub const MASK: &str = "****";

/// `config` serialized and masked (see the module documentation).
#[must_use]
pub fn masked(config: &crate::Config) -> Value {
    let mut value = serde_json::to_value(config).unwrap_or(Value::Null);
    mask(&mut value);
    value
}

/// Masks every secret in `value`, in place.
pub fn mask(value: &mut Value) {
    match value {
        Value::String(text) => *text = mask_text(text),
        Value::Array(items) => items.iter_mut().for_each(mask),
        Value::Object(fields) => {
            for (name, field) in fields.iter_mut() {
                if holds_secret(name, field) {
                    *field = Value::String(MASK.to_owned());
                } else {
                    mask(field);
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Whether the setting `name` holds a secret as `value` (not a file, path or header naming
/// one; nothing to hide in an empty or unset one).
fn holds_secret(name: &str, value: &Value) -> bool {
    let empty = match value {
        Value::Null => true,
        Value::String(text) => text.is_empty(),
        _ => false,
    };
    if empty {
        return false;
    }
    let name = compact(name);
    let points_elsewhere = ["file", "files", "path", "paths", "dir", "header", "env"]
        .iter()
        .any(|end| name.ends_with(end));
    if points_elsewhere {
        return false;
    }
    // `[tls] key` and the like: a path to the key, shown.
    if name == "key" {
        return !value.as_str().is_some_and(looks_like_path);
    }
    names_secret(&name)
}

/// `name` lowercased, without `_`, `-`, `.` and spaces.
fn compact(name: &str) -> String {
    name.chars()
        .filter(|c| !matches!(c, '_' | '-' | '.' | ' '))
        .flat_map(char::to_lowercase)
        .collect()
}

/// Whether a compacted name names a secret.
fn names_secret(name: &str) -> bool {
    [
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "credential",
        "apikey",
        "privatekey",
        "accesskey",
    ]
    .iter()
    .any(|word| name.contains(word))
        || name == "key"
        || (name.len() > 3 && name.ends_with("key"))
}

fn looks_like_path(text: &str) -> bool {
    text.contains('/')
        || text.contains('\\')
        || std::path::Path::new(text)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("pem") || ext.eq_ignore_ascii_case("key"))
}

/// `text` with every secret in it masked: passwords before `@`, and values after a name
/// naming a secret (see the module documentation).
#[must_use]
pub fn mask_text(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut hidden = vec![false; chars.len()];
    for (at, &c) in chars.iter().enumerate() {
        match c {
            '@' => hide_password_before(&chars, at, &mut hidden),
            '=' | ':' => hide_value_after(&chars, at, &mut hidden),
            _ => {}
        }
    }
    let mut out = String::with_capacity(text.len());
    let mut in_mask = false;
    for (c, hide) in chars.iter().zip(&hidden) {
        if *hide {
            if !in_mask {
                out.push_str(MASK);
                in_mask = true;
            }
        } else {
            out.push(*c);
            in_mask = false;
        }
    }
    out
}

/// Where a text's parts end: what a password before `@` or a value never runs past.
fn ends_part(c: char) -> bool {
    c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>' | '(' | ')' | '`')
}

/// Hides the password of the user info ending at `@` (`at`): from its first `:` on. The user
/// info starts after `//`, the start or the end of a part; past an `@` before it only when
/// no `:` came yet (a password holding `@`).
fn hide_password_before(chars: &[char], at: usize, hidden: &mut [bool]) {
    let mut start = at;
    let mut colon_seen = false;
    while start > 0 {
        let c = chars[start - 1];
        if ends_part(c) || (c == '/' && start >= 2 && chars[start - 2] == '/') {
            break;
        }
        if c == '@' && colon_seen {
            break;
        }
        if c == ':' {
            colon_seen = true;
        }
        start -= 1;
    }
    let Some(colon) = (start..at).find(|&i| chars[i] == ':') else {
        return;
    };
    for flag in &mut hidden[colon + 1..at] {
        *flag = true;
    }
}

/// Hides the value after `=` or `:` (`at`) when the name before it names a secret: to the
/// end of the part, or its closing quote.
fn hide_value_after(chars: &[char], at: usize, hidden: &mut [bool]) {
    // The name: letters, digits, `_ - . %` right before (a quote and spaces allowed between).
    let mut end = at;
    while end > 0 && chars[end - 1] == ' ' {
        end -= 1;
    }
    if end > 0 && matches!(chars[end - 1], '"' | '\'') {
        end -= 1;
    }
    let mut start = end;
    while start > 0
        && (chars[start - 1].is_alphanumeric() || matches!(chars[start - 1], '_' | '-' | '.' | '%'))
    {
        start -= 1;
    }
    if start == end {
        return;
    }
    let name: String = chars[start..end].iter().collect();
    if !names_secret(&compact(&percent_decoded(&name))) {
        return;
    }
    // A URL's scheme (`token://…`) isn't a value: its user info is masked on its own.
    if chars[at] == ':' && chars.get(at + 1) == Some(&'/') {
        return;
    }
    let mut from = at + 1;
    while from < chars.len() && chars[from] == ' ' {
        from += 1;
    }
    let quote = chars.get(from).copied().filter(|c| matches!(c, '"' | '\''));
    if quote.is_some() {
        from += 1;
    }
    let mut to = from;
    while to < chars.len() {
        let c = chars[to];
        let ends = match quote {
            Some(q) => c == q,
            None => ends_part(c) || matches!(c, '&' | ';' | '#'),
        };
        if ends {
            break;
        }
        to += 1;
    }
    for flag in &mut hidden[from..to] {
        *flag = true;
    }
}

/// `text` with its `%XX` escapes decoded (a bad one kept as it is).
fn percent_decoded(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let Some(hex) = text.get(i + 1..i + 3)
            && let Ok(byte) = u8::from_str_radix(hex, 16)
        {
            out.push(byte);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
