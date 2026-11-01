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
//!   and `API-KEY` alike); a path to a secret file (`token_file`, `[tls] key` holding a path)
//!   is shown: it is not the secret.
//! - **In any text, a password before `@`** is `****`, the user, host, port and path kept
//!   (`postgres://schedr:****@db:5432/schedr`): with a scheme or `//` or none (`u:p@db`),
//!   several in one text, a password holding `@`, `:`, `/`, `'` or parentheses.
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
    mask_under(value, "");
}

/// Masks the secrets in `value`, a task's own data (its output), in place: every text as in a
/// configuration, and a field whose name has a word naming a secret (`password`, `api_key`,
/// `clientSecret`, `access_token`), by whole words, so `partition_key` or `sort_key` is
/// shown: a task names its fields as it likes (decision 131).
pub fn mask_data(value: &mut Value) {
    match value {
        Value::String(text) => *text = mask_text(text),
        Value::Array(items) => items.iter_mut().for_each(mask_data),
        Value::Object(fields) => {
            for (name, field) in fields.iter_mut() {
                let empty = field.is_null() || field.as_str().is_some_and(str::is_empty);
                if !empty && names_secret_in_words(name) {
                    *field = Value::String(MASK.to_owned());
                } else {
                    mask_data(field);
                }
            }
            mask_keys(fields);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Masks each of `fields`' names as a text (a task may key its output by an address with
/// its password). Two names masked alike stay apart: the second is `<name> (2)`, and so on.
fn mask_keys(fields: &mut serde_json::Map<String, Value>) {
    if !fields.keys().any(|name| mask_text(name) != *name) {
        return;
    }
    let mut masked = serde_json::Map::new();
    for (name, field) in std::mem::take(fields) {
        let shown = mask_text(&name);
        let mut unique = shown.clone();
        let mut n = 1;
        while masked.contains_key(&unique) {
            n += 1;
            unique = format!("{shown} ({n})");
        }
        masked.insert(unique, field);
    }
    *fields = masked;
}

/// Whether a word of `name` names a secret: its words split at anything but a letter or a
/// digit, before an upper-case letter following a lower-case one or a digit (`apiKey`: `api`,
/// `key`), and before the last of a run of upper-case letters followed by a lower-case one
/// (`HTTPPassword`: `http`, `password`; `AWSAccessKeyId`: `aws`, `access`, `key`, `id`).
fn names_secret_in_words(name: &str) -> bool {
    let chars: Vec<char> = name.chars().collect();
    let mut words: Vec<String> = Vec::new();
    let mut word = String::new();
    for (at, &c) in chars.iter().enumerate() {
        if !c.is_alphanumeric() {
            words.push(std::mem::take(&mut word));
            continue;
        }
        let previous = at.checked_sub(1).map(|p| chars[p]);
        let next_lower = chars.get(at + 1).is_some_and(|n| n.is_lowercase());
        let starts_word = c.is_uppercase()
            && previous.is_some_and(|p| {
                p.is_lowercase() || p.is_ascii_digit() || (p.is_uppercase() && next_lower)
            });
        if starts_word {
            words.push(std::mem::take(&mut word));
        }
        word.extend(c.to_lowercase());
    }
    words.push(word);
    words.retain(|w| !w.is_empty());
    let alone = [
        "password",
        "passwords",
        "passwd",
        "pwd",
        "passphrase",
        "secret",
        "secrets",
        "token",
        "tokens",
        "credential",
        "credentials",
        "apikey",
        "privatekey",
        "accesskey",
        "secretkey",
    ];
    words.iter().any(|w| alone.contains(&w.as_str()))
        || words.windows(2).any(|pair| {
            pair[1] == "key" && matches!(pair[0].as_str(), "api" | "private" | "access" | "secret")
        })
}

/// Masks `value`, the field of the table named `table` (`""`: none).
fn mask_under(value: &mut Value, table: &str) {
    match value {
        Value::String(text) => *text = mask_text(text),
        Value::Array(items) => items.iter_mut().for_each(|item| mask_under(item, table)),
        Value::Object(fields) => {
            for (name, field) in fields.iter_mut() {
                if holds_secret(table, name, field) {
                    *field = Value::String(MASK.to_owned());
                } else {
                    mask_under(field, name);
                }
            }
            mask_keys(fields);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Whether the setting `name` of the table `table` holds a secret as `value` (not a file,
/// path or header naming one; nothing to hide in an empty or unset one).
fn holds_secret(table: &str, name: &str, value: &Value) -> bool {
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
    // `[tls] key`: a path to the key (schedr's setting), shown. Elsewhere a `key` holding a
    // `/` may be a secret (Base64).
    if name == "key" {
        return !(table == "tls" && value.as_str().is_some_and(looks_like_path));
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
/// naming a secret (see the module documentation). Its time grows with the text's length,
/// never faster.
#[must_use]
pub fn mask_text(text: &str) -> String {
    mask_text_after("", text)
}

/// `text` masked as [`mask_text`] masks it, read after `before`, the text just before it,
/// which is masked with it but not shown: a value begun in `before` (a quoted one going on
/// over lines, a password whose name is there) is hidden in `text` too. A part of a log is
/// masked so, the bytes before it as `before`.
#[must_use]
pub fn mask_text_after(before: &str, text: &str) -> String {
    let chars: Vec<char> = before.chars().chain(text.chars()).collect();
    let mut hidden = vec![false; chars.len()];
    hide_passwords(&chars, &mut hidden);
    for at in 0..chars.len() {
        // A `=` inside a value already hidden is part of it.
        if matches!(chars[at], '=' | ':') && !hidden[at] {
            hide_value_after(&chars, at, &mut hidden);
        }
    }
    let skip = before.chars().count();
    let mut out = String::with_capacity(text.len());
    let mut in_mask = false;
    for (c, hide) in chars.iter().zip(&hidden).skip(skip) {
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

/// Where a token ends: no password or value runs past it. Not `'`, `(` or `)`, which a
/// password may hold.
fn ends_token(c: char) -> bool {
    c.is_whitespace() || matches!(c, '"' | '<' | '>' | '`')
}

/// Hides every password before `@`. A text's tokens are split at each `://` into user infos'
/// segments; in a segment with an `@`, everything from its first `:` to its last `@` is
/// hidden (a password may hold `@`, `:`, `/`, `,`; a segment holding two user infos without
/// a scheme is hidden from the first one's password to the second one's host).
fn hide_passwords(chars: &[char], hidden: &mut [bool]) {
    let mut start = 0;
    let mut colon: Option<usize> = None;
    let mut last_at: Option<usize> = None;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let scheme_end =
            c == ':' && chars.get(i + 1) == Some(&'/') && chars.get(i + 2) == Some(&'/');
        let network_path = c == '/' && chars.get(i + 1) == Some(&'/') && i == start;
        if ends_token(c) || scheme_end || network_path {
            end_segment(colon, last_at, hidden);
            (colon, last_at) = (None, None);
            i += if scheme_end {
                3
            } else if network_path {
                2
            } else {
                1
            };
            start = i;
            continue;
        }
        match c {
            ':' if colon.is_none() => colon = Some(i),
            '@' => last_at = Some(i),
            _ => {}
        }
        i += 1;
    }
    end_segment(colon, last_at, hidden);
}

/// Hides a segment's password: from its first `:` to its last `@`.
fn end_segment(colon: Option<usize>, last_at: Option<usize>, hidden: &mut [bool]) {
    if let (Some(colon), Some(at)) = (colon, last_at)
        && colon < at
    {
        hidden[colon + 1..at]
            .iter_mut()
            .for_each(|flag| *flag = true);
    }
}

/// Hides the value after `=` or `:` (`at`) when the name before it names a secret: to the
/// end of its token (or `&`, `;`, `#` in a query), or its closing quote (a `\` escaping the
/// next character).
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
            Some(_) if c == '\\' => {
                to += 2;
                continue;
            }
            Some(q) => c == q,
            None => ends_token(c) || matches!(c, '&' | ';' | '#'),
        };
        if ends {
            break;
        }
        to += 1;
    }
    let to = to.min(chars.len());
    hidden[from..to].iter_mut().for_each(|flag| *flag = true);
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
