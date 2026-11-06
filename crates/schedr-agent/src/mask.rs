//! Masking secret values in a task's log and output (decision 167): each value the attempt
//! fetched, written `****`.
//!
//! A value is masked as it is, as JSON escapes it (`"` and `\` in a value, a JSON log line;
//! also with every non-ASCII character as `\uXXXX`, as Python's `json.dumps` writes it), as
//! Python's `repr` quotes it (an exception's message), and line by line when it spans several
//! lines; a form shorter than `min_mask_length` isn't (masking `1` would mangle the log). In a
//! stored output a number equal to a value is masked too. Streams are masked as they are
//! copied, across the chunks they are read in: only bytes that could start a value are held
//! back, until the next chunk or the end.
//!
//! Masking guards against a value shown by accident: a task holding a value can always
//! write it in a form no mask knows (split between stdout and stderr, encoded, reversed).

use std::fmt::Write as _;
use std::sync::{Mutex, PoisonError};

use schedr_model::Encoding;
use serde_json::Value;

#[cfg(test)]
mod tests;

/// The mask a process's output goes through: none, or an attempt's values (compared by
/// identity, so a [`crate::process::ProcessSpec`] stays comparable).
#[derive(Debug, Clone, Default)]
pub struct Mask(pub Option<std::sync::Arc<MaskSet>>);

impl PartialEq for Mask {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Some(a), Some(b)) => std::sync::Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        }
    }
}

impl Eq for Mask {}

/// Labels and values, never shown by `Debug`.
#[derive(Default)]
struct Values(Vec<(String, String)>);

impl std::fmt::Debug for Values {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} values", self.0.len())
    }
}

/// What a masked value is written as.
pub const MASK: &str = "****";

/// The values masked for one attempt, and how often each was masked.
#[derive(Debug)]
pub struct MaskSet {
    min_length: usize,
    inner: Mutex<Inner>,
}

impl MaskSet {
    /// Masks in `other` too whatever is masked here, now and from now on: output shared by
    /// several attempts can't be told apart, so it masks them all.
    pub fn also_in(&self, other: std::sync::Arc<Self>) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        for (label, value) in &inner.values.0 {
            other.add(label, value);
        }
        inner.also.push(other);
    }
}

#[derive(Debug, Default)]
struct Inner {
    /// Each form and the secret it comes from (an index into `labels`), longest first.
    forms: Vec<(Vec<u8>, usize)>,
    /// For each first byte, the forms starting with it (indexes into `forms`, longest
    /// first): a byte no form starts with is passed on at once.
    by_first: Vec<Vec<usize>>,
    /// The secrets, as written in Python (never their values).
    labels: Vec<String>,
    /// How many times each secret was masked.
    counts: Vec<u64>,
    /// The values added, for [`MaskSet::also_in`].
    values: Values,
    /// The sets that mask these too.
    also: Vec<std::sync::Arc<MaskSet>>,
}

impl MaskSet {
    /// No values; forms shorter than `min_length` characters are never masked.
    #[must_use]
    pub fn new(min_length: usize) -> Self {
        Self {
            min_length,
            inner: Mutex::default(),
        }
    }

    /// Masks `value` of the secret `label` from now on. Returns `false` when it is too short
    /// to be masked (nothing is added).
    pub fn add(&self, label: &str, value: &str) -> bool {
        let min = self.min_length.max(1);
        if value.chars().count() < min {
            return false;
        }
        let mut forms = vec![
            value.to_owned(),
            json_escaped(value, false),
            json_escaped(value, true),
            python_repr_escaped(value),
            // As a URL's value is written (decision 188): a driver's error may quote the URL.
            Encoding::Url.apply(value),
        ];
        // Single-quoted for a shell, `'` written `'\''` (`task-ctx connection --env`).
        let quoted: Vec<String> = forms
            .iter()
            .filter(|form| form.contains('\''))
            .map(|form| form.replace('\'', "'\\''"))
            .collect();
        forms.extend(quoted);
        if value.contains('\n') {
            forms.extend(
                value
                    .split('\n')
                    .map(|line| line.trim_end_matches('\r'))
                    .filter(|line| line.chars().count() >= min)
                    .map(str::to_owned),
            );
        }
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let index = if let Some(index) = inner.labels.iter().position(|l| l == label) {
            index
        } else {
            inner.labels.push(label.to_owned());
            inner.counts.push(0);
            inner.labels.len() - 1
        };
        for form in forms {
            let form = form.into_bytes();
            if !form.is_empty() && !inner.forms.iter().any(|(f, _)| *f == form) {
                inner.forms.push((form, index));
            }
        }
        inner.index();
        inner.values.0.push((label.to_owned(), value.to_owned()));
        let also = inner.also.clone();
        drop(inner);
        for other in also {
            other.add(label, value);
        }
        true
    }

    /// Whether nothing is masked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .forms
            .is_empty()
    }

    /// `bytes` with every value masked.
    #[must_use]
    pub fn mask(&self, bytes: &[u8]) -> Vec<u8> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.scan(bytes, false).0
    }

    /// `text` with every value masked.
    #[must_use]
    pub fn mask_text(&self, text: &str) -> String {
        let masked = self.mask(text.as_bytes());
        // Only whole values (valid UTF-8 themselves) are replaced, by ASCII: still UTF-8.
        String::from_utf8(masked)
            .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
    }

    /// A JSON value with every string, object key and number masked. A masked number becomes
    /// the text it is masked in; two keys masked alike stay apart (`**** (2)`, and so on).
    #[must_use]
    pub fn mask_json(&self, value: &Value) -> Value {
        if self.is_empty() {
            return value.clone();
        }
        match value {
            Value::String(text) => Value::String(self.mask_text(text)),
            // A value turned into a number (`int(password)`): written out, it is the value.
            Value::Number(number) => {
                let text = number.to_string();
                let masked = self.mask_text(&text);
                if masked == text {
                    value.clone()
                } else {
                    Value::String(masked)
                }
            }
            Value::Array(items) => Value::Array(items.iter().map(|v| self.mask_json(v)).collect()),
            Value::Object(entries) => {
                let mut masked = serde_json::Map::new();
                for (key, field) in entries {
                    let shown = self.mask_text(key);
                    let mut unique = shown.clone();
                    let mut n = 1;
                    // Nor taking the name of a key that wasn't masked.
                    while masked.contains_key(&unique)
                        || (unique != *key && entries.contains_key(&unique))
                    {
                        n += 1;
                        unique = format!("{shown} ({n})");
                    }
                    masked.insert(unique, self.mask_json(field));
                }
                Value::Object(masked)
            }
            Value::Null | Value::Bool(_) => value.clone(),
        }
    }

    /// Each secret masked at least once, with how many times.
    #[must_use]
    pub fn counts(&self) -> Vec<(String, u64)> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner
            .labels
            .iter()
            .zip(&inner.counts)
            .filter(|(_, count)| **count > 0)
            .map(|(label, count)| (label.clone(), *count))
            .collect()
    }

    /// A masking stream: what it is fed comes out masked, held back only where a value may
    /// continue in the next chunk.
    #[must_use]
    pub fn stream(&self) -> MaskStream<'_> {
        MaskStream {
            set: self,
            pending: Vec::new(),
        }
    }
}

impl Inner {
    /// Sorts the forms longest first and indexes them by their first byte.
    fn index(&mut self) {
        self.forms
            .sort_by_key(|(form, _)| std::cmp::Reverse(form.len()));
        let mut by_first = vec![Vec::new(); 256];
        for (index, (form, _)) in self.forms.iter().enumerate() {
            by_first[usize::from(form[0])].push(index);
        }
        self.by_first = by_first;
    }

    /// `bytes` masked from the start, and how many of them it took. With `partial`, it stops
    /// at the first place where a value may begin and continue past the end (the rest is
    /// held for the next chunk); a value that may grow into a longer one waits too.
    fn scan(&mut self, bytes: &[u8], partial: bool) -> (Vec<u8>, usize) {
        if self.forms.is_empty() {
            return (bytes.to_vec(), bytes.len());
        }
        let mut out = Vec::with_capacity(bytes.len());
        let mut at = 0;
        'scan: while at < bytes.len() {
            let candidates = &self.by_first[usize::from(bytes[at])];
            if candidates.is_empty() {
                out.push(bytes[at]);
                at += 1;
                continue;
            }
            let rest = &bytes[at..];
            if partial
                && candidates.iter().any(|&i| {
                    let form = &self.forms[i].0;
                    form.len() > rest.len() && form.starts_with(rest)
                })
            {
                break;
            }
            for &i in candidates {
                let (form, secret) = &self.forms[i];
                if rest.starts_with(form) {
                    out.extend_from_slice(MASK.as_bytes());
                    self.counts[*secret] += 1;
                    at += form.len();
                    continue 'scan;
                }
            }
            out.push(bytes[at]);
            at += 1;
        }
        (out, at)
    }
}

/// A stream being masked (one per pipe: stdout and stderr each have their own).
#[derive(Debug)]
pub struct MaskStream<'a> {
    set: &'a MaskSet,
    pending: Vec<u8>,
}

impl MaskStream<'_> {
    /// The next chunk, masked; the bytes that may begin a value are kept for the next one.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        self.pending.extend_from_slice(chunk);
        let mut inner = self
            .set
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let (masked, taken) = inner.scan(&self.pending, true);
        self.pending.drain(..taken);
        masked
    }

    /// What is left, masked: the stream ended.
    pub fn finish(&mut self) -> Vec<u8> {
        let rest = std::mem::take(&mut self.pending);
        self.set.mask(&rest)
    }
}

/// `value` inside a JSON string, without its quotes; with `ascii`, every non-ASCII character
/// as `\uXXXX` (a surrogate pair beyond the BMP), as Python's `json.dumps` writes it.
fn json_escaped(value: &str, ascii: bool) -> String {
    let quoted = serde_json::to_string(value).unwrap_or_default();
    let inner = quoted
        .strip_prefix('"')
        .and_then(|q| q.strip_suffix('"'))
        .unwrap_or(&quoted);
    if !ascii {
        return inner.to_owned();
    }
    let mut out = String::with_capacity(inner.len());
    for c in inner.chars() {
        if c.is_ascii() {
            out.push(c);
        } else {
            let mut units = [0; 2];
            for unit in c.encode_utf16(&mut units) {
                let _ = write!(out, "\\u{unit:04x}");
            }
        }
    }
    out
}

/// `value` inside Python's `repr` of it, without its quotes (an exception's message is often
/// the `repr` of its argument: `KeyError('...')`).
fn python_repr_escaped(value: &str) -> String {
    let quote = if value.contains('\'') && !value.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if python_printable(c) => out.push(c),
            c => {
                let code = u32::from(c);
                let _ = match code {
                    ..=0xff => write!(out, "\\x{code:02x}"),
                    0x100..=0xffff => write!(out, "\\u{code:04x}"),
                    _ => write!(out, "\\U{code:08x}"),
                };
            }
        }
    }
    out
}

/// Python's `str.isprintable` for one character, which decides what its `repr` escapes:
/// every category but the controls, formats, surrogates, private use, unassigned, and the
/// separators other than the ASCII space.
fn python_printable(c: char) -> bool {
    use unicode_general_category::{GeneralCategory as G, get_general_category};
    c == ' '
        || !matches!(
            get_general_category(c),
            G::Control
                | G::Format
                | G::Surrogate
                | G::PrivateUse
                | G::Unassigned
                | G::LineSeparator
                | G::ParagraphSeparator
                | G::SpaceSeparator
        )
}
