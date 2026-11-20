//! A log's structured lines (decision 514) as the Logs tab shows them: each its time, its level
//! and message, and its fields, in a line marked with its level for the tab's level filter;
//! every other line through the ANSI converter, as before.

use schedr_model::log_line::LogLine;

use crate::ansi;

/// `text` (whole lines, perhaps a last one cut) as HTML: structured lines as such, the rest
/// through `colours`, whose style carries over plain lines only.
pub fn to_html(colours: &mut ansi::Converter, text: &str) -> String {
    let mut out = String::with_capacity(text.len() + text.len() / 4);
    let mut plain = String::new();
    for line in text.split_inclusive('\n') {
        match LogLine::parse(line) {
            Some(parsed) => {
                out.push_str(&colours.push(&std::mem::take(&mut plain)));
                out.push_str(&structured(&parsed, line.ends_with('\n')));
            }
            None => plain.push_str(line),
        }
    }
    out.push_str(&colours.push(&plain));
    out
}

/// One structured line: `<span class="sl lv-<level>">`, its parts in spans of their own, and
/// its newline inside it when it has one, so a line the level filter hides leaves no empty
/// line behind.
fn structured(line: &LogLine, newline: bool) -> String {
    let level = line.level.name();
    let mut html = format!("<span class=\"sl lv-{level}\">");
    if let Some(time) = &line.time {
        html.push_str("<span class=\"sl-time\">");
        html.push_str(&ansi::to_html(time));
        html.push_str("</span> ");
    }
    html.push_str("<span class=\"sl-level\">");
    html.push_str(line.level.shown());
    html.push_str("</span> <span class=\"sl-message\">");
    html.push_str(&ansi::to_html(&line.message));
    html.push_str("</span>");
    let mut long = Vec::new();
    for (name, value) in &line.fields {
        let shown = match value {
            serde_json::Value::String(s) if s.contains('\n') => {
                long.push((name, s));
                continue;
            }
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        html.push_str(" <span class=\"sl-field\"><span class=\"sl-name\">");
        html.push_str(&ansi::to_html(name));
        html.push_str("</span>=");
        html.push_str(&ansi::to_html(&shown));
        html.push_str("</span>");
    }
    for (name, text) in long {
        html.push_str("\n<span class=\"sl-long\"><span class=\"sl-name\">");
        html.push_str(&ansi::to_html(name));
        html.push_str("</span>:\n");
        html.push_str(&ansi::to_html(text.trim_end()));
        html.push_str("</span>");
    }
    if newline {
        html.push('\n');
    }
    html.push_str("</span>");
    html
}

/// The longest line a download holds to render whole: past it, it goes on as it is.
pub const DOWNLOAD_LINE_MAX: usize = 64 << 20;

/// A downloaded log passed on as it comes (Codex's second review of M5.2i5): whole characters
/// only (one split between chunks held, not garbled), and with `plain`, structured lines
/// rendered once they end; a line past [`DOWNLOAD_LINE_MAX`] goes on as it is, not held.
#[derive(Debug)]
pub struct Download {
    plain: bool,
    held: Vec<u8>,
    /// How much of `held` has no newline in it.
    scanned: usize,
    /// Within a line passed on unrendered: the rest of it goes on as it comes.
    passing: bool,
}

impl Download {
    /// A download, its structured lines rendered with `plain`.
    #[must_use]
    pub const fn new(plain: bool) -> Self {
        Self {
            plain,
            held: Vec::new(),
            scanned: 0,
            passing: false,
        }
    }

    /// What `bytes` let pass on; `last` (none come after) passes on all that is held.
    #[must_use]
    pub fn take(&mut self, bytes: &[u8], last: bool) -> String {
        self.held.extend_from_slice(bytes);
        if last {
            let held = std::mem::take(&mut self.held);
            self.scanned = 0;
            let text = String::from_utf8_lossy(&held);
            if std::mem::take(&mut self.passing) {
                return match text.find('\n') {
                    Some(at) => {
                        let (rest_of_line, after) = text.split_at(at + 1);
                        format!("{rest_of_line}{}", self.shown(after))
                    }
                    None => text.into_owned(),
                };
            }
            return self.shown(&text);
        }
        // The long line's end: passed on as it is, the lines after it rendered again, a part
        // line among them held as any is (agy's review of M5.2i5: one split after it came out
        // raw).
        if self.passing
            && let Some(at) = self.held.iter().position(|b| *b == b'\n')
        {
            let rest_of_line: Vec<u8> = self.held.drain(..=at).collect();
            self.passing = false;
            self.scanned = 0;
            let mut out = String::from_utf8_lossy(&rest_of_line).into_owned();
            out.push_str(&self.take(&[], false));
            return out;
        }
        let end = if self.plain && !self.passing {
            let newline = self.held[self.scanned..]
                .iter()
                .rposition(|b| *b == b'\n')
                .map(|at| self.scanned + at + 1);
            match newline {
                Some(end) => end,
                None if self.held.len() > DOWNLOAD_LINE_MAX => {
                    self.passing = true;
                    whole_chars(&self.held)
                }
                None => {
                    self.scanned = self.held.len();
                    return String::new();
                }
            }
        } else {
            whole_chars(&self.held)
        };
        let out: Vec<u8> = self.held.drain(..end).collect();
        self.scanned = 0;
        let text = String::from_utf8_lossy(&out);
        if self.passing {
            return text.into_owned();
        }
        self.shown(&text)
    }

    fn shown(&self, text: &str) -> String {
        if self.plain {
            schedr_model::log_line::rendered(text)
        } else {
            text.to_owned()
        }
    }
}

/// How many of `bytes` are whole characters: a character cut at the end is left out.
fn whole_chars(bytes: &[u8]) -> usize {
    // Not UTF-8 at all inside: all of it, shown replaced, as the log view does.
    match std::str::from_utf8(bytes) {
        Err(error) if error.error_len().is_none() => error.valid_up_to(),
        _ => bytes.len(),
    }
}

#[cfg(test)]
mod tests;
