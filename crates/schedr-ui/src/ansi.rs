//! Logs' ANSI colours as HTML (see PROJECT.md, Web UI: logs are converted on the server).
//!
//! SGR sequences (`ESC [ ... m`) become `<span>`s with classes the stylesheet colours for both
//! themes: `fg-0`..`fg-15` and `bg-0`..`bg-15` (the 16 standard colours; 256-colour and RGB
//! ones are dropped), `bold`, `faint`, `italic`, `underline`. Every other escape sequence is
//! removed; the text is HTML-escaped.

#[cfg(test)]
mod tests;

/// The style in force.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Style {
    fg: Option<u8>,
    bg: Option<u8>,
    /// [`BOLD`], [`DIM`], [`ITALIC`], [`UNDERLINE`].
    attributes: u8,
}

const BOLD: u8 = 1;
const DIM: u8 = 2;
const ITALIC: u8 = 4;
const UNDERLINE: u8 = 8;

impl Style {
    fn classes(self) -> String {
        let mut classes = Vec::new();
        if let Some(fg) = self.fg {
            classes.push(format!("fg-{fg}"));
        }
        if let Some(bg) = self.bg {
            classes.push(format!("bg-{bg}"));
        }
        for (attribute, name) in [
            (BOLD, "bold"),
            (DIM, "faint"),
            (ITALIC, "italic"),
            (UNDERLINE, "underline"),
        ] {
            if self.attributes & attribute != 0 {
                classes.push(name.to_owned());
            }
        }
        classes.join(" ")
    }

    /// Applies one SGR sequence's parameters.
    fn apply(&mut self, params: &str) {
        let codes: Vec<u16> = if params.is_empty() {
            vec![0]
        } else {
            params
                .split(';')
                .map(|p| p.parse().unwrap_or(u16::MAX))
                .collect()
        };
        let mut i = 0;
        while i < codes.len() {
            match codes[i] {
                0 => *self = Self::default(),
                1 => self.attributes |= BOLD,
                2 => self.attributes |= DIM,
                3 => self.attributes |= ITALIC,
                4 => self.attributes |= UNDERLINE,
                22 => self.attributes &= !(BOLD | DIM),
                23 => self.attributes &= !ITALIC,
                24 => self.attributes &= !UNDERLINE,
                code @ 30..=37 => self.fg = Some(colour(code - 30)),
                39 => self.fg = None,
                code @ 40..=47 => self.bg = Some(colour(code - 40)),
                49 => self.bg = None,
                code @ 90..=97 => self.fg = Some(colour(code - 90 + 8)),
                code @ 100..=107 => self.bg = Some(colour(code - 100 + 8)),
                // 256-colour (`5;n`) and RGB (`2;r;g;b`) colours: skipped with their arguments.
                38 | 48 => {
                    i += match codes.get(i + 1) {
                        Some(5) => 2,
                        Some(2) => 4,
                        _ => 0,
                    };
                }
                _ => {}
            }
            i += 1;
        }
    }
}

fn colour(index: u16) -> u8 {
    u8::try_from(index).unwrap_or(0)
}

/// `text` with its ANSI colours as HTML spans, everything HTML-escaped.
#[must_use]
pub fn to_html(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + text.len() / 8);
    let mut style = Style::default();
    let mut open = false;
    let mut chars = text.char_indices().peekable();
    while let Some((at, c)) = chars.next() {
        if c != '\u{1b}' {
            match c {
                '<' => out.push_str("&lt;"),
                '>' => out.push_str("&gt;"),
                '&' => out.push_str("&amp;"),
                '"' => out.push_str("&quot;"),
                '\'' => out.push_str("&#39;"),
                _ => out.push(c),
            }
            continue;
        }
        match chars.peek().map(|&(_, next)| next) {
            // CSI: parameters and intermediates, then a final byte in @..~.
            Some('[') => {
                chars.next();
                let start = at + 2;
                let mut end = start;
                let mut last = None;
                for (i, next) in chars.by_ref() {
                    if ('@'..='~').contains(&next) {
                        end = i;
                        last = Some(next);
                        break;
                    }
                }
                if last == Some('m') {
                    let before = style;
                    style.apply(&text[start..end]);
                    if style != before {
                        if open {
                            out.push_str("</span>");
                            open = false;
                        }
                        if style != Style::default() {
                            out.push_str("<span class=\"");
                            out.push_str(&style.classes());
                            out.push_str("\">");
                            open = true;
                        }
                    }
                }
            }
            // OSC: up to BEL or ESC \.
            Some(']') => {
                chars.next();
                while let Some((_, next)) = chars.next() {
                    if next == '\u{7}' {
                        break;
                    }
                    if next == '\u{1b}' && chars.peek().map(|&(_, n)| n) == Some('\\') {
                        chars.next();
                        break;
                    }
                }
            }
            // Any other escape: ESC, its intermediate bytes (` `../, e.g. `ESC ( B`), and its
            // final byte.
            Some(_) => {
                for (_, next) in chars.by_ref() {
                    if !(' '..='/').contains(&next) {
                        break;
                    }
                }
            }
            None => {}
        }
    }
    if open {
        out.push_str("</span>");
    }
    out
}
