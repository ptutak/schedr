//! Documentation sources generated from repository files (see PROJECT.md, Repository and
//! tooling: Documentation).
//!
//! The output is engine-neutral Markdown: `PROJECT.md` split into one page per `##` chapter,
//! `README.md` and `PLAN.md` as pages, and a page linking each crate's `rustdoc`. Only the
//! engine's committed index pages (`docs/`) pick these files up.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use crate::GeneratedFile;

#[cfg(test)]
mod tests;

/// Where the `rustdoc` output is in the built site, relative to the generated pages (they
/// are in `_generated/`; `rustdoc` is copied to `rust/` at the site root).
pub const RUST_DOCS_FROM_PAGES: &str = "../rust";

/// The directory, relative to the repository root, that receives the generated sources.
pub const DOCS_GENERATED_DIR: &str = "docs/_generated";

/// One chapter of `PROJECT.md`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chapter {
    /// The chapter's title (the `##` heading's text); for the part before the first chapter,
    /// the document's `#` title.
    pub title: String,
    /// The chapter as a page: its title as a `#` heading, its sections one level up.
    pub page: String,
}

/// A crate whose `rustdoc` the site links to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrateDoc {
    /// The package name, e.g. `schedr-model`.
    pub name: String,
    /// The library target's name, which names its `rustdoc` directory, e.g. `schedr_model`.
    pub lib_name: String,
    /// The package description.
    pub description: String,
}

/// Whether `line` opens or closes a fenced code block (```` ``` ```` or `~~~`).
fn is_fence(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("```") || trimmed.starts_with("~~~")
}

/// The heading level of `line` (`## Title` is 2), or `None` if it isn't an ATX heading.
#[must_use]
pub fn heading_level(line: &str) -> Option<usize> {
    let hashes = line.chars().take_while(|&c| c == '#').count();
    let rest = &line[hashes..];
    ((1..=6).contains(&hashes) && (rest.is_empty() || rest.starts_with(' '))).then_some(hashes)
}

/// Raises every heading outside code blocks by one level (`### A` becomes `## A`); `#`
/// headings stay as they are.
#[must_use]
pub fn promote_headings(text: &str) -> String {
    let mut in_code = false;
    let mut lines = Vec::new();
    for line in text.lines() {
        if is_fence(line) {
            in_code = !in_code;
        }
        match heading_level(line) {
            Some(level) if !in_code && level > 1 => lines.push(&line[1..]),
            _ => lines.push(line),
        }
    }
    let mut result = lines.join("\n");
    if text.ends_with('\n') {
        result.push('\n');
    }
    result
}

/// Splits a Markdown document at its `##` headings (outside code blocks).
///
/// The part before the first `##` heading is the first chapter, titled by the document's `#`
/// heading (or `Introduction` without one). Each chapter's page starts with its title as a
/// `#` heading, followed by its body with headings raised one level.
#[must_use]
pub fn split_chapters(markdown: &str) -> Vec<Chapter> {
    let mut parts: Vec<(Option<String>, Vec<&str>)> = vec![(None, Vec::new())];
    let mut in_code = false;
    for line in markdown.lines() {
        if is_fence(line) {
            in_code = !in_code;
        }
        if !in_code && heading_level(line) == Some(2) {
            parts.push((Some(line[3..].trim().to_owned()), Vec::new()));
        } else if let Some((_, body)) = parts.last_mut() {
            body.push(line);
        }
    }
    parts
        .into_iter()
        .filter_map(|(title, body)| {
            let body = body.join("\n");
            match title {
                Some(title) => {
                    let page = format!("# {title}\n{}", promote_headings(&format!("{body}\n")));
                    Some(Chapter { title, page })
                }
                None if body.trim().is_empty() => None,
                None => {
                    let title = body
                        .lines()
                        .find(|l| heading_level(l) == Some(1))
                        .map_or_else(|| "Introduction".to_owned(), |l| l[2..].trim().to_owned());
                    let page = if body.lines().any(|l| heading_level(l) == Some(1)) {
                        format!("{}\n", body.trim_end())
                    } else {
                        format!("# Introduction\n\n{}\n", body.trim())
                    };
                    Some(Chapter { title, page })
                }
            }
        })
        .collect()
}

/// A file-name slug: lowercase ASCII letters and digits, other characters as single `-`.
#[must_use]
pub fn slug(title: &str) -> String {
    let mut slug = String::new();
    for c in title.chars() {
        if c.is_ascii_alphanumeric() {
            slug.push(c.to_ascii_lowercase());
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    slug.trim_end_matches('-').to_owned()
}

/// The Rust API page: one link per crate to its `rustdoc` (see [`RUST_DOCS_FROM_PAGES`]).
#[must_use]
pub fn rust_api_page(crates: &[CrateDoc]) -> String {
    let mut page = String::from(
        "# Rust API\n\nGenerated by `rustdoc` from the `///` and `//!` comments of each crate.\n\n",
    );
    page.push_str("```{raw} html\n<ul>\n");
    for c in crates {
        let _ = writeln!(
            page,
            "<li><a href=\"{RUST_DOCS_FROM_PAGES}/{}/index.html\"><code>{}</code></a>: {}</li>",
            c.lib_name,
            c.name,
            html_escape(&c.description)
        );
    }
    page.push_str("</ul>\n```\n");
    page
}

/// Escapes text for HTML.
fn html_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Every generated documentation source, with paths relative to the repository root.
#[must_use]
pub fn docs_sources(
    project: &str,
    readme: &str,
    plan: &str,
    crates: &[CrateDoc],
) -> Vec<GeneratedFile> {
    let dir = Path::new(DOCS_GENERATED_DIR);
    let mut files = vec![
        GeneratedFile {
            path: dir.join("overview.md"),
            contents: readme.to_owned(),
        },
        GeneratedFile {
            path: dir.join("plan.md"),
            contents: plan.to_owned(),
        },
        GeneratedFile {
            path: dir.join("rust-api.md"),
            contents: rust_api_page(crates),
        },
    ];
    for (index, chapter) in split_chapters(project).into_iter().enumerate() {
        let name: PathBuf = format!("{index:02}-{}.md", slug(&chapter.title)).into();
        files.push(GeneratedFile {
            path: dir.join("design").join(name),
            contents: chapter.page,
        });
    }
    files
}

/// The crates to document, from `cargo metadata --no-deps --format-version 1` output:
/// every workspace package with a library target, except development tools (`publish =
/// false`), sorted by name.
///
/// # Errors
///
/// Returns a message if the output isn't the expected JSON.
pub fn crates_from_metadata(metadata: &str) -> Result<Vec<CrateDoc>, String> {
    let json: serde_json::Value =
        serde_json::from_str(metadata).map_err(|e| format!("cargo metadata: {e}"))?;
    let packages = json["packages"]
        .as_array()
        .ok_or("cargo metadata: no `packages`")?;
    let mut crates: Vec<CrateDoc> = packages
        .iter()
        .filter(|p| p["publish"] != serde_json::json!([]))
        .filter_map(|p| {
            let lib = p["targets"].as_array()?.iter().find(|t| {
                t["kind"]
                    .as_array()
                    .is_some_and(|k| k.iter().any(|k| k == "lib"))
            })?;
            Some(CrateDoc {
                name: p["name"].as_str()?.to_owned(),
                lib_name: lib["name"].as_str()?.replace('-', "_"),
                description: p["description"].as_str().unwrap_or_default().to_owned(),
            })
        })
        .collect();
    crates.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(crates)
}
