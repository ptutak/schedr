//! Development tasks for the schedr repository: generating the committed files under
//! `schema/` (JSON Schemas and the REST API's OpenAPI description) and checking that they are up to date (see PROJECT.md, Rust/Python integration:
//! shared model and schemas), and generating the documentation sources (see [`docs`] and
//! [`reference`]).
//!
//! The logic here is pure; `main.rs` does the reading and writing.

use std::path::{Path, PathBuf};

pub mod docs;
pub mod reference;

#[cfg(test)]
mod tests;

/// The directory, relative to the repository root, that holds the generated schemas.
pub const SCHEMA_DIR: &str = "schema";

/// A generated file: its path relative to the repository root and its contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedFile {
    /// The path relative to the repository root.
    pub path: PathBuf,
    /// The exact contents to commit.
    pub contents: String,
}

/// Renders a JSON value the way generated files are committed: pretty-printed, ending with a
/// newline.
///
/// # Panics
///
/// Never in practice: a `serde_json::Value` always serializes.
#[must_use]
pub fn render_json(value: &serde_json::Value) -> String {
    let mut text = serde_json::to_string_pretty(value).expect("a JSON value serializes");
    text.push('\n');
    text
}

/// Every generated schema file, with its contents from the current model.
///
/// # Panics
///
/// Never in practice: a generated schema always serializes.
#[must_use]
pub fn schema_files() -> Vec<GeneratedFile> {
    let file = |name: &str, schema: schemars::Schema| GeneratedFile {
        path: Path::new(SCHEMA_DIR).join(name),
        contents: render_json(
            &serde_json::to_value(schema).expect("a generated schema serializes"),
        ),
    };
    vec![
        file("dag.schema.json", schedr_model::schema::dag_schema()),
        file("runner.schema.json", schedr_model::schema::runner_schema()),
        GeneratedFile {
            path: Path::new(SCHEMA_DIR).join("openapi.json"),
            contents: render_json(&schedr_api::openapi_json()),
        },
    ]
}

/// The files whose committed contents differ from `files`; `read` returns a file's committed
/// contents, or `None` if it doesn't exist.
#[must_use]
pub fn stale_files(files: &[GeneratedFile], read: impl Fn(&Path) -> Option<String>) -> Vec<&Path> {
    files
        .iter()
        .filter(|f| read(&f.path).as_deref() != Some(f.contents.as_str()))
        .map(|f| f.path.as_path())
        .collect()
}

/// The message shown when generated files are stale.
#[must_use]
pub fn stale_message(stale: &[&Path]) -> String {
    let list: Vec<String> = stale.iter().map(|p| format!("  {}", p.display())).collect();
    format!(
        "generated files are out of date:\n{}\nrun `make schema` and commit the result",
        list.join("\n")
    )
}

/// The usage message.
pub const USAGE: &str = "usage: cargo xtask schema [--check] | cargo xtask docs-sources";

/// What `xtask` was asked to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// Write the generated files.
    WriteSchemas,
    /// Check that the committed files are up to date.
    CheckSchemas,
    /// Generate the documentation sources under [`docs::DOCS_GENERATED_DIR`].
    DocsSources,
}

/// Parses the command-line arguments (without the program name).
///
/// # Errors
///
/// Returns [`USAGE`] for anything but `schema`, `schema --check` or `docs-sources`.
pub fn parse_args<S: AsRef<str>>(args: &[S]) -> Result<Command, String> {
    let args: Vec<&str> = args.iter().map(AsRef::as_ref).collect();
    match args.as_slice() {
        ["schema"] => Ok(Command::WriteSchemas),
        ["schema", "--check"] => Ok(Command::CheckSchemas),
        ["docs-sources"] => Ok(Command::DocsSources),
        _ => Err(USAGE.to_owned()),
    }
}
