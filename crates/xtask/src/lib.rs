//! Development tasks for the schedr repository: generating the committed files under
//! `schema/` and checking that they are up to date (see PROJECT.md, Rust/Python integration:
//! shared model and schemas).
//!
//! The logic here is pure; `main.rs` does the reading and writing.

use std::path::{Path, PathBuf};

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
    let dag = serde_json::to_value(schedr_model::schema::dag_schema())
        .expect("a generated schema serializes");
    vec![GeneratedFile {
        path: Path::new(SCHEMA_DIR).join("dag.schema.json"),
        contents: render_json(&dag),
    }]
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

/// What `xtask` was asked to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// Write the generated files.
    WriteSchemas,
    /// Check that the committed files are up to date.
    CheckSchemas,
}

/// Parses the command-line arguments (without the program name).
///
/// # Errors
///
/// Returns a usage message for anything but `schema` or `schema --check`.
pub fn parse_args<S: AsRef<str>>(args: &[S]) -> Result<Command, String> {
    let args: Vec<&str> = args.iter().map(AsRef::as_ref).collect();
    match args.as_slice() {
        ["schema"] => Ok(Command::WriteSchemas),
        ["schema", "--check"] => Ok(Command::CheckSchemas),
        _ => Err("usage: cargo xtask schema [--check]".to_owned()),
    }
}
