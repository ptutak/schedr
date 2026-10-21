//! `cargo xtask schema [--check]` writes or checks the generated files under `schema/`;
//! `cargo xtask docs-sources` regenerates the documentation sources under `docs/_generated/`.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use xtask::docs::{DOCS_GENERATED_DIR, crates_from_metadata, docs_sources};
use xtask::{Command, GeneratedFile, parse_args, schema_files, stale_files, stale_message};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = match parse_args(&args) {
        Ok(command) => command,
        Err(usage) => {
            eprintln!("{usage}");
            return ExitCode::from(2);
        }
    };
    let root = repository_root();
    match command {
        Command::WriteSchemas => {
            write_files(&root, &schema_files());
            ExitCode::SUCCESS
        }
        Command::CheckSchemas => {
            let files = schema_files();
            let stale = stale_files(&files, |p| std::fs::read_to_string(root.join(p)).ok());
            if stale.is_empty() {
                ExitCode::SUCCESS
            } else {
                eprintln!("{}", stale_message(&stale));
                ExitCode::FAILURE
            }
        }
        Command::DocsSources => {
            let read = |name: &str| {
                std::fs::read_to_string(root.join(name))
                    .unwrap_or_else(|e| panic!("read {name}: {e}"))
            };
            let metadata = std::process::Command::new(env!("CARGO"))
                .args(["metadata", "--no-deps", "--format-version", "1"])
                .current_dir(&root)
                .output()
                .expect("run cargo metadata");
            let crates = match crates_from_metadata(&String::from_utf8_lossy(&metadata.stdout)) {
                Ok(crates) => crates,
                Err(message) => {
                    eprintln!("{message}");
                    return ExitCode::FAILURE;
                }
            };
            let generated = root.join(DOCS_GENERATED_DIR);
            if generated.exists() {
                std::fs::remove_dir_all(&generated).expect("clear the generated directory");
            }
            let files = docs_sources(
                &read("PROJECT.md"),
                &read("README.md"),
                &read("PLAN.md"),
                &crates,
            );
            write_files(&root, &files);
            ExitCode::SUCCESS
        }
    }
}

/// Writes generated files under the repository root, creating directories as needed.
fn write_files(root: &Path, files: &[GeneratedFile]) {
    for file in files {
        let path = root.join(&file.path);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).expect("create a directory for a generated file");
        }
        std::fs::write(&path, &file.contents).expect("write a generated file");
        println!("wrote {}", file.path.display());
    }
}

/// The repository root: two levels above this crate's manifest (`crates/xtask`).
fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}
