//! `cargo xtask schema [--check]`: writes or checks the generated files under `schema/`.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use xtask::{Command, parse_args, schema_files, stale_files, stale_message};

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
    let files = schema_files();
    match command {
        Command::WriteSchemas => {
            for file in &files {
                let path = root.join(&file.path);
                if let Some(dir) = path.parent() {
                    std::fs::create_dir_all(dir).expect("create the schema directory");
                }
                std::fs::write(&path, &file.contents).expect("write a generated file");
                println!("wrote {}", file.path.display());
            }
            ExitCode::SUCCESS
        }
        Command::CheckSchemas => {
            let stale = stale_files(&files, |p| std::fs::read_to_string(root.join(p)).ok());
            if stale.is_empty() {
                ExitCode::SUCCESS
            } else {
                eprintln!("{}", stale_message(&stale));
                ExitCode::FAILURE
            }
        }
    }
}

/// The repository root: two levels above this crate's manifest (`crates/xtask`).
fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}
