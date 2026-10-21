//! Scanning a DAG root: the files it holds, and its content hash.
//!
//! A root is scanned recursively, following symbolic links (git-sync switches a link to each
//! new checkout), skipping what `.schedrignore` files exclude (gitignore syntax, in any
//! directory), `.git` and `__pycache__` directories and `*.pyc` files. Every other file counts
//! for the content hash, not only `.py` files, since tasks read SQL, configuration and other
//! files next to their DAG files; only `.py` files are parsed (see PROJECT.md, DAG code
//! delivery and Code versions and change detection).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

use ignore::WalkBuilder;
use sha2::{Digest, Sha256};

#[cfg(test)]
mod tests;

/// The name of ignore files.
pub const IGNORE_FILE: &str = ".schedrignore";

/// Directories never scanned.
pub const SKIPPED_DIRS: [&str; 2] = [".git", "__pycache__"];

/// The prefix of content hashes, naming the algorithm.
pub const HASH_PREFIX: &str = "sha256:";

/// A failure to scan or hash a root.
#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    /// The root's directory can't be resolved.
    #[error("can't read DAG root {}: {source}", root.display())]
    Root {
        /// The root's directory.
        root: PathBuf,
        /// The error.
        source: io::Error,
    },
    /// An entry under the root can't be read (often a file removed during the scan).
    #[error("scanning DAG root {}: {source}", root.display())]
    Walk {
        /// The root's directory.
        root: PathBuf,
        /// The error.
        source: ignore::Error,
    },
    /// A path under the root isn't valid UTF-8 (paths are sent to the master as text).
    #[error("{}: the path isn't valid UTF-8", path.display())]
    NotUtf8 {
        /// The path.
        path: PathBuf,
    },
    /// A file can't be read while hashing.
    #[error("reading {}: {source}", path.display())]
    Read {
        /// The file.
        path: PathBuf,
        /// The error.
        source: io::Error,
    },
}

/// What a file looked like when scanned: enough to notice that it changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stamp {
    /// The size in bytes.
    pub len: u64,
    /// The modification time, where the filesystem reports one.
    pub modified: Option<SystemTime>,
}

/// The files of a root at one moment. Two snapshots are equal when the root resolves to the
/// same directory and every file has the same path, size and modification time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    root: PathBuf,
    files: BTreeMap<String, Stamp>,
}

impl Snapshot {
    /// The root's directory, with symbolic links resolved.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The files, by path relative to the root with `/` separators, in path order.
    #[must_use]
    pub const fn files(&self) -> &BTreeMap<String, Stamp> {
        &self.files
    }

    /// The `.py` files: the ones parse mode imports, in path order.
    #[must_use]
    pub fn python_files(&self) -> Vec<String> {
        self.files
            .keys()
            .filter(|path| Path::new(path).extension().is_some_and(|ext| ext == "py"))
            .cloned()
            .collect()
    }
}

/// Scans a root.
///
/// # Errors
///
/// [`ScanError`] if the root can't be resolved, an entry can't be read, or a path isn't
/// UTF-8.
pub fn scan(root: &Path) -> Result<Snapshot, ScanError> {
    let resolved = root.canonicalize().map_err(|source| ScanError::Root {
        root: root.to_owned(),
        source,
    })?;
    let mut walk = WalkBuilder::new(&resolved);
    walk.standard_filters(false)
        .follow_links(true)
        .add_custom_ignore_filename(IGNORE_FILE)
        .filter_entry(|entry| {
            let is_dir = entry.file_type().is_some_and(|t| t.is_dir());
            let name = entry.file_name().to_string_lossy();
            let skipped_dir = is_dir && SKIPPED_DIRS.contains(&name.as_ref());
            let bytecode = Path::new(name.as_ref())
                .extension()
                .is_some_and(|ext| ext == "pyc");
            !skipped_dir && !bytecode
        });
    let mut files = BTreeMap::new();
    for entry in walk.build() {
        let entry = entry.map_err(|source| ScanError::Walk {
            root: resolved.clone(),
            source,
        })?;
        if !entry.file_type().is_some_and(|t| t.is_file()) {
            continue;
        }
        let metadata = entry.metadata().map_err(|source| ScanError::Walk {
            root: resolved.clone(),
            source,
        })?;
        let relative = relative_path(&resolved, entry.path())?;
        let stamp = Stamp {
            len: metadata.len(),
            modified: metadata.modified().ok(),
        };
        files.insert(relative, stamp);
    }
    Ok(Snapshot {
        root: resolved,
        files,
    })
}

/// The content hash of a snapshot's files: `sha256:` and the hex digest of each file's path,
/// length and bytes, in path order. It depends only on the files' paths and contents, so every
/// worker with the same code gets the same hash.
///
/// # Errors
///
/// [`ScanError::Read`] if a file can't be read (e.g. it was removed since the scan).
pub fn content_hash(snapshot: &Snapshot) -> Result<String, ScanError> {
    let mut hasher = Sha256::new();
    for path in snapshot.files.keys() {
        let full = snapshot.root.join(path);
        let bytes =
            std::fs::read(&full).map_err(|source| ScanError::Read { path: full, source })?;
        hasher.update(path.as_bytes());
        hasher.update([0]);
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(&bytes);
    }
    let digest = hasher.finalize();
    let mut hash = String::from(HASH_PREFIX);
    for byte in digest {
        let _ = write!(hash, "{byte:02x}");
    }
    Ok(hash)
}

fn relative_path(root: &Path, path: &Path) -> Result<String, ScanError> {
    let relative = path.strip_prefix(root).unwrap_or(path);
    let mut parts = Vec::new();
    for component in relative.components() {
        if let Component::Normal(part) = component {
            let part = part.to_str().ok_or_else(|| ScanError::NotUtf8 {
                path: path.to_owned(),
            })?;
            parts.push(part);
        }
    }
    Ok(parts.join("/"))
}
