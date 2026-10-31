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
    /// The copy of a root's code can't be written (decision 102).
    #[error("copying DAG code to {}: {source}", path.display())]
    Copy {
        /// What was being written.
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
        hash_file(&mut hasher, path, &full)
            .map_err(|source| ScanError::Read { path: full, source })?;
    }
    Ok(hex_hash(hasher))
}

/// A root's code, copied by [`capture`]: its content hash and the read-only copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Captured {
    /// The content hash of the copied bytes.
    pub hash: String,
    /// The copy: `<store>/<hash>`, the hash's `:` a `-`.
    pub dir: PathBuf,
    /// Whether this capture made the copy (else it was in `store` already, perhaps in use).
    pub fresh: bool,
}

/// Copies a snapshot's files into `store` and hashes the bytes copied (decision 102): parse
/// jobs and tasks import from the copy, so the code that runs is the code hashed, whatever
/// happens to the root afterwards (git-sync deleting a checkout, an edit in place). A
/// followed link is copied as its target's contents; the copy is read-only (executable bits
/// kept), so no task changes another's code by accident. The hash is the same as
/// [`content_hash`]'s.
///
/// The root is hashed first: a copy already in `store` for that hash is used as it is, and
/// nothing is written. A copy is complete whenever it has its hash's name: it is written
/// under a temporary name and renamed into place, and [`remove_capture`] renames it away
/// before deleting it.
///
/// # Errors
///
/// [`ScanError::Read`] if a file can't be read, [`ScanError::Copy`] if the copy can't be
/// written.
pub fn capture(snapshot: &Snapshot, store: &Path) -> Result<Captured, ScanError> {
    use std::os::unix::fs::PermissionsExt;
    let copy_error = |path: &Path| {
        let path = path.to_path_buf();
        move |source| ScanError::Copy { path, source }
    };
    let hash = content_hash(snapshot)?;
    let dir = store.join(hash.replace(':', "-"));
    if dir.is_dir() {
        // Already copied (the same code again, e.g. a git-sync commit outside this root).
        return Ok(Captured {
            hash,
            dir,
            fresh: false,
        });
    }
    std::fs::create_dir_all(store).map_err(copy_error(store))?;
    let partial = Partial::new(store).map_err(copy_error(store))?;
    let mut hasher = Sha256::new();
    for path in snapshot.files.keys() {
        let full = snapshot.root.join(path);
        let read_error = |source| ScanError::Read {
            path: full.clone(),
            source,
        };
        let mut source = std::fs::File::open(&full).map_err(read_error)?;
        let mode = source.metadata().map_err(read_error)?.permissions().mode();
        let target = partial.path().join(path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(copy_error(parent))?;
        }
        // Streamed, never whole in memory (B63); a read error is the root's, a write
        // error the copy's.
        let mut copy = std::fs::File::create(&target).map_err(copy_error(&target))?;
        let mut buffer = vec![0; COPY_BUFFER];
        loop {
            let read = io::Read::read(&mut source, &mut buffer).map_err(read_error)?;
            if read == 0 {
                break;
            }
            io::Write::write_all(&mut copy, &buffer[..read]).map_err(copy_error(&target))?;
        }
        drop(copy);
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(mode & 0o555))
            .map_err(copy_error(&target))?;
        // The copy's bytes: nothing changes them now.
        hash_file(&mut hasher, path, &target).map_err(copy_error(&target))?;
    }
    // What was copied: the files may have changed since they were hashed above.
    let hash = hex_hash(hasher);
    let dir = store.join(hash.replace(':', "-"));
    if dir.is_dir() {
        return Ok(Captured {
            hash,
            dir,
            fresh: false,
        });
    }
    set_dirs_read_only(partial.path()).map_err(copy_error(partial.path()))?;
    // macOS renames a directory only with write permission on it (its `..` changes; Linux
    // doesn't ask): the top one is made read-only once in place.
    std::fs::set_permissions(partial.path(), std::fs::Permissions::from_mode(0o755))
        .map_err(copy_error(partial.path()))?;
    std::fs::rename(partial.path(), &dir).map_err(copy_error(&dir))?;
    partial.kept();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555))
        .map_err(copy_error(&dir))?;
    Ok(Captured {
        hash,
        dir,
        fresh: true,
    })
}

/// A new temporary name in `store`: `.<kind>-<pid>-<n>`. (A leftover of a killed agent is
/// removed with the other copies nothing uses.)
fn temporary(store: &Path, kind: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    store.join(format!(".{kind}-{}-{n}", std::process::id()))
}

/// A copy being written, `<store>/.capture-<pid>-<n>`: removed when dropped unless kept (a
/// failed or duplicate capture leaves nothing behind).
struct Partial(Option<PathBuf>);

impl Partial {
    fn new(store: &Path) -> io::Result<Self> {
        let path = temporary(store, "capture");
        std::fs::create_dir(&path)?;
        Ok(Self(Some(path)))
    }

    fn path(&self) -> &Path {
        self.0.as_deref().unwrap_or(Path::new(""))
    }

    /// Renamed into place: nothing left to remove.
    fn kept(mut self) {
        self.0 = None;
    }
}

impl Drop for Partial {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = remove_capture(path);
        }
    }
}

/// Makes every directory under (and including) `dir` read-only: no file added or removed.
fn set_dirs_read_only(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            set_dirs_read_only(&entry.path())?;
        }
    }
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o555))
}

/// Removes a copy made by [`capture`], read-only as it is: first renamed to a temporary name,
/// so a removal cut short never leaves a partial copy under its hash's name (which a later
/// capture of that hash would use as it is).
///
/// # Errors
///
/// The I/O error of making it writable again or of removing it.
pub fn remove_capture(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fn writable(dir: &Path) -> io::Result<()> {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755))?;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                writable(&entry.path())?;
            }
        }
        Ok(())
    }
    let temporary_already = dir
        .file_name()
        .is_some_and(|name| name.to_string_lossy().starts_with('.'));
    let doomed = match dir.parent() {
        Some(store) if !temporary_already => {
            let doomed = temporary(store, "removing");
            // Writable first: macOS renames a directory only then (see `capture`).
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755))?;
            std::fs::rename(dir, &doomed)?;
            doomed
        }
        _ => dir.to_path_buf(),
    };
    writable(&doomed)?;
    std::fs::remove_dir_all(&doomed)
}

/// The buffer a file is read through when hashed or copied: a file is never read whole
/// (B63), however big.
const COPY_BUFFER: usize = 64 << 10;

/// Adds file `full`, at `path` in its root, to a content hash: its path, a NUL, its length
/// and its bytes, read through a buffer. Its length is its size when opened; a file whose
/// size changes while it is read is an error (it was being written: the scan is retried).
fn hash_file(hasher: &mut Sha256, path: &str, full: &Path) -> io::Result<()> {
    let mut file = std::fs::File::open(full)?;
    let size = file.metadata()?.len();
    hasher.update(path.as_bytes());
    hasher.update([0]);
    hasher.update(size.to_be_bytes());
    let mut buffer = vec![0; COPY_BUFFER];
    let mut read_total = 0;
    loop {
        let read = io::Read::read(&mut file, &mut buffer)?;
        if read == 0 {
            break;
        }
        read_total += read as u64;
        if read_total > size {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    if read_total != size {
        return Err(io::Error::other(format!(
            "its size changed while it was read ({size} bytes, then {read_total})"
        )));
    }
    Ok(())
}

/// `sha256:` and the hex digest.
fn hex_hash(hasher: Sha256) -> String {
    let digest = hasher.finalize();
    let mut hash = String::from(HASH_PREFIX);
    for byte in digest {
        let _ = write!(hash, "{byte:02x}");
    }
    hash
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
