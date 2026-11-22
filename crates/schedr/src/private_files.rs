//! Standalone's files kept private (decision 520): its gRPC and log ports refuse other
//! processes, but its database and secrets are files, so what standalone creates is made for
//! its user alone (the data directory 0700, the SQLite file 0600; SQLite gives its `-wal` and
//! `-shm` files the database's permissions) and what exists already, or a file holding a
//! secret, others on the machine can read is warned of at start. Nothing of the user's is
//! changed.

use std::fs::{DirBuilder, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

/// The permission bits of others (neither the owner nor the group).
const OTHERS: u32 = 0o007;

/// `path`'s permission bits when others may use it; none when they may not, or it doesn't
/// exist.
#[must_use]
pub fn open_to_others(path: &Path) -> Option<u32> {
    let mode = std::fs::metadata(path).ok()?.permissions().mode() & 0o777;
    (mode & OTHERS != 0).then_some(mode)
}

/// The warning for `path` (`what` it is) others may use: `mode` and the fix.
#[must_use]
pub fn warning(what: &str, path: &Path, mode: u32) -> String {
    format!(
        "{what} `{}` is open to other users on this machine (mode {mode:o}): run \
         `chmod o-rwx {}`",
        path.display(),
        path.display()
    )
}

/// Creates the data directory `dir` for its user alone when it doesn't exist (and the folders
/// above it it needs); the warning when it exists open to others.
///
/// # Errors
///
/// When it can't be created.
pub fn data_dir(dir: &Path) -> io::Result<Option<String>> {
    DirBuilder::new().recursive(true).mode(0o700).create(dir)?;
    Ok(open_to_others(dir).map(|mode| warning("the data directory", dir, mode)))
}

/// The SQLite file of `url`, unless it is in memory.
#[must_use]
pub fn sqlite_file(url: &str) -> Option<PathBuf> {
    let rest = url
        .strip_prefix("sqlite://")
        .or_else(|| url.strip_prefix("sqlite:"))?;
    let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
    if path.is_empty() || path == ":memory:" || query.split('&').any(|p| p == "mode=memory") {
        return None;
    }
    Some(PathBuf::from(path))
}

/// Creates the database `url`'s SQLite file for its user alone when it doesn't exist (an empty
/// file is an empty database); the warning when it exists open to others. Nothing for another
/// database.
///
/// # Errors
///
/// When it can't be created.
pub fn database(url: &str) -> io::Result<Option<String>> {
    let Some(file) = sqlite_file(url) else {
        return Ok(None);
    };
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&file)
    {
        Ok(_) => Ok(None),
        // A symbolic link to nothing exists too, and SQLite would create its target with the
        // umask's mode: said so (agy's review of M5.2i14).
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists && !file.exists() => Ok(Some(format!(
            "the database `{}` is a symbolic link to nothing: SQLite would create what it names \
             with the default permissions; create it first (`touch` it, `chmod 600`)",
            file.display()
        ))),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            Ok(open_to_others(&file).map(|mode| warning("the database", &file, mode)))
        }
        Err(e) => Err(e),
    }
}

/// The warnings for the files of `files` (what each holds, and where) open to others.
#[must_use]
pub fn secret_files(files: &[(&str, Option<&Path>)]) -> Vec<String> {
    files
        .iter()
        .filter_map(|(what, path)| {
            let path = (*path)?;
            open_to_others(path).map(|mode| warning(what, path, mode))
        })
        .collect()
}
