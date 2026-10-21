//! The protocol version and the master's acceptance rule.
//!
//! The master accepts workers with the **same major** and an **equal or lower minor**
//! version; upgrade order is master first, then workers (see PROJECT.md, Master–worker
//! protocol).

use crate::pb::ProtocolVersion;

#[cfg(test)]
mod tests;

/// The protocol version this build speaks.
pub const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion { major: 0, minor: 1 };

/// Why the master refuses a worker's protocol version.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VersionMismatch {
    /// The worker didn't send a version.
    #[error("the worker sent no protocol version")]
    Missing,
    /// A different major version: incompatible.
    #[error(
        "worker protocol {worker_major}.{worker_minor} has a different major version than the master's {master_major}.{master_minor}"
    )]
    Major {
        /// The worker's major version.
        worker_major: u32,
        /// The worker's minor version.
        worker_minor: u32,
        /// The master's major version.
        master_major: u32,
        /// The master's minor version.
        master_minor: u32,
    },
    /// A newer minor version than the master's: upgrade the master first.
    #[error(
        "worker protocol {worker_major}.{worker_minor} is newer than the master's {master_major}.{master_minor}; upgrade the master first"
    )]
    NewerMinor {
        /// The worker's major version.
        worker_major: u32,
        /// The worker's minor version.
        worker_minor: u32,
        /// The master's major version.
        master_major: u32,
        /// The master's minor version.
        master_minor: u32,
    },
}

/// Checks a worker's protocol version against the master's.
///
/// # Errors
///
/// Returns a [`VersionMismatch`] for a missing version, another major version, or a newer
/// minor version.
pub fn check_worker_version(
    master: ProtocolVersion,
    worker: Option<ProtocolVersion>,
) -> Result<(), VersionMismatch> {
    let worker = worker.ok_or(VersionMismatch::Missing)?;
    let fields = (worker.major, worker.minor, master.major, master.minor);
    if worker.major != master.major {
        let (worker_major, worker_minor, master_major, master_minor) = fields;
        return Err(VersionMismatch::Major {
            worker_major,
            worker_minor,
            master_major,
            master_minor,
        });
    }
    if worker.minor > master.minor {
        let (worker_major, worker_minor, master_major, master_minor) = fields;
        return Err(VersionMismatch::NewerMinor {
            worker_major,
            worker_minor,
            master_major,
            master_minor,
        });
    }
    Ok(())
}
