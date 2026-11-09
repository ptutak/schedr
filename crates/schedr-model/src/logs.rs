//! The worker's log endpoint as both sides see it (the worker serves it, the webserver
//! reads it; see archive/PROJECT.md, Task logs).

/// The header naming the worker that answered.
pub const WORKER_ID_HEADER: &str = "schedr-worker-id";

/// The header with the log's size in bytes when it was read.
pub const LOG_SIZE_HEADER: &str = "schedr-log-size";

/// The header saying whether the attempt still runs on the worker (`true` / `false`).
pub const RUNNING_HEADER: &str = "schedr-attempt-running";

/// The most bytes one request returns (and the default).
pub const MAX_CHUNK: u64 = 1 << 20;

/// How many bytes before a part of a log the webserver reads too, masking them with the part
/// and not showing them: a value begun there (a quoted one going on over lines) is hidden in
/// the part too (decision 131).
pub const MASK_CONTEXT: u64 = 64 * 1024;

/// The most bytes a worker's log endpoint returns at once: a part and the bytes before it.
pub const MAX_READ: u64 = MAX_CHUNK + MASK_CONTEXT;

/// The fewest bytes one request returns when asked for less: one UTF-8 character, so a part
/// always moves a reader on.
pub const MIN_CHUNK: u64 = 4;

#[cfg(test)]
mod tests;
