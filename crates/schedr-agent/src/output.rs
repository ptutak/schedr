//! The captured stdout and stderr of a process: the last part up to the output size limit.
//!
//! When a stream exceeds the limit, the output keeps **the last part** and is marked
//! truncated; the full stream is always in the task's log file (see PROJECT.md, Task
//! assignment: `Finished`).

use std::collections::VecDeque;

use serde_json::{Value, json};

#[cfg(test)]
mod tests;

/// The last `limit` bytes of a stream.
#[derive(Debug, Clone)]
pub struct TailBuffer {
    limit: usize,
    data: VecDeque<u8>,
    truncated: bool,
}

impl TailBuffer {
    /// An empty buffer keeping at most `limit` bytes.
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            data: VecDeque::new(),
            truncated: false,
        }
    }

    /// Appends bytes, dropping the oldest ones beyond the limit.
    pub fn push(&mut self, bytes: &[u8]) {
        self.data.extend(bytes);
        if self.data.len() > self.limit {
            let excess = self.data.len() - self.limit;
            self.data.drain(..excess);
            self.truncated = true;
        }
    }

    /// Whether bytes were dropped.
    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }

    /// The kept text. Invalid UTF-8 is replaced; when truncated, a character cut in two at
    /// the start is dropped.
    #[must_use]
    pub fn text(&self) -> String {
        let bytes: Vec<u8> = self.data.iter().copied().collect();
        let start = if self.truncated {
            bytes
                .iter()
                .take(3)
                .take_while(|b| (**b & 0b1100_0000) == 0b1000_0000)
                .count()
        } else {
            0
        };
        String::from_utf8_lossy(&bytes[start..]).into_owned()
    }
}

/// The exit code reported for a process killed by a signal: `128 + signal`, as shells do.
#[must_use]
pub const fn signal_exit_code(signal: i32) -> i32 {
    128 + signal
}

/// A `ProcessResult` as stored (the output of `command` and `script` tasks).
#[must_use]
pub fn process_result(stdout: &TailBuffer, stderr: &TailBuffer, exit_code: i32) -> Value {
    json!({
        "stdout": stdout.text(),
        "stderr": stderr.text(),
        "exit_code": exit_code,
        "stdout_truncated": stdout.truncated(),
        "stderr_truncated": stderr.truncated(),
    })
}
