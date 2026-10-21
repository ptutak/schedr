//! The schedr binary's library part: everything `main.rs` runs, kept here so it can be tested.

#[cfg(test)]
mod tests;

/// The schedr version, shared by the binary and the Python package.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the line `schedr` prints when run without arguments.
#[must_use]
pub fn banner() -> String {
    format!("schedr {VERSION}")
}
