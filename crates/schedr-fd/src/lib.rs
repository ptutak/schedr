//! A task's inherited context channel, the one raw descriptor schedr adopts (decision 147).
//!
//! The agent starts a command task with one end of a datagram socket pair left open and its
//! number in `SCHEDR_CTX_FD` (decision 143). Rust can only refer to a descriptor known by
//! number through `unsafe` code, and the rest of the workspace forbids it; this crate holds the
//! single block, and keeps it small: it **borrows** the number only to duplicate it, then
//! checks the copy. The inherited descriptor is never owned (never closed here), so it can't be closed twice;
//! the caller gets its own close-on-exec copy.

use std::os::fd::{BorrowedFd, OwnedFd, RawFd};

use rustix::io::Errno;
use rustix::net::{AddressFamily, SocketType};

#[cfg(test)]
mod tests;

/// Why an inherited descriptor isn't the task's context channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Inherited {
    /// The number is one of stdin, stdout, stderr, or negative.
    #[error("descriptor {0} can't be a context channel")]
    Reserved(RawFd),
    /// Nothing is open at that number (a parent process closed it).
    #[error("descriptor {0} isn't open")]
    Closed(RawFd),
    /// Something else is open there: not a Unix datagram socket (the number was reused).
    #[error("descriptor {0} isn't a Unix datagram socket")]
    NotChannel(RawFd),
    /// Duplicating it failed (e.g. the process is out of descriptors).
    #[error("descriptor {fd} can't be duplicated: {errno}")]
    Duplicate {
        /// The descriptor.
        fd: RawFd,
        /// The error.
        errno: Errno,
    },
}

/// A close-on-exec copy of the inherited Unix datagram socket at `fd`.
///
/// Call it before starting threads that open or close descriptors: the number is borrowed
/// while it is copied, and must not be closed or replaced meanwhile. What is checked and
/// returned is the copy, so a later change of the number doesn't matter.
///
/// # Errors
///
/// [`Inherited`] when `fd` is stdin, stdout, stderr or negative, isn't open, isn't a Unix
/// datagram socket, or can't be duplicated.
pub fn inherited_datagram(fd: RawFd) -> Result<OwnedFd, Inherited> {
    if fd <= 2 {
        return Err(Inherited::Reserved(fd));
    }
    #[expect(
        unsafe_code,
        reason = "the only way to refer to an inherited descriptor by number (decision 147)"
    )]
    // SAFETY: the borrow lives only for the one `fcntl(F_DUPFD_CLOEXEC)` below, which either
    // fails (`EBADF`: nothing open there) or makes this call's own copy; everything after it
    // works on that copy, so the number being closed or replaced later can't change what is
    // checked or returned. The borrow is never closed nor wrapped in an owning type, so no
    // other owner can be closed twice (an inherited descriptor has none in this process: no
    // Rust code opened it). The one precondition left: no other thread closes or replaces the
    // number during that call. Callers adopt the channel before starting threads (the CLI does
    // it first thing, before its runtime), which is documented on this function.
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let copy = match rustix::io::fcntl_dupfd_cloexec(borrowed, 3) {
        Ok(copy) => copy,
        Err(Errno::BADF) => return Err(Inherited::Closed(fd)),
        Err(errno) => return Err(Inherited::Duplicate { fd, errno }),
    };
    let datagram = rustix::net::sockopt::socket_type(&copy) == Ok(SocketType::DGRAM);
    let unix = rustix::net::getsockname(&copy)
        .is_ok_and(|address| address.address_family() == AddressFamily::UNIX);
    if !(datagram && unix) {
        return Err(Inherited::NotChannel(fd));
    }
    Ok(copy)
}
