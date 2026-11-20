//! Taking over the worker's socket (`SCHEDR_AGENT_FD`): the crate's one `unsafe` block, as in
//! schedr's own `schedr-fd` (decision 147), kept here so the crate stands on its own (decision
//! note 17).

use std::os::fd::{BorrowedFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;

use rustix::io::{Errno, FdFlags};
use rustix::net::{AddressFamily, SocketType};

/// Why the inherited descriptor isn't the worker's socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SocketError {
    /// The number is one of stdin, stdout, stderr, or negative.
    #[error("descriptor {0} can't be the worker's socket")]
    Reserved(RawFd),
    /// Nothing is open at that number.
    #[error("descriptor {0} isn't open")]
    Closed(RawFd),
    /// Something else is open there.
    #[error("descriptor {0} isn't a Unix stream socket")]
    NotStream(RawFd),
    /// Duplicating it, or marking it, failed.
    #[error("descriptor {fd}: {errno}")]
    Failed {
        /// The descriptor.
        fd: RawFd,
        /// The error.
        errno: Errno,
    },
}

/// The worker's socket at `fd`, as the plugin's own close-on-exec copy; the original is made
/// close-on-exec too, so a process the plugin starts doesn't get the worker's channel.
///
/// Call it before starting threads that open or close descriptors.
///
/// # Errors
///
/// [`SocketError`] when `fd` is stdin, stdout, stderr or negative, isn't open, isn't a Unix
/// stream socket, or can't be duplicated or marked.
pub fn inherited(fd: RawFd) -> Result<UnixStream, SocketError> {
    if fd <= 2 {
        return Err(SocketError::Reserved(fd));
    }
    #[expect(
        unsafe_code,
        reason = "the only way to refer to an inherited descriptor by number (decision 147)"
    )]
    // SAFETY: the borrow lives only for the `fcntl` calls below: one duplicating it (failing with
    // `EBADF` when nothing is open there) and two setting its close-on-exec flag; no owner is
    // made and nothing closes it, so no owner can be closed twice (an inherited descriptor has
    // none in this process). Everything after works on the copy. The one precondition: no other
    // thread closes or replaces the number meanwhile, which is why it is taken before threads.
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let copy: OwnedFd = match rustix::io::fcntl_dupfd_cloexec(borrowed, 3) {
        Ok(copy) => copy,
        Err(Errno::BADF) => return Err(SocketError::Closed(fd)),
        Err(errno) => return Err(SocketError::Failed { fd, errno }),
    };
    let flags =
        rustix::io::fcntl_getfd(borrowed).map_err(|errno| SocketError::Failed { fd, errno })?;
    rustix::io::fcntl_setfd(borrowed, flags | FdFlags::CLOEXEC)
        .map_err(|errno| SocketError::Failed { fd, errno })?;
    let stream = rustix::net::sockopt::socket_type(&copy) == Ok(SocketType::STREAM);
    let unix = rustix::net::getsockname(&copy)
        .is_ok_and(|address| address.address_family() == AddressFamily::UNIX);
    if !(stream && unix) {
        return Err(SocketError::NotStream(fd));
    }
    Ok(UnixStream::from(copy))
}
