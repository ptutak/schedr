use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixDatagram, UnixStream};

use pretty_assertions::assert_eq;

use super::*;

#[test]
fn copies_a_unix_datagram_socket_and_keeps_the_original_open() {
    let (inherited, peer) = UnixDatagram::pair().unwrap();
    let copy = inherited_datagram(inherited.as_raw_fd()).unwrap();
    assert_ne!(copy.as_raw_fd(), inherited.as_raw_fd());
    UnixDatagram::from(copy).send(b"via the copy").unwrap();
    let mut buffer = [0; 32];
    let n = peer.recv(&mut buffer).unwrap();
    assert_eq!(&buffer[..n], b"via the copy");
    // The original is still open and usable: the copy's drop didn't close it.
    inherited.send(b"original").unwrap();
    let n = peer.recv(&mut buffer).unwrap();
    assert_eq!(&buffer[..n], b"original");
}

#[test]
fn the_copy_is_close_on_exec() {
    let (inherited, _peer) = UnixDatagram::pair().unwrap();
    let copy = inherited_datagram(inherited.as_raw_fd()).unwrap();
    let flags = rustix::io::fcntl_getfd(&copy).unwrap();
    assert!(flags.contains(rustix::io::FdFlags::CLOEXEC));
}

#[test]
fn refuses_stdio_and_negative_numbers() {
    for fd in [-1, 0, 1, 2] {
        assert_eq!(inherited_datagram(fd).err(), Some(Inherited::Reserved(fd)));
    }
}

#[test]
fn refuses_a_closed_number() {
    let (closed, _peer) = UnixDatagram::pair().unwrap();
    let fd = closed.as_raw_fd();
    drop(closed);
    // Nothing else in this test binary opens descriptors between these lines on this
    // thread; another test's could reuse the number, which `NotChannel` would also cover.
    let result = inherited_datagram(fd);
    assert!(
        matches!(result, Err(Inherited::Closed(n) | Inherited::NotChannel(n)) if n == fd),
        "{result:?}"
    );
}

#[test]
fn refuses_a_stream_socket_and_a_file() {
    let (stream, _peer) = UnixStream::pair().unwrap();
    assert_eq!(
        inherited_datagram(stream.as_raw_fd()).err(),
        Some(Inherited::NotChannel(stream.as_raw_fd()))
    );
    let file = std::fs::File::open("/dev/null").unwrap();
    assert_eq!(
        inherited_datagram(file.as_raw_fd()).err(),
        Some(Inherited::NotChannel(file.as_raw_fd()))
    );
}

#[test]
fn what_is_returned_is_what_was_checked_even_if_the_number_changes() {
    use std::os::fd::OwnedFd;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (datagram, _peer) = UnixDatagram::pair().unwrap();
    let (stream, _stream_peer) = UnixStream::pair().unwrap();
    let datagram = OwnedFd::from(datagram);
    let stream = OwnedFd::from(stream);
    let mut target = rustix::io::dup(&datagram).unwrap();
    let number = target.as_raw_fd();
    let stop = Arc::new(AtomicBool::new(false));
    let stopping = Arc::clone(&stop);
    // Replaces the number back and forth (safe `dup2`), racing the check and the copy.
    let swapping = std::thread::spawn(move || {
        while !stopping.load(Ordering::Relaxed) {
            rustix::io::dup2(&datagram, &mut target).unwrap();
            rustix::io::dup2(&stream, &mut target).unwrap();
        }
        target
    });
    for _ in 0..5000 {
        if let Ok(copy) = inherited_datagram(number) {
            assert_eq!(
                rustix::net::sockopt::socket_type(&copy).unwrap(),
                rustix::net::SocketType::DGRAM
            );
        }
    }
    stop.store(true, Ordering::Relaxed);
    drop(swapping.join().unwrap());
}
