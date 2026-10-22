//! mTLS on schedr's internal links (see PROJECT.md, Master–worker protocol: `tls.mode`).
//!
//! In `app` mode every role has a certificate and a key, and trusts one CA: the master serves
//! gRPC with its certificate and asks workers for theirs, a worker serves its log endpoint the
//! same way and presents its certificate to the master, and the webserver presents its own to
//! the workers. In `mesh` mode a service mesh does all of this and schedr sends plain traffic.
//!
//! - [`material`]: the PEM files read and turned into rustls configurations.
//! - [`reload`]: [`Tls`], the configurations in use, read again when the files change.
//! - [`mod@incoming`]: a listener that does the TLS handshake (or none), for tonic and axum.
//! - [`connect`]: a gRPC channel to the master over TLS (or plain).
//!
//! With the `testing` feature, `testing` makes certificates for tests.

pub mod connect;
pub mod incoming;
pub mod material;
pub mod reload;
#[cfg(feature = "testing")]
pub mod testing;

pub use connect::{channel, error_text};
pub use incoming::{Conn, Incoming, Protocol, incoming};
pub use material::{TlsError, TlsFiles};
pub use reload::{Reload, Tls};
