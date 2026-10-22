//! Accepted connections, after the TLS handshake when there is TLS: one listener type for the
//! master's gRPC server (a [`Stream`] for tonic) and a worker's log endpoint (an axum
//! [`Listener`]).
//!
//! Handshakes run beside the accept loop, each with a time limit, so a slow or silent client
//! holds up no one. A handshake that fails over certificates (e.g. a client without one from
//! the CA) is logged as a warning; one that just ends (a TCP probe) only at debug level. At
//! most [`MAX_HANDSHAKES`] run at once; beyond that, accepting waits.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::serve::Listener;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_stream::Stream;
use tonic::transport::server::{Connected, TcpConnectInfo};

use crate::reload::Tls;

/// How long a client has to complete the TLS handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How many handshakes may run at once; beyond that, accepting waits (a flood of connections
/// that never finish holds this many sockets, not all of them).
pub const MAX_HANDSHAKES: usize = 256;

/// Which configuration the handshake uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// gRPC (the master).
    Grpc,
    /// HTTP/1.1 (a worker's log endpoint).
    Http,
}

/// An accepted connection.
#[derive(Debug)]
pub enum Conn {
    /// Plain TCP (`mesh` mode, standalone).
    Plain(TcpStream),
    /// After a TLS handshake that checked the client's certificate.
    Tls(Box<TlsStream<TcpStream>>),
}

impl Conn {
    fn tcp(&self) -> &TcpStream {
        match self {
            Self::Plain(tcp) => tcp,
            Self::Tls(tls) => tls.get_ref().0,
        }
    }
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(tcp) => Pin::new(tcp).poll_read(cx, buf),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(tcp) => Pin::new(tcp).poll_write(cx, buf),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(tcp) => Pin::new(tcp).poll_flush(cx),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(tcp) => Pin::new(tcp).poll_shutdown(cx),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_shutdown(cx),
        }
    }
}

impl Connected for Conn {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> TcpConnectInfo {
        TcpConnectInfo {
            local_addr: self.tcp().local_addr().ok(),
            remote_addr: self.tcp().peer_addr().ok(),
        }
    }
}

/// Connections accepted on a listener; the accept loop stops when this is dropped.
#[derive(Debug)]
pub struct Incoming {
    accepted: mpsc::Receiver<(Conn, SocketAddr)>,
    local: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for Incoming {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Accepts connections on `listener`, with a TLS handshake for `protocol` when `tls` is given.
///
/// # Errors
///
/// An I/O error if the listener's address can't be read.
pub fn incoming(
    listener: TcpListener,
    tls: Option<Tls>,
    protocol: Protocol,
) -> io::Result<Incoming> {
    let local = listener.local_addr()?;
    let (accepted_tx, accepted) = mpsc::channel(64);
    let task = tokio::spawn(accept_loop(listener, tls, protocol, accepted_tx));
    Ok(Incoming {
        accepted,
        local,
        task,
    })
}

async fn accept_loop(
    listener: TcpListener,
    tls: Option<Tls>,
    protocol: Protocol,
    accepted: mpsc::Sender<(Conn, SocketAddr)>,
) {
    let handshakes = Arc::new(Semaphore::new(MAX_HANDSHAKES));
    loop {
        // Never fails: the semaphore isn't closed.
        let Ok(permit) = handshakes.clone().acquire_owned().await else {
            return;
        };
        let (tcp, peer) = match listener.accept().await {
            Ok(connection) => connection,
            Err(error) => {
                // E.g. too many open files: wait instead of spinning.
                tracing::warn!(%error, "can't accept a connection");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let _ = tcp.set_nodelay(true);
        let Some(tls) = &tls else {
            if accepted.send((Conn::Plain(tcp), peer)).await.is_err() {
                return;
            }
            continue;
        };
        let config = match protocol {
            Protocol::Grpc => tls.grpc_server(),
            Protocol::Http => tls.http_server(),
        };
        let accepted = accepted.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let handshake = TlsAcceptor::from(config).accept(tcp);
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake).await {
                Ok(Ok(stream)) => {
                    let _ = accepted.send((Conn::Tls(Box::new(stream)), peer)).await;
                }
                Ok(Err(error)) if error.kind() == io::ErrorKind::InvalidData => {
                    tracing::warn!(%peer, %error, "TLS handshake refused");
                }
                Ok(Err(error)) => tracing::debug!(%peer, %error, "TLS handshake ended"),
                Err(_) => tracing::debug!(%peer, "TLS handshake timed out"),
            }
        });
    }
}

impl Incoming {
    /// The address the listener is bound to.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local
    }
}

impl Stream for Incoming {
    type Item = io::Result<Conn>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .accepted
            .poll_recv(cx)
            .map(|accepted| accepted.map(|(conn, _)| Ok(conn)))
    }
}

impl Listener for Incoming {
    type Io = Conn;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Conn, SocketAddr) {
        match self.accepted.recv().await {
            Some(accepted) => accepted,
            // The accept loop never ends while this exists.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }
}
