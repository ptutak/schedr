//! Accepted connections, after the TLS handshake when there is TLS: one listener type for the
//! master's gRPC server (a [`Stream`] for tonic) and a worker's log endpoint (an axum
//! [`Listener`]).
//!
//! Handshakes run beside the accept loop, each with a time limit, so a slow or silent client
//! holds up no one. A handshake that fails over certificates (e.g. a client without one from
//! the CA) is logged as a warning; one that just ends (a TCP probe) only at debug level. At
//! most its `handshakes_at_once` ([`MAX_HANDSHAKES`] by default) run at once; beyond that,
//! accepting waits.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::serve::Listener;
use rustls::pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_stream::Stream;
use tonic::transport::server::Connected;

use crate::reload::Tls;

/// How long a client has to complete the TLS handshake, unless `handshake_timeout` says.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a listener waits after a failed accept, unless `accept_retry` says.
pub const ACCEPT_RETRY: Duration = Duration::from_millis(100);

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
    /// HTTP/1.1 for `schedr ui` alone (a webserver's UI port: the client's certificate must
    /// name `[tls] ui_name`).
    Ui,
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

/// What a handler knows of the connection a request came on (decision 520): its addresses, and
/// over TLS the peer's own certificate, checked by the handshake already.
#[derive(Debug, Clone)]
pub struct Peer {
    /// The peer's address.
    pub remote_addr: Option<SocketAddr>,
    /// The peer's end-entity certificate; none over plain TCP.
    pub certificate: Option<CertificateDer<'static>>,
}

impl Peer {
    /// Whether the peer's certificate names worker `worker_id`: the DNS name
    /// `<worker_id>.<worker_name>`, exactly, in its subject alternative names (decision 520). A
    /// wildcard (`*.<worker_name>`) names no one worker, and so none: matched by the usual name
    /// check, it would let one certificate be every worker. The ID must be a lowercase DNS
    /// label: DNS names ignore case and worker IDs don't, so `W1` and `w1` would share one
    /// certificate (agy's review of M5.2i14).
    ///
    /// # Errors
    ///
    /// Why not, for the refusal: no certificate (plain TCP), a worker ID that isn't a DNS
    /// label, a certificate that can't be read or doesn't name it.
    pub fn names_worker(&self, worker_id: &str, worker_name: &str) -> Result<(), String> {
        let Some(der) = &self.certificate else {
            return Err("the connection carries no certificate".to_owned());
        };
        let wanted = format!("{worker_id}.{worker_name}");
        let label_ok = !worker_id.is_empty()
            && worker_id.len() <= 63
            && worker_id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            && !worker_id.starts_with('-')
            && !worker_id.ends_with('-');
        if !label_ok {
            return Err(format!(
                "worker ID `{worker_id}` isn't a lowercase DNS label (letters a-z, digits, `-`), \
                 so no certificate can name it"
            ));
        }
        let name = ServerName::try_from(wanted.clone())
            .map_err(|_| format!("`{wanted}` isn't a DNS name"))?;
        let certificate = webpki::EndEntityCert::try_from(der)
            .map_err(|e| format!("the certificate can't be read: {e}"))?;
        let exact = certificate
            .valid_dns_names()
            .any(|presented| presented.eq_ignore_ascii_case(&wanted));
        if exact && certificate.verify_is_valid_for_subject_name(&name).is_ok() {
            Ok(())
        } else {
            Err(format!("its certificate doesn't name `{wanted}`"))
        }
    }
}

impl Connected for Conn {
    type ConnectInfo = Peer;

    fn connect_info(&self) -> Peer {
        let certificate = match self {
            Self::Plain(_) => None,
            Self::Tls(tls) => tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|chain| chain.first())
                .map(|der| der.clone().into_owned()),
        };
        Peer {
            remote_addr: self.tcp().peer_addr().ok(),
            certificate,
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
    accept_retry: Duration,
) -> io::Result<Incoming> {
    let local = listener.local_addr()?;
    let (accepted_tx, accepted) = mpsc::channel(64);
    let task = tokio::spawn(accept_loop(
        listener,
        tls,
        protocol,
        accept_retry,
        accepted_tx,
    ));
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
    accept_retry: Duration,
    accepted: mpsc::Sender<(Conn, SocketAddr)>,
) {
    let most = tls.as_ref().map_or(MAX_HANDSHAKES, Tls::handshakes_at_once);
    let handshakes = Arc::new(Semaphore::new(most));
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
                tokio::time::sleep(accept_retry).await;
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
            Protocol::Ui => tls.ui_server(),
        };
        let accepted = accepted.clone();
        let handshake_timeout = tls.handshake_timeout();
        tokio::spawn(async move {
            let _permit = permit;
            let handshake = TlsAcceptor::from(config).accept(tcp);
            match tokio::time::timeout(handshake_timeout, handshake).await {
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
