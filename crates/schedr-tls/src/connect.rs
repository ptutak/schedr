//! A worker's gRPC channel to the master: plain for an `http://` URL, TLS with the worker's
//! certificate for an `https://` one (the master's certificate must name the URL's host).
//!
//! The handshake is done here rather than by tonic, so each connection uses the material in
//! use at that moment (see [`crate::reload`]), within the same time limit as the servers'.
//! Internal links never go through an HTTP proxy.

use std::net::SocketAddr;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, lookup_host};
use tokio::time::Instant;
use tokio_rustls::TlsConnector;
use tonic::transport::{Channel, Endpoint, Uri};

use crate::reload::Tls;

#[cfg(test)]
mod tests;

/// The port of an `http://` URL without one.
const HTTP_PORT: u16 = 80;

/// The port of an `https://` URL without one.
const HTTPS_PORT: u16 = 443;

/// How long connecting to the master may take, unless said otherwise.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// [`channel_within`] the default limit.
///
/// # Errors
///
/// As [`channel_within`].
pub async fn channel(url: &str, tls: Option<&Tls>) -> Result<Channel, String> {
    channel_within(url, tls, CONNECT_TIMEOUT).await
}

/// Connects to the master at `url`, over TLS when `tls` is given, giving the connection itself
/// `connect` (the TLS handshake has its own limit): an address that answers nothing (a pod just
/// killed) would otherwise hold the caller for the kernel's retries, about two minutes (B296).
///
/// # Errors
///
/// Why the URL can't be used or the connection failed, as text.
pub async fn channel_within(
    url: &str,
    tls: Option<&Tls>,
    connect: Duration,
) -> Result<Channel, String> {
    let endpoint = Endpoint::from_shared(url.to_owned()).map_err(|e| e.to_string())?;
    if tls.is_none() && endpoint.uri().scheme_str() != Some("http") {
        return Err(format!(
            "`{url}` needs TLS, which isn't configured (or use an http:// URL)"
        ));
    }
    let tls = tls.cloned();
    // Each connection the channel makes is made here, so a later one has the same limits.
    let connector = tower::service_fn(move |uri: Uri| {
        let tls = tls.clone();
        async move {
            // An IPv6 address comes in brackets.
            let host = uri.host().unwrap_or_default();
            let host = host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned();
            let port = uri.port_u16();
            let Some(tls) = tls else {
                let tcp = tcp(&host, port.unwrap_or(HTTP_PORT), connect).await?;
                return Ok(TokioIo::new(Box::new(tcp) as Box<dyn Connection>));
            };
            let name = ServerName::try_from(host.clone())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            let tcp = tcp(&host, port.unwrap_or(HTTPS_PORT), connect).await?;
            let handshake = TlsConnector::from(tls.grpc_client()).connect(name, tcp);
            let stream = tokio::time::timeout(tls.handshake_timeout(), handshake)
                .await
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "the master didn't complete the TLS handshake",
                    )
                })??;
            Ok::<_, std::io::Error>(TokioIo::new(Box::new(stream) as Box<dyn Connection>))
        }
    });
    endpoint
        .connect_with_connector(connector)
        .await
        .map_err(|e| error_text(&e))
}

/// A connection, plain or over TLS.
trait Connection: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Connection for T {}

/// A TCP connection to `host`, resolving it included, within `limit`: its addresses one after
/// another, each given a share of what is left, so one that answers nothing leaves the next its
/// time.
async fn tcp(host: &str, port: u16, limit: Duration) -> std::io::Result<TcpStream> {
    let until = Instant::now() + limit;
    let late = || std::io::Error::new(std::io::ErrorKind::TimedOut, unanswered(limit));
    let addresses: Vec<_> = tokio::time::timeout_at(until, lookup_host((host, port)))
        .await
        .map_err(|_| late())??
        .collect();
    first_answering(&addresses, until, late).await
}

/// A connection to the first of `addresses` to answer, each given a share of the time left.
async fn first_answering(
    addresses: &[SocketAddr],
    until: Instant,
    late: impl Fn() -> std::io::Error,
) -> std::io::Result<TcpStream> {
    let mut last = std::io::Error::new(std::io::ErrorKind::NotFound, "no address for the host");
    let mut left = u32::try_from(addresses.len()).unwrap_or(u32::MAX);
    for &address in addresses {
        let share = until.saturating_duration_since(Instant::now()) / left.max(1);
        left = left.saturating_sub(1);
        match tokio::time::timeout(share, TcpStream::connect(address)).await {
            Ok(Ok(stream)) => {
                let _ = stream.set_nodelay(true);
                return Ok(stream);
            }
            Ok(Err(error)) => last = error,
            Err(_) => last = late(),
        }
    }
    Err(last)
}

/// Why a connection nothing answered was given up.
fn unanswered(limit: Duration) -> String {
    format!("the master's address didn't answer within {limit:?}")
}

/// The error with its causes, which carry the reason (a refused certificate, say).
#[must_use]
pub fn error_text(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}
