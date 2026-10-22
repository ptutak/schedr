//! A worker's gRPC channel to the master: plain for an `http://` URL, TLS with the worker's
//! certificate for an `https://` one (the master's certificate must name the URL's host).
//!
//! The handshake is done here rather than by tonic, so each connection uses the material in
//! use at that moment (see [`crate::reload`]), within the same time limit as the servers'.
//! Internal links never go through an HTTP proxy.

use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tonic::transport::{Channel, Endpoint, Uri};

use crate::incoming::HANDSHAKE_TIMEOUT;
use crate::reload::Tls;

#[cfg(test)]
mod tests;

/// The port of an `https://` URL without one.
const HTTPS_PORT: u16 = 443;

/// Connects to the master at `url`, over TLS when `tls` is given.
///
/// # Errors
///
/// Why the URL can't be used or the connection failed, as text.
pub async fn channel(url: &str, tls: Option<&Tls>) -> Result<Channel, String> {
    let endpoint = Endpoint::from_shared(url.to_owned()).map_err(|e| e.to_string())?;
    let Some(tls) = tls else {
        return endpoint.connect().await.map_err(|e| error_text(&e));
    };
    let tls = tls.clone();
    let connector = tower::service_fn(move |uri: Uri| {
        let tls = tls.clone();
        async move {
            // An IPv6 address comes in brackets.
            let host = uri.host().unwrap_or_default();
            let host = host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned();
            let port = uri.port_u16().unwrap_or(HTTPS_PORT);
            let name = ServerName::try_from(host.clone())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            let tcp = TcpStream::connect((host, port)).await?;
            let _ = tcp.set_nodelay(true);
            let handshake = TlsConnector::from(tls.grpc_client()).connect(name, tcp);
            let stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake)
                .await
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "the master didn't complete the TLS handshake",
                    )
                })??;
            Ok::<_, std::io::Error>(TokioIo::new(stream))
        }
    });
    endpoint
        .connect_with_connector(connector)
        .await
        .map_err(|e| error_text(&e))
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
