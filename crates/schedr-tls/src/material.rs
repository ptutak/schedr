//! The certificate files and the rustls configurations built from them.
//!
//! - **Servers** (the master's gRPC, a worker's log endpoint) present their certificate and
//!   require a client certificate signed by the CA.
//! - **The webserver's UI port** (`[webserver] ui_port`) requires, beyond that, a client
//!   certificate naming `[tls] ui_name`: only `schedr ui` may pass users' names on there
//!   (decision 36); any other certificate fails the handshake.
//! - **Clients** present their certificate. A worker checks that the master's certificate
//!   names the host of `[worker] master_url`; the webserver checks that a worker's names
//!   `[tls] worker_name`, whatever address it reached the worker at (workers are reached by
//!   pod IP or pod DNS name, which a shared certificate can't list).
//!
//! Only TLS 1.3, with the aws-lc-rs provider.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};

#[cfg(test)]
mod tests;

/// The ALPN protocol of gRPC.
pub const ALPN_H2: &[u8] = b"h2";

/// The ALPN protocol of the log endpoint.
pub const ALPN_HTTP1: &[u8] = b"http/1.1";

/// Where a role's certificate material is (`[tls] ca`, `cert`, `key`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    /// The CA certificates peers must be signed by (PEM, one or more).
    pub ca: PathBuf,
    /// This role's certificate, followed by any intermediates (PEM).
    pub cert: PathBuf,
    /// This role's private key (PEM: PKCS#8, PKCS#1 or SEC1).
    pub key: PathBuf,
}

/// The files' bytes, as read (compared to notice a change).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contents {
    /// The CA file.
    pub ca: Vec<u8>,
    /// The certificate file.
    pub cert: Vec<u8>,
    /// The key file.
    pub key: Vec<u8>,
}

/// Why the material can't be used.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A file can't be read.
    #[error("can't read [tls] {key} `{}`: {source}", path.display())]
    Read {
        /// The configuration key naming the file.
        key: &'static str,
        /// The file.
        path: PathBuf,
        /// Why.
        source: std::io::Error,
    },
    /// A file holds nothing usable.
    #[error("[tls] {key} `{}` {reason}", path.display())]
    Invalid {
        /// The configuration key naming the file.
        key: &'static str,
        /// The file.
        path: PathBuf,
        /// What is wrong with it.
        reason: String,
    },
    /// The certificate and the key don't go together, or rustls refuses them.
    #[error("[tls] cert `{}` and key `{}` can't be used together: {reason}", cert.display(), key.display())]
    Pair {
        /// The certificate file.
        cert: PathBuf,
        /// The key file.
        key: PathBuf,
        /// Why.
        reason: String,
    },
    /// `[tls] worker_name` isn't a DNS name or an IP address.
    #[error("[tls] worker_name `{0}` isn't a DNS name or an IP address")]
    WorkerName(String),
    /// `[tls] ui_name` isn't a DNS name or an IP address.
    #[error("[tls] ui_name `{0}` isn't a DNS name or an IP address")]
    UiName(String),
}

impl TlsFiles {
    /// Reads the three files.
    ///
    /// # Errors
    ///
    /// [`TlsError::Read`] naming the file that can't be read.
    pub fn read(&self) -> Result<Contents, TlsError> {
        let read = |key: &'static str, path: &Path| {
            std::fs::read(path).map_err(|source| TlsError::Read {
                key,
                path: path.to_owned(),
                source,
            })
        };
        Ok(Contents {
            ca: read("ca", &self.ca)?,
            cert: read("cert", &self.cert)?,
            key: read("key", &self.key)?,
        })
    }
}

/// The rustls configurations of a role, built from one set of files.
#[derive(Debug, Clone)]
pub struct Configs {
    /// The master's gRPC server (ALPN `h2`).
    pub grpc_server: Arc<ServerConfig>,
    /// A worker's log endpoint (ALPN `http/1.1`).
    pub http_server: Arc<ServerConfig>,
    /// A worker's connection to the master (ALPN `h2`; the master's name is the URL's host).
    pub grpc_client: Arc<ClientConfig>,
    /// The webserver's connections to workers (ALPN `http/1.1`; the name checked is
    /// `[tls] worker_name`).
    pub worker_client: Arc<ClientConfig>,
    /// The webserver's UI port (ALPN `http/1.1`; the client's certificate must name
    /// `[tls] ui_name`).
    pub ui_server: Arc<ServerConfig>,
    /// `schedr ui`'s connections to webservers' UI ports (ALPN `http/1.1`; the webserver's
    /// name is the URL's host).
    pub api_client: Arc<ClientConfig>,
}

/// The aws-lc-rs provider every configuration uses.
#[must_use]
pub fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// Builds the configurations from `contents` (read from `files`, named in errors); workers'
/// certificates must name `worker_name`.
///
/// # Errors
///
/// [`TlsError`] when a file holds no certificate or key, the key doesn't match the
/// certificate, or `worker_name` isn't a name.
pub fn build(
    contents: &Contents,
    files: &TlsFiles,
    (worker_name, ui_name): (&str, &str),
) -> Result<Configs, TlsError> {
    let worker_name = ServerName::try_from(worker_name.to_owned())
        .map_err(|_| TlsError::WorkerName(worker_name.to_owned()))?;
    let ui_name = ServerName::try_from(ui_name.to_owned())
        .map_err(|_| TlsError::UiName(ui_name.to_owned()))?;
    let roots = Arc::new(roots(&contents.ca, &files.ca)?);
    let chain = certificates(&contents.cert, "cert", &files.cert)?;
    let key = PrivateKeyDer::from_pem_slice(&contents.key).map_err(|e| TlsError::Invalid {
        key: "key",
        path: files.key.clone(),
        reason: format!("holds no usable private key ({e})"),
    })?;
    let pair = |e: rustls::Error| TlsError::Pair {
        cert: files.cert.clone(),
        key: files.key.clone(),
        reason: e.to_string(),
    };
    let provider = provider();
    let client_verifier =
        WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
            .build()
            .map_err(|e| TlsError::Invalid {
                key: "ca",
                path: files.ca.clone(),
                reason: e.to_string(),
            })?;
    let server_verifier = WebPkiServerVerifier::builder_with_provider(roots, provider.clone())
        .build()
        .map_err(|e| TlsError::Invalid {
            key: "ca",
            path: files.ca.clone(),
            reason: e.to_string(),
        })?;
    let server = |alpn: &[u8], verifier: Arc<dyn ClientCertVerifier>| {
        let mut config = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(pair)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain.clone(), key.clone_key())
            .map_err(pair)?;
        config.alpn_protocols = vec![alpn.to_vec()];
        Ok::<_, TlsError>(Arc::new(config))
    };
    let client = |alpn: &[u8], verifier: Arc<dyn ServerCertVerifier>| {
        let mut config = ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(pair)?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(chain.clone(), key.clone_key())
            .map_err(pair)?;
        config.alpn_protocols = vec![alpn.to_vec()];
        Ok::<_, TlsError>(Arc::new(config))
    };
    let named = Arc::new(FixedName {
        inner: server_verifier.clone(),
        name: worker_name,
    });
    let ui_only = Arc::new(NamedClient {
        inner: client_verifier.clone(),
        name: ui_name,
    });
    Ok(Configs {
        grpc_server: server(ALPN_H2, client_verifier.clone())?,
        http_server: server(ALPN_HTTP1, client_verifier)?,
        grpc_client: client(ALPN_H2, server_verifier.clone())?,
        worker_client: client(ALPN_HTTP1, named)?,
        ui_server: server(ALPN_HTTP1, ui_only)?,
        api_client: client(ALPN_HTTP1, server_verifier)?,
    })
}

fn certificates(
    pem: &[u8],
    key: &'static str,
    path: &Path,
) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let invalid = |reason: String| TlsError::Invalid {
        key,
        path: path.to_owned(),
        reason,
    };
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| invalid(format!("isn't PEM ({e})")))?;
    if certificates.is_empty() {
        return Err(invalid("holds no certificate".to_owned()));
    }
    Ok(certificates)
}

fn roots(pem: &[u8], path: &Path) -> Result<RootCertStore, TlsError> {
    let mut roots = RootCertStore::empty();
    for certificate in certificates(pem, "ca", path)? {
        roots.add(certificate).map_err(|e| TlsError::Invalid {
            key: "ca",
            path: path.to_owned(),
            reason: format!("holds a certificate that can't be a CA ({e})"),
        })?;
    }
    Ok(roots)
}

/// Checks a client's certificate as the CA's verifier does, and that it names `name`.
#[derive(Debug)]
struct NamedClient {
    inner: Arc<dyn ClientCertVerifier>,
    name: ServerName<'static>,
}

impl ClientCertVerifier for NamedClient {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let verified = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;
        let certificate = webpki::EndEntityCert::try_from(end_entity)
            .map_err(|e| rustls::Error::General(format!("the client's certificate: {e}")))?;
        certificate
            .verify_is_valid_for_subject_name(&self.name)
            .map_err(|_| {
                rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)
            })?;
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Checks a server's certificate against a fixed name instead of the one connected to.
#[derive(Debug)]
struct FixedName {
    inner: Arc<WebPkiServerVerifier>,
    name: ServerName<'static>,
}

impl ServerCertVerifier for FixedName {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.inner
            .verify_server_cert(end_entity, intermediates, &self.name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
