//! Certificates for tests (the `testing` feature): a CA, and certificates it signs for given
//! names, usable both as server and as client certificates (like one certificate per pod).

use std::net::IpAddr;
use std::path::Path;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};

use crate::material::TlsFiles;

#[cfg(test)]
mod tests;

/// A certificate authority for tests.
#[derive(Debug)]
pub struct TestCa {
    issuer: Issuer<'static, KeyPair>,
    pem: String,
}

/// A certificate and its key, as PEM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestCert {
    /// The certificate.
    pub cert: String,
    /// The private key.
    pub key: String,
}

impl TestCa {
    /// A new CA named `name`.
    ///
    /// # Panics
    ///
    /// If rcgen can't make the certificate (never with these parameters).
    #[must_use]
    pub fn new(name: &str) -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("no names");
        params.distinguished_name.push(DnType::CommonName, name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let key = KeyPair::generate().expect("a key");
        let pem = params.self_signed(&key).expect("a CA certificate").pem();
        Self {
            issuer: Issuer::new(params, key),
            pem,
        }
    }

    /// The CA's certificate (PEM).
    #[must_use]
    pub fn pem(&self) -> &str {
        &self.pem
    }

    /// A certificate signed by this CA naming `names` (DNS names or IP addresses), for servers
    /// and clients alike.
    ///
    /// # Panics
    ///
    /// If a name can't go into a certificate.
    #[must_use]
    pub fn issue(&self, names: &[&str]) -> TestCert {
        let names: Vec<String> = names.iter().map(|n| (*n).to_owned()).collect();
        let mut params = CertificateParams::new(names.clone()).expect("valid names");
        if let Some(first) = names.first().filter(|n| n.parse::<IpAddr>().is_err()) {
            params.distinguished_name.push(DnType::CommonName, first);
        }
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let key = KeyPair::generate().expect("a key");
        let cert = params.signed_by(&key, &self.issuer).expect("a certificate");
        TestCert {
            cert: cert.pem(),
            key: key.serialize_pem(),
        }
    }

    /// Writes this CA and a certificate for `names` into `dir` as `<prefix>-ca.crt`,
    /// `<prefix>.crt` and `<prefix>.key`.
    ///
    /// # Panics
    ///
    /// If the files can't be written.
    #[must_use]
    pub fn write(&self, dir: &Path, prefix: &str, names: &[&str]) -> TlsFiles {
        let files = TlsFiles {
            ca: dir.join(format!("{prefix}-ca.crt")),
            cert: dir.join(format!("{prefix}.crt")),
            key: dir.join(format!("{prefix}.key")),
        };
        let issued = self.issue(names);
        std::fs::write(&files.ca, &self.pem).expect("the CA file");
        std::fs::write(&files.cert, issued.cert).expect("the certificate file");
        std::fs::write(&files.key, issued.key).expect("the key file");
        files
    }
}

/// Accepts any server certificate: for probing what a server asks of a client.
#[derive(Debug)]
struct AnyServer(std::sync::Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AnyServer {
    fn verify_server_cert(
        &self,
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Talks TLS to `address` (ALPN `alpn`) trusting any server and presenting no certificate,
/// sending `request`: `Ok` with what the server answered, `Err` with why it refused (a server
/// requiring a client certificate refuses with the first read, TLS 1.3).
///
/// # Errors
///
/// The handshake's or the first read's error, as text: the server's refusal.
///
/// # Panics
///
/// If the client can't be built or the address isn't reached at all.
pub async fn without_a_certificate(
    address: std::net::SocketAddr,
    alpn: &[u8],
    request: &[u8],
) -> Result<Vec<u8>, String> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let provider = crate::material::provider();
    let mut config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("TLS 1.3")
        .dangerous()
        .with_custom_certificate_verifier(std::sync::Arc::new(AnyServer(provider)))
        .with_no_client_auth();
    config.alpn_protocols = vec![alpn.to_vec()];
    let tcp = tokio::net::TcpStream::connect(address)
        .await
        .expect("the address reached");
    let name = rustls::pki_types::ServerName::IpAddress(address.ip().into());
    let mut stream = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
        .connect(name, tcp)
        .await
        .map_err(|e| e.to_string())?;
    stream.write_all(request).await.map_err(|e| e.to_string())?;
    let mut answer = vec![0; 256];
    let read = stream.read(&mut answer).await.map_err(|e| e.to_string())?;
    answer.truncate(read);
    Ok(answer)
}
