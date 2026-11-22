//! Certificates made in memory (decision 520): standalone's internal links (the master's gRPC
//! port, the worker's log endpoint) under mTLS with nothing on disk. A CA made at start, living
//! as long as the process, signs a certificate of [`LIFETIME`], made again every
//! [`RENEW_AFTER`]; another process on the machine has no key to connect with.

use std::net::IpAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};

use crate::material::{Contents, TlsError};
use crate::reload::Tls;

/// How long a certificate made here is valid.
pub const LIFETIME: Duration = Duration::from_hours(24);

/// How often a new one replaces it: halfway, so a late renewal still has hours to go.
pub const RENEW_AFTER: Duration = Duration::from_hours(12);

/// How far back a certificate's validity starts, for a clock a moment behind.
const BACKDATED: Duration = Duration::from_mins(5);

/// A CA in memory and the names its certificates carry.
pub struct Ephemeral {
    issuer: Issuer<'static, KeyPair>,
    ca: String,
    names: Vec<String>,
}

impl std::fmt::Debug for Ephemeral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Not its key.
        f.debug_struct("Ephemeral")
            .field("names", &self.names)
            .finish_non_exhaustive()
    }
}

fn failed(what: &str, error: &rcgen::Error) -> TlsError {
    TlsError::Generate(format!("{what}: {error}"))
}

impl Ephemeral {
    /// A new CA, whose certificates name `names` (DNS names or IP addresses).
    ///
    /// # Errors
    ///
    /// [`TlsError::Generate`] if a key or the CA's certificate can't be made, or a name can't
    /// go into a certificate.
    pub fn new(names: &[&str]) -> Result<Self, TlsError> {
        let mut params = CertificateParams::new(Vec::<String>::new())
            .map_err(|e| failed("the CA's parameters", &e))?;
        params
            .distinguished_name
            .push(DnType::CommonName, "schedr standalone CA (in memory)");
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let key = KeyPair::generate().map_err(|e| failed("the CA's key", &e))?;
        let ca = params
            .self_signed(&key)
            .map_err(|e| failed("the CA's certificate", &e))?
            .pem();
        let names: Vec<String> = names.iter().map(|n| (*n).to_owned()).collect();
        CertificateParams::new(names.clone()).map_err(|e| failed("the names", &e))?;
        Ok(Self {
            issuer: Issuer::new(params, key),
            ca,
            names,
        })
    }

    /// A new certificate and key, valid from a moment before `now` for [`LIFETIME`], with the
    /// CA: the three files' contents, as if read.
    ///
    /// # Errors
    ///
    /// [`TlsError::Generate`] if the key or the certificate can't be made.
    pub fn issue(&self, now: SystemTime) -> Result<Contents, TlsError> {
        let mut params =
            CertificateParams::new(self.names.clone()).map_err(|e| failed("the names", &e))?;
        if let Some(first) = self.names.iter().find(|n| n.parse::<IpAddr>().is_err()) {
            params.distinguished_name.push(DnType::CommonName, first);
        }
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        // rcgen's time, in whole seconds (its type is the `time` crate's, not named here).
        let time = |at: SystemTime| {
            let seconds = at.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
            rcgen::date_time_ymd(1970, 1, 1) + Duration::from_secs(seconds)
        };
        params.not_before = time(now.checked_sub(BACKDATED).unwrap_or(now));
        params.not_after = time(now + LIFETIME);
        let key = KeyPair::generate().map_err(|e| failed("the key", &e))?;
        let cert = params
            .signed_by(&key, &self.issuer)
            .map_err(|e| failed("the certificate", &e))?;
        Ok(Contents {
            ca: self.ca.clone().into_bytes(),
            cert: cert.pem().into_bytes(),
            key: key.serialize_pem().into_bytes(),
        })
    }

    /// Material in memory for a role whose workers are named `worker_name` (the certificate
    /// must name it too), from a certificate issued now.
    ///
    /// # Errors
    ///
    /// [`TlsError`] if the certificate can't be made or used.
    pub fn tls(&self, worker_name: &str) -> Result<Tls, TlsError> {
        Tls::in_memory(self.issue(SystemTime::now())?, worker_name, worker_name)
    }

    /// Replaces `tls`'s certificate with a new one every `every` of wall time until `stop`
    /// completes (new connections use it; open ones go on). The certificate's validity is wall
    /// time, so this looks at the wall clock at least once a minute rather than sleeping
    /// `every`: a sleep doesn't count a machine's suspend, which the certificate's expiry does
    /// (agy's and Grok's reviews of M5.2i14). A failure keeps the old one, logged, and is
    /// tried again at the next look.
    pub async fn renew(self, tls: Tls, every: Duration, stop: impl Future<Output = ()>) {
        tokio::pin!(stop);
        let look = Duration::from_mins(1).min(every);
        let mut issued = SystemTime::now();
        loop {
            tokio::select! {
                () = &mut stop => return,
                () = tokio::time::sleep(look) => {}
            }
            let now = SystemTime::now();
            // Due, or the clock went back past the issue (its validity is then unknown).
            let due = now.duration_since(issued).map_or(true, |age| age >= every);
            if !due {
                continue;
            }
            match self.issue(now).and_then(|contents| tls.replace(contents)) {
                Ok(()) => {
                    tracing::info!("in-memory TLS certificate renewed");
                    issued = now;
                }
                Err(error) => {
                    tracing::error!(%error, "the in-memory TLS certificate can't be renewed");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
