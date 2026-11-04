//! The Vault secret storage (see PROJECT.md, Secret storage): KV v2 reads of a mount, with a
//! token from the environment or Vault's Kubernetes auth (the pod's service-account token).
//! The agent's own token is renewed before it expires; with Kubernetes auth, an expired or
//! refused one is replaced by logging in again, once per read. Values that aren't strings
//! are read as their JSON text (decision 171).
//!
//! A token never reaches a log, an error or a `Debug` print.

use std::fmt::{self, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::{Duration, Instant};

use reqwest::{Client, StatusCode};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::secrets::{ReadFuture, SecretData, SecretError, SecretStorage};

#[cfg(test)]
mod tests;

/// A token is renewed when this part of its time to live has passed.
const RENEW_AT: f64 = 2.0 / 3.0;

/// The shortest wait between two renewals (a token with a very short time to live).
const MIN_RENEW_WAIT: Duration = Duration::from_secs(1);

/// How the agent logs in to Vault.
#[derive(Clone, PartialEq, Eq)]
pub enum VaultAuth {
    /// A token given to the agent (`SCHEDR_CONFIG__SECRET_STORAGE__<NAME>__TOKEN`).
    Token(String),
    /// Vault's Kubernetes auth method: the pod's service-account token for a Vault token.
    Kubernetes {
        /// The Vault role.
        role: String,
        /// The auth method's mount (`kubernetes`).
        mount: String,
        /// The service-account token's file.
        jwt_file: PathBuf,
    },
}

impl fmt::Debug for VaultAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Token(_) => f.write_str("Token(****)"),
            Self::Kubernetes {
                role,
                mount,
                jwt_file,
            } => f
                .debug_struct("Kubernetes")
                .field("role", role)
                .field("mount", mount)
                .field("jwt_file", jwt_file)
                .finish(),
        }
    }
}

/// One Vault storage's settings.
#[derive(Debug, Clone)]
pub struct VaultSettings {
    /// Vault's address, `https://vault.example.com:8200`.
    pub address: String,
    /// The KV v2 mount (`secret`).
    pub mount: String,
    /// The Vault Enterprise namespace, if any.
    pub namespace: Option<String>,
    /// The CA certificates (PEM) Vault's certificate is checked against, instead of the
    /// system's.
    pub ca_cert: Option<Vec<u8>>,
    /// How the agent logs in.
    pub auth: VaultAuth,
    /// How long one request may take.
    pub request_timeout: Duration,
    /// How long connecting may take.
    pub connect_timeout: Duration,
}

/// The agent's Vault token and when to renew it.
struct Session {
    token: String,
    /// When to renew it; `None`: it doesn't expire, or can't be renewed.
    renew_at: Option<Instant>,
    /// When it expires; `None`: never.
    expires: Option<Instant>,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("token", &"****")
            .field("renew_at", &self.renew_at)
            .field("expires", &self.expires)
            .finish()
    }
}

/// The last login's failure, for the reads that waited on it.
#[derive(Debug, Default)]
struct Logins {
    failed: Option<SecretError>,
}

/// A Vault KV v2 mount, read with the agent's own token.
///
/// The session is held only to copy or replace it, never across a call to Vault: a read
/// doesn't wait for a renewal while the token is still good. Logins go one at a time
/// (`logins`); the reads waiting on one that fails get its error, not a login each.
#[derive(Debug)]
pub struct VaultStorage {
    client: Client,
    settings: VaultSettings,
    session: StdMutex<Option<Session>>,
    logins: Mutex<Logins>,
    /// How many logins were tried (succeeded or not).
    tried: AtomicU64,
    /// Itself, for the renewal task (which ends once the storage is dropped).
    me: Weak<Self>,
    /// Whether the renewal task runs.
    renewing: AtomicBool,
}

impl VaultStorage {
    /// The storage; nothing is sent to Vault until the first read.
    ///
    /// # Errors
    ///
    /// When the CA certificate doesn't read or the HTTP client can't be made.
    pub fn new(settings: VaultSettings) -> Result<Arc<Self>, String> {
        let mut builder = Client::builder()
            .timeout(settings.request_timeout)
            .connect_timeout(settings.connect_timeout)
            // A redirect would carry the token or the login's JWT elsewhere; Vault's own
            // clients follow none either. A 3xx is an answer like any other (unavailable).
            .redirect(reqwest::redirect::Policy::none());
        if let Some(pem) = &settings.ca_cert {
            let certs = reqwest::Certificate::from_pem_bundle(pem)
                .map_err(|e| format!("the Vault CA certificate doesn't read: {e}"))?;
            builder = builder.tls_certs_only(certs);
        }
        let client = builder
            .build()
            .map_err(|e| format!("no HTTP client for Vault: {e}"))?;
        Ok(Arc::new_cyclic(|me| Self {
            client,
            settings,
            session: StdMutex::new(None),
            logins: Mutex::new(Logins::default()),
            tried: AtomicU64::new(0),
            me: me.clone(),
            renewing: AtomicBool::new(false),
        }))
    }

    fn url(&self, path: &str) -> String {
        format!("{}/v1/{path}", self.settings.address.trim_end_matches('/'))
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let request = self.client.request(method, self.url(path));
        match &self.settings.namespace {
            Some(namespace) => request.header("X-Vault-Namespace", namespace),
            None => request,
        }
    }

    fn session(&self) -> std::sync::MutexGuard<'_, Option<Session>> {
        // A panic while holding it left a whole value: nothing to repair.
        self.session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The current token, while it lasts.
    fn current(&self) -> Option<String> {
        self.session()
            .as_ref()
            .filter(|s| s.expires.is_none_or(|at| Instant::now() < at))
            .map(|s| s.token.clone())
    }

    /// The token to read with: the current one, or a new one.
    async fn token(&self) -> Result<String, SecretError> {
        if let Some(token) = self.current() {
            return Ok(token);
        }
        let tried = self.tried.load(Ordering::SeqCst);
        let mut logins = self.logins.lock().await;
        // Another read logged in meanwhile, or failed to: its outcome is this one's.
        if let Some(token) = self.current() {
            return Ok(token);
        }
        if self.tried.load(Ordering::SeqCst) != tried
            && let Some(error) = &logins.failed
        {
            return Err(error.clone());
        }
        let token = self.log_in_now(&mut logins).await?;
        drop(logins);
        self.start_renewing();
        Ok(token)
    }

    /// Logs in (the caller holds `logins`) and keeps the new session.
    async fn log_in_now(&self, logins: &mut Logins) -> Result<String, SecretError> {
        let outcome = self.log_in().await;
        self.tried.fetch_add(1, Ordering::SeqCst);
        match outcome {
            Ok(fresh) => {
                logins.failed = None;
                let token = fresh.token.clone();
                *self.session() = Some(fresh);
                Ok(token)
            }
            Err(error) => {
                logins.failed = Some(error.clone());
                Err(error)
            }
        }
    }

    /// A new session: the given token looked up, or a Kubernetes login.
    async fn log_in(&self) -> Result<Session, SecretError> {
        match &self.settings.auth {
            VaultAuth::Token(token) => {
                let response = self
                    .request(reqwest::Method::GET, "auth/token/lookup-self")
                    .header("X-Vault-Token", token)
                    .send()
                    .await
                    .map_err(unreachable)?;
                let body = answer(response, "the token's lookup").await?;
                let ttl = body["data"]["ttl"].as_u64().unwrap_or(0);
                let renewable = body["data"]["renewable"].as_bool().unwrap_or(false);
                Ok(self::session(token.clone(), ttl, renewable))
            }
            VaultAuth::Kubernetes {
                role,
                mount,
                jwt_file,
            } => {
                let jwt = tokio::fs::read_to_string(jwt_file).await.map_err(|e| {
                    SecretError::Denied(format!(
                        "Vault's Kubernetes login: can't read the service-account token {}: {e}",
                        jwt_file.display()
                    ))
                })?;
                let response = self
                    .request(reqwest::Method::POST, &format!("auth/{mount}/login"))
                    .json(&serde_json::json!({"role": role, "jwt": jwt.trim()}))
                    .send()
                    .await
                    .map_err(unreachable)?;
                let body = answer(response, "the Kubernetes login").await?;
                let auth = &body["auth"];
                let token = auth["client_token"].as_str().ok_or_else(|| {
                    SecretError::Unavailable(
                        "Vault's Kubernetes login answered without a token".to_owned(),
                    )
                })?;
                let ttl = auth["lease_duration"].as_u64().unwrap_or(0);
                let renewable = auth["renewable"].as_bool().unwrap_or(false);
                Ok(self::session(token.to_owned(), ttl, renewable))
            }
        }
    }

    /// Starts the task renewing the token, once.
    fn start_renewing(&self) {
        if self.renewing.swap(true, Ordering::SeqCst) {
            return;
        }
        let me = self.me.clone();
        tokio::spawn(async move {
            loop {
                let Some(storage) = me.upgrade() else { return };
                let renew_at = storage.session().as_ref().and_then(|s| s.renew_at);
                drop(storage);
                let Some(renew_at) = renew_at else {
                    // Nothing to renew until a new session: checked again later.
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    continue;
                };
                tokio::time::sleep_until(renew_at.into()).await;
                let Some(storage) = me.upgrade() else { return };
                storage.renew().await;
            }
        });
    }

    /// Renews the token; on failure, logs in again (Kubernetes auth) or keeps it until it
    /// expires (a given token).
    async fn renew(&self) {
        let Some(token) = self.session().as_ref().map(|s| s.token.clone()) else {
            return;
        };
        let renewed = self
            .request(reqwest::Method::POST, "auth/token/renew-self")
            .header("X-Vault-Token", &token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(unreachable);
        let renewed = match renewed {
            Ok(response) => answer(response, "the token's renewal").await,
            Err(error) => Err(error),
        };
        match renewed {
            Ok(body) => {
                let auth = &body["auth"];
                let ttl = auth["lease_duration"].as_u64().unwrap_or(0);
                let renewable = auth["renewable"].as_bool().unwrap_or(false);
                let mut session = self.session();
                // Replaced meanwhile (a login after a refused read): that one stays.
                if session.as_ref().is_some_and(|s| s.token == token) {
                    *session = Some(self::session(token, ttl, renewable));
                }
            }
            Err(error) => {
                if let VaultAuth::Kubernetes { .. } = self.settings.auth {
                    let mut logins = self.logins.lock().await;
                    match self.log_in_now(&mut logins).await {
                        Ok(_) => return,
                        Err(again) => {
                            tracing::warn!(error = %again, "Vault: no new token");
                        }
                    }
                } else {
                    tracing::warn!(%error, "Vault: the token wasn't renewed");
                }
                // Tried again shortly, while the token lasts.
                if let Some(current) = self.session().as_mut() {
                    current.renew_at = Some(Instant::now() + Duration::from_secs(10));
                    if current.expires.is_some_and(|at| at <= Instant::now()) {
                        current.renew_at = None;
                    }
                }
            }
        }
    }

    /// Forgets the session (a refused token), so the next read logs in again.
    fn forget(&self, token: &str) {
        let mut session = self.session();
        if session.as_ref().is_some_and(|s| s.token == token) {
            *session = None;
        }
    }

    async fn read_once(&self, path: &str, token: &str) -> Result<SecretData, SecretError> {
        let mount = &self.settings.mount;
        let response = self
            .request(
                reqwest::Method::GET,
                &format!("{mount}/data/{}", encode_path(path)),
            )
            .header("X-Vault-Token", token)
            .send()
            .await
            .map_err(unreachable)?;
        let body = answer(response, READ).await?;
        let data = match &body["data"]["data"] {
            Value::Object(data) => data,
            // A deleted or destroyed latest version (Vault answers those with a 404 too).
            Value::Null if body["data"]["metadata"].is_object() => {
                return Err(SecretError::NotFound(
                    "no such secret in Vault (its latest version is deleted)".to_owned(),
                ));
            }
            // Not KV v2's shape (a KV v1 mount, a proxy's JSON): not "no such secret".
            _ => {
                return Err(SecretError::Unavailable(format!(
                    "Vault's answer to the read isn't a KV v2 secret (is `{mount}` a KV v2 mount?)"
                )));
            }
        };
        Ok(data
            .iter()
            .map(|(key, value)| (key.clone(), text(value)))
            .collect())
    }
}

impl SecretStorage for VaultStorage {
    fn read<'a>(&'a self, path: &'a str) -> ReadFuture<'a> {
        Box::pin(async move {
            if path.split('/').any(|part| part == "." || part == "..") {
                return Err(SecretError::NotFound(
                    "a Vault path has no `.` or `..` part".to_owned(),
                ));
            }
            let token = self.token().await?;
            match self.read_once(path, &token).await {
                // A token revoked or expired early: with Kubernetes auth, a login again.
                Err(SecretError::Denied(_))
                    if matches!(self.settings.auth, VaultAuth::Kubernetes { .. }) =>
                {
                    self.forget(&token);
                    let token = self.token().await?;
                    self.read_once(path, &token).await
                }
                result => result,
            }
        })
    }
}

/// A session for `token`, renewed at [`RENEW_AT`] of `ttl` seconds if `renewable`.
fn session(token: String, ttl: u64, renewable: bool) -> Session {
    let now = Instant::now();
    if ttl == 0 {
        return Session {
            token,
            renew_at: None,
            expires: None,
        };
    }
    let ttl = Duration::from_secs(ttl);
    let renew_at = renewable.then(|| now + ttl.mul_f64(RENEW_AT).max(MIN_RENEW_WAIT));
    Session {
        token,
        renew_at,
        expires: Some(now + ttl),
    }
}

/// A value as text: a string as it is, anything else as its JSON (decision 171).
fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// Each part of a secret path percent-encoded (a part is never `.` or `..`).
fn encode_path(path: &str) -> String {
    path.split('/')
        .map(|part| {
            let mut out = String::with_capacity(part.len());
            for byte in part.bytes() {
                if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
                    out.push(char::from(byte));
                } else {
                    let _ = write!(out, "%{byte:02X}");
                }
            }
            out
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn unreachable(error: reqwest::Error) -> SecretError {
    // Without the URL: it holds the path, which is fine, but the error says enough.
    SecretError::Unavailable(format!("Vault is unreachable: {}", error.without_url()))
}

/// What a secret's read is called in messages (and how `answer` knows one).
const READ: &str = "the read";

/// The JSON body of a success; otherwise the error its status means, with Vault's messages.
async fn answer(response: reqwest::Response, what: &str) -> Result<Value, SecretError> {
    let status = response.status();
    let body = response.json::<Value>().await;
    if status.is_success() {
        // Not JSON (a proxy's page, a cut answer): Vault isn't answering, which isn't "no
        // such secret".
        return body.map_err(|e| {
            SecretError::Unavailable(format!(
                "Vault answered {what} ({status}) with no JSON: {}",
                e.without_url()
            ))
        });
    }
    // Vault's own refusals are JSON (`{"errors": [...]}`); anything else came from elsewhere.
    let from_vault = body.as_ref().is_ok_and(Value::is_object);
    let body = body.unwrap_or(Value::Null);
    let errors: Vec<&str> = body["errors"]
        .as_array()
        .map(|list| list.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let detail = if errors.is_empty() {
        String::new()
    } else {
        format!(": {}", errors.join("; "))
    };
    let message = format!("Vault refused {what} ({status}){detail}");
    Err(match status {
        // Only Vault's own: a proxy's 404 page isn't "no such secret" (a fallback would
        // stand in for a secret that may well be there).
        StatusCode::NOT_FOUND if what == READ && from_vault => {
            SecretError::NotFound(format!("no such secret in Vault ({status})"))
        }
        // Not a secret's path: a login's or a lookup's (a wrong auth mount), which no retry
        // mends.
        StatusCode::NOT_FOUND if what == READ => SecretError::Unavailable(message),
        StatusCode::NOT_FOUND
        | StatusCode::FORBIDDEN
        | StatusCode::UNAUTHORIZED
        | StatusCode::BAD_REQUEST => SecretError::Denied(message),
        _ => SecretError::Unavailable(message),
    })
}
