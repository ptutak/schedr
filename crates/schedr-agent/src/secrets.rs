//! Secret storages on the worker, and the secrets of one attempt (see PROJECT.md, Secret
//! storage).
//!
//! The agent fetches every secret an attempt names or declares before its process starts
//! ([`AttemptSecrets::fetch`]); a secret under a declared prefix is fetched when the task
//! reads it. The master never sees a value. Each fetched value is masked in the task's log
//! and output ([`crate::mask`]).

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use schedr_model::{SecretDecl, SecretName};
use serde_json::Value;

use crate::mask::MaskSet;

#[cfg(test)]
mod tests;

/// The default `[secret_storage] min_mask_length`: shorter values aren't masked (masking `1`
/// or `true` would mangle the logs), like Airflow's `min_length_masked_secret`.
pub const DEFAULT_MIN_MASK_LENGTH: usize = 5;

/// The default `cache_ttl` of a storage: how long a fetched secret is kept in memory.
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60);

/// Why a secret can't be read: the attempt's error kind follows from it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SecretError {
    /// The storage is unreachable, timed out, or failed (usually temporary: retried).
    #[error("{0}")]
    Unavailable(String),
    /// Permission denied (a configuration error: not retried).
    #[error("{0}")]
    Denied(String),
    /// No such path or key (usually a typo or a missing secret: not retried).
    #[error("{0}")]
    NotFound(String),
}

impl SecretError {
    /// The attempt's error kind (`Finished.error_kind`), which decides the retry.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Unavailable(_) => "secret_unavailable",
            Self::Denied(_) => "secret_denied",
            Self::NotFound(_) => "secret_not_found",
        }
    }

    /// The same error with `name` in front of its message.
    #[must_use]
    pub fn about(self, name: &SecretName) -> Self {
        let named = |message: String| format!("`{name}`: {message}");
        match self {
            Self::Unavailable(m) => Self::Unavailable(named(m)),
            Self::Denied(m) => Self::Denied(named(m)),
            Self::NotFound(m) => Self::NotFound(named(m)),
        }
    }
}

/// A secret as a storage holds it: its keys and their values (a Vault KV v2 secret).
pub type SecretData = BTreeMap<String, String>;

/// A future a storage's read returns.
pub type ReadFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SecretData, SecretError>> + Send + 'a>>;

/// A kind of secret storage (Vault, the `db` storage, values in memory).
pub trait SecretStorage: Send + Sync + fmt::Debug {
    /// The whole secret at `path`.
    ///
    /// # Errors
    ///
    /// A [`SecretError`] when it can't be read.
    fn read<'a>(&'a self, path: &'a str) -> ReadFuture<'a>;
}

/// Secrets held in memory: `schedr dags test --secret` and `run_dag(secrets=...)` (tests and
/// local runs), never a production storage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryStorage {
    secrets: BTreeMap<String, SecretData>,
}

impl MemoryStorage {
    /// Values by `path#key`.
    #[must_use]
    pub fn new(values: impl IntoIterator<Item = (SecretName, String)>) -> Self {
        let mut secrets: BTreeMap<String, SecretData> = BTreeMap::new();
        for (name, value) in values {
            secrets
                .entry(name.path)
                .or_default()
                .insert(name.key.unwrap_or_default(), value);
        }
        Self { secrets }
    }

    /// Reads `path#key=value` (as `--secret` takes it).
    ///
    /// # Errors
    ///
    /// Why it isn't one: no `=`, no `#key`, or a bad path or key.
    pub fn parse(spec: &str) -> Result<(SecretName, String), String> {
        let (name, value) = spec
            .split_once('=')
            .ok_or_else(|| format!("`{spec}`: a secret is `path#key=value`"))?;
        let (path, key) = name
            .split_once('#')
            .ok_or_else(|| format!("`{name}`: name the key, `path#key=value`"))?;
        let name = SecretName::new(path, Some(key));
        name.check()?;
        Ok((name, value.to_owned()))
    }
}

impl SecretStorage for MemoryStorage {
    fn read<'a>(&'a self, path: &'a str) -> ReadFuture<'a> {
        Box::pin(async move {
            self.secrets
                .get(path)
                .cloned()
                .ok_or_else(|| SecretError::NotFound("no such secret".to_owned()))
        })
    }
}

/// One configured storage, with what is said about it.
#[derive(Debug, Clone)]
pub struct Storage {
    /// The storage.
    pub storage: Arc<dyn SecretStorage>,
    /// How long a fetched secret is kept in memory; zero: not kept.
    pub cache_ttl: Duration,
    /// Paths whose values aren't masked (non-secret configuration, `unmasked_prefixes`).
    pub unmasked_prefixes: Vec<String>,
}

impl Storage {
    /// A storage with the default cache and every value masked.
    #[must_use]
    pub fn new(storage: Arc<dyn SecretStorage>) -> Self {
        Self {
            storage,
            cache_ttl: DEFAULT_CACHE_TTL,
            unmasked_prefixes: Vec::new(),
        }
    }
}

/// The worker's secret storages: each by name, the default one, and a cache of what was
/// fetched (in memory only, never on disk).
#[derive(Debug, Default)]
pub struct SecretStores {
    storages: BTreeMap<String, Storage>,
    default: Option<String>,
    /// For a test run: these storages answer for every name a DAG uses (`--secret` values
    /// are named by `path#key`, whatever storage the DAG reads them from).
    any_name: bool,
    /// `[secret_storage] min_mask_length`.
    pub min_mask_length: usize,
    cache: Mutex<BTreeMap<(String, String), (Instant, SecretData)>>,
}

impl SecretStores {
    /// No storages: every secret is "not found".
    #[must_use]
    pub fn none() -> Self {
        Self {
            min_mask_length: DEFAULT_MIN_MASK_LENGTH,
            ..Self::default()
        }
    }

    /// The storages by name, `default` the one an unnamed secret comes from.
    ///
    /// # Errors
    ///
    /// When `default` isn't one of them.
    pub fn new(
        storages: BTreeMap<String, Storage>,
        default: Option<String>,
        min_mask_length: usize,
    ) -> Result<Self, String> {
        if let Some(name) = &default
            && !storages.contains_key(name)
        {
            return Err(format!(
                "[secret_storage] default = \"{name}\" isn't a configured storage"
            ));
        }
        Ok(Self {
            storages,
            default,
            any_name: false,
            min_mask_length,
            cache: Mutex::default(),
        })
    }

    /// Values in memory standing for every storage (a test run: `dags test --secret`).
    #[must_use]
    pub fn for_test(values: MemoryStorage) -> Self {
        let mut storage = Storage::new(Arc::new(values));
        storage.cache_ttl = Duration::ZERO;
        Self {
            storages: BTreeMap::from([("test".to_owned(), storage)]),
            default: Some("test".to_owned()),
            any_name: true,
            min_mask_length: DEFAULT_MIN_MASK_LENGTH,
            cache: Mutex::default(),
        }
    }

    /// The configured storages' names (reported in `Register`; the master checks DAGs
    /// against them). A test run reports none: it answers for any.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        if self.any_name {
            return Vec::new();
        }
        self.storages.keys().cloned().collect()
    }

    /// The storage `name` comes from, with its configured name.
    fn storage_of(&self, name: &SecretName) -> Result<(&str, &Storage), SecretError> {
        let wanted = match (&name.storage, self.any_name) {
            (Some(storage), false) => storage.as_str(),
            (None, false) | (_, true) => self.default.as_deref().ok_or_else(|| {
                SecretError::NotFound(
                    "this worker has no default secret storage (`[secret_storage] default`)"
                        .to_owned(),
                )
            })?,
        };
        self.storages
            .get_key_value(wanted)
            .map(|(name, storage)| (name.as_str(), storage))
            .ok_or_else(|| {
                SecretError::NotFound(format!(
                    "no secret storage `{wanted}` on this worker (`[secret_storage.{wanted}]`)"
                ))
            })
    }

    /// Whether `name`'s value is masked (not under its storage's `unmasked_prefixes`).
    #[must_use]
    pub fn masked(&self, name: &SecretName) -> bool {
        self.storage_of(name).is_ok_and(|(_, storage)| {
            !storage
                .unmasked_prefixes
                .iter()
                .any(|prefix| name.path.starts_with(prefix.as_str()))
        })
    }

    /// The value `name` stands for: a key's value as a string, a whole secret as an object.
    ///
    /// # Errors
    ///
    /// A [`SecretError`] naming the secret.
    pub async fn fetch(&self, name: &SecretName) -> Result<Value, SecretError> {
        let data = self.read(name).await.map_err(|e| e.about(name))?;
        match &name.key {
            None => Ok(Value::Object(
                data.into_iter()
                    .map(|(k, v)| (k, Value::String(v)))
                    .collect(),
            )),
            Some(key) => data.get(key).cloned().map(Value::String).ok_or_else(|| {
                SecretError::NotFound(format!("`{name}`: the secret has no key `{key}`"))
            }),
        }
    }

    async fn read(&self, name: &SecretName) -> Result<SecretData, SecretError> {
        let (storage_name, storage) = self.storage_of(name)?;
        let key = (storage_name.to_owned(), name.path.clone());
        let ttl = storage.cache_ttl;
        if !ttl.is_zero() {
            let cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some((at, data)) = cache.get(&key)
                && at.elapsed() < ttl
            {
                return Ok(data.clone());
            }
        }
        let data = storage.storage.read(&name.path).await?;
        if !ttl.is_zero() {
            let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            cache.retain(|_, (at, _)| at.elapsed() < ttl);
            cache.insert(key, (Instant::now(), data.clone()));
        }
        Ok(data)
    }
}

/// The secrets of one attempt: what was fetched before it started, what it declares, and
/// the values masked in its log and output.
#[derive(Debug)]
pub struct AttemptSecrets {
    stores: Arc<SecretStores>,
    declared: Vec<SecretDecl>,
    fetched: Mutex<BTreeMap<SecretName, Value>>,
    /// The values masked in the task's log and output (grows with on-demand reads).
    pub mask: Arc<MaskSet>,
    /// The error kind of the last on-demand read that failed (decision 168): a failed
    /// attempt gets it.
    failed: Mutex<Option<&'static str>>,
    /// Secrets too short to be masked, for the log's warning.
    pub too_short: Mutex<Vec<SecretName>>,
}

impl AttemptSecrets {
    /// No secrets (a task that names none).
    #[must_use]
    pub fn none(stores: Arc<SecretStores>) -> Self {
        let min = stores.min_mask_length;
        Self {
            stores,
            declared: Vec::new(),
            fetched: Mutex::default(),
            mask: Arc::new(MaskSet::new(min)),
            failed: Mutex::default(),
            too_short: Mutex::default(),
        }
    }

    /// Fetches every secret `needed` (named in the task's references or declared) before the
    /// task starts.
    ///
    /// # Errors
    ///
    /// The first [`SecretError`]: nothing runs with a missing secret.
    pub async fn fetch(
        stores: Arc<SecretStores>,
        needed: &[SecretName],
        declared: Vec<SecretDecl>,
    ) -> Result<Self, SecretError> {
        let mut secrets = Self::none(stores);
        secrets.declared = declared;
        let mut fetched = BTreeMap::new();
        for name in needed {
            if fetched.contains_key(name) {
                continue;
            }
            let value = secrets.stores.fetch(name).await?;
            secrets.add_mask(name, &value);
            fetched.insert(name.clone(), value);
        }
        secrets.fetched = Mutex::new(fetched);
        Ok(secrets)
    }

    /// The values fetched before the start, by name.
    #[must_use]
    pub fn fetched(&self) -> BTreeMap<SecretName, Value> {
        self.fetched
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// A secret the running task reads (`task-ctx secret get`, `task_ctx.secret(...)`): one
    /// it declares, fetched before the start, or one under a declared prefix, fetched now.
    ///
    /// # Errors
    ///
    /// [`SecretError::Denied`] for a secret it doesn't declare; a fetch's error otherwise.
    /// A failure is remembered for the attempt's error kind (decision 168).
    pub async fn read(&self, name: &SecretName) -> Result<Value, SecretError> {
        let result = self.read_inner(name).await;
        if let Err(error) = &result {
            *self.failed.lock().unwrap_or_else(PoisonError::into_inner) = Some(error.kind());
        }
        result
    }

    async fn read_inner(&self, name: &SecretName) -> Result<Value, SecretError> {
        if !self.declared.iter().any(|d| d.covers(name)) {
            return Err(SecretError::Denied(format!(
                "`{name}` isn't declared in the task's `secrets=`"
            )));
        }
        let known = self
            .fetched
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned();
        if let Some(value) = known {
            return Ok(value);
        }
        // A key of a whole secret fetched already, whichever declaration covers it.
        let whole = SecretName {
            key: None,
            ..name.clone()
        };
        {
            let fetched = self.fetched.lock().unwrap_or_else(PoisonError::into_inner);
            if let (Some(Value::Object(keys)), Some(key)) = (fetched.get(&whole), &name.key) {
                return keys.get(key).cloned().ok_or_else(|| {
                    SecretError::NotFound(format!("`{name}`: the secret has no key `{key}`"))
                });
            }
        }
        let value = self.stores.fetch(name).await?;
        self.add_mask(name, &value);
        self.fetched
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.clone(), value.clone());
        Ok(value)
    }

    /// The error kind of the last on-demand read that failed, if any.
    #[must_use]
    pub fn failed_kind(&self) -> Option<&'static str> {
        *self.failed.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Masks `value` from now on, unless its storage leaves it unmasked; a value too short
    /// to mask is noted for the log's warning.
    fn add_mask(&self, name: &SecretName, value: &Value) {
        if !self.stores.masked(name) {
            return;
        }
        let values: Vec<&str> = match value {
            Value::String(text) => vec![text.as_str()],
            Value::Object(keys) => keys.values().filter_map(Value::as_str).collect(),
            _ => Vec::new(),
        };
        for text in values {
            if !self.mask.add(&name.to_string(), text) {
                let mut short = self
                    .too_short
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if !short.contains(name) {
                    short.push(name.clone());
                }
            }
        }
    }
}
