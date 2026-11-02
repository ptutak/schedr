//! The `db` secret storage (standalone; see PROJECT.md, Secret storage): one row per secret
//! path, like a Vault KV v2 secret, its `data` a JSON object of key to value. Each value is
//! encrypted with XChaCha20-Poly1305 under `SCHEDR_SECRET_KEY`, bound to its `path#key` (the
//! associated data), so a value can't be moved to another key or path; paths and key names
//! stay readable, so listing needs no key. A dump, a backup or someone reading the database
//! sees no value.
//!
//! A stored value is `v1:` and the base64 of its 24-byte nonce and the ciphertext with its tag.

use std::collections::BTreeMap;
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use chacha20poly1305::aead::{Aead, Generate, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use jiff::Timestamp;

use crate::backend::Backend;
use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, in_tx, on_pool, sql, sql_text, storable_json};

#[cfg(test)]
mod tests;

/// The environment variable holding the key.
pub const SECRET_KEY_ENV: &str = "SCHEDR_SECRET_KEY";

/// The prefix of an encrypted value: the format's version.
const VERSION: &str = "v1:";

/// The nonce's length (`XChaCha20`'s 192 bits).
const NONCE_LEN: usize = 24;

/// The key secrets are encrypted under: 32 bytes. Its `Debug` never shows it.
#[derive(Clone)]
pub struct SecretKey(chacha20poly1305::Key);

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretKey(****)")
    }
}

/// Why a secret can't be written or read.
#[derive(Debug, thiserror::Error)]
pub enum SecretsError {
    /// No key, or one that isn't 32 bytes of base64.
    #[error("{0}")]
    Key(String),
    /// A value that doesn't decrypt: another key, or a value moved or changed in the table.
    #[error(
        "`{0}` can't be decrypted: it was written under another `SCHEDR_SECRET_KEY`, or \
         changed in the table"
    )]
    Decrypt(String),
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
}

impl SecretKey {
    /// A new random key.
    ///
    /// # Errors
    ///
    /// [`SecretsError::Key`] if the system's random source fails.
    pub fn generate() -> Result<Self, SecretsError> {
        chacha20poly1305::Key::try_generate()
            .map(Self)
            .map_err(|e| SecretsError::Key(format!("no random bytes for a key: {e}")))
    }

    /// The key from its base64 text (as `SCHEDR_SECRET_KEY` holds it).
    ///
    /// # Errors
    ///
    /// [`SecretsError::Key`] if it isn't base64 of 32 bytes (the message never shows it).
    pub fn from_base64(text: &str) -> Result<Self, SecretsError> {
        let bytes = STANDARD.decode(text.trim()).map_err(|_| {
            SecretsError::Key(format!(
                "`{SECRET_KEY_ENV}` isn't base64 (`schedr secrets new-key` makes one)"
            ))
        })?;
        let key = chacha20poly1305::Key::try_from(bytes.as_slice()).map_err(|_| {
            SecretsError::Key(format!(
                "`{SECRET_KEY_ENV}` must be 32 bytes, got {} (`schedr secrets new-key` makes \
                 one)",
                bytes.len()
            ))
        })?;
        Ok(Self(key))
    }

    /// The key from the environment.
    ///
    /// # Errors
    ///
    /// [`SecretsError::Key`] if it isn't set or isn't a key.
    pub fn from_env(env: &BTreeMap<String, String>) -> Result<Self, SecretsError> {
        let text = env.get(SECRET_KEY_ENV).filter(|t| !t.trim().is_empty());
        let text = text.ok_or_else(|| {
            SecretsError::Key(format!(
                "`{SECRET_KEY_ENV}` isn't set: the `db` secret storage encrypts every value \
                 under it (`schedr secrets new-key` makes one; keep it as you keep the \
                 database's password)"
            ))
        })?;
        Self::from_base64(text)
    }

    /// The key as base64, for `schedr secrets new-key`.
    #[must_use]
    pub fn to_base64(&self) -> String {
        STANDARD.encode(self.0.as_slice())
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new(&self.0)
    }

    /// `value` encrypted for `path#key`.
    ///
    /// # Errors
    ///
    /// [`SecretsError::Key`] if the system's random source fails (no nonce).
    pub fn encrypt(&self, path: &str, key: &str, value: &str) -> Result<String, SecretsError> {
        let nonce = XNonce::try_generate()
            .map_err(|e| SecretsError::Key(format!("no random bytes for a nonce: {e}")))?;
        let aad = format!("{path}#{key}");
        let sealed = self
            .cipher()
            .encrypt(
                &nonce,
                Payload {
                    msg: value.as_bytes(),
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| SecretsError::Key(format!("`{aad}` can't be encrypted")))?;
        let mut bytes = nonce.to_vec();
        bytes.extend(sealed);
        Ok(format!("{VERSION}{}", STANDARD.encode(bytes)))
    }

    /// The value stored for `path#key`.
    ///
    /// # Errors
    ///
    /// [`SecretsError::Decrypt`] if it isn't one written under this key for `path#key`.
    pub fn decrypt(&self, path: &str, key: &str, stored: &str) -> Result<String, SecretsError> {
        let aad = format!("{path}#{key}");
        let failed = || SecretsError::Decrypt(aad.clone());
        let bytes = stored
            .strip_prefix(VERSION)
            .and_then(|b64| STANDARD.decode(b64).ok())
            .filter(|bytes| bytes.len() > NONCE_LEN)
            .ok_or_else(failed)?;
        let (nonce, sealed) = bytes.split_at(NONCE_LEN);
        let nonce = XNonce::try_from(nonce).map_err(|_| failed())?;
        let plain = self
            .cipher()
            .decrypt(
                &nonce,
                Payload {
                    msg: sealed,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| failed())?;
        String::from_utf8(plain).map_err(|_| failed())
    }
}

/// A secret path and its key names (never the values), for `schedr secrets list`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretEntry {
    /// The path.
    pub path: String,
    /// Its keys' names.
    pub keys: Vec<String>,
    /// When it was last written.
    pub updated_at: String,
}

impl Database {
    /// Sets `values` in the secret at `path`, encrypted under `key`: the given keys are
    /// written, the others kept (the secret is made if it doesn't exist).
    ///
    /// # Errors
    ///
    /// [`SecretsError`] if a value can't be encrypted or the database fails.
    pub async fn set_secret(
        &self,
        key: &SecretKey,
        path: &str,
        values: &BTreeMap<String, String>,
        now: Timestamp,
    ) -> Result<(), SecretsError> {
        // One statement merging in the database: two writes to one path at once both keep
        // their keys (a read, a merge here and a write lost one on Postgres; Codex's finding).
        const UPSERT: &str = "INSERT INTO secrets (path, data, created_at, updated_at) \
            VALUES ($1, $2{json}, $3{ts}, $3{ts}) \
            ON CONFLICT (path) DO UPDATE SET data = {merged}, \
            updated_at = excluded.updated_at";
        let mut sealed = BTreeMap::new();
        for (name, value) in values {
            sealed.insert(name.clone(), key.encrypt(path, name, value)?);
        }
        let text = storable_json(serde_json::to_string(&sealed).unwrap_or_default());
        in_tx!(self, |tx, backend| {
            // An object's keys given replace the stored ones, the others are kept.
            let merged = match backend {
                Backend::Postgres => "secrets.data || excluded.data",
                Backend::Sqlite => "json_patch(secrets.data, excluded.data)",
            };
            let query = UPSERT.replace("{merged}", merged);
            sqlx::query(sql_text(backend, &query))
                .bind(path)
                .bind(&text)
                .bind(db_time(now))
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// The secret at `path`, its values decrypted; `None` if there is none.
    ///
    /// # Errors
    ///
    /// [`SecretsError`] if a value doesn't decrypt under `key` or the database fails.
    pub async fn secret(
        &self,
        key: &SecretKey,
        path: &str,
    ) -> Result<Option<BTreeMap<String, String>>, SecretsError> {
        const READ: &str = "SELECT data{text} FROM secrets WHERE path = $1";
        let stored: Option<(String,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, READ))
                .bind(path)
                .fetch_optional(pool)
                .await?
        });
        let Some((text,)) = stored else {
            return Ok(None);
        };
        let data: BTreeMap<String, String> = from_json("secrets.data", &text)?;
        data.iter()
            .map(|(name, sealed)| Ok((name.clone(), key.decrypt(path, name, sealed)?)))
            .collect::<Result<_, SecretsError>>()
            .map(Some)
    }

    /// The secrets whose path starts with `prefix`, by path: their key names, never values.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn secret_entries(&self, prefix: &str) -> Result<Vec<SecretEntry>, DbError> {
        const LIST: &str = "SELECT path, data{text}, {ts<}updated_at{>ts} FROM secrets \
            ORDER BY path";
        let rows: Vec<(String, String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, LIST)).fetch_all(pool).await?
        });
        rows.into_iter()
            .filter(|(path, _, _)| path.starts_with(prefix))
            .map(|(path, text, updated_at)| {
                let data: BTreeMap<String, String> = from_json("secrets.data", &text)?;
                Ok(SecretEntry {
                    path,
                    keys: data.into_keys().collect(),
                    updated_at,
                })
            })
            .collect()
    }

    /// Removes the secret at `path`; `false` if there was none.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn delete_secret(&self, path: &str) -> Result<bool, DbError> {
        const DELETE: &str = "DELETE FROM secrets WHERE path = $1";
        let mut removed = 0;
        in_tx!(self, |tx, backend| {
            removed = sqlx::query(sql(backend, DELETE))
                .bind(path)
                .execute(&mut *tx)
                .await?
                .rows_affected();
        });
        Ok(removed > 0)
    }
}
