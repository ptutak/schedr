//! `[secret_storage]` made into the agent's storages, per role (see PROJECT.md, Secret
//! storage): standalone always has the `db` storage (its default when nothing else is
//! configured); a worker in a distributed deployment has only what is configured, and never
//! `db` (it has no database connection, and a secret never passes through the master).

use std::collections::BTreeMap;
use std::sync::Arc;

use schedr_agent::secrets::{DbStorage, SecretStores, Storage};
use schedr_config::config::{SecretStorageSection, SecretStorageType};
use schedr_config::loader::DB_STORAGE;
use schedr_db::Database;
use schedr_db::secrets::SecretKey;

use crate::app::AppError;

#[cfg(test)]
mod tests;

/// The storages to make once the role knows whether it has a database: the configuration and
/// `SCHEDR_SECRET_KEY`, or why there is none.
#[derive(Debug, Clone)]
pub struct SecretPlan {
    section: SecretStorageSection,
    key: Result<SecretKey, String>,
    /// Storages given whole instead (`dags test --secret`: values in memory).
    given: Option<Arc<SecretStores>>,
}

impl SecretPlan {
    /// The plan from the configuration and the environment (the key read now: the environment
    /// isn't kept).
    #[must_use]
    pub fn new(section: &SecretStorageSection, env: &BTreeMap<String, String>) -> Self {
        Self {
            section: section.clone(),
            key: SecretKey::from_env(env).map_err(|e| e.to_string()),
            given: None,
        }
    }

    /// These storages, whatever is configured (a test run's values in memory).
    #[must_use]
    pub fn given(mut self, stores: SecretStores) -> Self {
        self.given = Some(Arc::new(stores));
        self
    }

    /// The storages a role runs with: the given ones, else [`Self::stores`]'s.
    ///
    /// # Errors
    ///
    /// As [`Self::stores`].
    pub fn for_role(&self, db: Option<&Database>) -> Result<Arc<SecretStores>, AppError> {
        match &self.given {
            Some(stores) => Ok(stores.clone()),
            None => self.stores(db).map(Arc::new),
        }
    }

    /// The storages: with `db` (the standalone's own database) when there is one, else a
    /// worker's.
    ///
    /// # Errors
    ///
    /// [`AppError::BadArgument`] for a `db` storage, or `default = "db"`, without a database
    /// (a worker in a distributed deployment).
    pub fn stores(&self, db: Option<&Database>) -> Result<SecretStores, AppError> {
        let mut storages = BTreeMap::new();
        for (name, configured) in &self.section.storages {
            let storage: Arc<dyn schedr_agent::secrets::SecretStorage> = match configured.kind {
                SecretStorageType::Db => {
                    let db =
                        db.ok_or_else(|| standalone_only(&format!("[secret_storage.{name}]")))?;
                    Arc::new(DbStorage::with_key(db.clone(), self.key.clone()))
                }
            };
            storages.insert(
                name.clone(),
                Storage {
                    storage,
                    cache_ttl: configured.cache_ttl,
                    unmasked_prefixes: configured.unmasked_prefixes.clone(),
                },
            );
        }
        let configured = storages.len();
        if let Some(db) = db
            && !storages.contains_key(DB_STORAGE)
        {
            let storage = DbStorage::with_key(db.clone(), self.key.clone());
            storages.insert(DB_STORAGE.to_owned(), Storage::new(Arc::new(storage)));
        }
        let default = match &self.section.default {
            Some(default) if default == DB_STORAGE && db.is_none() => {
                return Err(standalone_only("[secret_storage] default = \"db\""));
            }
            Some(default) => Some(default.clone()),
            // The only one configured; else standalone's own (several without a default are
            // refused when the configuration loads, decision 172).
            None if configured == 1 => self.section.storages.keys().next().cloned(),
            None if db.is_some() => Some(DB_STORAGE.to_owned()),
            None => None,
        };
        SecretStores::new(storages, default, self.section.min_mask_length)
            .map_err(AppError::BadArgument)
    }
}

fn standalone_only(what: &str) -> AppError {
    AppError::BadArgument(format!(
        "{what}: the `db` secret storage is standalone's only (a worker has no database \
         connection, and secrets never pass through the master); use Vault, and move the \
         secrets there with `schedr secrets export`"
    ))
}
