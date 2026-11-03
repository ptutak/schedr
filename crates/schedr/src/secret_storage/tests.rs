use pretty_assertions::assert_eq;
use schedr_config::config::SecretStorageConfig;
use schedr_db::secrets::SECRET_KEY_ENV;
use schedr_model::SecretName;

use super::*;

fn db_storage() -> SecretStorageConfig {
    SecretStorageConfig {
        kind: SecretStorageType::Db,
        address: None,
        mount: None,
        namespace: None,
        ca_cert: None,
        auth: None,
        token: None,
        cache_ttl: std::time::Duration::ZERO,
        unmasked_prefixes: Vec::new(),
    }
}

/// A migrated database holding `etl/db#password`, and the environment with its key.
async fn database() -> (Database, BTreeMap<String, String>) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let key = SecretKey::generate().unwrap();
    let values = BTreeMap::from([("password".to_owned(), "hunter22".to_owned())]);
    db.set_secret(
        &key,
        "etl/db",
        &values,
        "2026-11-02T20:00:00Z".parse().unwrap(),
    )
    .await
    .unwrap();
    (
        db,
        BTreeMap::from([(SECRET_KEY_ENV.to_owned(), key.to_base64())]),
    )
}

fn password(storage: Option<&str>) -> SecretName {
    SecretName {
        storage: storage.map(str::to_owned),
        path: "etl/db".into(),
        key: Some("password".into()),
        default: None,
    }
}

#[tokio::test]
async fn standalone_has_db_as_its_default_with_nothing_configured() {
    let (db, env) = database().await;
    let stores = SecretPlan::new(&SecretStorageSection::default(), &env)
        .stores(Some(&db))
        .unwrap();
    assert_eq!(stores.names(), ["db"]);
    assert_eq!(
        stores.fetch(&password(None)).await,
        Ok(serde_json::json!("hunter22"))
    );
    assert_eq!(
        stores.fetch(&password(Some("db"))).await,
        Ok(serde_json::json!("hunter22"))
    );
}

#[tokio::test]
async fn the_only_storage_configured_is_the_default_and_db_stays_reachable() {
    let (db, env) = database().await;
    let mut section = SecretStorageSection::default();
    section.storages.insert("local".into(), db_storage());
    let stores = SecretPlan::new(&section, &env).stores(Some(&db)).unwrap();
    assert_eq!(stores.names(), ["db", "local"]);
    assert_eq!(
        stores.fetch(&password(None)).await,
        Ok(serde_json::json!("hunter22"))
    );
    assert_eq!(
        stores.fetch(&password(Some("db"))).await,
        Ok(serde_json::json!("hunter22"))
    );
}

#[test]
fn a_worker_never_has_the_db_storage() {
    let env = BTreeMap::new();
    let none = SecretPlan::new(&SecretStorageSection::default(), &env)
        .stores(None)
        .unwrap();
    assert_eq!(none.names(), Vec::<String>::new());
    let mut section = SecretStorageSection::default();
    section.storages.insert("db".into(), db_storage());
    let error = SecretPlan::new(&section, &env)
        .stores(None)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("[secret_storage.db]: the `db` secret storage is standalone's only"),
        "{error}"
    );
    let section = SecretStorageSection {
        default: Some("db".into()),
        ..SecretStorageSection::default()
    };
    let error = SecretPlan::new(&section, &env)
        .stores(None)
        .unwrap_err()
        .to_string();
    assert!(error.contains("default = \"db\""), "{error}");
}

fn vault_storage(ca_cert: Option<std::path::PathBuf>) -> SecretStorageConfig {
    SecretStorageConfig {
        kind: SecretStorageType::Vault,
        address: Some("http://127.0.0.1:1".into()),
        auth: Some(schedr_config::config::VaultAuthConfig {
            method: schedr_config::config::VaultAuthMethod::Kubernetes,
            role: Some("schedr-worker".into()),
            mount: None,
            token_file: None,
        }),
        ca_cert,
        ..db_storage()
    }
}

#[tokio::test]
async fn a_worker_has_its_vault_as_the_default() {
    let env = BTreeMap::new();
    let mut section = SecretStorageSection::default();
    section.storages.insert("vault".into(), vault_storage(None));
    let stores = SecretPlan::new(&section, &env).stores(None).unwrap();
    assert_eq!(stores.names(), ["vault"]);
    // The default, with Kubernetes auth's default token file (none outside a pod): denied,
    // from Vault's login, not "no default storage".
    let error = stores.fetch(&password(None)).await.unwrap_err();
    assert_eq!(error.kind(), "secret_denied", "{error}");
    assert!(
        error
            .to_string()
            .contains("/var/run/secrets/kubernetes.io/serviceaccount/token"),
        "{error}"
    );
    // A CA certificate that can't be read stops the start.
    let mut section = SecretStorageSection::default();
    section.storages.insert(
        "vault".into(),
        vault_storage(Some("/nonexistent/ca.pem".into())),
    );
    let error = SecretPlan::new(&section, &env)
        .stores(None)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("[secret_storage.vault] ca_cert: can't read /nonexistent/ca.pem"),
        "{error}"
    );
}
