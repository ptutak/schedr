use pretty_assertions::assert_eq;
use schedr_config::config::SecretStorageConfig;
use schedr_db::secrets::SECRET_KEY_ENV;
use schedr_model::SecretName;

use super::*;

fn db_storage() -> SecretStorageConfig {
    SecretStorageConfig {
        kind: SecretStorageType::Db,
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
