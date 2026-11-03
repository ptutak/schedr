use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn name(storage: Option<&str>, path: &str, key: Option<&str>) -> SecretName {
    SecretName {
        storage: storage.map(str::to_owned),
        path: path.to_owned(),
        key: key.map(str::to_owned),
        default: None,
    }
}

fn memory() -> MemoryStorage {
    MemoryStorage::new([
        (
            name(None, "etl/db", Some("password")),
            "hunter22".to_owned(),
        ),
        (name(None, "etl/db", Some("user")), "etl".to_owned()),
        (
            name(None, "etl/url", Some("value")),
            "postgresql://etl:extr%3Acted@db/x".to_owned(),
        ),
        (
            name(None, "tenants/a", Some("token")),
            "token-of-a".to_owned(),
        ),
        (
            name(None, "variables/bucket", Some("value")),
            "raw-bucket".to_owned(),
        ),
    ])
}

/// Counts its reads, and fails with `error` when set.
#[derive(Debug, Default)]
struct Counting {
    reads: AtomicUsize,
    error: Option<SecretError>,
}

impl SecretStorage for Counting {
    fn read<'a>(&'a self, _path: &'a str) -> ReadFuture<'a> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            match &self.error {
                Some(error) => Err(error.clone()),
                None => Ok(BTreeMap::from([("k".to_owned(), "value-1".to_owned())])),
            }
        })
    }
}

fn stores(storage: Storage) -> Arc<SecretStores> {
    Arc::new(
        SecretStores::new(
            BTreeMap::from([("main".to_owned(), storage)]),
            Some("main".to_owned()),
            5,
        )
        .unwrap(),
    )
}

#[test]
fn a_secret_spec_is_parsed() {
    assert_eq!(
        MemoryStorage::parse("etl/db#password=a=b").unwrap(),
        (name(None, "etl/db", Some("password")), "a=b".to_owned())
    );
    for bad in ["etl/db#password", "etl/db=x", "etl//db#k=x", "etl#=x"] {
        assert!(MemoryStorage::parse(bad).is_err(), "{bad}");
    }
}

#[tokio::test]
async fn keys_and_whole_secrets_are_fetched() {
    let stores = stores(Storage::new(Arc::new(memory())));
    let password = stores.fetch(&name(None, "etl/db", Some("password"))).await;
    assert_eq!(password, Ok(json!("hunter22")));
    let whole = stores.fetch(&name(None, "etl/db", None)).await;
    assert_eq!(whole, Ok(json!({"password": "hunter22", "user": "etl"})));
    let missing = stores.fetch(&name(None, "etl/db", Some("port"))).await;
    assert_eq!(missing.unwrap_err().kind(), "secret_not_found");
    let error = stores
        .fetch(&name(None, "nope", Some("k")))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "`secrets['nope']['k']`: no such secret");
    let other = stores
        .fetch(&name(Some("vault"), "etl/db", Some("user")))
        .await;
    let other = other.unwrap_err().to_string();
    assert!(
        other.contains("no secret storage `vault` on this worker"),
        "{other}"
    );
}

#[tokio::test]
async fn a_worker_without_a_default_storage_says_so() {
    let stores = SecretStores::none();
    let error = stores.fetch(&name(None, "a", Some("k"))).await.unwrap_err();
    assert!(
        error.to_string().contains("no default secret storage"),
        "{error}"
    );
    assert!(SecretStores::new(BTreeMap::new(), Some("x".into()), 5).is_err());
}

#[tokio::test]
async fn fetched_secrets_are_cached_for_the_ttl() {
    let counting = Arc::new(Counting::default());
    let mut storage = Storage::new(counting.clone());
    storage.cache_ttl = Duration::from_millis(200);
    let stores = stores(storage);
    let k = name(None, "a", Some("k"));
    stores.fetch(&k).await.unwrap();
    stores.fetch(&name(None, "a", None)).await.unwrap();
    assert_eq!(counting.reads.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(250)).await;
    stores.fetch(&k).await.unwrap();
    assert_eq!(counting.reads.load(Ordering::SeqCst), 2);

    let uncached = Arc::new(Counting::default());
    let mut storage = Storage::new(uncached.clone());
    storage.cache_ttl = Duration::ZERO;
    let stores = self::stores(storage);
    stores.fetch(&k).await.unwrap();
    stores.fetch(&k).await.unwrap();
    assert_eq!(uncached.reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_cached_secret_is_dropped_when_it_expires_without_another_read() {
    // From Grok's security review of M4.6b: kept until the next fetch, it stayed in memory.
    let mut storage = Storage::new(Arc::new(Counting::default()));
    storage.cache_ttl = Duration::from_millis(50);
    let stores = stores(storage);
    stores.fetch(&name(None, "a", Some("k"))).await.unwrap();
    assert_eq!(stores.cache.lock().unwrap().len(), 1);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(stores.cache.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_storage_error_keeps_its_kind_and_names_the_secret() {
    for (error, kind) in [
        (
            SecretError::Unavailable("connection refused".into()),
            "secret_unavailable",
        ),
        (
            SecretError::Denied("permission denied".into()),
            "secret_denied",
        ),
        (
            SecretError::NotFound("no such path".into()),
            "secret_not_found",
        ),
    ] {
        let stores = stores(Storage::new(Arc::new(Counting {
            error: Some(error),
            ..Counting::default()
        })));
        let failed = stores.fetch(&name(None, "a", Some("k"))).await.unwrap_err();
        assert_eq!(failed.kind(), kind);
        assert!(
            failed.to_string().starts_with("`secrets['a']['k']`: "),
            "{failed}"
        );
    }
}

#[tokio::test]
async fn an_attempt_reads_only_what_it_declares() {
    let mut storage = Storage::new(Arc::new(memory()));
    storage.unmasked_prefixes = vec!["variables/".into()];
    let stores = stores(storage);
    let declared = vec![
        SecretDecl::Secret(name(None, "etl/db", None)),
        SecretDecl::Prefix {
            storage: None,
            prefix: "tenants/".into(),
        },
        SecretDecl::Secret(name(None, "variables/bucket", Some("value"))),
    ];
    let needed = [
        name(None, "etl/db", None),
        name(None, "variables/bucket", Some("value")),
    ];
    let secrets = AttemptSecrets::fetch(stores, &needed, declared)
        .await
        .unwrap();
    // A key of the whole secret, from what was fetched; a prefix's secret, fetched now.
    let user = secrets.read(&name(None, "etl/db", Some("user"))).await;
    assert_eq!(user, Ok(json!("etl")));
    let token = secrets.read(&name(None, "tenants/a", Some("token"))).await;
    assert_eq!(token, Ok(json!("token-of-a")));
    assert_eq!(secrets.failed_kind(), None);
    let undeclared = secrets.read(&name(None, "other/x", Some("k"))).await;
    assert_eq!(undeclared.unwrap_err().kind(), "secret_denied");
    assert_eq!(secrets.failed_kind(), Some("secret_denied"));
    let missing = secrets.read(&name(None, "tenants/b", Some("token"))).await;
    assert_eq!(missing.unwrap_err().kind(), "secret_not_found");
    assert_eq!(secrets.failed_kind(), Some("secret_not_found"));
    // Masked: the fetched values (the prefix's too), not the unmasked prefix's, not "etl"
    // (too short, noted for the warning).
    let masked = secrets.mask.mask_text("hunter22 token-of-a raw-bucket etl");
    assert_eq!(masked, "**** **** raw-bucket etl");
    assert_eq!(
        *secrets.too_short.lock().unwrap(),
        [name(None, "etl/db", None)]
    );
}

#[tokio::test]
async fn a_test_run_answers_for_every_storage() {
    let stores = SecretStores::for_test(memory());
    assert_eq!(stores.names(), Vec::<String>::new());
    let named = stores
        .fetch(&name(Some("vault"), "etl/db", Some("password")))
        .await;
    assert_eq!(named, Ok(json!("hunter22")));
}

/// Answers once, then is down.
#[derive(Debug, Default)]
struct OnceThenDown {
    reads: AtomicUsize,
}

impl SecretStorage for OnceThenDown {
    fn read<'a>(&'a self, _path: &'a str) -> ReadFuture<'a> {
        Box::pin(async move {
            if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(BTreeMap::from([("k".to_owned(), "value-of-k".to_owned())]))
            } else {
                Err(SecretError::Unavailable("storage went down".into()))
            }
        })
    }
}

#[tokio::test]
async fn a_key_of_a_fetched_whole_secret_is_read_whichever_declaration_covers_it() {
    // A prefix declared before the whole secret (review of M4.6b): no second fetch.
    let storage = Arc::new(OnceThenDown::default());
    let mut uncached = Storage::new(storage.clone());
    uncached.cache_ttl = Duration::ZERO;
    let declared = vec![
        SecretDecl::Prefix {
            storage: None,
            prefix: "tenants/".into(),
        },
        SecretDecl::Secret(name(None, "tenants/a", None)),
    ];
    let needed = [name(None, "tenants/a", None)];
    let secrets = AttemptSecrets::fetch(stores(uncached), &needed, declared)
        .await
        .unwrap();
    let read = secrets.read(&name(None, "tenants/a", Some("k"))).await;
    assert_eq!(read, Ok(json!("value-of-k")));
    assert_eq!(storage.reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_short_value_is_noted_once() {
    let short = MemoryStorage::new([
        (name(None, "a", Some("x")), "1".to_owned()),
        (name(None, "a", Some("y")), "2".to_owned()),
    ]);
    let declared = vec![SecretDecl::Secret(name(None, "a", None))];
    let needed = [name(None, "a", None)];
    let secrets = AttemptSecrets::fetch(stores(Storage::new(Arc::new(short))), &needed, declared)
        .await
        .unwrap();
    assert_eq!(*secrets.too_short.lock().unwrap(), [name(None, "a", None)]);
}

#[tokio::test]
async fn the_db_storage_reads_the_table_under_the_key() {
    use schedr_db::secrets::{SECRET_KEY_ENV, SecretKey};
    let db = schedr_db::Database::connect("sqlite::memory:")
        .await
        .unwrap();
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
    let env = |key: &SecretKey| BTreeMap::from([(SECRET_KEY_ENV.to_owned(), key.to_base64())]);
    let read = |storage: DbStorage| {
        let stores = stores(Storage::new(Arc::new(storage)));
        async move { stores.fetch(&name(None, "etl/db", Some("password"))).await }
    };
    assert_eq!(
        read(DbStorage::new(db.clone(), &env(&key))).await,
        Ok(json!("hunter22"))
    );
    let missing = DbStorage::new(db.clone(), &env(&key));
    let stores_missing = stores(Storage::new(Arc::new(missing)));
    let gone = stores_missing.fetch(&name(None, "nope", Some("k"))).await;
    assert_eq!(gone.unwrap_err().kind(), "secret_not_found");
    // No key, or another one: a configuration problem, not retried.
    let keyless = DbStorage::new(db.clone(), &BTreeMap::new());
    assert!(
        keyless
            .key_problem()
            .is_some_and(|p| p.contains("isn't set"))
    );
    let error = read(keyless).await.unwrap_err();
    assert_eq!(error.kind(), "secret_denied");
    assert!(
        error.to_string().contains("`SCHEDR_SECRET_KEY` isn't set"),
        "{error}"
    );
    let other = SecretKey::generate().unwrap();
    let error = read(DbStorage::new(db.clone(), &env(&other)))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), "secret_denied");
    assert!(!error.to_string().contains("hunter22"), "{error}");
}

#[tokio::test]
async fn a_default_stands_in_for_a_missing_secret_or_key_unmasked() {
    let with_default = |path: &str, key: &str, default: &str| SecretName {
        default: Some(default.into()),
        ..name(None, path, Some(key))
    };
    let stores = stores(Storage::new(Arc::new(memory())));
    let needed = [
        // Present: the value, masked; a missing key and a missing secret: the default.
        with_default("etl/db", "password", "fallback-pw"),
        with_default("etl/db", "port", "5432-default"),
        with_default("nowhere", "k", "eu-west-default"),
    ];
    let secrets = AttemptSecrets::fetch(stores.clone(), &needed, Vec::new())
        .await
        .unwrap();
    let fetched = secrets.fetched();
    assert_eq!(fetched[&needed[0]], json!("hunter22"));
    assert_eq!(fetched[&needed[1]], json!("5432-default"));
    assert_eq!(fetched[&needed[2]], json!("eu-west-default"));
    // A default is written in the DAG: not a secret, so not masked.
    assert_eq!(
        secrets
            .mask
            .mask_text("hunter22 5432-default eu-west-default"),
        "**** 5432-default eu-west-default"
    );

    // A storage the worker lacks stays an error: a default never hides configuration.
    let elsewhere = SecretName {
        storage: Some("vault".into()),
        ..with_default("etl/db", "password", "x")
    };
    let error = AttemptSecrets::fetch(stores, &[elsewhere], Vec::new())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("no secret storage `vault`"),
        "{error}"
    );
}

fn registry(params: &serde_json::Value) -> Arc<Registry> {
    let def: schedr_model::ConnectionDef =
        serde_json::from_value(json!({"name": "db", "params": params})).unwrap();
    Arc::new(Registry {
        connections: BTreeMap::from([("db".to_owned(), def)]),
        refused: BTreeMap::from([("twice".to_owned(), "defined in a.py and b.py".to_owned())]),
    })
}

#[tokio::test]
async fn a_connection_is_rendered_with_its_secrets_masked_whatever_the_task_declares() {
    let secret =
        |path: &str, key: &str| json!({"ref": {"kind": "secret", "path": path, "key": key}});
    let params = json!({
        "dsn": {"template": [
            {"text": "postgres://"}, {"ref": {"kind": "secret", "path": "etl/db", "key": "user"}},
            {"text": "@db/x"}
        ]},
        "password": secret("etl/db", "password"),
        "whole": {"ref": {"kind": "secret", "path": "etl/db"}},
        "port": {"literal": 5432},
        "region": {"ref": {"kind": "secret", "path": "nowhere", "key": "k", "default": "eu"}},
    });
    let secrets = AttemptSecrets::fetch(stores(Storage::new(Arc::new(memory()))), &[], Vec::new())
        .await
        .unwrap()
        .with_connections(
            registry(&params),
            vec!["db".to_owned(), "twice".to_owned(), "other".to_owned()],
        );
    assert!(secrets.has_connections());
    assert_eq!(secrets.mask.mask_text("hunter22"), "hunter22");
    let values = secrets.connection("db").await.unwrap();
    assert_eq!(
        values,
        BTreeMap::from([
            ("dsn".to_owned(), json!("postgres://etl@db/x")),
            ("password".to_owned(), json!("hunter22")),
            (
                "whole".to_owned(),
                json!({"password": "hunter22", "user": "etl"})
            ),
            ("port".to_owned(), json!(5432)),
            ("region".to_owned(), json!("eu")),
        ])
    );
    // Masked from the moment it is read; a default isn't a secret.
    assert_eq!(secrets.mask.mask_text("hunter22 eu"), "**** eu");
    assert_eq!(secrets.failed_kind(), None);
}

#[tokio::test]
async fn a_password_a_command_gets_out_of_a_secret_url_is_masked_in_every_form() {
    // Codex's finding 3: the masker knows the whole URL; the password taken out of it, and
    // its `.pgpass` form, are new.
    let def: schedr_model::ConnectionDef = serde_json::from_value(json!({
        "name": "db", "type": "sql",
        "params": {"url": {"ref": {"kind": "secret", "path": "etl/url", "key": "value"}}},
    }))
    .unwrap();
    let registry = Arc::new(Registry {
        connections: BTreeMap::from([("db".to_owned(), def)]),
        refused: BTreeMap::new(),
    });
    let dir = tempfile::tempdir().unwrap();
    let secrets = AttemptSecrets::fetch(stores(Storage::new(Arc::new(memory()))), &[], Vec::new())
        .await
        .unwrap()
        .with_connections(registry, vec!["db".to_owned()])
        .with_connection_dir(dir.path().join("a1"));
    let variables = secrets.connection_env("db", "DB").await.unwrap();
    assert_eq!(variables["DB_PASSWORD"], "extr:cted");
    assert_eq!(secrets.mask.mask_text("extr:cted extr\\:cted"), "**** ****");
}

#[tokio::test]
async fn a_connection_that_cant_be_opened_says_why_with_its_kind() {
    let params = json!({"token": {"ref": {"kind": "secret", "path": "gone", "key": "t"}}});
    let secrets = AttemptSecrets::none(stores(Storage::new(Arc::new(memory())))).with_connections(
        registry(&params),
        vec!["db".to_owned(), "twice".to_owned(), "other".to_owned()],
    );
    let unknown = secrets.connection("other").await.unwrap_err();
    assert_eq!(unknown.kind(), "connection_not_found");
    assert!(
        unknown
            .to_string()
            .contains("connection `other` isn't defined"),
        "{unknown}"
    );
    let twice = secrets.connection("twice").await.unwrap_err();
    assert_eq!(twice.to_string(), "defined in a.py and b.py");
    let missing = secrets.connection("db").await.unwrap_err();
    assert_eq!(missing.kind(), "secret_not_found");
    // The connection, then the secret, each named once.
    let message = missing.to_string();
    assert!(
        message.starts_with("connection `db`: `secrets['gone']['t']`: "),
        "{message}"
    );
    assert_eq!(message.matches("secrets['gone']").count(), 1, "{message}");
    // The attempt's error kind if it then fails (decision 168).
    assert_eq!(secrets.failed_kind(), Some("secret_not_found"));
}

#[tokio::test]
async fn only_a_declared_connection_opens_and_declared_ones_open_before_the_start() {
    let params =
        json!({"password": {"ref": {"kind": "secret", "path": "etl/db", "key": "password"}}});
    let secrets = AttemptSecrets::none(stores(Storage::new(Arc::new(memory()))))
        .with_connections(registry(&params), Vec::new());
    assert!(!secrets.has_connections());
    let denied = secrets.connection("db").await.unwrap_err();
    assert_eq!(denied.kind(), "connection_denied");
    assert_eq!(
        denied.to_string(),
        "connection `db` isn't declared in the task's `connections=`"
    );

    let secrets = AttemptSecrets::none(stores(Storage::new(Arc::new(memory()))))
        .with_connections(registry(&params), vec!["db".to_owned()]);
    secrets.open_declared().await.unwrap();
    // Masked before any of the task's code runs.
    assert_eq!(secrets.mask.mask_text("hunter22"), "****");

    let secrets = AttemptSecrets::none(stores(Storage::new(Arc::new(memory()))))
        .with_connections(registry(&params), vec!["db".to_owned(), "gone".to_owned()]);
    assert_eq!(
        secrets.open_declared().await.unwrap_err().kind(),
        "connection_not_found"
    );
}

#[tokio::test]
async fn a_test_runs_values_stand_for_a_declared_connection() {
    let given = BTreeMap::from([(
        "db".to_owned(),
        BTreeMap::from([("password".to_owned(), json!("from-the-test"))]),
    )]);
    let stores =
        Arc::new(SecretStores::for_test(MemoryStorage::new([])).with_test_connections(given));
    let params =
        json!({"password": {"ref": {"kind": "secret", "path": "etl/db", "key": "password"}}});
    let secrets =
        AttemptSecrets::none(stores).with_connections(registry(&params), vec!["db".to_owned()]);
    assert_eq!(
        secrets.connection("db").await.unwrap(),
        BTreeMap::from([("password".to_owned(), json!("from-the-test"))])
    );
}
